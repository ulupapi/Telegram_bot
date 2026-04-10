from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Iterable

import httpx

from database import StoredMessage, TaskRecord


@dataclass(frozen=True)
class StatusReport:
    done: list[str]
    in_progress: list[str]
    blocked: list[str]
    tasks: list[TaskRecord]


class AIExtractor:
    def __init__(
        self,
        *,
        provider: str,
        model: str,
        gemini_api_key: str | None = None,
        openai_api_key: str | None = None,
        openai_base_url: str | None = None,
        amvera_api_key: str | None = None,
        amvera_base_url: str | None = None,
    ) -> None:
        self.provider = provider.strip().lower()
        self.model = model
        self._gemini_client = None
        self._openai_client = None
        self._amvera_api_key = None
        self._amvera_base_url = None

        if self.provider == "gemini":
            if not gemini_api_key:
                raise RuntimeError("GEMINI_API_KEY is required when LLM_PROVIDER=gemini")
            from google import genai

            self._gemini_client = genai.Client(api_key=gemini_api_key)
        elif self.provider == "openai":
            if not openai_api_key:
                raise RuntimeError("OPENAI_API_KEY is required when LLM_PROVIDER=openai")
            from openai import OpenAI

            self._openai_client = OpenAI(
                api_key=openai_api_key,
                base_url=openai_base_url or None,
            )
        elif self.provider == "amvera":
            if not amvera_api_key:
                raise RuntimeError("AMVERA_LLM_API_KEY is required when LLM_PROVIDER=amvera")
            if not amvera_base_url:
                raise RuntimeError("AMVERA_LLM_BASE_URL is required when LLM_PROVIDER=amvera")
            self._amvera_api_key = amvera_api_key
            base_url = amvera_base_url.strip()
            if not base_url:
                raise RuntimeError("AMVERA_LLM_BASE_URL is empty")
            if not base_url.startswith(("http://", "https://")):
                base_url = f"https://{base_url}"
            self._amvera_base_url = base_url.rstrip("/")
        else:
            raise RuntimeError("LLM_PROVIDER must be one of: gemini, openai, amvera")

    def extract_status(self, messages: Iterable[StoredMessage]) -> StatusReport:
        messages_list = list(messages)
        if not messages_list:
            return StatusReport(done=[], in_progress=[], blocked=[], tasks=[])

        prompt = self._build_prompt(messages_list)
        raw = self._ask_model(prompt)
        payload = self._parse_json(raw)

        done = self._to_text_list(payload.get("done"))
        in_progress = self._to_text_list(payload.get("in_progress"))
        blocked = self._to_text_list(payload.get("blocked"))

        tasks: list[TaskRecord] = []
        raw_tasks = payload.get("tasks")
        if isinstance(raw_tasks, list):
            for idx, item in enumerate(raw_tasks, start=1):
                if not isinstance(item, dict):
                    continue
                title = str(item.get("title", "")).strip()
                if not title:
                    continue
                external_id = str(item.get("id", f"T{idx}")).strip() or f"T{idx}"
                assignee = str(item.get("assignee", "Не указан")).strip() or "Не указан"
                status = self._normalize_status(str(item.get("status", "in_progress")))
                tasks.append(
                    TaskRecord(
                        external_id=external_id,
                        title=title,
                        assignee=assignee,
                        status=status,
                    )
                )

        return StatusReport(
            done=done,
            in_progress=in_progress,
            blocked=blocked,
            tasks=tasks,
        )

    def _build_prompt(self, messages: list[StoredMessage]) -> str:
        lines = []
        for msg in messages:
            lines.append(f"[{msg.created_at}] {msg.user_name}: {msg.text}")
        dialog = "\n".join(lines)

        return (
            "Ты аналитик задач команды. На входе обсуждение из одной Telegram-ветки.\n"
            "Выдели результат в трех блоках:\n"
            "1) done - что уже сделано,\n"
            "2) in_progress - что в работе,\n"
            "3) blocked - что зависло или ждет внешних действий.\n"
            "Также верни tasks - массив структурированных задач.\n"
            "Отвечай строго JSON-объектом без markdown и без комментариев.\n\n"
            "Формат:\n"
            "{\n"
            '  "done": ["..."],\n'
            '  "in_progress": ["..."],\n'
            '  "blocked": ["..."],\n'
            '  "tasks": [\n'
            '    {"id":"T1","title":"...","assignee":"...","status":"done|in_progress|blocked"}\n'
            "  ]\n"
            "}\n\n"
            "Если данных по секции нет, верни пустой массив.\n"
            "Используй короткие, конкретные формулировки.\n\n"
            "Диалог:\n"
            f"{dialog}"
        )

    def _ask_model(self, prompt: str) -> str:
        if self.provider == "gemini":
            response = self._gemini_client.models.generate_content(
                model=self.model,
                contents=prompt,
            )
            return getattr(response, "text", "") or ""
        if self.provider == "amvera":
            return self._ask_amvera(prompt)

        messages = [
            {
                "role": "system",
                "content": "Return only JSON object without markdown.",
            },
            {"role": "user", "content": prompt},
        ]
        try:
            response = self._openai_client.chat.completions.create(
                model=self.model,
                temperature=0,
                response_format={"type": "json_object"},
                messages=messages,
            )
        except Exception:
            response = self._openai_client.chat.completions.create(
                model=self.model,
                temperature=0,
                messages=messages,
            )
        content = response.choices[0].message.content
        return content or ""

    def _ask_amvera(self, prompt: str) -> str:
        base_url = self._amvera_base_url
        model = self.model.strip()
        if not base_url.startswith(("http://", "https://")):
            raise RuntimeError(
                "AMVERA_LLM_BASE_URL must start with http:// or https://"
            )
        endpoint = self._amvera_endpoint_for_model(model)
        url = f"{base_url}/models/{endpoint}"

        payload = {
            "model": model,
            "messages": [
                {"role": "system", "text": "Return only JSON object without markdown."},
                {"role": "user", "text": prompt},
            ],
        }
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Auth-Token": f"Bearer {self._amvera_api_key}",
        }

        with httpx.Client(timeout=45.0) as client:
            response = client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        # GPT-like models often return OpenAI-style payload.
        choices = data.get("choices")
        if isinstance(choices, list) and choices:
            message = choices[0].get("message", {})
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str) and content.strip():
                    return content

        # Llama-like payload shape in Amvera examples.
        result = data.get("result")
        if isinstance(result, dict):
            alternatives = result.get("alternatives")
            if isinstance(alternatives, list) and alternatives:
                alt_message = alternatives[0].get("message", {})
                if isinstance(alt_message, dict):
                    text = alt_message.get("text")
                    if isinstance(text, str) and text.strip():
                        return text

        # Fallbacks for provider-specific variants.
        if isinstance(data.get("text"), str):
            return data["text"]
        if isinstance(data.get("content"), str):
            return data["content"]

        # Keep parser behavior consistent: best-effort JSON string.
        return json.dumps(data, ensure_ascii=False)

    def _amvera_endpoint_for_model(self, model: str) -> str:
        low = model.lower()
        if low.startswith("gpt"):
            return "gpt"
        if low.startswith("llama"):
            return "llama"
        if low.startswith("deepseek"):
            return "deepseek"
        if low.startswith("qwen"):
            return "qwen"
        return "gpt"

    def _parse_json(self, raw: str) -> dict:
        text = (raw or "").strip()
        if not text:
            return {}

        if text.startswith("```"):
            text = re.sub(r"^```[a-zA-Z]*", "", text).strip()
            text = re.sub(r"```$", "", text).strip()

        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if match:
            text = match.group(0)

        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError:
            return {}
        return {}

    def _to_text_list(self, value: object) -> list[str]:
        if not isinstance(value, list):
            return []
        result: list[str] = []
        for item in value:
            text = str(item).strip()
            if text:
                result.append(text)
        return result

    def _normalize_status(self, raw_status: str) -> str:
        value = raw_status.strip().lower().replace("-", "_").replace(" ", "_")
        if value in {"done", "completed", "ready", "сделано", "готово"}:
            return "done"
        if value in {"blocked", "stuck", "waiting", "зависло", "ожидание"}:
            return "blocked"
        return "in_progress"
