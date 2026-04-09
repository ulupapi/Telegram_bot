const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
const SESSION_TOKEN_KEY = 'workers_bot_session_token';

const state = {
  token: '',
  user: null
};

const el = {
  userLine: document.getElementById('userLine'),
  scheduleForm: document.getElementById('scheduleForm'),
  scheduleDate: document.getElementById('scheduleDate'),
  scheduleStart: document.getElementById('scheduleStart'),
  scheduleEnd: document.getElementById('scheduleEnd'),
  scheduleList: document.getElementById('scheduleList'),
  historyList: document.getElementById('historyList'),
  toast: document.getElementById('toast')
};

function showToast(text) {
  el.toast.textContent = text;
  el.toast.classList.add('show');
  setTimeout(() => el.toast.classList.remove('show'), 2400);
}

function minutesLabel(value) {
  const minutes = Number(value || 0);
  const h = Math.floor(minutes / 60);
  const m = minutes % 60;
  return `${h} ч ${m} мин`;
}

function formatDateTime(iso) {
  if (!iso) return '-';
  return new Date(iso).toLocaleString('ru-RU', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
  });
}

function setTab(tabName) {
  document.querySelectorAll('.tab').forEach((tab) => {
    tab.classList.toggle('active', tab.dataset.tab === tabName);
  });

  document.querySelectorAll('.screen').forEach((screen) => {
    screen.classList.toggle('active', screen.id === `screen-${tabName}`);
  });
}

function getSavedToken() {
  try {
    return localStorage.getItem(SESSION_TOKEN_KEY) || '';
  } catch (error) {
    return '';
  }
}

function saveToken(token) {
  try {
    localStorage.setItem(SESSION_TOKEN_KEY, token);
  } catch (error) {
    // ignore
  }
}

function clearSavedToken() {
  try {
    localStorage.removeItem(SESSION_TOKEN_KEY);
  } catch (error) {
    // ignore
  }
}

async function api(path, options = {}) {
  const method = options.method || 'GET';
  const body = options.body || null;
  const auth = options.auth !== false;

  const headers = {
    'Content-Type': 'application/json'
  };

  if (auth && state.token) {
    headers.Authorization = `Bearer ${state.token}`;
  }

  const response = await fetch(path, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    if (response.status === 401) {
      state.token = '';
      clearSavedToken();
    }
    throw new Error(data.error || 'Ошибка запроса');
  }

  return data;
}

async function loginAndBootstrap() {
  const cachedToken = getSavedToken();
  if (cachedToken) {
    state.token = cachedToken;
    try {
      return await api('/api/my/bootstrap');
    } catch (error) {
      state.token = '';
      clearSavedToken();
    }
  }

  const params = new URLSearchParams(window.location.search);
  const payload = {
    initData: tg ? tg.initData : ''
  };

  if (!payload.initData) {
    payload.devTelegramId = params.get('dev_id') || '100000001';
    payload.devUsername = params.get('dev_username') || 'local_user';
    payload.devFullName = params.get('dev_name') || 'Локальный Пользователь';
  }

  const data = await api('/api/auth/telegram', {
    method: 'POST',
    body: payload,
    auth: false
  });

  state.token = data.token;
  saveToken(data.token);
  return data;
}

function renderSchedules(items) {
  if (!items.length) {
    el.scheduleList.innerHTML = '<p class="small">Записей пока нет.</p>';
    return;
  }

  el.scheduleList.innerHTML = items
    .map(
      (item) => `
      <div class="row">
        <div class="left">
          <strong>${item.date}</strong>
          <span class="small">${item.start_time} - ${item.end_time}</span>
        </div>
        <button class="btn btn-outline" data-delete-schedule="${item.id}">Удалить</button>
      </div>
    `
    )
    .join('');
}

function renderHistory(items) {
  if (!items.length) {
    el.historyList.innerHTML = '<p class="small">История смен пока пустая.</p>';
    return;
  }

  el.historyList.innerHTML = items
    .map(
      (item) => `
      <div class="row">
        <div class="left">
          <strong>${formatDateTime(item.start_datetime)} - ${formatDateTime(item.end_datetime)}</strong>
          <span class="small">Длительность: ${minutesLabel(item.duration_minutes)}</span>
        </div>
      </div>
    `
    )
    .join('');
}

function applyBootstrapData(data) {
  state.user = data.user;
  el.userLine.textContent = `${state.user.full_name}`;

  renderSchedules(data.schedules || []);
  renderHistory(data.shifts || []);
}

async function loadMainData() {
  const data = await api('/api/my/bootstrap');
  applyBootstrapData(data);
}

async function handleAddSchedule(event) {
  event.preventDefault();

  const date = el.scheduleDate.value;
  const start = el.scheduleStart.value;
  const end = el.scheduleEnd.value;

  await api('/api/my/schedules', {
    method: 'POST',
    body: {
      date,
      start_time: start,
      end_time: end
    }
  });

  showToast('Расписание сохранено');
  el.scheduleForm.reset();
  await loadMainData();
}

async function handleDeleteSchedule(scheduleId) {
  await api(`/api/my/schedules/${scheduleId}`, { method: 'DELETE' });
  showToast('Запись удалена');
  await loadMainData();
}

function attachEvents() {
  document.getElementById('tabs').addEventListener('click', (event) => {
    const tab = event.target.closest('.tab');
    if (!tab) return;
    setTab(tab.dataset.tab);
  });

  el.scheduleForm.addEventListener('submit', async (event) => {
    try {
      await handleAddSchedule(event);
    } catch (error) {
      showToast(error.message);
    }
  });

  el.scheduleList.addEventListener('click', async (event) => {
    const button = event.target.closest('[data-delete-schedule]');
    if (!button) return;

    try {
      await handleDeleteSchedule(button.dataset.deleteSchedule);
    } catch (error) {
      showToast(error.message);
    }
  });
}

async function init() {
  try {
    if (tg) {
      tg.ready();
      tg.expand();
    }

    attachEvents();
    const bootstrapData = await loginAndBootstrap();
    applyBootstrapData(bootstrapData);
  } catch (error) {
    console.error(error);
    showToast(error.message || 'Ошибка инициализации');
    el.userLine.textContent = 'Ошибка авторизации';
  }
}

init();
