const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;

const state = {
  token: '',
  user: null,
  activeShift: null,
  hours: null,
  timerId: null
};

const el = {
  userLine: document.getElementById('userLine'),
  statusPill: document.getElementById('statusPill'),
  shiftStateText: document.getElementById('shiftStateText'),
  shiftTimer: document.getElementById('shiftTimer'),
  todaySummary: document.getElementById('todaySummary'),
  startShiftBtn: document.getElementById('startShiftBtn'),
  endShiftBtn: document.getElementById('endShiftBtn'),
  scheduleForm: document.getElementById('scheduleForm'),
  scheduleDate: document.getElementById('scheduleDate'),
  scheduleStart: document.getElementById('scheduleStart'),
  scheduleEnd: document.getElementById('scheduleEnd'),
  scheduleList: document.getElementById('scheduleList'),
  historyList: document.getElementById('historyList'),
  hoursGrid: document.getElementById('hoursGrid'),
  managerTab: document.getElementById('managerTab'),
  managerActiveList: document.getElementById('managerActiveList'),
  managerUsersList: document.getElementById('managerUsersList'),
  managerHoursList: document.getElementById('managerHoursList'),
  managerSchedulesList: document.getElementById('managerSchedulesList'),
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

function formatTimer(seconds) {
  const total = Math.max(0, Math.floor(seconds));
  const hh = String(Math.floor(total / 3600)).padStart(2, '0');
  const mm = String(Math.floor((total % 3600) / 60)).padStart(2, '0');
  const ss = String(total % 60).padStart(2, '0');
  return `${hh}:${mm}:${ss}`;
}

function setTab(tabName) {
  document.querySelectorAll('.tab').forEach((tab) => {
    tab.classList.toggle('active', tab.dataset.tab === tabName);
  });

  document.querySelectorAll('.screen').forEach((screen) => {
    screen.classList.toggle('active', screen.id === `screen-${tabName}`);
  });
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
    throw new Error(data.error || 'Ошибка запроса');
  }

  return data;
}

async function login() {
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
  state.user = data.user;
}

function renderMain() {
  const user = state.user;
  const activeShift = state.activeShift;
  const onShift = Boolean(activeShift);

  el.userLine.textContent = `${user.full_name} (${user.role})`;
  el.statusPill.textContent = onShift ? 'На смене' : 'Не на смене';
  el.statusPill.classList.toggle('active', onShift);

  el.shiftStateText.textContent = onShift
    ? `Смена начата: ${formatDateTime(activeShift.start_datetime)}`
    : 'Статус: не на смене';

  el.startShiftBtn.disabled = onShift;
  el.endShiftBtn.disabled = !onShift;

  const today = state.hours ? state.hours.total_today_minutes : 0;
  el.todaySummary.textContent = minutesLabel(today);

  startOrStopTimer();
}

function startOrStopTimer() {
  if (state.timerId) {
    clearInterval(state.timerId);
    state.timerId = null;
  }

  if (!state.activeShift || !state.activeShift.start_datetime) {
    el.shiftTimer.textContent = '00:00:00';
    return;
  }

  const startedAt = new Date(state.activeShift.start_datetime).getTime();

  const tick = () => {
    const now = Date.now();
    const sec = (now - startedAt) / 1000;
    el.shiftTimer.textContent = formatTimer(sec);
  };

  tick();
  state.timerId = setInterval(tick, 1000);
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

function renderHours(hours) {
  const cards = [
    { title: 'Сегодня', key: 'total_today_minutes' },
    { title: 'Неделя', key: 'total_week_minutes' },
    { title: 'Месяц', key: 'total_month_minutes' },
    { title: 'За всё время', key: 'total_all_time_minutes' }
  ];

  el.hoursGrid.innerHTML = cards
    .map(
      (card) => `
      <div class="card">
        <div class="small">${card.title}</div>
        <div class="value">${minutesLabel(hours[card.key] || 0)}</div>
      </div>
    `
    )
    .join('');
}

function renderManager(data) {
  const activeRows = data.activeNow || [];
  const users = data.users || [];
  const summaries = data.summaries || [];
  const schedules = data.schedules || [];

  el.managerActiveList.innerHTML = activeRows.length
    ? activeRows
        .map(
          (item) => `
            <div class="row">
              <div class="left">
                <strong>${item.full_name}</strong>
                <span class="small">С ${formatDateTime(item.start_datetime)}</span>
              </div>
            </div>
          `
        )
        .join('')
    : '<p class="small">Сейчас никто не на смене.</p>';

  el.managerUsersList.innerHTML = users.length
    ? users
        .map(
          (user) => `
            <div class="row">
              <div class="left">
                <strong>${user.full_name}</strong>
                <span class="small">Роль: ${user.role}</span>
              </div>
            </div>
          `
        )
        .join('')
    : '<p class="small">Сотрудники не найдены.</p>';

  el.managerHoursList.innerHTML = summaries.length
    ? summaries
        .map(
          (row) => `
            <div class="row">
              <div class="left">
                <strong>${row.full_name}</strong>
                <span class="small">Сегодня: ${minutesLabel(row.total_today_minutes)}, за всё время: ${minutesLabel(row.total_all_time_minutes)}</span>
              </div>
            </div>
          `
        )
        .join('')
    : '<p class="small">Данные по часам отсутствуют.</p>';

  el.managerSchedulesList.innerHTML = schedules.length
    ? schedules
        .map(
          (row) => `
            <div class="row">
              <div class="left">
                <strong>${row.full_name}</strong>
                <span class="small">${row.date} ${row.start_time} - ${row.end_time}</span>
              </div>
            </div>
          `
        )
        .join('')
    : '<p class="small">Будущих расписаний нет.</p>';
}

async function loadMainData() {
  const [me, schedules, shifts, hours] = await Promise.all([
    api('/api/me'),
    api('/api/my/schedules'),
    api('/api/my/shifts'),
    api('/api/my/hours')
  ]);

  state.user = me.user;
  state.activeShift = me.active_shift;
  state.hours = hours.hours;

  renderMain();
  renderSchedules(schedules.items || []);
  renderHistory(shifts.items || []);
  renderHours(hours.hours || {});

  const isManager = state.user.role === 'manager' || state.user.role === 'admin';
  el.managerTab.hidden = !isManager;

  if (isManager) {
    const dashboard = await api('/api/manager/dashboard');
    renderManager(dashboard);
  }
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

  const schedules = await api('/api/my/schedules');
  renderSchedules(schedules.items || []);
}

async function handleDeleteSchedule(scheduleId) {
  await api(`/api/my/schedules/${scheduleId}`, { method: 'DELETE' });
  showToast('Запись удалена');

  const schedules = await api('/api/my/schedules');
  renderSchedules(schedules.items || []);
}

async function handleStartShift() {
  await api('/api/my/shifts/start', { method: 'POST' });
  showToast('Смена начата');
  await loadMainData();
}

async function handleEndShift() {
  await api('/api/my/shifts/end', { method: 'POST' });
  showToast('Смена завершена');
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

  el.startShiftBtn.addEventListener('click', async () => {
    try {
      await handleStartShift();
    } catch (error) {
      showToast(error.message);
    }
  });

  el.endShiftBtn.addEventListener('click', async () => {
    try {
      await handleEndShift();
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
    await login();
    await loadMainData();
  } catch (error) {
    console.error(error);
    showToast(error.message || 'Ошибка инициализации');
    el.userLine.textContent = 'Ошибка авторизации';
  }
}

init();
