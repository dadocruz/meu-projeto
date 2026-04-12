(() => {
  'use strict';

  const BACKEND_PROD_URL = 'https://gaveta-monitor.onrender.com';
  const BACKEND_LOCAL_URL = 'http://localhost:3000';
  const STORAGE_KEY = 'gaveta-radar-v5-artists';
  const DASHBOARD_CACHE_KEY = 'gaveta-radar-v5-dashboard-cache';
  const CLOUD_SYNC_BOOTSTRAP_KEY = 'gaveta-radar-v5-cloud-bootstrap';
  const FULL_REFRESH_INTERVAL_MS = 7 * 24 * 60 * 60 * 1000;
  const LAST_FULL_REFRESH_KEY = 'gaveta-radar-v5-last-full-refresh';

  function initialBackendUrl() {
    const forced = new URLSearchParams(window.location.search).get('api');
    if (forced) return forced;
    const host = window.location.hostname;
    const isLocalHost = host === 'localhost' || host === '127.0.0.1';
    const isFileProtocol = window.location.protocol === 'file:';
    return (isLocalHost || isFileProtocol) ? BACKEND_LOCAL_URL : BACKEND_PROD_URL;
  }

  let activeBackendUrl = initialBackendUrl();

  function backendCandidates() {
    if (activeBackendUrl === BACKEND_LOCAL_URL) return [BACKEND_LOCAL_URL, BACKEND_PROD_URL];
    if (activeBackendUrl === BACKEND_PROD_URL) return [BACKEND_PROD_URL, BACKEND_LOCAL_URL];
    return [activeBackendUrl, BACKEND_PROD_URL, BACKEND_LOCAL_URL];
  }

  async function fetchBackendJson(pathname, options = {}) {
    let lastError = null;
    for (const baseUrl of backendCandidates()) {
      try {
        const res = await fetch(`${baseUrl}${pathname}`, options);
        const data = await res.json();
        if (!res.ok || !data.ok) throw new Error(data.error || 'Erro no servidor');
        activeBackendUrl = baseUrl;
        return data;
      } catch (err) {
        lastError = err;
      }
    }
    throw lastError || new Error('Não foi possível conectar ao backend.');
  }

  async function fetchDashboardPayload(artists) {
    return fetchBackendJson('/api/dashboard', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ artists }),
    });
  }

  const qs = id => document.getElementById(id);
  const esc = (v = '') => String(v)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const addDrawer = qs('addDrawer');
  const manageDrawer = qs('manageDrawer');
  const backdrop = qs('drawerBackdrop');
  const menuBtn = qs('menuBtn');
  const menuDrawer = qs('menuDrawer');
  const menuBackdrop = qs('menuBackdrop');
  const menuRefreshBtn = qs('menuRefreshBtn');
  const menuExportBtn = qs('menuExportBtn');
  const menuImportBtn = qs('menuImportBtn');
  const menuManageBtn = qs('menuManageBtn');
  const menuAddBtn = qs('menuAddBtn');
  const menuCloseBtn = qs('menuCloseBtn');
  const formError = qs('formError');
  const refreshBtn = qs('refreshBtn');
  const artBtn = qs('artesBtn');
  const menuArtBtn = qs('menuArtBtn');
  const artBadge = qs('artCount');
  const artSection = qs('artSection');
  const artList = qs('artList');
  const artsDrawer = qs('artesDrawer');
  const closeArtes = qs('closeArtes');
  const artTaskList = qs('artTaskList');
  const artTabs = qs('artTabs');
  const toggleManualTask = qs('toggleManualTask');
  const manualTaskForm = qs('manualTaskForm');
  const manualTaskArtist = qs('manualTaskArtist');
  const manualTaskTitle = qs('manualTaskTitle');
  const manualTaskNotes = qs('manualTaskNotes');
  const manualArtistList = qs('artistNamesList');
  const cancelManualTask = qs('cancelManualTask');
  const createManualTaskBtn = qs('createManualTask');
  const sumPending = qs('sumPending');
  const sumDoing = qs('sumDoing');
  const sumReady = qs('sumReady');
  const sumDelivered = qs('sumDelivered');
  const ART_TASKS_KEY = 'gaveta-radar-v5-art-tasks';
  const ART_TASK_TAB_KEY = 'gaveta-radar-v5-art-tab';
  const ART_TASK_ID_PREFIX = 'art-task-';
  const TASK_STATUSES = ['pending', 'doing', 'ready', 'delivered'];
  const TASK_STATUS_LABELS = {
    pending: 'Pendente',
    doing: 'Em andamento',
    ready: 'Pronta',
    delivered: 'Entregue',
  };
  const MONTHLY_LISTENER_STEPS = [5000, 50000, 100000];
  const YOUTUBE_SIGNAL_STEPS = [10000, 50000, 100000, 250000, 500000, 1000000, 2500000, 5000000, 10000000];
  const SIGNAL_WINDOW = {
    spotifyListeners: 0.10,
    singlePlays: 0.05,
    youtube: 0.10,
  };
  let artistsCloudSyncQueue = Promise.resolve();
  let latestDashboardArtists = [];
  let latestArtSignals = [];
  let activeArtTab = localStorage.getItem(ART_TASK_TAB_KEY) || 'pending';

  function getLastFullRefresh() {
    return Number(localStorage.getItem(LAST_FULL_REFRESH_KEY) || 0);
  }

  function setLastFullRefresh(ts) {
    localStorage.setItem(LAST_FULL_REFRESH_KEY, String(ts));
  }

  function getFullRefreshRemainingMs() {
    const elapsed = Date.now() - getLastFullRefresh();
    return Math.max(0, FULL_REFRESH_INTERVAL_MS - elapsed);
  }

  function formatMmSs(ms) {
    const totalSec = Math.ceil(ms / 1000);
    const mm = String(Math.floor(totalSec / 60)).padStart(2, '0');
    const ss = String(totalSec % 60).padStart(2, '0');
    return `${mm}:${ss}`;
  }

  function formatRemainingLong(ms) {
    const totalMinutes = Math.ceil(ms / 60000);
    const days = Math.floor(totalMinutes / (24 * 60));
    const hours = Math.floor((totalMinutes % (24 * 60)) / 60);
    const mins = totalMinutes % 60;
    if (days > 0) return `${days}d ${hours}h`;
    if (hours > 0) return `${hours}h ${mins}m`;
    return `${formatMmSs(ms)}`;
  }

  function updateRefreshButtonState() {
    const remaining = getFullRefreshRemainingMs();
    if (remaining > 0) {
      refreshBtn.disabled = true;
      refreshBtn.textContent = `Geral em ${formatRemainingLong(remaining)}`;
    } else {
      refreshBtn.disabled = false;
      refreshBtn.textContent = 'Atualizar geral (semanal)';
    }
  }

  function confirmFullRefresh() {
    const remaining = getFullRefreshRemainingMs();
    if (remaining > 0) {
      const nextDate = new Date(getLastFullRefresh() + FULL_REFRESH_INTERVAL_MS).toLocaleString('pt-BR');
      alert(`Atualização geral bloqueada até ${nextDate}.`);
      return false;
    }

    const confirmed = window.confirm('Atualização geral vai consumir créditos da API para TODOS os artistas. Deseja continuar?');
    if (!confirmed) return false;

    const typed = window.prompt('Digite ATUALIZAR GERAL para confirmar.');
    return typed === 'ATUALIZAR GERAL';
  }

  function extractSpotifyId(v = '') {
    const s = String(v).trim();
    for (const p of [
      /open\.spotify\.com\/(?:intl-[^/]+\/)?artist\/([A-Za-z0-9]{22})/i,
      /spotify:artist:([A-Za-z0-9]{22})/i,
      /^([A-Za-z0-9]{22})$/,
    ]) { const m = s.match(p); if (m) return m[1]; }
    return null;
  }

  function isYouTubeChannel(v = '') {
    return /youtube\.com\/(channel\/|c\/|user\/|@)/i.test(String(v).trim());
  }

  function safeJsonParse(raw, fallback) {
    try {
      return JSON.parse(raw);
    } catch {
      return fallback;
    }
  }

  function makeId(prefix = ART_TASK_ID_PREFIX) {
    if (window.crypto?.randomUUID) return `${prefix}${window.crypto.randomUUID()}`;
    return `${prefix}${Date.now()}-${Math.random().toString(16).slice(2)}`;
  }

  function normalizeMetricValue(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  function formatMilestoneValue(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) return '--';
    return compact(number).replace('mil', 'mil');
  }

  function milestoneStepForMonthlyListeners(value) {
    if (value >= 500000) return 100000;
    if (value >= 50000) return 50000;
    return 5000;
  }

  function nextMilestone(value, kind) {
    const current = normalizeMetricValue(value);
    if (!Number.isFinite(current) || current < 0) return null;

    if (kind === 'monthlyListeners') {
      const step = milestoneStepForMonthlyListeners(current);
      return Math.max(step, Math.ceil(current / step) * step);
    }

    if (kind === 'singlePlays') {
      const step = 50000;
      return Math.max(step, Math.ceil(current / step) * step);
    }

    if (kind === 'youtube') {
      const hit = YOUTUBE_SIGNAL_STEPS.find(threshold => current <= threshold);
      if (hit) return hit;
      const last = YOUTUBE_SIGNAL_STEPS[YOUTUBE_SIGNAL_STEPS.length - 1];
      const step = last * 2;
      return Math.max(step, Math.ceil(current / step) * step);
    }

    return null;
  }

  function signalWithinWindow(current, target, windowPct) {
    if (!Number.isFinite(current) || !Number.isFinite(target) || target <= 0) return false;
    return current >= target * (1 - windowPct);
  }

  function shortMetricLabel(metric) {
    if (metric === 'monthlyListeners') return 'Ouvintes mensais';
    if (metric === 'singlePlays') return 'Plays do single';
    if (metric === 'youtubeSubscribers') return 'Inscritos no YouTube';
    if (metric === 'youtubeViews') return 'Visualizações no YouTube';
    if (metric === 'videoViews') return 'Views do vídeo';
    return 'Métrica';
  }

  function buildTaskKey(signal) {
    return [signal.artistId || signal.artistName || '', signal.metric, signal.metricKey || '', signal.target || ''].join('::');
  }

  function loadArtTasks() {
    return safeJsonParse(localStorage.getItem(ART_TASKS_KEY) || '[]', []);
  }

  function saveArtTasks(tasks) {
    localStorage.setItem(ART_TASKS_KEY, JSON.stringify(tasks));
    renderArtCounters();
    if (qs('artesDrawer') && !artsDrawer.classList.contains('hidden')) {
      renderArtTasks();
    }
  }

  function getTaskStatusLabel(status) {
    return TASK_STATUS_LABELS[status] || 'Pendente';
  }

  function getTaskTabCount(tasks, status) {
    return tasks.filter(task => task.status === status).length;
  }

  function renderArtCounters() {
    const tasks = loadArtTasks();
    const pendingCount = tasks.filter(task => task.status !== 'delivered').length;
    if (artBadge) artBadge.textContent = String(pendingCount);
    if (qs('artesCount')) qs('artesCount').textContent = String(pendingCount);
    if (sumPending) sumPending.textContent = String(getTaskTabCount(tasks, 'pending'));
    if (sumDoing) sumDoing.textContent = String(getTaskTabCount(tasks, 'doing'));
    if (sumReady) sumReady.textContent = String(getTaskTabCount(tasks, 'ready'));
    if (sumDelivered) sumDelivered.textContent = String(getTaskTabCount(tasks, 'delivered'));
  }

  function getArtistNamesForManualTask() {
    return latestDashboardArtists
      .filter(artist => !artist?.error)
      .map(artist => artist.artistName)
      .filter(Boolean);
  }

  function syncManualArtistList() {
    if (!manualArtistList) return;
    const names = getArtistNamesForManualTask();
    manualArtistList.innerHTML = names.map(name => `<option value="${esc(name)}"></option>`).join('');
  }

  function openArtsDrawer(tab = activeArtTab) {
    activeArtTab = TASK_STATUSES.includes(tab) ? tab : 'pending';
    localStorage.setItem(ART_TASK_TAB_KEY, activeArtTab);
    closeDrawers();
    openDrawer(artsDrawer);
    renderArtTasks();
  }

  function closeArtsComposer() {
    manualTaskForm.classList.remove('visible');
    toggleManualTask.textContent = 'Abrir';
    manualTaskArtist.value = '';
    manualTaskTitle.value = '';
    manualTaskNotes.value = '';
  }

  function getTasks() {
    return loadArtTasks();
  }

  function upsertTask(task) {
    const tasks = getTasks();
    const existingIndex = tasks.findIndex(item => item.sourceKey && item.sourceKey === task.sourceKey);
    const finalTask = {
      id: task.id || makeId(),
      artistName: task.artistName || '',
      artistImage: task.artistImage || '',
      title: task.title || 'Arte',
      sourceKey: task.sourceKey || null,
      metric: task.metric || 'manual',
      metricLabel: task.metricLabel || '',
      currentValue: normalizeMetricValue(task.currentValue),
      targetValue: normalizeMetricValue(task.targetValue),
      thresholdWindow: task.thresholdWindow || null,
      notes: task.notes || '',
      status: TASK_STATUSES.includes(task.status) ? task.status : 'pending',
      asset: task.asset || null,
      createdAt: task.createdAt || new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };

    if (existingIndex >= 0) {
      tasks[existingIndex] = { ...tasks[existingIndex], ...finalTask, id: tasks[existingIndex].id };
    } else {
      tasks.push(finalTask);
    }

    saveArtTasks(tasks);
    return finalTask;
  }

  function updateTask(taskId, mutator) {
    const tasks = getTasks();
    const index = tasks.findIndex(task => task.id === taskId);
    if (index === -1) return null;
    const nextTask = mutator({ ...tasks[index] });
    if (!nextTask) return tasks[index];
    nextTask.updatedAt = new Date().toISOString();
    tasks[index] = nextTask;
    saveArtTasks(tasks);
    return nextTask;
  }

  function deleteTask(taskId) {
    const tasks = getTasks().filter(task => task.id !== taskId);
    saveArtTasks(tasks);
  }

  function cycleTaskStatus(taskId) {
    updateTask(taskId, task => {
      const currentIndex = TASK_STATUSES.indexOf(task.status);
      const nextStatus = TASK_STATUSES[(currentIndex + 1) % TASK_STATUSES.length];
      return { ...task, status: nextStatus };
    });
    renderArtTasks();
  }

  function formatTaskMetric(task) {
    if (task.metricLabel && task.targetValue != null) {
      return `${task.metricLabel} · meta ${compact(task.targetValue)}`;
    }
    if (task.metricLabel) return task.metricLabel;
    return 'Arte manual';
  }

  function taskAssetPreview(asset) {
    if (!asset?.dataUrl) return '';
    return `<img class="task-preview" src="${esc(asset.dataUrl)}" alt="Preview da arte" loading="lazy">`;
  }

  function signalTargetLabel(signal) {
    return `${compact(signal.current)} / ${compact(signal.target)}`;
  }

  function ensureTaskFromSignal(signal) {
    const task = upsertTask({
      artistName: signal.artistName,
      artistImage: signal.artistImage,
      title: signal.taskTitle,
      sourceKey: signal.key,
      metric: signal.metric,
      metricLabel: signal.metricLabel,
      currentValue: signal.current,
      targetValue: signal.target,
      thresholdWindow: signal.window,
      notes: signal.notes || '',
      status: 'pending',
      createdAt: new Date().toISOString(),
    });
    openArtsDrawer('pending');
    return task;
  }

  function milestoneSignalsFromArtist(artist) {
    if (!artist || artist.error) return [];

    const signals = [];
    const artistId = String(artist.spotifyArtistId || '').trim();
    const artistName = artist.artistName || 'Artista';
    const artistImage = artist.imageUrl || artist.youtube?.channelThumbnail || '';
    const monthlyListeners = normalizeMetricValue(artist.spotify?.monthlyListeners?.value);
    const singles = Array.isArray(artist.spotify?.singles) ? artist.spotify.singles : [];
    const youtube = artist.youtube || {};

    if (Number.isFinite(monthlyListeners) && monthlyListeners >= 5000) {
      const target = nextMilestone(monthlyListeners, 'monthlyListeners');
      if (signalWithinWindow(monthlyListeners, target, SIGNAL_WINDOW.spotifyListeners)) {
        signals.push({
          key: `${artistId}:spotify-listeners:${target}`,
          artistId,
          artistName,
          artistImage,
          metric: 'monthlyListeners',
          metricLabel: shortMetricLabel('monthlyListeners'),
          current: monthlyListeners,
          target,
          window: SIGNAL_WINDOW.spotifyListeners,
          title: `${artistName} | Ouvintes mensais ${compact(target)}`,
          taskTitle: `${artistName} | Ouvintes mensais ${compact(target)}`,
          notes: `Marco detectado: ${compact(monthlyListeners)} ouvintes de ${compact(target)}.`,
        });
      }
    }

    singles.slice(0, 5).forEach((single, index) => {
      const plays = normalizeMetricValue(single?.plays);
      if (!Number.isFinite(plays) || plays <= 0) return;
      const target = nextMilestone(plays, 'singlePlays');
      if (!signalWithinWindow(plays, target, SIGNAL_WINDOW.singlePlays)) return;

      signals.push({
        key: `${artistId}:single:${index}:${target}`,
        artistId,
        artistName,
        artistImage,
        metric: 'singlePlays',
        metricLabel: shortMetricLabel('singlePlays'),
        current: plays,
        target,
        window: SIGNAL_WINDOW.singlePlays,
        title: `${artistName} | Single perto de ${compact(target)} plays`,
        taskTitle: `${artistName} | Comemorar ${compact(target)} plays`,
        notes: `${single?.title || 'Single'} está em ${compact(plays)} de ${compact(target)} plays.`,
      });
    });

    const youtubeMilestoneMetrics = [
      { key: 'youtubeSubscribers', label: 'Inscritos no YouTube', current: youtube.subscribers },
      { key: 'youtubeViews', label: 'Views do canal', current: youtube.channelViews },
    ];

    youtubeMilestoneMetrics.forEach(item => {
      const value = normalizeMetricValue(item.current);
      if (!Number.isFinite(value) || value <= 0) return;
      const target = nextMilestone(value, 'youtube');
      if (!signalWithinWindow(value, target, SIGNAL_WINDOW.youtube)) return;
      signals.push({
        key: `${artistId}:${item.key}:${target}`,
        artistId,
        artistName,
        artistImage,
        metric: item.key,
        metricLabel: item.label,
        current: value,
        target,
        window: SIGNAL_WINDOW.youtube,
        title: `${artistName} | ${item.label} perto de ${compact(target)}`,
        taskTitle: `${artistName} | Arte ${item.label} ${compact(target)}`,
        notes: `Marco do YouTube em ${compact(value)} de ${compact(target)}.`,
      });
    });

    (youtube.latestVideos || []).forEach((video, index) => {
      const views = normalizeMetricValue(video?.views);
      if (!Number.isFinite(views) || views <= 0) return;
      const target = nextMilestone(views, 'youtube');
      if (!signalWithinWindow(views, target, SIGNAL_WINDOW.youtube)) return;
      signals.push({
        key: `${artistId}:video:${index}:${target}`,
        artistId,
        artistName,
        artistImage,
        metric: 'videoViews',
        metricLabel: shortMetricLabel('videoViews'),
        current: views,
        target,
        window: SIGNAL_WINDOW.youtube,
        title: `${artistName} | Vídeo perto de ${compact(target)} views`,
        taskTitle: `${artistName} | Arte vídeo ${compact(target)} views`,
        notes: `${video?.title || 'Vídeo'} está com ${compact(views)} views de ${compact(target)}.`,
      });
    });

    return signals;
  }

  function buildArtSignals(artists) {
    const tasks = loadArtTasks();
    const deliveredKeys = new Set(tasks.filter(task => task.status === 'delivered').map(task => task.sourceKey).filter(Boolean));
    const openKeys = new Set(tasks.filter(task => task.status !== 'delivered').map(task => task.sourceKey).filter(Boolean));

    return (artists || [])
      .flatMap(artist => milestoneSignalsFromArtist(artist))
      .filter(signal => !deliveredKeys.has(signal.key))
      .map(signal => ({
        ...signal,
        existingTask: openKeys.has(signal.key),
      }))
      .sort((a, b) => Number(b.target || 0) - Number(a.target || 0));
  }

  function renderArtSignals(signals) {
    latestArtSignals = signals || [];
    if (!signals || !signals.length) {
      artSection.classList.add('hidden');
      artList.innerHTML = '';
      renderArtCounters();
      return;
    }

    artSection.classList.remove('hidden');
    artBadge.textContent = String(signals.length);
    artList.innerHTML = signals.map(signal => `
      <button type="button" class="art-card" data-signal-key="${esc(signal.key)}">
        <img class="art-art" src="${esc(signal.artistImage || '')}" alt="${esc(signal.artistName || 'Artista')}" loading="lazy">
        <div class="art-info">
          <div class="art-artist">${esc(signal.artistName || 'Artista')}</div>
          <div class="art-title">${esc(signal.title || signal.taskTitle || 'Arte')}</div>
          <div class="art-meta">${esc(signal.metricLabel || 'Métrica')} · ${esc(signalTargetLabel(signal))} · meta ${esc(compact(signal.target))}</div>
          <div class="signal-note">${signal.existingTask ? 'Já existe tarefa aberta para este marco.' : 'Clique para criar a tarefa automaticamente.'}</div>
        </div>
        <div class="art-cta">
          <span class="cta-label">${signal.existingTask ? 'Abrir tarefa' : 'Criar arte'}</span>
          <div class="cta-meta">${esc(compact(signal.current))} → ${esc(compact(signal.target))}</div>
        </div>
      </button>
    `).join('');

    artList.querySelectorAll('[data-signal-key]').forEach(btn => {
      btn.addEventListener('click', () => {
        const signal = latestArtSignals.find(item => item.key === btn.dataset.signalKey);
        if (!signal) return;
        ensureTaskFromSignal(signal);
      });
    });
  }

  function renderArtTasks() {
    renderArtCounters();
    syncManualArtistList();

    const tasks = loadArtTasks();
    const visibleTasks = tasks.filter(task => task.status === activeArtTab);
    artTabs.querySelectorAll('.art-tab').forEach(tab => {
      tab.classList.toggle('active', tab.dataset.taskTab === activeArtTab);
    });

    if (!visibleTasks.length) {
      artTaskList.innerHTML = '<div class="task-empty">Nenhuma tarefa neste status.</div>';
      return;
    }

    artTaskList.innerHTML = visibleTasks.map(task => {
      const statusLabel = getTaskStatusLabel(task.status);
      const metricLabel = task.metricLabel || 'Arte manual';
      const created = task.createdAt ? new Date(task.createdAt).toLocaleString('pt-BR') : '--';
      const currentTarget = task.targetValue != null
        ? `<span class="task-pill purple">${compact(task.currentValue)} / ${compact(task.targetValue)}</span>`
        : '';
      return `
        <article class="task-card" data-task-id="${esc(task.id)}">
          <div class="task-top">
            <div>
              <h4 class="task-title">${esc(task.title || 'Arte')}</h4>
              <div class="task-meta">${esc(task.artistName || 'Artista')} · ${esc(metricLabel)} · criado em ${esc(created)}</div>
            </div>
            <button type="button" class="task-status ${esc(task.status)}" data-cycle-status="${esc(task.id)}">${esc(statusLabel)}</button>
          </div>
          <div class="task-pill-row">
            <span class="task-pill green">${esc(task.artistName || 'Artista')}</span>
            ${currentTarget}
            ${task.sourceKey ? '<span class="task-pill orange">Automática</span>' : '<span class="task-pill purple">Manual</span>'}
          </div>
          <div class="task-body">
            ${task.asset?.dataUrl ? `<a href="${esc(task.asset.dataUrl)}" download="${esc(task.asset.fileName || 'arte.png')}">${taskAssetPreview(task.asset)}</a>` : '<div class="task-empty-preview">Sem upload<br>PNG/JPG</div>'}
            <div class="task-fields">
              <div class="task-grid">
                <div class="field">
                  <label>Upload da arte</label>
                  <div class="task-actions">
                    <button type="button" class="btn dark sm" data-upload-task="${esc(task.id)}">Enviar PNG/JPG</button>
                    ${task.asset?.dataUrl ? `<a class="btn dark sm" href="${esc(task.asset.dataUrl)}" download="${esc(task.asset.fileName || 'arte.png')}">Baixar</a>` : ''}
                  </div>
                  <input class="task-file" type="file" accept="image/png,image/jpeg" data-file-task="${esc(task.id)}" />
                </div>
                <div class="field">
                  <label>Origem</label>
                  <div class="hint">${esc(formatTaskMetric(task))}</div>
                </div>
              </div>
              <div class="field">
                <label>Notas</label>
                <textarea class="input task-note" data-task-notes="${esc(task.id)}" placeholder="Briefing, referências, prazo...">${esc(task.notes || '')}</textarea>
              </div>
              <div class="task-actions">
                <button type="button" class="btn dark sm" data-delete-task="${esc(task.id)}">Excluir</button>
              </div>
            </div>
          </div>
        </article>
      `;
    }).join('');

    artTaskList.querySelectorAll('[data-cycle-status]').forEach(btn => {
      btn.addEventListener('click', () => {
        cycleTaskStatus(btn.dataset.cycleStatus);
      });
    });

    artTaskList.querySelectorAll('[data-upload-task]').forEach(btn => {
      btn.addEventListener('click', () => {
        artTaskList.querySelector(`[data-file-task="${CSS.escape(btn.dataset.uploadTask)}"]`)?.click();
      });
    });

    artTaskList.querySelectorAll('[data-file-task]').forEach(input => {
      input.addEventListener('change', async () => {
        const file = input.files?.[0];
        input.value = '';
        if (!file) return;

        if (!/^image\/(png|jpeg)$/.test(file.type)) {
          alert('Use apenas PNG ou JPG.');
          return;
        }

        const dataUrl = await new Promise((resolve, reject) => {
          const reader = new FileReader();
          reader.onload = () => resolve(String(reader.result || ''));
          reader.onerror = () => reject(new Error('Falha ao ler a imagem.'));
          reader.readAsDataURL(file);
        });

        updateTask(input.dataset.fileTask, task => ({
          ...task,
          asset: {
            fileName: file.name,
            fileType: file.type,
            fileSize: file.size,
            dataUrl,
          },
        }));
        renderArtTasks();
      });
    });

    artTaskList.querySelectorAll('[data-task-notes]').forEach(textarea => {
      textarea.addEventListener('change', () => {
        updateTask(textarea.dataset.taskNotes, task => ({
          ...task,
          notes: textarea.value,
        }));
      });
    });

    artTaskList.querySelectorAll('[data-delete-task]').forEach(btn => {
      btn.addEventListener('click', () => {
        const confirmed = window.confirm('Excluir esta tarefa?');
        if (!confirmed) return;
        deleteTask(btn.dataset.deleteTask);
        renderArtTasks();
      });
    });

    renderArtCounters();
  }

  function submitManualTask() {
    const artistName = manualTaskArtist.value.trim();
    const title = manualTaskTitle.value.trim();
    const notes = manualTaskNotes.value.trim();

    if (!artistName) return alert('Digite o artista.');
    if (!title) return alert('Digite o título da arte.');

    upsertTask({
      artistName,
      title,
      notes,
      metric: 'manual',
      metricLabel: 'Arte manual',
      status: 'pending',
    });

    closeArtsComposer();
    renderArtTasks();
  }

  function setArtTab(tab) {
    if (!TASK_STATUSES.includes(tab)) return;
    activeArtTab = tab;
    localStorage.setItem(ART_TASK_TAB_KEY, tab);
    renderArtTasks();
  }

  function normalizeArtistEntry(item = {}) {
    const artistName = String(item?.artistName || item?.name || '').trim();
    const spotifyUrl = String(item?.spotifyUrl || '').trim();
    const youtubeUrl = String(item?.youtubeUrl || '').trim();
    const spotifyArtistId = extractSpotifyId(spotifyUrl) || String(item?.spotifyArtistId || '').trim() || undefined;

    if (!artistName || !spotifyArtistId) return null;

    return {
      artistName,
      spotifyUrl,
      youtubeUrl,
      spotifyArtistId,
    };
  }

  function sanitizeArtists(list) {
    const bySpotifyId = new Map();
    (Array.isArray(list) ? list : []).forEach(item => {
      const normalized = normalizeArtistEntry(item);
      if (!normalized) return;
      bySpotifyId.set(normalized.spotifyArtistId, normalized);
    });
    return [...bySpotifyId.values()];
  }

  function getArtists() {
    try {
      const raw = JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]');
      return sanitizeArtists(raw);
    } catch {
      return [];
    }
  }

  async function fetchCloudArtists() {
    const data = await fetchBackendJson('/api/artists');
    return sanitizeArtists(data.artists);
  }

  async function pushCloudArtists(artists) {
    const data = await fetchBackendJson('/api/artists', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ artists: sanitizeArtists(artists) }),
    });
    return sanitizeArtists(data.artists);
  }

  function queueCloudSync(artists) {
    const cleaned = sanitizeArtists(artists);
    artistsCloudSyncQueue = artistsCloudSyncQueue
      .catch(() => null)
      .then(async () => {
        try {
          await pushCloudArtists(cleaned);
        } catch {
          // Sem bloquear a UI quando o backend estiver temporariamente indisponível.
        }
      });
    return artistsCloudSyncQueue;
  }

  async function bootstrapArtistsFromCloud() {
    const localArtists = getArtists();
    try {
      const cloudArtists = await fetchCloudArtists();
      if (cloudArtists.length) {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(cloudArtists));
        localStorage.setItem(CLOUD_SYNC_BOOTSTRAP_KEY, '1');
        return cloudArtists;
      }

      if (localArtists.length) {
        await pushCloudArtists(localArtists);
        localStorage.setItem(CLOUD_SYNC_BOOTSTRAP_KEY, '1');
      }
      return localArtists;
    } catch {
      // Se não conseguir sincronizar agora, segue com o cache local e tenta de novo depois.
      return localArtists;
    }
  }

  function artistsFingerprint(artists) {
    const normalized = (artists || []).map(a => ({
      artistName: String(a?.artistName || '').trim().toLowerCase(),
      spotifyUrl: String(a?.spotifyUrl || '').trim(),
      youtubeUrl: String(a?.youtubeUrl || '').trim(),
      spotifyArtistId: String(a?.spotifyArtistId || '').trim(),
      cmArtistId: Number(a?.cmArtistId || 0) || undefined,
    }));
    return JSON.stringify(normalized);
  }

  function loadCachedDashboard() {
    try {
      const raw = JSON.parse(localStorage.getItem(DASHBOARD_CACHE_KEY) || 'null');
      return raw && typeof raw === 'object' ? raw : null;
    } catch {
      return null;
    }
  }

  function saveCachedDashboard(artists, payload) {
    const record = {
      savedAt: Date.now(),
      artistsFingerprint: artistsFingerprint(artists),
      payload,
    };
    localStorage.setItem(DASHBOARD_CACHE_KEY, JSON.stringify(record));
  }

  function loadMatchingCachedDashboard(artists) {
    const cached = loadCachedDashboard();
    if (!cached?.payload || !cached?.savedAt || !cached?.artistsFingerprint) return null;
    if (!Array.isArray(cached.payload.artists)) return null;

    const map = new Map();
    cached.payload.artists.forEach(item => {
      const id = String(item?.spotifyArtistId || '').trim();
      if (id) map.set(id, item);
    });

    const present = [];
    const missing = [];
    (artists || []).forEach(a => {
      const id = extractSpotifyId(a.spotifyUrl) || String(a.spotifyArtistId || '').trim();
      if (!id) {
        missing.push(a);
        return;
      }
      if (map.has(id)) present.push(map.get(id));
      else missing.push(a);
    });

    return { cached, present, missing };
  }

  function computePriorityLocal(artist) {
    let score = 0;
    score += Number(artist?.spotify?.monthlyListeners?.value || 0) / 100000;
    score += Number(artist?.spotify?.popularity || 0) / 100;
    score += Number(artist?.youtube?.channelViews || 0) / 10000000;
    score += Number(artist?.spotify?.followers || 0) / 200000;
    score += Number(artist?.spotify?.singles?.[0]?.plays || 0) / 1000000;
    score += Number(artist?.spotify?.topTracks?.[0]?.popularity || 0) / 100;
    score += Number(artist?.youtube?.latestVideos?.[0]?.views || 0) / 1000000;
    return score;
  }

  function buildDerivedData(artists) {
    const validArtists = (artists || []).filter(a => a && !a.error);
    const ranking = validArtists
      .map(item => ({
        artistName: item.artistName,
        imageUrl: item.imageUrl,
        monthlyListeners: item.spotify?.monthlyListeners?.value || 0,
        spotifyPopularity: item.spotify?.popularity || 0,
        youtubeViews: item.youtube?.channelViews || 0,
        priorityScore: computePriorityLocal(item),
      }))
      .sort((a, b) => b.priorityScore - a.priorityScore)
      .slice(0, 10);

    const trafficAlerts = [];
    validArtists.forEach(artist => {
      (artist.youtube?.latestVideos || []).forEach(video => {
        if (video?.needsTraffic) {
          trafficAlerts.push({
            artistName: artist.artistName,
            artistImage: artist.imageUrl,
            videoTitle: video.title,
            videoUrl: video.youtubeUrl,
            thumbnail: video.thumbnail,
            views: video.views,
            daysOld: video.daysOld,
            publishedAt: video.publishedAt,
          });
        }
      });
    });

    return { ranking, trafficAlerts };
  }

  function mergeLiveWithCachedArtists(livePayload, cachedPayload) {
    if (!livePayload || !Array.isArray(livePayload.artists) || !cachedPayload || !Array.isArray(cachedPayload.artists)) {
      return livePayload;
    }

    const bySpotifyId = new Map();
    cachedPayload.artists.forEach(item => {
      const id = String(item?.spotifyArtistId || '').trim();
      if (id) bySpotifyId.set(id, item);
    });

    const artists = livePayload.artists.map(item => {
      if (!item?.error) return item;
      const id = String(item?.spotifyArtistId || '').trim();
      const cachedItem = bySpotifyId.get(id);
      return cachedItem || item;
    });

    return {
      ...livePayload,
      artists,
    };
  }

  function renderDashboardPayload(data) {
    latestDashboardArtists = Array.isArray(data.artists) ? data.artists : [];
    qs('artistGrid').innerHTML = (data.artists || []).map(artistCard).join('');
    renderRanking(data.ranking || []);
    renderTrafficAlerts(data.trafficAlerts || []);
    renderArtSignals(buildArtSignals(latestDashboardArtists));
    renderArtTasks();
  }

  function saveArtists(data, options = {}) {
    const cleaned = sanitizeArtists(data);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(cleaned));
    renderSavedArtists();

    if (options.sync !== false) {
      queueCloudSync(cleaned);
    }

    return cleaned;
  }

  function exportArtists() {
    const artists = getArtists();
    const payload = {
      version: 1,
      exportedAt: new Date().toISOString(),
      artists,
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-');
    a.href = url;
    a.download = `gaveta-artists-${stamp}.json`;
    a.click();
    URL.revokeObjectURL(url);
  }

  async function importArtists(file) {
    const text = await file.text();
    let parsed;
    try {
      parsed = JSON.parse(text);
    } catch {
      throw new Error('Arquivo JSON inválido.');
    }

    const incoming = Array.isArray(parsed) ? parsed : parsed?.artists;
    if (!Array.isArray(incoming)) throw new Error('Formato inválido: lista de artistas não encontrada.');

    const normalized = incoming.map(item => ({
      artistName: String(item?.artistName || item?.name || '').trim(),
      spotifyUrl: String(item?.spotifyUrl || '').trim(),
      youtubeUrl: String(item?.youtubeUrl || '').trim(),
      spotifyArtistId: String(item?.spotifyArtistId || '').trim() || undefined,
    })).filter(item => item.artistName && extractSpotifyId(item.spotifyUrl));

    if (!normalized.length) throw new Error('Nenhum artista válido foi encontrado no arquivo.');

    const dedupe = new Map();
    normalized.forEach(item => dedupe.set(item.artistName.toLowerCase(), item));
    const finalList = [...dedupe.values()];

    saveArtists(finalList);
    await loadDashboard();
  }

  function mergeArtistIdsFromResponse(responseArtists) {
    if (!Array.isArray(responseArtists) || !responseArtists.length) return;

    const current = getArtists();
    if (!current.length) return;

    let changed = false;
    const bySpotifyId = new Map();
    responseArtists.forEach(item => {
      const spId = String(item?.spotifyArtistId || '').trim();
      if (spId) bySpotifyId.set(spId, item);
    });

    const merged = current.map(entry => {
      const localSpotifyId = extractSpotifyId(entry.spotifyUrl) || String(entry.spotifyArtistId || '').trim();
      const hit = bySpotifyId.get(localSpotifyId);
      if (!hit) return entry;

      const nextSpotifyArtistId = String(hit.spotifyArtistId || localSpotifyId || '').trim() || undefined;

      if (entry.spotifyArtistId !== nextSpotifyArtistId) {
        changed = true;
        return {
          ...entry,
          spotifyArtistId: nextSpotifyArtistId,
        };
      }

      return entry;
    });

    if (changed) saveArtists(merged, { sync: false });
  }

  const fmtN = new Intl.NumberFormat('pt-BR', { notation: 'compact', maximumFractionDigits: 1 });
  function compact(v) {
    const n = Number(v);
    if (v === null || v === undefined || v === '' || Number.isNaN(n)) return '--';
    return fmtN.format(n);
  }
  function delta(val) {
    const n = Number(val);
    if (val === null || val === undefined || val === '' || Number.isNaN(n)) return '';
    if (n > 0) return `<span class="delta up">▲${n.toFixed(1)}%</span>`;
    if (n < 0) return `<span class="delta dn">▼${Math.abs(n).toFixed(1)}%</span>`;
    return '<span class="delta neu">→0%</span>';
  }
  function fmtDate(v) {
    if (!v) return '';
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? '' : d.toLocaleDateString('pt-BR');
  }

  function openDrawer(w) {
    backdrop.classList.remove('hidden');
    addDrawer.classList.add('hidden');
    manageDrawer.classList.add('hidden');
    artsDrawer.classList.add('hidden');
    w.classList.remove('hidden');
  }

  function closeDrawers() {
    backdrop.classList.add('hidden');
    addDrawer.classList.add('hidden');
    manageDrawer.classList.add('hidden');
    artsDrawer.classList.add('hidden');
    formError.classList.add('hidden');
    formError.textContent = '';
  }

  function renderSavedArtists() {
    const list = qs('savedArtistsList');
    const items = getArtists();
    if (!items.length) {
      list.innerHTML = '<div class="empty">Nenhum artista cadastrado ainda.</div>';
      return;
    }

    list.innerHTML = items.map((item, i) => `
      <div class="saved-item">
        <div>
          <h4>${esc(item.artistName)}</h4>
          <p>${esc(item.spotifyUrl)}</p>
          <p>${esc(item.youtubeUrl || '--')}</p>
        </div>
        <button class="btn dark sm" data-rm="${i}">Excluir</button>
      </div>
    `).join('');

    list.querySelectorAll('[data-rm]').forEach(btn => {
      btn.addEventListener('click', () => {
        saveArtists(getArtists().filter((_, i) => i !== Number(btn.dataset.rm)));
        loadDashboard();
      });
    });
  }

  function renderTrafficAlerts(alerts) {
    const section = qs('trafficSection');
    const list = qs('trafficList');
    const badge = qs('trafficCount');

    if (!alerts || !alerts.length) {
      section.classList.add('hidden');
      return;
    }

    section.classList.remove('hidden');
    badge.textContent = String(alerts.length);
    list.innerHTML = alerts.map(a => `
      <a class="traffic-card" href="${esc(a.videoUrl || '#')}" target="_blank" rel="noopener">
        <img class="traffic-thumb" src="${esc(a.thumbnail || '')}" alt="${esc(a.videoTitle || 'Video')}" loading="lazy">
        <div class="traffic-info">
          <div class="traffic-artist">${esc(a.artistName || 'Artista')}</div>
          <div class="traffic-title">${esc(a.videoTitle || 'Video')}</div>
          <div class="traffic-meta">${fmtDate(a.publishedAt)} · ${Number(a.daysOld) || 0}d atrás</div>
        </div>
        <div class="traffic-cta">
          <span class="cta-label">Solicitar tráfego</span>
          <div class="cta-views">${compact(a.views)} Visualizações</div>
        </div>
      </a>
    `).join('');
  }

  function singleRow(s) {
    const hasPlays = s.plays !== null && s.plays !== undefined;
    const playsLabel = hasPlays ? compact(s.plays) : '--';
    return `
      <div class="media-row">
        <a href="${esc(s.spotifyUrl || '#')}" target="_blank" rel="noopener" title="Abrir no Spotify">
          <img class="media-thumb" src="${esc(s.coverUrl || '')}" alt="${esc(s.title || 'Single')}" loading="lazy">
        </a>
        <div class="media-info">
          <div class="media-title-line">
            <div class="media-title"><a href="${esc(s.spotifyUrl || '#')}" target="_blank" rel="noopener">${esc(s.title || 'Single')}</a></div>
            <span class="single-pill plays">${esc(playsLabel)} plays</span>
          </div>
          <div class="media-date">${fmtDate(s.releaseDate) || 'sem data'}</div>
        </div>
        <div class="media-stat">
          ${hasPlays
            ? `<div class="media-num">${compact(s.plays)}</div><div class="media-lbl">Plays</div>`
            : '<div class="no-data">--</div>'}
        </div>
      </div>
    `;
  }

  function videoRow(v) {
    return `
      <div class="media-row">
        <a href="${esc(v.youtubeUrl || '#')}" target="_blank" rel="noopener" title="Abrir no YouTube">
          <img class="media-thumb yt-thumb" src="${esc(v.thumbnail || '')}" alt="${esc(v.title || 'Video')}" loading="lazy">
        </a>
        <div class="media-info">
          <div class="media-title"><a href="${esc(v.youtubeUrl || '#')}" target="_blank" rel="noopener">${esc(v.title || 'Video')}</a></div>
          <div class="media-date">${fmtDate(v.publishedAt) || 'sem data'}${v.daysOld < 999 ? ` · ${v.daysOld}d` : ''}</div>
          ${v.needsTraffic ? '<span class="traffic-tag">SOLICITAR TRÁFEGO</span>' : ''}
        </div>
        <div class="media-stat">
          <div class="media-num">${compact(v.views)}</div>
          <div class="media-lbl">Visualizações</div>
        </div>
      </div>
    `;
  }

  function artistCard(a) {
    if (a.error) {
      return `
        <article class="card artist-card">
          <div class="error-card"><strong>${esc(a.artistName || 'Artista')}</strong><br>${esc(a.error)}</div>
        </article>
      `;
    }

    const sp = a.spotify || {};
    const yt = a.youtube || {};
    const ml = sp.monthlyListeners || null;
    const popularity = sp.popularity || null;
    const fl = sp.followers || null;
    const singles = Array.isArray(sp.singles) ? sp.singles : [];
    const topTracks = Array.isArray(sp.topTracks) ? sp.topTracks : [];
    const vids = Array.isArray(yt.latestVideos) ? yt.latestVideos : [];

    return `
      <article class="card artist-card">
        <div class="artist-top">
          <img class="artist-avatar" src="${esc(a.imageUrl || yt.channelThumbnail || '')}" alt="${esc(a.artistName || 'Artista')}" loading="lazy">
          <div class="artist-info">
            <div class="artist-name">
              <a href="${esc(a.spotifyArtistUrl || '#')}" target="_blank" rel="noopener">${esc(a.artistName || 'Artista')}</a>
            </div>
            <div class="artist-ts">Atualizado ${new Date(a.fetchedAt).toLocaleString('pt-BR')}</div>
          </div>
        </div>
        <div class="pill-row">
          <div class="tag sp"><svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><circle cx="12" cy="12" r="12" fill="#1DB954"/><path d="M6 9.5c3.8-1.1 8.2-.8 11.8 1" stroke="#0B0B10" stroke-width="1.6" fill="none" stroke-linecap="round"/><path d="M6.8 12.4c3.1-.8 6.7-.6 9.6.8" stroke="#0B0B10" stroke-width="1.5" fill="none" stroke-linecap="round"/><path d="M7.6 15c2.2-.5 4.7-.4 6.8.5" stroke="#0B0B10" stroke-width="1.4" fill="none" stroke-linecap="round"/></svg>Spotify</div>
          <div class="tag yt"><svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><rect x="2" y="6" width="20" height="12" rx="4" fill="#ff0033"/><path d="M10 9.5 15.5 12 10 14.5z" fill="#fff"/></svg>YouTube</div>
        </div>

        <div class="platforms">
          <div class="platform sp-block">
            <div class="plat-head"><svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><circle cx="12" cy="12" r="12" fill="#1DB954"/><path d="M6 9.5c3.8-1.1 8.2-.8 11.8 1" stroke="#0B0B10" stroke-width="1.6" fill="none" stroke-linecap="round"/><path d="M6.8 12.4c3.1-.8 6.7-.6 9.6.8" stroke="#0B0B10" stroke-width="1.5" fill="none" stroke-linecap="round"/><path d="M7.6 15c2.2-.5 4.7-.4 6.8.5" stroke="#0B0B10" stroke-width="1.4" fill="none" stroke-linecap="round"/></svg> Spotify</div>
            <div class="plat-metrics">
              <div class="mc">
                <div class="mc-k">Ouvintes mensais</div>
                <div class="mc-v">${compact(ml?.value)}</div>
                <div class="mc-s">${esc(ml?.source === 'gemini-web' ? 'Estimado via Gemini' : (ml?.source === 'spotify-page' ? 'No spotify' : (ml?.source || ''))) || '&nbsp;'}</div>
              </div>
              <div class="mc">
                <div class="mc-k">Seguidores</div>
                <div class="mc-v">${compact(fl)}</div>
                <div class="mc-s">${popularity !== null ? `Popularidade ${popularity}/100` : 'Canal Spotify'}</div>
              </div>
            </div>
            <div class="media-list">
              ${(singles.length ? singles.map(singleRow).join('') : '').trim() || (topTracks.length ? topTracks.map(t => `
                <div class="media-row">
                  <a href="${esc(t.spotifyUrl || '#')}" target="_blank" rel="noopener" title="Abrir no Spotify">
                    <img class="media-thumb" src="${esc(t.imageUrl || '')}" alt="${esc(t.title || 'Track')}" loading="lazy">
                  </a>
                  <div class="media-info">
                    <div class="media-title"><a href="${esc(t.spotifyUrl || '#')}" target="_blank" rel="noopener">${esc(t.title || 'Track')}</a></div>
                    <div class="media-date">${fmtDate(t.releaseDate) || 'sem data'}</div>
                  </div>
                  <div class="media-stat">
                    <div class="media-num">${t.popularity || '--'}</div><div class="media-lbl">pop.</div>
                  </div>
                </div>
              `).join('') : '<div class="empty">Sem singles/top tracks.</div>')}
            </div>
          </div>

          <div class="platform yt-block">
            <div class="plat-head"><svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><rect x="2" y="6" width="20" height="12" rx="4" fill="#ff0033"/><path d="M10 9.5 15.5 12 10 14.5z" fill="#fff"/></svg> YouTube</div>
            <div class="plat-metrics">
              <div class="mc">
                <div class="mc-k">Inscritos</div>
                <div class="mc-v">${compact(yt.subscribers)}</div>
                <div class="mc-s">${esc(yt.channelTitle || '--')}</div>
              </div>
              <div class="mc">
                <div class="mc-k">Views canal</div>
                <div class="mc-v">${compact(yt.channelViews)}</div>
                <div class="mc-s">Total acumulado</div>
              </div>
            </div>
            <div class="media-list">
              ${vids.length ? vids.map(videoRow).join('') : '<div class="empty">Sem vídeos recentes.</div>'}
            </div>
          </div>
        </div>
      </article>
    `;
  }

  function renderRanking(ranking) {
    const box = qs('ranking');
    if (!ranking.length) {
      box.innerHTML = '<div class="empty">Cadastre artistas para montar o ranking.</div>';
      return;
    }

    box.innerHTML = ranking.map((item, i) => `
      <div class="rank-item">
        <div class="rank-pos">${i + 1}</div>
        <img src="${esc(item.imageUrl || '')}" alt="${esc(item.artistName || 'Artista')}" loading="lazy">
        <div class="rank-meta">
          <h4>${esc(item.artistName || 'Artista')}</h4>
          <p>SP ${item.monthlyListeners ? compact(item.monthlyListeners) : item.spotifyPopularity} · YT ${compact(item.youtubeViews)}</p>
        </div>
      </div>
    `).join('');
  }

  async function loadDashboard(options = {}) {
    const forceRefresh = Boolean(options.forceRefresh);
    const artistsToRefresh = Array.isArray(options.artistsToRefresh) ? options.artistsToRefresh : null;
    let artists = getArtists();
    qs('lastSync').textContent = artists.length ? (forceRefresh ? 'Atualizando...' : 'Carregando...') : 'Nenhum artista';
    renderSavedArtists();

    try {
      artists = await bootstrapArtistsFromCloud();
    } catch {
      // Mantém a lista local se a nuvem estiver indisponível.
    }

    artists = getArtists();

    if (!artists.length) {
      qs('artistGrid').innerHTML = '<div class="empty" style="grid-column:1/-1">Cadastre artistas para iniciar o monitoramento.</div>';
      renderRanking([]);
      qs('trafficSection').classList.add('hidden');
      artSection.classList.add('hidden');
      artList.innerHTML = '';
      return;
    }

    if (forceRefresh) {
      const remaining = getFullRefreshRemainingMs();
      if (remaining > 0) {
        qs('lastSync').textContent = `Atualização geral disponível em ${formatRemainingLong(remaining)}`;
        updateRefreshButtonState();
        return;
      }
    }

    const cacheState = loadMatchingCachedDashboard(artists);

    try {
      let dashboardArtists = artists;
      if (artistsToRefresh?.length) {
        const partial = await fetchDashboardPayload(artistsToRefresh);
        const current = getArtists();
        const bySpotifyId = new Map((partial.artists || []).map(item => [String(item?.spotifyArtistId || '').trim(), item]));
        dashboardArtists = current.map(item => {
          const id = extractSpotifyId(item.spotifyUrl) || String(item.spotifyArtistId || '').trim();
          return bySpotifyId.get(id) || item;
        });
      }

      const liveData = await fetchDashboardPayload(dashboardArtists);
      const data = liveData;

      mergeArtistIdsFromResponse(data.artists || []);
      renderDashboardPayload(data);
      saveCachedDashboard(artists, data);
      if (forceRefresh) setLastFullRefresh(Date.now());
      updateRefreshButtonState();
      const backendLabel = activeBackendUrl === BACKEND_LOCAL_URL ? 'local' : 'produção';
      qs('lastSync').textContent = `Sincronizado ${new Date(data.fetchedAt).toLocaleString('pt-BR')} (${backendLabel})`;
    } catch (err) {
      if (cacheState?.cached?.payload) {
        renderDashboardPayload(cacheState.cached.payload);
        const cacheDate = new Date(cacheState.cached.savedAt).toLocaleString('pt-BR');
        qs('lastSync').textContent = `Sem crédito da API. Exibindo último snapshot ${cacheDate}`;
        return;
      }

      qs('artistGrid').innerHTML = `<div class="empty" style="grid-column:1/-1">${esc(err.message)}</div>`;
      qs('lastSync').textContent = 'Falha na sincronização';
    }
  }

  function saveArtist() {
    const name = qs('artistName').value.trim();
    const spUrl = qs('spotifyUrl').value.trim();
    const ytUrl = qs('youtubeUrl').value.trim();

    const err = msg => {
      formError.textContent = msg;
      formError.classList.remove('hidden');
    };

    if (!name) return err('Digite o nome do artista.');
    if (!extractSpotifyId(spUrl)) return err('Cole um link válido do artista no Spotify.');
    if (ytUrl && !isYouTubeChannel(ytUrl)) return err('Cole um link válido de canal no YouTube.');

    const items = getArtists();
    if (items.some(i => i.artistName.toLowerCase() === name.toLowerCase())) return err('Artista já cadastrado.');

    const newArtist = { artistName: name, spotifyUrl: spUrl, youtubeUrl: ytUrl };
    items.push(newArtist);
    saveArtists(items);

    qs('artistName').value = '';
    qs('spotifyUrl').value = '';
    qs('youtubeUrl').value = '';
    closeDrawers();
    loadDashboard({ artistsToRefresh: [newArtist] });
  }

  function openMenuDrawer() {
    menuBackdrop.classList.remove('hidden');
    menuDrawer.classList.remove('hidden');
  }

  function closeMenuDrawer() {
    menuBackdrop.classList.add('hidden');
    menuDrawer.classList.add('hidden');
  }

  function bindCoreEventListeners() {
    qs('addBtn').addEventListener('click', () => openDrawer(addDrawer));
    qs('refreshBtn').addEventListener('click', () => {
      if (!confirmFullRefresh()) return;
      loadDashboard({ forceRefresh: true });
    });
    qs('manageBtn').addEventListener('click', () => openDrawer(manageDrawer));
    artBtn.addEventListener('click', () => openArtsDrawer());
    qs('closeAdd').addEventListener('click', closeDrawers);
    qs('closeManage').addEventListener('click', closeDrawers);
    closeArtes.addEventListener('click', closeDrawers);
    backdrop.addEventListener('click', closeDrawers);
    qs('saveArtist').addEventListener('click', saveArtist);

    qs('exportBtn').addEventListener('click', exportArtists);
    qs('importBtn').addEventListener('click', () => qs('importInput').click());
    qs('importInput').addEventListener('change', async (e) => {
      const file = e.target.files?.[0];
      e.target.value = '';
      if (!file) return;

      try {
        await importArtists(file);
        alert('Lista importada com sucesso.');
      } catch (error) {
        alert(error.message || 'Falha ao importar lista.');
      }
    });

    ['artistName', 'spotifyUrl', 'youtubeUrl'].forEach(id =>
      qs(id).addEventListener('keydown', e => {
        if (e.key === 'Enter') saveArtist();
      })
    );
  }

  function bindMenuEventListeners() {
    menuBtn.addEventListener('click', openMenuDrawer);
    menuBackdrop.addEventListener('click', closeMenuDrawer);
    menuCloseBtn.addEventListener('click', closeMenuDrawer);
    menuAddBtn.addEventListener('click', () => { closeMenuDrawer(); openDrawer(addDrawer); });
    menuArtBtn.addEventListener('click', () => { closeMenuDrawer(); openArtsDrawer(); });
    menuManageBtn.addEventListener('click', () => { closeMenuDrawer(); openDrawer(manageDrawer); });
    menuRefreshBtn.addEventListener('click', () => { closeMenuDrawer(); if (confirmFullRefresh()) loadDashboard({ forceRefresh: true }); });
    menuExportBtn.addEventListener('click', () => { closeMenuDrawer(); exportArtists(); });
    menuImportBtn.addEventListener('click', () => { closeMenuDrawer(); qs('importInput').click(); });
  }

  function bindArtsEventListeners() {
    artTabs.querySelectorAll('.art-tab').forEach(tab => {
      tab.addEventListener('click', () => setArtTab(tab.dataset.taskTab));
    });

    toggleManualTask.addEventListener('click', () => {
      manualTaskForm.classList.toggle('visible');
      toggleManualTask.textContent = manualTaskForm.classList.contains('visible') ? 'Fechar' : 'Abrir';
    });

    cancelManualTask.addEventListener('click', closeArtsComposer);
    createManualTaskBtn.addEventListener('click', submitManualTask);
  }

  function startUiTimers() {
    setInterval(updateRefreshButtonState, 30000);
  }

  async function bootstrapApp() {
    const bootstrapped = localStorage.getItem(CLOUD_SYNC_BOOTSTRAP_KEY) === '1';
    if (!bootstrapped || getArtists().length === 0) {
      await bootstrapArtistsFromCloud();
    } else {
      queueCloudSync(getArtists());
    }

    renderSavedArtists();
    loadDashboard();
  }

  function initApp() {
    bindCoreEventListeners();
    bindMenuEventListeners();
    bindArtsEventListeners();
    startUiTimers();
    updateRefreshButtonState();
    renderArtCounters();
    syncManualArtistList();
    bootstrapApp();
  }

  initApp();
})();
