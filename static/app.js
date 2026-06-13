const API = 'https://richard-scheduler.duckdns.org';

// --- Tab switching ---
function switchTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.getElementById(`tab-${name}`).classList.add('active');
  event.target.classList.add('active');

  if (name === 'dlq') loadDLQ();
}

// --- Payload templates ---
const TEMPLATES = {
  send_email: { to: 'test@example.com', subject: 'Hello', body: 'This is a test email.' },
  webhook_delivery: { url: 'https://webhook.site/test', data: { event: 'test' } },
  log_processing: { log_entry: 'User logged in from IP 192.168.1.1', severity: 'info' },
};

function updatePayloadTemplate() {
  const type = document.getElementById('job-type').value;
  document.getElementById('job-payload').value = JSON.stringify(TEMPLATES[type], null, 2);
}

// Set default on load
window.addEventListener('DOMContentLoaded', () => {
  updatePayloadTemplate();
  loadStats();
  loadJobs();
  startSSE();
});

// --- SSE Live Updates ---
function startSSE() {
    const source = new EventSource(`${API}/events`);
  
    source.onmessage = (e) => {
      const filter = document.getElementById('status-filter').value;
      if (!filter) {
        const jobs = JSON.parse(e.data);
        renderJobsTable(jobs);
      }
      loadStats();
    };
  
    source.onerror = () => {
      document.getElementById('connection-status').textContent = '● Disconnected';
      document.getElementById('connection-status').className = 'badge badge-danger';
    };
  }

// --- Stats ---
async function loadStats() {
  try {
    const res = await fetch(`${API}/jobs/stats`);
    const data = await res.json();
    document.getElementById('stat-pending').textContent = data.pending ?? 0;
    document.getElementById('stat-processing').textContent = data.processing ?? 0;
    document.getElementById('stat-completed').textContent = data.completed ?? 0;
    document.getElementById('stat-failed').textContent = data.failed ?? 0;
    document.getElementById('stat-cancelled').textContent = data.cancelled ?? 0;
    document.getElementById('stat-queue').textContent = data.queue_size ?? 0;
  } catch (e) {
    console.error('Stats error:', e);
  }
}

// Refresh stats every 5 seconds
setInterval(loadStats, 5000);

// --- Jobs Table ---
async function loadJobs() {
  const status = document.getElementById('status-filter').value;
  const url = status ? `${API}/jobs/?status=${status}` : `${API}/jobs/`;
  try {
    const res = await fetch(url);
    const jobs = await res.json();
    renderJobsTable(jobs);
  } catch (e) {
    document.getElementById('jobs-table-body').innerHTML =
      '<tr><td colspan="9" class="empty">Failed to load jobs</td></tr>';
  }
}

function filterJobs() { 
    loadJobs();
  }

function renderJobsTable(jobs) {
  const tbody = document.getElementById('jobs-table-body');
  if (!jobs.length) {
    tbody.innerHTML = '<tr><td colspan="9" class="empty">No jobs found</td></tr>';
    return;
  }

  tbody.innerHTML = jobs.map(job => `
    <tr>
      <td title="${job.id}">${job.id.slice(0, 8)}...</td>
      <td>${job.type}</td>
      <td class="priority-${job.priority}">${job.priority === 1 ? 'High' : job.priority === 2 ? 'Medium' : 'Low'}</td>
      <td><span class="status-badge status-${job.status}">${job.status}</span></td>
      <td>${job.retry_count} / ${job.max_retries ?? 3}</td>
      <td>${job.scheduled_at ? new Date(job.scheduled_at).toLocaleString() : '—'}</td>
      <td>${job.interval ?? '—'}</td>
      <td>${new Date(job.created_at).toLocaleString()}</td>
      <td>
        ${job.status === 'pending' || job.status === 'processing'
          ? `<button class="btn-cancel" onclick="cancelJob('${job.id}')">Cancel</button>`
          : '—'}
      </td>
    </tr>
  `).join('');
}

// --- Cancel Job ---
async function cancelJob(jobId) {
  if (!confirm('Cancel this job?')) return;
  try {
    await fetch(`${API}/jobs/${jobId}/cancel`, { method: 'PATCH' });
    loadJobs();
    loadStats();
  } catch (e) {
    alert('Failed to cancel job');
  }
}

// --- Create Job ---
async function createJob() {
  const errorEl = document.getElementById('create-error');
  const successEl = document.getElementById('create-success');
  errorEl.style.display = 'none';
  successEl.style.display = 'none';

  const type = document.getElementById('job-type').value;
  const priority = parseInt(document.getElementById('job-priority').value);
  const scheduledAt = document.getElementById('job-scheduled-at').value;
  const interval = document.getElementById('job-interval').value;
  const isRecurring = document.getElementById('job-recurring').checked;
  const depsRaw = document.getElementById('job-dependencies').value;

  let payload;
  try {
    payload = JSON.parse(document.getElementById('job-payload').value);
  } catch {
    errorEl.textContent = 'Payload is not valid JSON';
    errorEl.style.display = 'block';
    return;
  }

  const dependencies = depsRaw
    ? depsRaw.split(',').map(s => s.trim()).filter(Boolean)
    : [];

  const body = {
    type,
    payload,
    priority,
    interval: interval || null,
    is_recurring: isRecurring,
    dependencies,
  };

  if (scheduledAt) {
    body.scheduled_at = new Date(scheduledAt).toISOString();
  }

  try {
    const res = await fetch(`${API}/jobs/`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    const data = await res.json();

    if (!res.ok) {
      errorEl.textContent = data.detail || 'Failed to create job';
      errorEl.style.display = 'block';
      return;
    }

    successEl.textContent = `Job created successfully! ID: ${data.id}`;
    successEl.style.display = 'block';
    loadStats();
    setTimeout(() => { successEl.style.display = 'none'; }, 4000);

  } catch (e) {
    errorEl.textContent = 'Network error — is the server running?';
    errorEl.style.display = 'block';
  }
}

// --- DLQ ---
async function loadDLQ() {
  try {
    const [dlqRes, statsRes] = await Promise.all([
      fetch(`${API}/dlq/`),
      fetch(`${API}/dlq/stats`),
    ]);

    const entries = await dlqRes.json();
    const stats = await statsRes.json();

    const alertEl = document.getElementById('dlq-alert');
    alertEl.style.display = stats.threshold_exceeded ? 'inline-block' : 'none';

    const tbody = document.getElementById('dlq-table-body');
    if (!entries.length) {
      tbody.innerHTML = '<tr><td colspan="8" class="empty">No jobs in DLQ</td></tr>';
      return;
    }

    tbody.innerHTML = entries.map(e => `
      <tr>
        <td title="${e.id}">${e.id.slice(0, 8)}...</td>
        <td title="${e.job_id}">${e.job_id.slice(0, 8)}...</td>
        <td>${e.type}</td>
        <td class="priority-${e.priority}">${e.priority === 1 ? 'High' : e.priority === 2 ? 'Medium' : 'Low'}</td>
        <td>${e.retry_count}</td>
        <td title="${e.error}">${e.error ? e.error.slice(0, 50) + '...' : '—'}</td>
        <td>${new Date(e.moved_at).toLocaleString()}</td>
        <td>
          <button class="btn-primary" style="padding:6px 12px;font-size:0.8rem" onclick="retryDLQ('${e.id}')">Retry</button>
        </td>
      </tr>
    `).join('');

  } catch (err) {
    document.getElementById('dlq-table-body').innerHTML =
      '<tr><td colspan="8" class="empty">Failed to load DLQ</td></tr>';
  }
}

// --- Retry DLQ job ---
async function retryDLQ(dlqId) {
  if (!confirm('Retry this job?')) return;
  try {
    await fetch(`${API}/dlq/${dlqId}/retry`, { method: 'POST' });
    loadDLQ();
    loadStats();
  } catch (e) {
    alert('Failed to retry job');
  }
}