const socket = io();

let currentEntity = null;

// expose subscribe globally and attach to button
function subscribe(){
  currentEntity = document.getElementById('userId').value;
  document.getElementById('features_grid').innerHTML = '';
  fetchExisting(currentEntity);
  console.log('Subscribed to', currentEntity);
}
window.subscribe = subscribe;

document.addEventListener('DOMContentLoaded', () => {
  const btn = document.getElementById('subscribeBtn');
  if (btn) btn.addEventListener('click', subscribe);
});
// When subscribing, fetch current cached features from backend
async function fetchExisting(entity){
  try{
    const res = await fetch(`/entity/${encodeURIComponent(entity)}`);
    if (!res.ok) return;
    const data = await res.json();
    // data is a map feature_name-> {value, computed_at}
    Object.entries(data).forEach(([fname, info]) => {
      makeCard(entity, fname, info.value, info.computed_at);
    });
  }catch(e){console.warn('fetchExisting', e)}
}

function makeCard(id, feature, value, computed_at){
  const cardId = `card_${feature.replace(/[^a-z0-9_]/gi,'_')}`;
  let card = document.getElementById(cardId);
  if (!card){
    card = document.createElement('div');
    card.className = 'feature-card';
    card.id = cardId;
    card.innerHTML = `<h3>${feature}</h3><div class="feature-value metric-small">${value}</div><div class="feature-meta">Last computed: <span class="computed_at">${computed_at}</span> — freshness: <span class="freshness">-</span>s</div>`;
    document.getElementById('features_grid').prepend(card);
  } else {
    card.querySelector('.feature-value').innerText = value;
    card.querySelector('.computed_at').innerText = computed_at;
  }
  // update freshness
  try{
    const ts = new Date(computed_at).getTime();
    const now = Date.now();
    const secs = Math.max(0, ((now - ts)/1000).toFixed(1));
    card.querySelector('.freshness').innerText = secs;
  }catch(e){console.warn(e)}
}

socket.on('feature_update', (data) => {
  if (!currentEntity) return;
  if (data.entity_id !== currentEntity) return;
  makeCard(data.entity_id, data.feature_name, data.feature_value, data.computed_at);
});

socket.on('metric_update', (m) => {
  try{
    if (m.metric_name === 'watermark_lag_seconds'){
      document.getElementById('watermark_lag').innerText = Number(m.value).toFixed(1);
    }
    if (m.metric_name === 'late_events_dropped_total'){
      document.getElementById('late_events').innerText = parseInt(m.value);
    }
  }catch(e){console.warn(e)}
});

// periodic status poll
async function pollStatus(){
  try{
    const res = await fetch('/status');
    const j = await res.json();
    document.getElementById('kafka_status').innerText = j.kafka.ok ? 'OK' : `Error: ${j.kafka.error || 'unknown'}`;
    document.getElementById('flink_status').innerText = j.flink.ok ? 'OK' : `Error: ${j.flink.error || 'unknown'}`;
  }catch(e){
    document.getElementById('kafka_status').innerText = 'Error';
    document.getElementById('flink_status').innerText = 'Error';
  }
}

setInterval(pollStatus, 5000);
pollStatus();

socket.on('connect', () => {
  console.log('socket connected');
  const el = document.getElementById('conn_status'); if (el) el.innerText = 'Socket: connected';
  stopFallbackPolling();
});
socket.on('disconnect', () => {
  console.log('socket disconnected');
  const el = document.getElementById('conn_status'); if (el) el.innerText = 'Socket: disconnected';
  startFallbackPolling();
});

// Fallback polling: if Socket.IO is unavailable, poll the entity endpoint every 5s
let _fallbackInterval = null;
function startFallbackPolling(){
  if (_fallbackInterval) return;
  _fallbackInterval = setInterval(() => {
    if (currentEntity) fetchExisting(currentEntity);
  }, 5000);
  console.warn('Started fallback polling for entity updates');
}
function stopFallbackPolling(){
  if (_fallbackInterval){
    clearInterval(_fallbackInterval);
    _fallbackInterval = null;
    console.warn('Stopped fallback polling');
  }
}
