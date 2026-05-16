const socket = io();

let currentUser = null;

function subscribe(){
  currentUser = document.getElementById('userId').value;
  document.getElementById('features').innerText = 'Subscribed to ' + currentUser + '\n(Features will appear here)';
}

socket.on('feature_update', (data) => {
  if (!currentUser) return;
  if (data.entity_id !== currentUser) return;
  const el = document.getElementById('features');
  const line = `${data.feature_name}: ${data.feature_value} (at ${data.computed_at})\n`;
  el.innerText = line + el.innerText;
});

socket.on('metric_update', (m) => {
  try{
    if (m.metric_name === 'watermark_lag_seconds'){
      document.getElementById('watermark_lag').innerText = m.value.toFixed(1);
    }
    if (m.metric_name === 'late_events_dropped_total'){
      document.getElementById('late_events').innerText = parseInt(m.value);
    }
  }catch(e){console.warn(e)}
});
