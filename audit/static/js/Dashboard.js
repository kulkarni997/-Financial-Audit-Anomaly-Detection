/* ============================================================
   DASHBOARD.JS — Chart Init, KPI Cards, Live Feed, Polling
   ============================================================ */

// ── Mock data (replace with real api.get() calls) ────────────
// In production: const summary = await api.get('/dashboard/summary/');
const MOCK_SUMMARY = {
  total_transactions: 4821,
  flagged_count: 143,
  critical_count: 12,
  avg_risk_score: 34.7,
  risk_breakdown: { low: 68, medium: 45, high: 18, critical: 12 },
  heatmap_data: [
    { dept:'Finance',   category:'Travel',    score:72 },
    { dept:'Finance',   category:'Software',  score:45 },
    { dept:'Finance',   category:'Vendors',   score:88 },
    { dept:'Finance',   category:'Payroll',   score:12 },
    { dept:'HR',        category:'Travel',    score:31 },
    { dept:'HR',        category:'Software',  score:19 },
    { dept:'HR',        category:'Vendors',   score:55 },
    { dept:'HR',        category:'Payroll',   score:62 },
    { dept:'IT',        category:'Travel',    score:24 },
    { dept:'IT',        category:'Software',  score:91 },
    { dept:'IT',        category:'Vendors',   score:47 },
    { dept:'IT',        category:'Payroll',   score:8 },
    { dept:'Ops',       category:'Travel',    score:66 },
    { dept:'Ops',       category:'Software',  score:38 },
    { dept:'Ops',       category:'Vendors',   score:79 },
    { dept:'Ops',       category:'Payroll',   score:21 },
    { dept:'Marketing', category:'Travel',    score:53 },
    { dept:'Marketing', category:'Software',  score:29 },
    { dept:'Marketing', category:'Vendors',   score:84 },
    { dept:'Marketing', category:'Payroll',   score:15 },
  ],
  top_vendors: [
    { name:'Acme Corp',      score:87, count:14 },
    { name:'TechSoft Ltd',   score:79, count:9  },
    { name:'GlobalTrade Co', score:71, count:22 },
    { name:'Pinnacle Svcs',  score:65, count:6  },
    { name:'SkyNet Systems', score:58, count:11 },
    { name:'RedLeaf Inc',    score:44, count:8  },
  ],
};

const MOCK_TRENDS = {
  labels:  ['Nov 1','Nov 3','Nov 5','Nov 7','Nov 9','Nov 11','Nov 13','Nov 15','Nov 17','Nov 19','Nov 21','Nov 23','Nov 25','Nov 27','Nov 29'],
  flagged: [4, 7, 3, 11, 6, 9, 5, 14, 8, 12, 3, 7, 10, 6, 9],
  total:   [120,134,98,145,111,160,102,178,133,155,99,141,162,118,143],
};

const MOCK_FEED = [
  { vendor:'Acme Corp',      amount:15400, category:'Vendors',  dept:'Finance',   score:91, level:'critical', time:'2m ago' },
  { vendor:'TechSoft Ltd',   amount:8750,  category:'Software', dept:'IT',        score:79, level:'high',     time:'14m ago' },
  { vendor:'GlobalTrade Co', amount:22000, category:'Vendors',  dept:'Ops',       score:71, level:'high',     time:'31m ago' },
  { vendor:'Pinnacle Svcs',  amount:3200,  category:'Travel',   dept:'Marketing', score:58, level:'medium',   time:'1h ago' },
  { vendor:'SkyNet Systems', amount:5000,  category:'Software', dept:'IT',        score:44, level:'medium',   time:'2h ago' },
  { vendor:'NovaTech',       amount:1800,  category:'Payroll',  dept:'HR',        score:38, level:'medium',   time:'3h ago' },
];

// ── Color helpers ─────────────────────────────────────────────
const RISK_COLORS = {
  low:      { line: '#22c55e', fill: 'rgba(34,197,94,.12)'  },
  medium:   { line: '#f59e0b', fill: 'rgba(245,158,11,.12)' },
  high:     { line: '#f97316', fill: 'rgba(249,115,22,.12)' },
  critical: { line: '#ef4444', fill: 'rgba(239,68,68,.12)'  },
};

function riskColor(score) {
  if (score >= 76) return RISK_COLORS.critical;
  if (score >= 51) return RISK_COLORS.high;
  if (score >= 26) return RISK_COLORS.medium;
  return RISK_COLORS.low;
}

function riskLevel(score) {
  if (score >= 76) return 'critical';
  if (score >= 51) return 'high';
  if (score >= 26) return 'medium';
  return 'low';
}

// ── Chart.js global defaults ──────────────────────────────────
Chart.defaults.color           = '#8a9bbf';
Chart.defaults.font.family     = "'DM Sans', sans-serif";
Chart.defaults.font.size       = 12;
Chart.defaults.plugins.legend.display = false;

// ── 1. KPI CARDS ──────────────────────────────────────────────
function renderKPICards(data) {
  document.getElementById("total-transactions").textContent = data.total_transactions;
  document.getElementById("flagged-count").textContent = data.flagged_count;
  document.getElementById("critical-count").textContent = data.critical_count;
  document.getElementById("avg-risk").textContent = data.avg_risk_score;
}

// ── 2. SPARKLINES (mini canvas per KPI card) ──────────────────
function renderSparkline(canvasId, dataArr, color) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return;
  new Chart(ctx, {
    type: 'line',
    data: {
      labels: dataArr.map((_, i) => i),
      datasets: [{ data: dataArr, borderColor: color, borderWidth: 1.5,
        fill: true,
        backgroundColor: color.replace('1)', '.12)').replace('#', 'rgba(').replace(/(.{2})(.{2})(.{2})/, (_, r, g, b) => ''),
        tension: .4, pointRadius: 0 }]
    },
    options: { responsive: true, maintainAspectRatio: false,
      scales: { x: { display:false }, y: { display:false } },
      plugins: { tooltip: { enabled:false } },
      animation: { duration:800 }
    }
  });
}

// ── 3. TREND LINE CHART ───────────────────────────────────────
function renderTrendChart(data) {
  const ctx = document.getElementById('trend-chart');
  if (!ctx) return;

  return new Chart(ctx, {
    type: 'line',
    data: {
      labels: data.labels,
      datasets: [
        {
          label: 'Flagged',
          data: data.flagged,
          borderColor: '#ef4444',
          backgroundColor: 'rgba(239,68,68,.08)',
          borderWidth: 2,
          fill: true,
          tension: .4,
          pointRadius: 3,
          pointBackgroundColor: '#ef4444',
          pointBorderColor: '#0c1528',
          pointBorderWidth: 2,
        },
        {
          label: 'Total',
          data: data.total,
          borderColor: '#3b82f6',
          backgroundColor: 'rgba(59,130,246,.06)',
          borderWidth: 1.5,
          fill: true,
          tension: .4,
          pointRadius: 0,
          borderDash: [4, 3],
        }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      scales: {
        x: {
          grid: { color: 'rgba(59,130,246,.06)' },
          ticks: { maxTicksLimit: 8 },
        },
        y: {
          grid: { color: 'rgba(59,130,246,.06)' },
          beginAtZero: true,
        }
      },
      plugins: {
        legend: { display: true, position: 'top',
          labels: { usePointStyle: true, pointStyle: 'circle', boxWidth: 8, padding: 16 }
        },
        tooltip: {
          backgroundColor: '#111e35',
          borderColor: 'rgba(59,130,246,.3)',
          borderWidth: 1,
          padding: 10,
          callbacks: {
            label: ctx => ` ${ctx.dataset.label}: ${ctx.parsed.y}`
          }
        }
      },
      animation: { duration: 900, easing: 'easeOutQuart' }
    }
  });
}

// ── 4. RISK DOUGHNUT ──────────────────────────────────────────
function renderDoughnut(data) {
  const ctx = document.getElementById('donut-chart');
  if (!ctx) return;

  const rb = data.risk_breakdown;
  const total = rb.low + rb.medium + rb.high + rb.critical;

  // Update center text
  const center = document.getElementById('donut-total');
  if (center) center.textContent = total;

  // Update legend values
  ['low','medium','high','critical'].forEach(l => {
    const el = document.getElementById(`legend-${l}`);
    if (el) el.textContent = rb[l];
  });

  return new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: ['Low','Medium','High','Critical'],
      datasets: [{
        data: [rb.low, rb.medium, rb.high, rb.critical],
        backgroundColor: ['#22c55e','#f59e0b','#f97316','#ef4444'],
        borderColor: '#0c1528',
        borderWidth: 3,
        hoverOffset: 6,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: true,
      cutout: '72%',
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#111e35',
          borderColor: 'rgba(59,130,246,.3)',
          borderWidth: 1,
          callbacks: {
            label: ctx => ` ${ctx.label}: ${ctx.parsed} (${Math.round(ctx.parsed/total*100)}%)`
          }
        }
      },
      animation: { animateRotate: true, duration: 900 }
    }
  });
}

// ── 5. TOP VENDORS ────────────────────────────────────────────
function renderVendors(vendors) {
  const container = document.getElementById('vendor-list');
  if (!container) return;
  container.innerHTML = '';
  vendors.forEach(v => {
    const level = riskLevel(v.score);
    const color = RISK_COLORS[level].line;
    container.insertAdjacentHTML('beforeend', `
      <div class="vendor-row">
        <div class="vendor-row-top">
          <span class="vendor-name">${v.name}</span>
          <span class="vendor-score" style="color:${color}">${v.score}</span>
        </div>
        <div class="vendor-bar-track">
          <div class="vendor-bar-fill" style="width:${v.score}%;background:${color}"></div>
        </div>
      </div>
    `);
  });
}

// ── 6. LIVE FEED ──────────────────────────────────────────────
function renderFeed(items) {
  const list = document.getElementById('feed-list');
  if (!list) return;
  list.innerHTML = '';
  items.forEach(item => {
    list.insertAdjacentHTML('beforeend', `
      <li class="feed-item ${item.level}">
        <div class="feed-score ${item.level}">${item.score}</div>
        <div class="feed-body">
          <div class="feed-vendor">${item.vendor}</div>
          <div class="feed-meta">${item.category} · ${item.dept} · ${item.time}</div>
        </div>
        <div class="feed-amount">$${item.amount.toLocaleString()}</div>
      </li>
    `);
  });
}

// ── 7. D3 HEATMAP ────────────────────────────────────────────
function renderHeatmap(data) {
  const container = document.getElementById('heatmap-container');
  if (!container || !window.d3) return;

  container.innerHTML = '';

  const depts      = [...new Set(data.map(d => d.dept))];
  const categories = [...new Set(data.map(d => d.category))];

  const cellSize = 48;
  const marginLeft = 72;
  const marginTop  = 40;
  const W = marginLeft + categories.length * cellSize + 10;
  const H = marginTop  + depts.length * cellSize + 10;

  const svg = d3.select('#heatmap-container')
    .append('svg')
    .attr('width', '100%')
    .attr('viewBox', `0 0 ${W} ${H}`)
    .style('overflow', 'visible');

  // Color scale: 0=green → 100=red through amber
  const colorScale = d3.scaleLinear()
    .domain([0, 35, 65, 100])
    .range(['#22c55e', '#f59e0b', '#f97316', '#ef4444']);

  // X axis (categories)
  svg.selectAll('.cat-label')
    .data(categories)
    .enter().append('text')
    .attr('x', (d, i) => marginLeft + i * cellSize + cellSize / 2)
    .attr('y', marginTop - 10)
    .attr('text-anchor', 'middle')
    .attr('font-size', 10)
    .attr('fill', '#8a9bbf')
    .text(d => d);

  // Y axis (depts)
  svg.selectAll('.dept-label')
    .data(depts)
    .enter().append('text')
    .attr('x', marginLeft - 8)
    .attr('y', (d, i) => marginTop + i * cellSize + cellSize / 2 + 4)
    .attr('text-anchor', 'end')
    .attr('font-size', 10)
    .attr('fill', '#8a9bbf')
    .text(d => d);

  // Cells
  const cells = svg.selectAll('.cell')
    .data(data)
    .enter().append('g')
    .attr('transform', d => {
      const x = marginLeft + categories.indexOf(d.category) * cellSize;
      const y = marginTop  + depts.indexOf(d.dept) * cellSize;
      return `translate(${x},${y})`;
    });

  cells.append('rect')
    .attr('width', cellSize - 3)
    .attr('height', cellSize - 3)
    .attr('rx', 5)
    .attr('fill', d => colorScale(d.score))
    .attr('opacity', 0)
    .transition().duration(600).delay((_, i) => i * 20)
    .attr('opacity', .85);

  cells.append('text')
    .attr('x', (cellSize - 3) / 2)
    .attr('y', (cellSize - 3) / 2 + 4)
    .attr('text-anchor', 'middle')
    .attr('font-size', 11)
    .attr('font-family', "'DM Mono', monospace")
    .attr('font-weight', '600')
    .attr('fill', d => d.score > 50 ? 'rgba(255,255,255,.9)' : 'rgba(0,0,0,.7)')
    .attr('opacity', 0)
    .text(d => d.score)
    .transition().duration(400).delay((_, i) => i * 20 + 200)
    .attr('opacity', 1);

  // Tooltip
  cells.append('title').text(d => `${d.dept} × ${d.category}: Risk ${d.score}`);
}

// ── 8. CHART PERIOD CONTROLS ──────────────────────────────────
let trendChartInstance = null;

function initPeriodControls() {
  document.querySelectorAll('.chart-pill').forEach(pill => {
    pill.addEventListener('click', () => {
      pill.closest('.chart-controls').querySelectorAll('.chart-pill')
        .forEach(p => p.classList.remove('active'));
      pill.classList.add('active');
      // In production: re-fetch with ?days=pill.dataset.days
    });
  });
}

// ── INIT ──────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {
  // Guard: redirect if not logged in
  // if (!Auth.isLoggedIn()) { window.location.href = '/login/'; return; }

  // In production replace mocks with:
  // const [summary, trends] = await Promise.all([
  //   api.get('/dashboard/summary/'),
  //   api.get('/dashboard/trends/?days=30')
  // ]);

  const summary = await api.get("dashboard/summary/");
  renderKPICards(summary);
  trendChartInstance = renderTrendChart(MOCK_TRENDS);
  renderDoughnut(summary);
  renderVendors(summary.top_vendors || []);
  renderFeed(MOCK_FEED);
  renderVendors(summary.top_vendors || []);
  initPeriodControls();

  // Sparklines
  renderSparkline('spark-total',    [98,120,134,145,111,160,102,178,133,143], '#3b82f6');
  renderSparkline('spark-flagged',  [7,9,11,8,14,10,12,9,11,14],             '#f59e0b');
  renderSparkline('spark-critical', [1,2,1,3,2,4,3,2,4,3],                  '#ef4444');
  renderSparkline('spark-score',    [38,36,39,35,37,34,36,33,35,34],         '#22c55e');
});

// ── SIDEBAR TOGGLE ────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  const sidebar = document.getElementById('sidebar');
  const content = document.getElementById('main-content');
  const toggle  = document.getElementById('sidebar-toggle');

  if (toggle && sidebar && content) {
    toggle.addEventListener('click', () => {
      sidebar.classList.toggle('collapsed');
      content.classList.toggle('sidebar-collapsed');
    });
  }
});