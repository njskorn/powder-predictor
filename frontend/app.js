/**
 * Powder Predictor Dashboard
 * Fetches and displays ski mountain conditions
 */

const API_BASE = '/api';

// State
let mountainData = {};

/**
 * Fetch mountain data from API
 */
async function fetchMountainData() {
    try {
        console.log('Fetching data from:', `${API_BASE}/summary`);
        const response = await fetch(`${API_BASE}/summary`);
        console.log('Response status:', response.status);
        
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const data = await response.json();
        console.log('Received data:', data);
        mountainData = data;
        
        renderMountains();
        hideLoading();
        
    } catch (error) {
        console.error('Error fetching data:', error);
        showError();
    }
}

/**
 * Render mountain cards
 */
function renderMountains() {
    console.log('Rendering mountains with data:', mountainData);
    const container = document.getElementById('mountains');
    container.innerHTML = '';
    
    const mountains = ['bretton-woods', 'cannon', 'cranmore'];
    
    mountains.forEach(mountain => {
        const data = mountainData[mountain];
        console.log(`Data for ${mountain}:`, data);
        
        if (!data || data.error) {
            console.log(`Rendering error card for ${mountain}`);
            container.innerHTML += renderErrorCard(mountain);
            return;
        }
        
        console.log(`Rendering mountain card for ${mountain}`);
        container.innerHTML += renderMountainCard(mountain, data);
    });
    
    updateLastUpdatedTime();
}

/**
 * Render a mountain card
 */
function renderMountainCard(mountain, data) {
    const displayName = mountain.split('-').map(w => 
        w.charAt(0).toUpperCase() + w.slice(1)
    ).join(' ');
    
    const trails = data.summary.trails;
    const lifts = data.summary.lifts;
    const temp = data.weather?.temperature_base?.value;
    const freshness = data.data_freshness;
    
    const isStale = freshness.is_stale;
    const ageMinutes = freshness.age_minutes;
    
    return `
        <div class="mountain-card ${isStale ? 'stale' : ''}">
            <div class="mountain-header">
                <h2>${displayName}</h2>
                <span class="freshness ${isStale ? 'stale' : 'fresh'}">
                    ${ageMinutes < 60 ? `${ageMinutes}m ago` : `${Math.floor(ageMinutes/60)}h ago`}
                </span>
            </div>
            
            <div class="stats-grid">
                <div class="stat">
                    <div class="stat-value">${trails.percent}%</div>
                    <div class="stat-label">Trails Open</div>
                    <div class="stat-detail">${trails.open}/${trails.total}</div>
                </div>
                
                <div class="stat">
                    <div class="stat-value">${lifts.percent}%</div>
                    <div class="stat-label">Lifts Open</div>
                    <div class="stat-detail">${lifts.open}/${lifts.total}</div>
                </div>
                
                ${temp !== null ? `
                <div class="stat">
                    <div class="stat-value">${temp}°F</div>
                    <div class="stat-label">Base Temp</div>
                </div>
                ` : ''}
            </div>
            
            ${trails.by_difficulty ? `
            <div class="difficulty-breakdown">
                <div class="difficulty-item">
                    <span class="difficulty-badge green">●</span>
                    <span>${trails.by_difficulty.green?.open || 0}/${trails.by_difficulty.green?.total || 0}</span>
                </div>
                <div class="difficulty-item">
                    <span class="difficulty-badge blue">●</span>
                    <span>${trails.by_difficulty.blue?.open || 0}/${trails.by_difficulty.blue?.total || 0}</span>
                </div>
                <div class="difficulty-item">
                    <span class="difficulty-badge black">●</span>
                    <span>${trails.by_difficulty.black?.open || 0}/${trails.by_difficulty.black?.total || 0}</span>
                </div>
                ${trails.by_difficulty.glades ? `
                <div class="difficulty-item">
                    <span class="difficulty-badge glades">🌲</span>
                    <span>${trails.by_difficulty.glades?.open || 0}/${trails.by_difficulty.glades?.total || 0}</span>
                </div>
                ` : ''}
            </div>
            ` : ''}
            
            <button class="view-details" onclick="viewDetails('${mountain}')">
                View Details →
            </button>
        </div>
    `;
}

/**
 * Render error card for a mountain
 */
function renderErrorCard(mountain) {
    const displayName = mountain.split('-').map(w => 
        w.charAt(0).toUpperCase() + w.slice(1)
    ).join(' ');
    
    return `
        <div class="mountain-card error">
            <div class="mountain-header">
                <h2>${displayName}</h2>
            </div>
            <p class="error-message">Data unavailable</p>
        </div>
    `;
}

/**
 * View detailed mountain report
 */
async function viewDetails(mountain) {
    try {
        const response = await fetch(`${API_BASE}/mountains/${mountain}`);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const data = await response.json();
        showDetailModal(mountain, data);
        
    } catch (error) {
        console.error('Error fetching details:', error);
        alert('Unable to load detailed report');
    }
}

/**
 * Show detail modal (placeholder for now)
 */
function showDetailModal(mountain, data) {
    // Open the modal with chart and data
    openModal(mountain);
}

/**
 * Update last updated timestamp
 */
function updateLastUpdatedTime() {
    const now = new Date().toLocaleTimeString();
    document.getElementById('update-time').textContent = now;
    document.getElementById('last-updated').classList.remove('hidden');
}

/**
 * Show/hide loading state
 */
function hideLoading() {
    document.getElementById('loading').classList.add('hidden');
    document.getElementById('error').classList.add('hidden');
}

function showError() {
    document.getElementById('loading').classList.add('hidden');
    document.getElementById('error').classList.remove('hidden');
}

/**
 * Modal and Chart Functions
 */
let currentChart = null;
let currentMountain = null;

async function openModal(mountain, days = 30) {
    const modal = document.getElementById('mountain-modal');
    const modalTitle = document.getElementById('modal-title');
    const chartLoading = document.getElementById('chart-loading');
    const chartContainer = document.getElementById('chart-container');
    const timeRangeSelector = document.getElementById('time-range-selector');
    
    // Store current mountain for time range changes
    currentMountain = mountain;
    
    // Set title
    const displayName = mountain.split('-').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
    modalTitle.textContent = displayName;
    
    // Update active time range button
    document.querySelectorAll('.time-range-btn').forEach(btn => {
        btn.classList.toggle('active', parseInt(btn.dataset.days) === days);
    });
    
    // Show modal
    modal.classList.add('show');
    
    // Show loading, hide chart and time selector
    chartLoading.style.display = 'block';
    chartContainer.classList.remove('show');
    timeRangeSelector.style.display = 'none';
    
    try {
        // Fetch both historical data and current data in parallel
        const [historyResponse, currentResponse] = await Promise.all([
            fetch(`${API_BASE}/mountains/${mountain}/history?days=${days}`),
            fetch(`${API_BASE}/mountains/${mountain}`)
        ]);
        
        const historyResult = await historyResponse.json();
        const currentData = await currentResponse.json();
        
        // Hide loading, show chart and time selector
        chartLoading.style.display = 'none';
        chartContainer.classList.add('show');
        timeRangeSelector.style.display = 'flex';
        
        // Render chart
        renderChart(historyResult.data);
        
        // Display snow report
        const snowReportText = document.getElementById('snow-report-text');
        const report = currentData.snow_report || currentData.narrative_report || '';
        snowReportText.textContent = report || 'No snow report available';
        
    } catch (error) {
        console.error('Error loading modal data:', error);
        chartLoading.innerHTML = '<p>Error loading data</p>';
    }
}

function closeModal() {
    const modal = document.getElementById('mountain-modal');
    modal.classList.remove('show');
    
    // Destroy chart to clean up
    if (currentChart) {
        currentChart.destroy();
        currentChart = null;
    }
}

function renderChart(data) {
    const canvas = document.getElementById('terrain-chart');
    const ctx = canvas.getContext('2d');
    
    // Destroy previous chart if exists
    if (currentChart) {
        currentChart.destroy();
    }
    
    // Prepare data
    const dates = data.map(d => d.date);
    const greenData = data.map(d => d.green);
    const blueData = data.map(d => d.blue);
    const blackData = data.map(d => d.black);
    const gladesData = data.map(d => d.glades);
    
    // Adaptive point size based on data density
    // 7 days = bigger points, 90 days = tiny points
    let pointRadius, pointHoverRadius;
    const dataLength = data.length;
    
    if (dataLength <= 10) {
        // 7 days - large points
        pointRadius = 4;
        pointHoverRadius = 6;
    } else if (dataLength <= 40) {
        // 30 days - small points
        pointRadius = 2;
        pointHoverRadius = 3.5;
    } else {
        // 90 days (season) - tiny points
        pointRadius = 1.2;
        pointHoverRadius = 2.5;
    }
    
    // Weekend shading plugin
    const weekendShading = {
        id: 'weekendShading',
        beforeDraw: (chart) => {
            const ctx = chart.ctx;
            const xAxis = chart.scales.x;
            const yAxis = chart.scales.y;
            
            ctx.save();
            
            dates.forEach((dateStr, index) => {
                const date = new Date(dateStr);
                const dayOfWeek = date.getDay();
                
                // 0 = Sunday, 6 = Saturday
                if (dayOfWeek === 0 || dayOfWeek === 6) {
                    const x = xAxis.getPixelForValue(index);
                    const nextX = index < dates.length - 1 ? xAxis.getPixelForValue(index + 1) : xAxis.right;
                    const width = nextX - x;
                    
                    ctx.fillStyle = 'rgba(148, 163, 184, 0.08)';  // Subtle light shade for dark mode
                    ctx.fillRect(x, yAxis.top, width, yAxis.bottom - yAxis.top);
                }
            });
            
            ctx.restore();
        }
    };
    
    // Create chart
    currentChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: dates.map(d => {
                const date = new Date(d);
                return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
            }),
            datasets: [
                {
                    label: 'Beginner',
                    data: greenData,
                    borderColor: '#10b981',  // Brighter green for dark mode
                    backgroundColor: 'rgba(16, 185, 129, 0.1)',
                    borderWidth: 3,  // Thicker lines
                    tension: 0.3,
                    fill: false,
                    pointRadius: pointRadius,
                    pointHoverRadius: pointHoverRadius
                },
                {
                    label: 'Intermediate',
                    data: blueData,
                    borderColor: '#60a5fa',  // Lighter blue
                    backgroundColor: 'rgba(96, 165, 250, 0.1)',
                    borderWidth: 3,
                    tension: 0.3,
                    fill: false,
                    pointRadius: pointRadius,
                    pointHoverRadius: pointHoverRadius
                },
                {
                    label: 'Advanced',
                    data: blackData,
                    borderColor: '#e2e8f0',  // Light gray/white - very visible on dark!
                    backgroundColor: 'rgba(226, 232, 240, 0.1)',
                    borderWidth: 3,
                    tension: 0.3,
                    fill: false,
                    pointRadius: pointRadius,
                    pointHoverRadius: pointHoverRadius
                },
                {
                    label: 'Glades',
                    data: gladesData,
                    borderColor: '#059669',  // Dark forest green (already good)
                    backgroundColor: 'rgba(5, 150, 105, 0.1)',
                    borderWidth: 3,
                    tension: 0.3,
                    fill: false,
                    borderDash: [5, 5],
                    pointRadius: pointRadius,
                    pointHoverRadius: pointHoverRadius
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false
            },
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        usePointStyle: true,
                        padding: 15,
                        color: '#cbd5e1',  // Light gray for legend text
                        font: {
                            size: 12,
                            weight: '600'
                        }
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(15, 23, 42, 0.95)',
                    titleColor: '#f1f5f9',
                    bodyColor: '#cbd5e1',
                    borderColor: 'rgba(148, 163, 184, 0.3)',
                    borderWidth: 1,
                    padding: 12,
                    boxPadding: 6,
                    callbacks: {
                        title: function(context) {
                            const index = context[0].dataIndex;
                            const date = new Date(dates[index]);
                            const dayOfWeek = date.toLocaleDateString('en-US', { weekday: 'short' });
                            return `${dayOfWeek}, ${context[0].label}`;
                        },
                        label: function(context) {
                            return `${context.dataset.label}: ${context.parsed.y} trails`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Trails Open',
                        color: '#cbd5e1',  // Light gray text
                        font: {
                            size: 13,
                            weight: '600'
                        }
                    },
                    ticks: {
                        stepSize: 5,
                        color: '#94a3b8'  // Lighter gray for tick labels
                    },
                    grid: {
                        color: 'rgba(148, 163, 184, 0.1)'  // Subtle grid lines
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Date',
                        color: '#cbd5e1',
                        font: {
                            size: 13,
                            weight: '600'
                        }
                    },
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45,
                        color: '#94a3b8'
                    },
                    grid: {
                        color: 'rgba(148, 163, 184, 0.1)'
                    }
                }
            }
        },
        plugins: [weekendShading]
    });
}

// Close modal when clicking outside
document.addEventListener('click', function(event) {
    const modal = document.getElementById('mountain-modal');
    if (event.target === modal) {
        closeModal();
    }
});

// Time range selector event listeners
document.querySelectorAll('.time-range-btn').forEach(btn => {
    btn.addEventListener('click', function() {
        const days = parseInt(this.dataset.days);
        if (currentMountain) {
            openModal(currentMountain, days);
        }
    });
});

/**
 * Initialize dashboard
 */
async function init() {
    console.log('Initializing Powder Predictor dashboard...');
    await fetchMountainData();
    
    // Refresh data every 5 minutes
    setInterval(fetchMountainData, 5 * 60 * 1000);
}

// Start when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}