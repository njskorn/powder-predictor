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
    // For now, just log to console
    console.log(`Details for ${mountain}:`, data);
    
    // TODO: Implement modal with full trail/lift lists
    alert(`Full details for ${mountain}\n\nTrails: ${data.trails?.length || 0}\nLifts: ${data.lifts?.length || 0}\nGlades: ${data.glades?.length || 0}\n\n(Full modal coming soon!)`);
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