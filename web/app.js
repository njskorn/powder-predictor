// Fetch snow reports from API
async function loadConditions() {
    const loading = document.getElementById('loading');
    const error = document.getElementById('error');
    const conditions = document.getElementById('conditions');

    try {
        const response = await fetch("/api/conditions");
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();

        // Hide loading
        loading.classList.add('hidden');

        // Show conditions
        conditions.classList.remove('hidden');

        // Render mountain information on cards
        renderMountains(data.mountains);

    } catch (err) {
        loading.classList.add('hidden');
        error.textContent = `Error loading conditions" ${err.message}`;
        error.classList.remove('hidden');
    }
}

function renderMountains(mountains) {
    const container = document.getElementById('conditions');

    if (mountains.length == 0) {
        container.innerHTML = '<p style="color: white; text-align: center; ">No data available yet. Check back soon!</p>';
        return;
    }

    container.innerHTML=mountains.map(mountain =>`
        <div class="mountain-card">
            <h2>${mountain.name}</h2>
            <div class="stat">
                <span class="stat-label">Trails Open</span>
                <span class="stat-value">${mountain.trails_open || 'N/A'}</span>
            </div>
            <div class="stat">
                <span class="stat-label">Lifts Open</span>
                <span class="stat-value">${mountain.lifts_open || 'N/A'}</span>
            </div>
            <div class="stat">
                <span class="stat-label">Recent Snow</span>
                <span class="stat-value">${mountain.recent_snow || 'N/A'}</span>
            </div>
            <div class="stat">
                <span class="stat-label">Season Total</span>
                <span class="stat-value">${mountain.season_snow || 'N/A'}</span>
            </div>
        </div>
    `).join('');
}

//Load conditions when page loads
document.addEventListener('DOMContentLoaded',loadConditions);