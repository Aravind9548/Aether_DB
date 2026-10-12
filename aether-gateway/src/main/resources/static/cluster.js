// State & Auth Check
let jwtToken = localStorage.getItem("jwt_token") || null;
let currentTenant = localStorage.getItem("tenant_id") || null;

if (!jwtToken || !currentTenant) {
    window.location.href = "index.html";
} else {
    document.getElementById("tenant-display").textContent = `Active: ${currentTenant}`;
}

// Initial Loading & Polling
document.addEventListener("DOMContentLoaded", () => {
    pollClusterStatus();
    setInterval(pollClusterStatus, 2000);

    // Bind Kill Leader button using generalized arming helper
    const killBtn = document.getElementById("kill-leader-btn");
    if (killBtn) {
        killBtn.addEventListener("click", () => {
            armDestructiveAction(killBtn, {
                defaultText: "Kill Leader",
                confirmText: "Confirm SIGKILL?",
                armedClass: "confirm-armed",
                timeoutMs: 4000,
                onExecute: executeKillLeader
            });
        });
    }

    // Load initial empty row or setup row
    const container = document.getElementById("batch-rows-container");
    if (container.children.length === 0) {
        addBatchRow();
    }
});

// Poll Cluster Status
async function pollClusterStatus() {
    try {
        const response = await fetch("/api/v1/cluster/status", {
            method: "GET",
            headers: {
                "Authorization": `Bearer ${jwtToken}`
            }
        });
        
        if (response.ok) {
            const data = await response.json();
            renderCluster(data);
        } else {
            console.error("Cluster poll failed: status " + response.status);
        }
    } catch (err) {
        console.error("Cluster poll failed: ", err);
    }
}

// Render node topology cards
function renderCluster(nodes) {
    const container = document.getElementById("topology-view");
    container.innerHTML = "";
    
    nodes.forEach(node => {
        const isLeader = node.role === "LEADER";
        const isHealthy = node.healthy;
        
        const card = document.createElement("div");
        card.className = `node-card ${isLeader ? 'leader' : ''} ${!isHealthy ? 'unhealthy' : ''}`;
        
        // Host label
        const addressName = node.address.split(":")[0]; // e.g. aether-engine-1
        
        let roleBadge = "";
        if (isHealthy) {
            if (isLeader) {
                // Inline crown SVG
                roleBadge = `
                    <span class="role-badge">
                        <svg class="crown-icon" viewBox="0 0 24 24" style="width: 12px; height: 12px; fill: currentColor; margin-right: 4px;">
                            <path d="M2 22h20v-2H2v2zm1-3l2.5-9 4.5 4 2.5-10 2.5 10 4.5-4 2.5 9H3z"/>
                        </svg>
                        LEADER
                    </span>
                `;
            } else {
                roleBadge = `<span class="role-badge">${node.role}</span>`;
            }
        } else {
            roleBadge = `<span class="role-badge" style="color: var(--error); border-color: rgba(239, 68, 68, 0.3); background: rgba(239, 68, 68, 0.1);">OFFLINE</span>`;
        }
        
        card.innerHTML = `
            <div class="node-header">
                <span class="node-title">${addressName}</span>
                ${roleBadge}
            </div>
            <div class="node-stats">
                <div class="node-stat-item">
                    <span class="node-stat-label">Term</span>
                    <span class="node-stat-val">${isHealthy ? node.term : '--'}</span>
                </div>
                <div class="node-stat-item">
                    <span class="node-stat-label">Commit Index</span>
                    <span class="node-stat-val">${isHealthy ? node.commitIndex : '--'}</span>
                </div>
                <div class="node-stat-item">
                    <span class="node-stat-label">Last Log Index</span>
                    <span class="node-stat-val">${isHealthy ? node.lastLogIndex : '--'}</span>
                </div>
                <div class="node-stat-item" style="margin-top: 6px;">
                    <span class="node-stat-label">Endpoint</span>
                    <span class="node-stat-val" style="font-size: 0.75rem; color: var(--text-secondary);">${node.address}</span>
                </div>
                <div class="node-stat-item" style="margin-top: 6px; border-top: 1px solid rgba(255, 255, 255, 0.05); padding-top: 6px;">
                    <span class="node-stat-label">Status</span>
                    <span class="node-stat-val" style="display: flex; align-items: center; gap: 6px;">
                        <span class="status-indicator ${isHealthy ? 'online' : 'offline'}"></span>
                        ${isHealthy ? 'HEALTHY' : 'UNREACHABLE'}
                    </span>
                </div>
            </div>
        `;
        container.appendChild(card);
    });
}

// Execute Chaos Kill Leader RPC
async function executeKillLeader() {
    appendBatchLog("sent", "[Chaos] Triggering leader SIGKILL process RPC...");
    try {
        const response = await fetch("/api/v1/cluster/kill-leader", {
            method: "POST",
            headers: {
                "Authorization": `Bearer ${jwtToken}`
            }
        });
        const resText = await response.text();
        if (response.ok) {
            const data = JSON.parse(resText);
            appendBatchLog("success", `[Chaos] Kill Leader acknowledged: ${data.acknowledged}. Raft transition triggered.`);
        } else {
            let errorMsg = resText;
            try {
                const errObj = JSON.parse(resText);
                errorMsg = errObj.error || resText;
            } catch(e) {}
            appendBatchLog("error", `[Chaos] Kill Leader failed: ${errorMsg}`);
        }
    } catch(err) {
        appendBatchLog("error", `[Chaos] Connection error during Kill Leader: ${err.message}`);
    }
    // Refresh status representation immediately
    pollClusterStatus();
}

// Batch Log console
function appendBatchLog(type, message) {
    const logContainer = document.getElementById("batch-execution-log");
    const placeholder = logContainer.querySelector(".text-muted");
    if (placeholder) {
        logContainer.innerHTML = "";
    }
    
    const entry = document.createElement("div");
    entry.className = `log-line ${type}`;
    
    const timestamp = new Date().toLocaleTimeString();
    entry.innerHTML = `<span style="color: var(--text-muted); font-size: 0.75rem; margin-right: 8px;">[${timestamp}]</span> ${message}`;
    logContainer.appendChild(entry);
    logContainer.scrollTop = logContainer.scrollHeight;
}

// Batch row manipulation
function addBatchRow(keyVal = { key: "", val: "" }) {
    const container = document.getElementById("batch-rows-container");
    const row = document.createElement("div");
    row.className = "batch-row";
    row.innerHTML = `
        <div class="input-group flex-1">
            <label>Key</label>
            <input type="text" class="batch-key-input" placeholder="e.g. batch_key_1" value="${keyVal.key}">
        </div>
        <div class="input-group flex-2">
            <label>Value</label>
            <input type="text" class="batch-value-input" placeholder="Value to store" value="${keyVal.val}">
        </div>
        <button type="button" class="batch-remove-btn" onclick="removeBatchRow(this)">×</button>
    `;
    container.appendChild(row);
}

function removeBatchRow(btn) {
    const row = btn.parentElement;
    const container = document.getElementById("batch-rows-container");
    if (container.children.length > 1) {
        row.remove();
    } else {
        row.querySelector(".batch-key-input").value = "";
        row.querySelector(".batch-value-input").value = "";
    }
}

function loadSampleKeys() {
    const container = document.getElementById("batch-rows-container");
    container.innerHTML = ""; // Clear existing rows
    
    for (let i = 1; i <= 10; i++) {
        addBatchRow({
            key: `demo_key_${i}`,
            val: `Sample telemetry payload for key ${i} - ${Math.floor(Math.random() * 1000)}`
        });
    }
    
    appendBatchLog("system", "Loaded 10 client-side sample key-value structures.");
}

// Run batch loop
async function runBatch() {
    const keyInputs = document.querySelectorAll(".batch-key-input");
    const valInputs = document.querySelectorAll(".batch-value-input");
    
    const entries = [];
    for (let i = 0; i < keyInputs.length; i++) {
        const key = keyInputs[i].value.trim();
        const value = valInputs[i].value.trim();
        if (key) {
            entries.push({ key, value });
        }
    }
    
    if (entries.length === 0) {
        alert("No valid batch entries to run!");
        return;
    }
    
    appendBatchLog("sent", `[Batch] Initiating loop for ${entries.length} keys...`);
    
    try {
        const response = await fetch("/api/v1/storage/batch", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${jwtToken}`
            },
            body: JSON.stringify(entries)
        });
        
        if (response.ok) {
            const results = await response.json();
            let successCount = 0;
            results.forEach(res => {
                if (res.ok) {
                    successCount++;
                    appendBatchLog("recv", `  SET "${res.key}" -> OK`);
                } else {
                    appendBatchLog("error", `  SET "${res.key}" -> FAILED: ${res.error}`);
                }
            });
            appendBatchLog("system", `[Batch] Complete. Successfully wrote ${successCount}/${entries.length} items.`);
            refreshKeyBrowser();
        } else {
            const errText = await response.text();
            appendBatchLog("error", `[Batch] Batch request failed: ${errText}`);
        }
    } catch(err) {
        appendBatchLog("error", `[Batch] Connection error running batch: ${err.message}`);
    }
}

// Scanning and browsing tenant workspace keys
async function refreshKeyBrowser() {
    const tableBody = document.querySelector("#browser-table tbody");
    tableBody.innerHTML = `
        <tr>
            <td colspan="3" class="text-center text-muted">Scanning cluster...</td>
        </tr>
    `;
    
    try {
        const response = await fetch("/api/v1/storage/keys", {
            method: "GET",
            headers: {
                "Authorization": `Bearer ${jwtToken}`
            }
        });
        
        if (response.ok) {
            const data = await response.json();
            tableBody.innerHTML = "";
            
            if (data.length === 0) {
                tableBody.innerHTML = `
                    <tr id="browser-empty-row">
                        <td colspan="3" class="text-center text-muted">No records found. Click Refresh to query the cluster.</td>
                    </tr>
                `;
                return;
            }
            
            data.forEach(entry => {
                const row = tableBody.insertRow();
                const cellKey = row.insertCell(0);
                cellKey.textContent = entry.key;
                
                const cellValue = row.insertCell(1);
                cellValue.textContent = entry.value;
                
                const cellShard = row.insertCell(2);
                cellShard.textContent = entry.shard;
                cellShard.style.color = "var(--glow-cyan)";
            });
        } else {
            const errText = await response.text();
            tableBody.innerHTML = `
                <tr>
                    <td colspan="3" class="text-center error">Failed to scan: ${errText}</td>
                </tr>
            `;
        }
    } catch(err) {
        tableBody.innerHTML = `
            <tr>
                <td colspan="3" class="text-center error">Connection error scanning keys: ${err.message}</td>
            </tr>
        `;
    }
}
