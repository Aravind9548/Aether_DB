// State Management
let jwtToken = localStorage.getItem("jwt_token") || null;
let currentTenant = localStorage.getItem("tenant_id") || null;
let activeTab = "login";
let sessionKeys = JSON.parse(localStorage.getItem("session_keys") || "{}");

// Elements
const authView = document.getElementById("auth-view");
const dashboardView = document.getElementById("dashboard-view");
const userStatusArea = document.getElementById("user-status-area");
const authError = document.getElementById("auth-error");
const authSuccess = document.getElementById("auth-success");
const consoleLogs = document.getElementById("console-logs");
const registryTable = document.getElementById("registry-table").getElementsByTagName("tbody")[0];

// Initialize View
document.addEventListener("DOMContentLoaded", () => {
    // Parse Google OAuth redirect parameters if present
    const urlParams = new URLSearchParams(window.location.search);
    const token = urlParams.get("token");
    const tenant = urlParams.get("tenant");
    if (token && tenant) {
        jwtToken = token;
        currentTenant = tenant;
        localStorage.setItem("jwt_token", jwtToken);
        localStorage.setItem("tenant_id", currentTenant);
        // Clear query parameters from URL without reloading
        window.history.replaceState({}, document.title, window.location.pathname);
    }
    checkAuthState();
});

function handleGoogleLogin() {
    window.location.href = "/oauth2/authorization/google";
}

function checkAuthState() {
    const navLinks = document.getElementById("nav-links");
    if (jwtToken && currentTenant) {
        // Show Dashboard
        authView.classList.add("hidden");
        dashboardView.classList.remove("hidden");
        if (navLinks) navLinks.classList.remove("hidden");
        
        // Update Nav Status
        userStatusArea.innerHTML = `
            <span class="status-indicator online"></span>
            <span class="status-text font-mono">Active: ${currentTenant}</span>
            <button class="logout-btn" onclick="logout()">Logout</button>
        `;
        
        document.getElementById("namespace-text").textContent = `${currentTenant.length}:${currentTenant}`;
        writeConsole("system", `[Auth] Authenticated session active for tenant "${currentTenant}".`);
        updateRegistryTable();
    } else {
        // Show Auth Panel
        authView.classList.remove("hidden");
        dashboardView.classList.add("hidden");
        if (navLinks) navLinks.classList.add("hidden");
        
        // Update Nav Status
        userStatusArea.innerHTML = `
            <span class="status-indicator offline"></span>
            <span class="status-text">Disconnected</span>
        `;
        
        document.getElementById("namespace-text").textContent = "N/A";
    }
}

// Tab switcher (Login / Register)
function switchTab(tab) {
    activeTab = tab;
    authError.classList.add("hidden");
    authSuccess.classList.add("hidden");

    const loginForm = document.getElementById("login-form");
    const registerForm = document.getElementById("register-form");
    const tabs = document.querySelectorAll(".tab-btn");

    if (tab === "login") {
        loginForm.classList.remove("hidden");
        registerForm.classList.add("hidden");
        tabs[0].classList.add("active");
        tabs[1].classList.remove("active");
    } else {
        loginForm.classList.add("hidden");
        registerForm.classList.remove("hidden");
        tabs[0].classList.remove("active");
        tabs[1].classList.add("active");
    }
}

// Handle Authentication (Login / Register)
async function handleAuth(event, type) {
    event.preventDefault();
    authError.classList.add("hidden");
    authSuccess.classList.add("hidden");

    const username = document.getElementById(`${type}-username`).value.trim();
    const password = document.getElementById(`${type}-password`).value.trim();

    const url = type === "login" ? "/auth/login" : "/auth/register";
    
    try {
        const response = await fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ username, password })
        });

        const data = await response.text();

        if (response.ok) {
            if (type === "login") {
                const json = JSON.parse(data);
                jwtToken = json.token;
                currentTenant = username;
                
                localStorage.setItem("jwt_token", jwtToken);
                localStorage.setItem("tenant_id", currentTenant);
                
                checkAuthState();
            } else {
                authSuccess.textContent = "Registration successful! You can now log in.";
                authSuccess.classList.remove("hidden");
                document.getElementById("register-form").reset();
                switchTab("login");
            }
        } else {
            authError.textContent = data || "Authentication failed.";
            authError.classList.remove("hidden");
        }
    } catch (err) {
        authError.textContent = "Unable to connect to Gateway server.";
        authError.classList.remove("hidden");
    }
}

// DEL is destructive - require a second click within a short window before
// it actually executes, instead of firing immediately like SET/GET.
function handleDelClick(btn, key) {
    armDestructiveAction(btn, {
        defaultText: "DEL",
        confirmText: "Confirm?",
        armedClass: "confirm-armed",
        timeoutMs: 4000,
        onExecute: () => {
            if (key !== undefined) {
                document.getElementById("op-key").value = key;
            }
            executeOp("DEL");
        }
    });
}

// Execute DB Operation
async function executeOp(op) {
    const keyInput = document.getElementById("op-key");
    const valInput = document.getElementById("op-value");
    
    const key = keyInput.value.trim();
    const val = valInput.value.trim();

    if (!key) {
        alert("Target Key is required!");
        return;
    }

    let url = `/api/v1/storage`;
    let options = {
        headers: {
            "Authorization": `Bearer ${jwtToken}`
        }
    };

    if (op === "SET") {
        url += `?key=${encodeURIComponent(key)}`;
        options.method = "POST";
        options.headers["Content-Type"] = "text/plain";
        options.body = val;
    } else if (op === "GET") {
        url += `/${encodeURIComponent(key)}`;
        options.method = "GET";
    } else if (op === "DEL") {
        url += `/${encodeURIComponent(key)}`;
        options.method = "DELETE";
    }

    const tStart = performance.now();
    writeConsole("sent", `[REQ] ${op} for key "${key}"` + (op === "SET" ? ` with value "${val}"` : ""));

    try {
        const response = await fetch(url, options);
        const elapsed = performance.now() - tStart;
        document.getElementById("latency-text").textContent = `${elapsed.toFixed(2)} ms`;

        const text = await response.text();
        
        const shardId = response.headers.get("X-Shard-Id");
        const replicatedCount = response.headers.get("X-Replicated-Count");
        const clusterSize = response.headers.get("X-Cluster-Size");
        
        let metaSuffix = "";
        if (shardId !== null) {
            metaSuffix += `  ·  shard ${shardId}`;
        }
        if (replicatedCount !== null && clusterSize !== null) {
            metaSuffix += `  ·  replicated ${replicatedCount}/${clusterSize}`;
        }

        if (response.ok) {
            writeConsole("recv", `[RESP] (status: ${response.status}) -> ${text}${metaSuffix}`);
            
            if (op === "SET") {
                sessionKeys[key] = { value: val, updated: new Date().toLocaleTimeString() };
            } else if (op === "GET") {
                sessionKeys[key] = { value: text, updated: new Date().toLocaleTimeString() };
            } else if (op === "DEL") {
                sessionKeys[key] = { value: "(deleted)", updated: new Date().toLocaleTimeString() };
            }
            
            localStorage.setItem("session_keys", JSON.stringify(sessionKeys));
            updateRegistryTable();
        } else {
            writeConsole("error", `[ERR] (status: ${response.status}) -> ${text}${metaSuffix}`);
            if (response.status === 401 || response.status === 403) {
                logout();
            }
        }
    } catch (err) {
        writeConsole("error", `[ERR] Connection error: ${err.message}`);
    }
}

// Log message to terminal console
function writeConsole(type, message) {
    const line = document.createElement("div");
    line.className = `log-line ${type}`;
    line.textContent = message;
    consoleLogs.appendChild(line);
    consoleLogs.scrollTop = consoleLogs.scrollHeight;
}

// Update Registry Table
function updateRegistryTable() {
    // Clear old rows
    registryTable.innerHTML = "";
    
    const keys = Object.keys(sessionKeys);
    
    if (keys.length === 0) {
        registryTable.innerHTML = `
            <tr id="no-keys-row">
                <td colspan="4" class="text-center text-muted">No keys queried in this session yet. Try executing GET/SET operations.</td>
            </tr>
        `;
        return;
    }

    keys.forEach(k => {
        const row = registryTable.insertRow();
        
        const cellKey = row.insertCell(0);
        cellKey.textContent = k;
        
        const cellVal = row.insertCell(1);
        cellVal.textContent = sessionKeys[k].value;
        if (sessionKeys[k].value === "(deleted)") cellVal.classList.add("text-muted");
        if (sessionKeys[k].value === "Key not found in your tenant workspace") cellVal.classList.add("error");
        
        const cellTime = row.insertCell(2);
        cellTime.textContent = sessionKeys[k].updated;
        
        const cellActions = row.insertCell(3);
        cellActions.innerHTML = `
            <button onclick="quickQuery('${k}', 'GET')" class="op-btn get-btn" style="padding: 3px 8px; font-size: 0.75rem; margin-right: 5px;">GET</button>
            <button onclick="handleDelClick(this, '${k}')" class="op-btn del-btn" style="padding: 3px 8px; font-size: 0.75rem;">DEL</button>
        `;
    });
}

function quickQuery(key, op) {
    document.getElementById("op-key").value = key;
    executeOp(op);
}

// Logout
function logout() {
    jwtToken = null;
    currentTenant = null;
    sessionKeys = {};
    localStorage.removeItem("jwt_token");
    localStorage.removeItem("tenant_id");
    localStorage.removeItem("session_keys");
    checkAuthState();
}
