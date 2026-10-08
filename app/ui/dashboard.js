(function () {
    "use strict";

    let csrfToken = "";
    let currentRole = "";

    function setBanner(message, isError) {
        const banner = document.getElementById("statusBanner");
        if (!banner) return;
        if (!message) {
            banner.classList.add("hidden");
            banner.textContent = "";
            return;
        }
        banner.classList.remove("hidden");
        banner.textContent = message;
        banner.className = isError ? "status-banner error-text" : "status-banner";
    }

    function updateSessionUI(authenticated, role) {
        const keyInput = document.getElementById("apiKey");
        const loginBtn = document.getElementById("loginBtn");
        const logoutBtn = document.getElementById("logoutBtn");
        const sessionPill = document.getElementById("sessionPill");

        if (authenticated) {
            currentRole = role || "AUTHENTICATED";
            if (keyInput) {
                keyInput.value = "";
                keyInput.classList.add("hidden");
            }
            if (loginBtn) loginBtn.classList.add("hidden");
            if (logoutBtn) logoutBtn.classList.remove("hidden");
            if (sessionPill) {
                sessionPill.textContent = "Session: " + currentRole;
                sessionPill.classList.remove("hidden");
            }
        } else {
            currentRole = "";
            csrfToken = "";
            if (keyInput) keyInput.classList.remove("hidden");
            if (loginBtn) loginBtn.classList.remove("hidden");
            if (logoutBtn) logoutBtn.classList.add("hidden");
            if (sessionPill) {
                sessionPill.textContent = "";
                sessionPill.classList.add("hidden");
            }
        }
    }

    function buildHeaders(method) {
        const headers = { "Accept": "application/json" };
        if (csrfToken && ["POST", "PUT", "PATCH", "DELETE"].includes((method || "GET").toUpperCase())) {
            headers["X-CSRF-Token"] = csrfToken;
        }
        const keyInput = document.getElementById("apiKey");
        if (!csrfToken && keyInput && keyInput.value.trim()) {
            headers["X-API-Key"] = keyInput.value.trim();
        }
        return headers;
    }

    function clearElement(el) {
        while (el.firstChild) {
            el.removeChild(el.firstChild);
        }
    }

    function createStatusBadge(statusText) {
        const span = document.createElement("span");
        const safeClass = String(statusText || "unknown").replace(/[^a-z0-9_]/gi, "");
        span.className = "badge badge-" + safeClass;
        span.textContent = String(statusText || "unknown");
        return span;
    }

    function renderEmptyRow(tbody, colSpan, message, isError) {
        clearElement(tbody);
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.colSpan = colSpan;
        td.className = isError ? "empty-cell error-text" : "empty-cell";
        td.textContent = message;
        tr.appendChild(td);
        tbody.appendChild(tr);
    }

    async function signIn() {
        const keyInput = document.getElementById("apiKey");
        const rawKey = keyInput ? keyInput.value.trim() : "";
        if (!rawKey) {
            setBanner("Please enter an API Key to start an authenticated session.", true);
            return;
        }
        try {
            const res = await fetch("/api/auth/login", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                credentials: "same-origin",
                body: JSON.stringify({ api_key: rawKey })
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({}));
                setBanner("Sign-in failed: " + (err.detail || res.statusText), true);
                return;
            }
            const data = await res.json();
            csrfToken = data.csrf_token || "";
            updateSessionUI(true, data.role);
            setBanner("", false);
            await refreshAll();
        } catch (err) {
            setBanner("Network error during sign-in.", true);
        }
    }

    async function signOut() {
        try {
            await fetch("/api/auth/logout", {
                method: "POST",
                headers: buildHeaders("POST"),
                credentials: "same-origin"
            });
        } catch (_) {
            // ignore
        }
        updateSessionUI(false, "");
        const delivBody = document.getElementById("deliveriesTable");
        const dlqBody = document.getElementById("dlqTable");
        if (delivBody) renderEmptyRow(delivBody, 6, "Signed out. Authenticate to load deliveries.", false);
        if (dlqBody) renderEmptyRow(dlqBody, 6, "Signed out. Authenticate to view DLQ.", false);
    }

    async function checkExistingSession() {
        try {
            const res = await fetch("/api/auth/me", {
                method: "GET",
                credentials: "same-origin"
            });
            if (res.ok) {
                const data = await res.json();
                if (data.authenticated) {
                    csrfToken = data.csrf_token || "";
                    updateSessionUI(true, data.role);
                    await refreshAll();
                }
            }
        } catch (_) {
            // No active session yet
        }
    }

    async function refreshAll() {
        const headers = buildHeaders("GET");
        if (!csrfToken && !headers["X-API-Key"]) {
            const delivBody = document.getElementById("deliveriesTable");
            if (delivBody) renderEmptyRow(delivBody, 6, "Please sign in with an API Key above to view deliveries.", false);
            return;
        }

        try {
            const statsRes = await fetch("/api/stats", { headers, credentials: "same-origin" });
            if (statsRes.ok) {
                const stats = await statsRes.json();
                document.getElementById("statTotal").textContent = String(stats.total_deliveries ?? 0);
                document.getElementById("statRate").textContent = String(stats.success_rate_percent ?? 0) + "%";
                document.getElementById("statSent").textContent = String(stats.by_status?.sent ?? 0);
                document.getElementById("statDlq").textContent = String(stats.dead_letter_queue_count ?? 0);
            }

            const delivRes = await fetch("/api/deliveries?limit=25", { headers, credentials: "same-origin" });
            const tbody = document.getElementById("deliveriesTable");
            if (!delivRes.ok) {
                renderEmptyRow(tbody, 6, "Authentication error (" + delivRes.status + "). Verify your session or API Key.", true);
                return;
            }

            const delivData = await delivRes.json();
            const delivList = Array.isArray(delivData) ? delivData : (delivData.deliveries || []);
            clearElement(tbody);

            if (delivList.length === 0) {
                renderEmptyRow(tbody, 6, "No deliveries recorded yet.", false);
            } else {
                for (const d of delivList) {
                    const tr = document.createElement("tr");

                    const tdId = document.createElement("td");
                    const codeId = document.createElement("code");
                    codeId.className = "mono-code";
                    codeId.textContent = String(d.delivery_id || "").substring(0, 14) + "...";
                    tdId.appendChild(codeId);
                    tr.appendChild(tdId);

                    const tdEvent = document.createElement("td");
                    const strongEv = document.createElement("strong");
                    strongEv.textContent = String(d.event_type || "unknown");
                    tdEvent.appendChild(strongEv);
                    tr.appendChild(tdEvent);

                    const tdRepo = document.createElement("td");
                    tdRepo.textContent = String(d.repo || "N/A");
                    tr.appendChild(tdRepo);

                    const tdStatus = document.createElement("td");
                    tdStatus.appendChild(createStatusBadge(d.status));
                    tr.appendChild(tdStatus);

                    const tdAttempts = document.createElement("td");
                    tdAttempts.textContent = String(d.attempts ?? 0);
                    tr.appendChild(tdAttempts);

                    const tdActions = document.createElement("td");
                    const traceBtn = document.createElement("button");
                    traceBtn.type = "button";
                    traceBtn.className = "btn-sm secondary";
                    traceBtn.textContent = "Trace";
                    traceBtn.addEventListener("click", () => inspectDelivery(String(d.delivery_id)));
                    tdActions.appendChild(traceBtn);

                    if (d.status !== "sent" && d.status !== "discarded") {
                        const replayBtn = document.createElement("button");
                        replayBtn.type = "button";
                        replayBtn.className = "btn-sm btn-replay";
                        replayBtn.textContent = "↻ Replay";
                        replayBtn.addEventListener("click", () => redriveDelivery(String(d.delivery_id)));
                        tdActions.appendChild(replayBtn);
                    }

                    tr.appendChild(tdActions);
                    tbody.appendChild(tr);
                }
            }

            const dlqRes = await fetch("/api/dlq", { headers, credentials: "same-origin" });
            const dlqBody = document.getElementById("dlqTable");
            if (dlqRes.ok) {
                const dlqData = await dlqRes.json();
                const dlqList = Array.isArray(dlqData) ? dlqData : (dlqData.dlq || []);
                clearElement(dlqBody);
                if (dlqList.length === 0) {
                    renderEmptyRow(dlqBody, 6, "🎉 Dead Letter Queue is empty!", false);
                } else {
                    for (const item of dlqList) {
                        const tr = document.createElement("tr");

                        const tdId = document.createElement("td");
                        const codeId = document.createElement("code");
                        codeId.className = "mono-code";
                        codeId.textContent = String(item.delivery_id || "").substring(0, 14) + "...";
                        tdId.appendChild(codeId);
                        tr.appendChild(tdId);

                        const tdEv = document.createElement("td");
                        tdEv.textContent = String(item.event_type || "");
                        tr.appendChild(tdEv);

                        const tdRepo = document.createElement("td");
                        tdRepo.textContent = String(item.repo || "N/A");
                        tr.appendChild(tdRepo);

                        const tdErr = document.createElement("td");
                        tdErr.className = "error-text";
                        tdErr.textContent = String(item.last_error || "Max retries exceeded").substring(0, 80);
                        tr.appendChild(tdErr);

                        const tdUpdated = document.createElement("td");
                        tdUpdated.className = "muted-text";
                        tdUpdated.textContent = item.updated_at ? new Date(item.updated_at).toLocaleTimeString() : "-";
                        tr.appendChild(tdUpdated);

                        const tdAct = document.createElement("td");
                        const redriveBtn = document.createElement("button");
                        redriveBtn.type = "button";
                        redriveBtn.className = "btn-sm btn-replay";
                        redriveBtn.textContent = "↻ Redrive";
                        redriveBtn.addEventListener("click", () => redriveDelivery(String(item.delivery_id)));
                        tdAct.appendChild(redriveBtn);

                        const discardBtn = document.createElement("button");
                        discardBtn.type = "button";
                        discardBtn.className = "btn-sm danger";
                        discardBtn.textContent = "✕ Discard";
                        discardBtn.addEventListener("click", () => discardDlq(String(item.delivery_id)));
                        tdAct.appendChild(discardBtn);

                        tr.appendChild(tdAct);
                        dlqBody.appendChild(tr);
                    }
                }
            }
        } catch (err) {
            setBanner("Error refreshing telemetry.", true);
        }
    }

    async function inspectDelivery(deliveryId) {
        const res = await fetch("/api/deliveries/" + encodeURIComponent(deliveryId), {
            headers: buildHeaders("GET"),
            credentials: "same-origin"
        });
        if (!res.ok) {
            setBanner("Failed to fetch delivery details (" + res.status + ")", true);
            return;
        }
        const d = await res.json();
        const modalBody = document.getElementById("modalBody");
        clearElement(modalBody);

        const metaDiv = document.createElement("div");
        const fields = [
            ["Delivery ID", d.delivery_id],
            ["Event", d.event_type],
            ["Repository", d.repo || "N/A"],
            ["Destinations", d.destinations || "default"],
            ["Status", d.status]
        ];
        for (const [label, val] of fields) {
            const p = document.createElement("div");
            const b = document.createElement("strong");
            b.textContent = label + ": ";
            p.appendChild(b);
            const span = document.createElement("span");
            span.textContent = String(val ?? "");
            p.appendChild(span);
            metaDiv.appendChild(p);
        }
        modalBody.appendChild(metaDiv);

        const h4 = document.createElement("h4");
        h4.textContent = "Granular Attempt History";
        modalBody.appendChild(h4);

        const attempts = d.attempt_history || [];
        if (attempts.length === 0) {
            const emptyP = document.createElement("p");
            emptyP.className = "muted-text";
            emptyP.textContent = "No dispatch attempts logged yet.";
            modalBody.appendChild(emptyP);
        } else {
            for (const a of attempts) {
                const card = document.createElement("div");
                card.className = "attempt-item";

                const topRow = document.createElement("div");
                topRow.className = "attempt-row";

                const titleB = document.createElement("strong");
                const triggerLabel = a.trigger_type ? " [" + String(a.trigger_type).toUpperCase() + "]" : "";
                titleB.textContent = "Attempt #" + String(a.attempt_number) + " → " + String(a.destination || "").toUpperCase() + triggerLabel;
                topRow.appendChild(titleB);

                const statusSpan = document.createElement("span");
                statusSpan.textContent = "HTTP " + String(a.http_status || "ERR") + " (" + String(a.response_time_ms ?? 0) + " ms)";
                topRow.appendChild(statusSpan);
                card.appendChild(topRow);

                if (a.error_message) {
                    const errDiv = document.createElement("div");
                    errDiv.className = "error-text";
                    errDiv.textContent = String(a.error_message);
                    card.appendChild(errDiv);
                }

                const timeDiv = document.createElement("div");
                timeDiv.className = "muted-text";
                timeDiv.textContent = a.created_at ? new Date(a.created_at).toISOString() : "";
                card.appendChild(timeDiv);

                modalBody.appendChild(card);
            }
        }

        document.getElementById("detailModal").classList.add("active");
    }

    async function redriveDelivery(deliveryId) {
        const res = await fetch("/api/deliveries/" + encodeURIComponent(deliveryId) + "/redrive", {
            method: "POST",
            headers: buildHeaders("POST"),
            credentials: "same-origin"
        });
        if (res.ok) {
            setBanner("Delivery queued for replay.", false);
            setTimeout(refreshAll, 500);
        } else {
            const err = await res.json().catch(() => ({}));
            setBanner("Redrive failed: " + (err.detail || res.statusText), true);
        }
    }

    async function discardDlq(deliveryId) {
        const res = await fetch("/api/dlq/" + encodeURIComponent(deliveryId) + "/discard", {
            method: "POST",
            headers: buildHeaders("POST"),
            credentials: "same-origin"
        });
        if (res.ok) {
            setBanner("DLQ item discarded.", false);
            refreshAll();
        } else {
            const err = await res.json().catch(() => ({}));
            setBanner("Discard failed: " + (err.detail || res.statusText), true);
        }
    }

    function closeModal() {
        document.getElementById("detailModal").classList.remove("active");
    }

    document.addEventListener("DOMContentLoaded", () => {
        const loginBtn = document.getElementById("loginBtn");
        const logoutBtn = document.getElementById("logoutBtn");
        const refreshBtn = document.getElementById("refreshBtn");
        const closeModalBtn = document.getElementById("closeModalBtn");

        if (loginBtn) loginBtn.addEventListener("click", signIn);
        if (logoutBtn) logoutBtn.addEventListener("click", signOut);
        if (refreshBtn) refreshBtn.addEventListener("click", refreshAll);
        if (closeModalBtn) closeModalBtn.addEventListener("click", closeModal);

        checkExistingSession();
    });
})();
