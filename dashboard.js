document.addEventListener("DOMContentLoaded", () => {
    // Chart.js Instances
    let revenueChart, countryChart, volumeChart, anomalyChart;

    // DOM Elements
    const btnToggle = document.getElementById("btn-toggle");
    const btnPause = document.getElementById("btn-pause");
    const btnReset = document.getElementById("btn-reset");
    const speedRange = document.getElementById("speed-range");
    const speedLabel = document.getElementById("speed-label");
    const statusBadge = document.getElementById("stream-status-badge");

    // Filter DOM Elements
    const searchInput = document.getElementById("search-input");
    const filterCountry = document.getElementById("filter-country");
    const filterStatus = document.getElementById("filter-status");
    const btnClearFilters = document.getElementById("btn-clear-filters");

    // Store raw data for real-time client filtering
    let latestTransactions = [];
    let latestAnomalies = [];
    let availableCountries = new Set();

    // Common Chart.js Options
    const commonChartOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
            x: {
                grid: { color: "rgba(51, 65, 85, 0.3)" },
                ticks: { color: "#94a3b8", font: { size: 10 }, maxRotation: 0, autoSkip: true }
            },
            y: {
                grid: { color: "rgba(51, 65, 85, 0.3)" },
                ticks: { color: "#94a3b8", font: { size: 10 } }
            }
        }
    };

    // 1. Initialize Revenue Line Chart
    const ctxRevenue = document.getElementById("chart-revenue").getContext("2d");
    revenueChart = new Chart(ctxRevenue, {
        type: "line",
        data: {
            labels: [],
            datasets: [{
                label: "Revenue per Micro-Batch ($)",
                data: [],
                borderColor: "#38bdf8",
                backgroundColor: "rgba(56, 189, 248, 0.15)",
                fill: true,
                tension: 0.3,
                borderWidth: 2,
                pointRadius: 3
            }]
        },
        options: {
            ...commonChartOptions,
            plugins: {
                tooltip: {
                    callbacks: {
                        label: (ctx) => `Revenue: $${parseFloat(ctx.raw || 0).toLocaleString('en-US', {minimumFractionDigits: 2})}`
                    }
                }
            },
            scales: {
                ...commonChartOptions.scales,
                y: {
                    ...commonChartOptions.scales.y,
                    beginAtZero: true,
                    ticks: {
                        color: "#94a3b8",
                        callback: (val) => `$${val.toLocaleString()}`
                    }
                }
            }
        }
    });

    // 2. Initialize Top 5 Country Doughnut Chart
    const ctxCountry = document.getElementById("chart-country").getContext("2d");
    countryChart = new Chart(ctxCountry, {
        type: "doughnut",
        data: {
            labels: [],
            datasets: [{
                data: [],
                backgroundColor: ["#38bdf8", "#c084fc", "#22d3ee", "#fbbf24", "#34d399", "#f43f5e"]
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { position: "right", labels: { color: "#94a3b8", font: { size: 11 } } },
                tooltip: {
                    callbacks: {
                        label: (ctx) => `${ctx.label}: $${parseFloat(ctx.raw || 0).toLocaleString('en-US', {minimumFractionDigits: 2})}`
                    }
                }
            }
        }
    });

    // 3. Initialize Volume Bar Chart
    const ctxVolume = document.getElementById("chart-volume").getContext("2d");
    volumeChart = new Chart(ctxVolume, {
        type: "bar",
        data: {
            labels: [],
            datasets: [{
                label: "Quantity",
                data: [],
                backgroundColor: "rgba(192, 132, 252, 0.7)",
                borderRadius: 4
            }]
        },
        options: commonChartOptions
    });

    // 4. Initialize Isolation Forest Anomaly Score Line Chart
    const ctxAnomaly = document.getElementById("chart-anomaly-scores").getContext("2d");
    anomalyChart = new Chart(ctxAnomaly, {
        type: "line",
        data: {
            labels: [],
            datasets: [{
                label: "Isolation Forest Anomaly Score",
                data: [],
                borderColor: "#f43f5e",
                backgroundColor: "rgba(244, 63, 94, 0.15)",
                fill: true,
                tension: 0.3,
                borderWidth: 2,
                pointRadius: 4,
                pointBackgroundColor: "#f43f5e"
            }]
        },
        options: {
            ...commonChartOptions,
            plugins: {
                tooltip: {
                    callbacks: {
                        label: (ctx) => `Anomaly Score: ${parseFloat(ctx.raw || 0).toFixed(3)}`
                    }
                }
            },
            scales: {
                ...commonChartOptions.scales,
                y: { min: 0.0, max: 1.0, ticks: { color: "#94a3b8", stepSize: 0.2 } }
            }
        }
    });

    // Fetch and Update Dashboard Metrics
    async function updateDashboard() {
        try {
            // Fetch KPIs
            const resMetrics = await fetch("/api/metrics");
            const metrics = await resMetrics.json();

            document.getElementById("kpi-revenue").textContent = `$${metrics.total_revenue.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
            document.getElementById("kpi-transactions").textContent = (metrics.total_records_processed || metrics.total_transactions).toLocaleString();
            document.getElementById("kpi-avg-value").textContent = `$${metrics.avg_transaction_value.toFixed(2)}`;
            document.getElementById("kpi-units").textContent = metrics.total_units_sold.toLocaleString();
            document.getElementById("kpi-anomalies").textContent = metrics.total_anomalies.toLocaleString();
            
            const kpiDatasetScale = document.getElementById("kpi-dataset-scale");
            if (kpiDatasetScale && metrics.total_dataset_records) {
                kpiDatasetScale.textContent = `${metrics.total_dataset_records.toLocaleString()} records`;
            }

            const kpiLatency = document.getElementById("kpi-latency");
            if (kpiLatency) {
                kpiLatency.textContent = `${metrics.avg_latency_ms || 0.0} ms`;
            }

            // Throughput KPI logic
            const streamState = metrics.stream_status;
            const isStreamingActive = streamState.is_running && !streamState.is_paused;
            const throughputElem = document.getElementById("kpi-throughput");
            const throughputSubtext = document.getElementById("kpi-throughput-subtext");

            if (isStreamingActive) {
                throughputElem.textContent = `${metrics.records_per_sec} rec/s`;
                if (throughputSubtext) {
                    throughputSubtext.innerHTML = `<i class="fa-solid fa-microchip"></i> Real-time throughput`;
                }
            } else {
                throughputElem.textContent = `Current: 0 rec/s`;
                if (throughputSubtext) {
                    const lastRate = metrics.last_records_per_sec || 0.0;
                    throughputSubtext.innerHTML = `<i class="fa-solid fa-clock-rotate-left"></i> Last: ${lastRate} rec/s`;
                }
            }

            // Update Stream Status Badge & Buttons
            if (isStreamingActive) {
                statusBadge.innerHTML = `<span class="status-dot green"></span> LIVE STREAMING`;
                statusBadge.style.borderColor = "rgba(16, 185, 129, 0.4)";
                btnToggle.innerHTML = `<i class="fa-solid fa-stop"></i> Stop Stream`;
                btnToggle.className = "btn btn-warning";
            } else if (streamState.is_paused) {
                statusBadge.innerHTML = `<span class="status-dot yellow"></span> PAUSED`;
                statusBadge.style.borderColor = "rgba(251, 191, 36, 0.4)";
                btnToggle.innerHTML = `<i class="fa-solid fa-play"></i> Resume`;
                btnToggle.className = "btn btn-success";
            } else {
                statusBadge.innerHTML = `<span class="status-dot red"></span> STOPPED`;
                statusBadge.style.borderColor = "rgba(244, 63, 94, 0.4)";
                btnToggle.innerHTML = `<i class="fa-solid fa-play"></i> Start Stream`;
                btnToggle.className = "btn btn-success";
            }

            // Fetch Charts
            const resCharts = await fetch("/api/charts");
            const chartData = await resCharts.json();

            // Revenue Timeline
            if (chartData.batch_labels) {
                revenueChart.data.labels = chartData.batch_labels;
            } else {
                revenueChart.data.labels = chartData.timestamps;
            }
            revenueChart.data.datasets[0].data = chartData.revenues;
            
            // Custom Tooltip showing Micro-Batch label, actual timestamp, and formatted revenue
            revenueChart.options.plugins.tooltip.callbacks.label = (ctx) => {
                const idx = ctx.dataIndex;
                const batchName = (chartData.batch_labels && chartData.batch_labels[idx]) ? chartData.batch_labels[idx] : `Micro-Batch ${idx + 1}`;
                const timeStr = (chartData.timestamps && chartData.timestamps[idx]) ? chartData.timestamps[idx] : '';
                const revVal = parseFloat(ctx.raw || 0).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
                return [`${batchName} • ${timeStr}`, `Revenue: $${revVal}`];
            };
            revenueChart.update();

            // Country Doughnut
            countryChart.data.labels = Object.keys(chartData.country_distribution);
            countryChart.data.datasets[0].data = Object.values(chartData.country_distribution);
            countryChart.update();

            // Volume Bar
            volumeChart.data.labels = chartData.timestamps;
            volumeChart.data.datasets[0].data = chartData.quantities;
            volumeChart.update();

            // Anomaly Scores
            anomalyChart.data.labels = chartData.timestamps;
            anomalyChart.data.datasets[0].data = chartData.anomaly_scores;
            anomalyChart.update();

            // Fetch Recent Transactions
            const resTx = await fetch("/api/transactions?limit=50");
            latestTransactions = await resTx.json();
            
            // Collect countries for dropdown filter
            latestTransactions.forEach(t => { if (t.Country) availableCountries.add(t.Country); });
            updateCountryDropdown();
            
            renderTransactionsTable();

            // Fetch Flagged Anomalies
            const resAnom = await fetch("/api/anomalies?limit=50");
            latestAnomalies = await resAnom.json();
            renderAnomaliesTable();

        } catch (err) {
            console.error("Error updating dashboard:", err);
        }
    }

    function updateCountryDropdown() {
        const currentVal = filterCountry.value;
        const sortedCountries = Array.from(availableCountries).sort();
        
        let html = `<option value="">All Countries</option>`;
        sortedCountries.forEach(c => {
            const selected = (c === currentVal) ? "selected" : "";
            html += `<option value="${c}" ${selected}>${c}</option>`;
        });
        filterCountry.innerHTML = html;
    }

    function filterRecords(records) {
        const search = searchInput.value.toLowerCase().trim();
        const country = filterCountry.value;
        const status = filterStatus.value;

        return records.filter(item => {
            const invoiceStr = (item.InvoiceNo || "").toString().toLowerCase();
            const descStr = (item.Description || "").toLowerCase();
            const matchesSearch = !search || invoiceStr.includes(search) || descStr.includes(search);

            const matchesCountry = !country || item.Country === country;

            let matchesStatus = true;
            if (status === "anomaly") {
                matchesStatus = item.is_anomaly === true;
            } else if (status === "normal") {
                matchesStatus = !item.is_anomaly && !item.IsCancellation;
            } else if (status === "cancel") {
                matchesStatus = item.IsCancellation === true;
            }

            return matchesSearch && matchesCountry && matchesStatus;
        });
    }

    function renderTransactionsTable() {
        const tbody = document.getElementById("table-transactions-body");
        const filtered = filterRecords(latestTransactions);

        if (!filtered || filtered.length === 0) {
            tbody.innerHTML = `<tr><td colspan="9" class="text-center text-muted">No matching transactions found.</td></tr>`;
            return;
        }

        tbody.innerHTML = filtered.map(t => {
            const isCancel = t.IsCancellation || t.Quantity < 0;
            const isAnom = t.is_anomaly;
            const rowClass = isAnom ? 'class="row-anomaly"' : '';

            const txTypeHtml = t.Quantity > 0 
                ? `<span class="badge badge-sale">Sale</span>` 
                : `<span class="badge badge-return">Return</span>`;

            let badgeHtml = `<span class="badge badge-normal">Normal</span>`;
            if (isAnom) {
                badgeHtml = `<span class="badge badge-anomaly">Anomaly</span>`;
            } else if (isCancel) {
                badgeHtml = `<span class="badge badge-cancel">Cancellation</span>`;
            }

            return `
                <tr ${rowClass}>
                    <td><strong>${t.InvoiceNo || 'N/A'}</strong></td>
                    <td>${t.FormattedDate || 'N/A'}</td>
                    <td>${txTypeHtml}</td>
                    <td>${t.Description || 'Unknown'}</td>
                    <td>${t.Quantity}</td>
                    <td>$${parseFloat(t.UnitPrice || 0).toFixed(2)}</td>
                    <td>$${parseFloat(t.TotalRevenue || 0).toFixed(2)}</td>
                    <td>${t.Country}</td>
                    <td>${badgeHtml}</td>
                </tr>
            `;
        }).join("");
    }

    function getAnomalyExplanation(type) {
        switch(type) {
            case "Isolation Forest Outlier":
                return "Isolation Forest multi-dimensional outlier";
            case "Bulk Order Surge":
                return "Rule-based trigger: Unusually high quantity (>300 units)";
            case "High-Value Item Spurt":
                return "Rule-based trigger: Unusually high price (>$150/unit)";
            case "High-Value Return":
                return "Rule-based trigger: High cancellation value (>$300)";
            case "Statistical Outlier":
                return "Statistical Z-score outlier";
            default:
                return "Standard transaction pattern";
        }
    }

    function getAnomalySeverity(score, type) {
        if (score >= 0.70 || type === "High-Value Return") {
            return `<span class="badge badge-high">High</span>`;
        } else if (score >= 0.55) {
            return `<span class="badge badge-medium">Medium</span>`;
        } else {
            return `<span class="badge badge-low">Low</span>`;
        }
    }

    function renderAnomaliesTable() {
        const tbody = document.getElementById("table-anomalies-body");
        const filtered = filterRecords(latestAnomalies);

        if (!filtered || filtered.length === 0) {
            tbody.innerHTML = `<tr><td colspan="10" class="text-center text-muted">No matching anomalies found.</td></tr>`;
            return;
        }

        tbody.innerHTML = filtered.map(a => {
            const explanation = getAnomalyExplanation(a.anomaly_type);
            const severityHtml = getAnomalySeverity(a.anomaly_score, a.anomaly_type);
            const txTypeHtml = a.Quantity > 0 
                ? `<span class="badge badge-sale">Sale</span>` 
                : `<span class="badge badge-return">Return</span>`;

            return `
                <tr class="row-anomaly">
                    <td><strong>${a.InvoiceNo}</strong></td>
                    <td>${a.FormattedDate || 'N/A'}</td>
                    <td>${txTypeHtml}</td>
                    <td><span class="badge badge-anomaly">${a.anomaly_type}</span></td>
                    <td><small class="text-muted">${explanation}</small></td>
                    <td><strong>${parseFloat(a.anomaly_score).toFixed(3)}</strong></td>
                    <td>${severityHtml}</td>
                    <td>${a.Quantity}</td>
                    <td>$${parseFloat(a.UnitPrice).toFixed(2)}</td>
                    <td>$${parseFloat(a.TotalRevenue).toFixed(2)}</td>
                </tr>
            `;
        }).join("");
    }

    // Filter event listeners
    searchInput.addEventListener("input", () => { renderTransactionsTable(); renderAnomaliesTable(); });
    filterCountry.addEventListener("change", () => { renderTransactionsTable(); renderAnomaliesTable(); });
    filterStatus.addEventListener("change", () => { renderTransactionsTable(); renderAnomaliesTable(); });

    btnClearFilters.addEventListener("click", () => {
        searchInput.value = "";
        filterCountry.value = "";
        filterStatus.value = "";
        renderTransactionsTable();
        renderAnomaliesTable();
    });

    // Stream Controls
    btnToggle.addEventListener("click", async () => {
        const resMetrics = await fetch("/api/metrics");
        const data = await resMetrics.json();
        const state = data.stream_status;

        const action = (state.is_running && !state.is_paused) ? "stop" : (state.is_paused ? "resume" : "start");
        await fetch("/api/stream/control", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ action })
        });
        updateDashboard();
    });

    btnPause.addEventListener("click", async () => {
        await fetch("/api/stream/control", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ action: "pause" })
        });
        updateDashboard();
    });

    btnReset.addEventListener("click", async () => {
        await fetch("/api/stream/control", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ action: "reset" })
        });
        latestTransactions = [];
        latestAnomalies = [];
        updateDashboard();
    });

    speedRange.addEventListener("change", async (e) => {
        const speed = parseFloat(e.target.value);
        speedLabel.textContent = `${speed.toFixed(1)}s`;
        await fetch("/api/stream/control", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ action: "set_speed", speed })
        });
    });

    // Tab Navigation Switcher
    const tabButtons = document.querySelectorAll(".tab-btn");
    const tabPanes = document.querySelectorAll(".tab-pane");

    tabButtons.forEach(btn => {
        btn.addEventListener("click", () => {
            const targetTab = btn.getAttribute("data-tab");

            tabButtons.forEach(b => b.classList.remove("active"));
            tabPanes.forEach(p => p.classList.remove("active"));

            btn.classList.add("active");
            const activePane = document.getElementById(targetTab);
            if (activePane) {
                activePane.classList.add("active");
            }

            // Resize charts so canvas updates layout properly when unhidden
            setTimeout(() => {
                revenueChart?.resize();
                countryChart?.resize();
                volumeChart?.resize();
                anomalyChart?.resize();
            }, 50);
        });
    });

    // Initial load and 1.5s Polling Loop
    updateDashboard();
    setInterval(updateDashboard, 1500);
});
