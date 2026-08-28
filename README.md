# Real-Time Big Data Analytics Pipeline & Isolation Forest Anomaly Detector

A high-performance, production-ready Python & Flask Data Science portfolio project that simulates a real-time e-commerce transaction stream, performs micro-batch feature engineering, detects anomalies with an Isolation Forest Machine Learning model, and visualizes live Business Intelligence (BI) metrics on an interactive Chart.js dashboard.

---

## 🌟 Architecture Overview

```
                          ┌────────────────────────┐
                          │   UCI Online Retail    │
                          │      Dataset CSV       │
                          └───────────┬────────────┘
                                      │
                                      ▼
                          ┌────────────────────────┐
                          │ Stream Replay Engine   │
                          │   (stream/stream.py)   │
                          └───────────┬────────────┘
                                      │ Micro-batches (1-2s delay)
                                      ▼
                          ┌────────────────────────┐
                          │ Real-Time Processor    │
                          │ (processing/proc.py)   │
                          └───────────┬────────────┘
                                      │ Cleaned & Engineered DF
                                      ▼
                          ┌────────────────────────┐
                          │ Isolation Forest Model │
                          │(models/anomaly_det.py) │
                          └───────────┬────────────┘
                                      │ Scored Micro-Batches + BI KPIs
                                      ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   Flask REST API Server (app.py)                       │
│    /api/metrics  |  /api/charts  |  /api/transactions  | /api/anomalies  │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ 1.5s Polling (JSON)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│               Interactive Modern BI Dashboard (UI)                     │
│    KPI Cards  |  Revenue Chart  |  Country Chart  |  Anomaly Table     │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 🔑 Key Features & Technical Highlights

1. **Chronological Streaming Replay**:
   - Historical transaction data is replayed as a simulated real-time micro-batch stream.
   - Replays historical transaction micro-batches in real-time.
   - Configurable stream controls: Start, Pause, Resume, Reset, and Speed Slider (0.1s to 2.0s per batch).

2. **Micro-Batch Cleansing & Feature Engineering**:
   - Handles missing product descriptions, customer IDs, and unit price formatting.
   - Categorizes positive quantities as `Sale` transactions and negative quantities as `Return` transactions.
   - Engineers `TotalRevenue` (`Quantity * UnitPrice`), `IsCancellation` flags, and extracted datetime features (`Hour`, `DayOfWeek`).

3. **Machine Learning Anomaly Detection**:
   - Implements `scikit-learn` **Isolation Forest** trained on transaction baseline data.
   - Combines statistical Z-scores and ML anomaly scores to detect:
     - **Bulk Order Surges** (Excessive quantities)
     - **High-Value Item Spurts** (Outlier prices)
     - **High-Value Cancellations** (Abnormal return values)
     - **Isolation Forest Outliers** (Multi-dimensional Isolation Forest flags)

4. **Live BI Dashboard (Chart.js + Modern UI)**:
   - Dynamic dark theme UI built with HTML5, CSS Grid/Flexbox, and FontAwesome icons.
   - Real-time Chart.js visualizers:
     - **Revenue Stream Line Chart**
     - **Top 5 Revenue by Country Doughnut Chart**
     - **Order Quantity Volume Bar Chart**
     - **Isolation Forest Anomaly Score Trend**
   - Live transaction feed table & dedicated anomaly log.

---

## 🛠️ Project Structure

```
Project 1/
├── app.py                      # Flask Application Server & REST API Routes
├── requirements.txt            # Python Dependencies
├── README.md                   # Complete Documentation & Architecture
│
├── data/
│   └── dataset.csv             # UCI Online Retail Dataset (500k+ transactions)
│
├── stream/
│   └── stream.py               # Thread-Safe Chronological Batch Streaming Engine
│
├── processing/
│   └── processor.py            # Real-Time Cleansing, Feature Engineering & BI Aggregator
│
├── models/
│   └── anomaly_detector.py     # Isolation Forest & Rule-Based Anomaly Scoring Engine
│
├── templates/
│   └── index.html              # Responsive BI Dashboard Layout
│
└── static/
    ├── style.css               # Modern Dark-Theme Dashboard Styles
    └── dashboard.js            # Real-Time Chart.js Polling & Interactive UI Controller
```

---

## 🚀 How to Run locally

### 1. Activate the Virtual Environment
```powershell
.\.venv\Scripts\Activate.ps1
```

### 2. Start the Flask Server & Real-Time Pipeline
```powershell
python app.py
```

### 3. View the Dashboard
Open your browser and navigate to:
```
http://127.0.0.1:5000/
```

---

## 💼 Interview Talking Points

* **Streaming Architecture**: Explains how streaming micro-batches simulate real-time message queues (like Kafka or RabbitMQ) in a lightweight local Python environment.
* **Isolation Forest Choice**: Isolation Forest isolates anomalies by randomly selecting a feature and splitting values. Unsupervised tree partitioning makes it ideally suited for high-speed streaming without requiring labeled anomaly data.
* **Feature Engineering in Micro-batches**: Demonstrates how rolling windows and real-time aggregation compute metrics on the fly without database bottlenecks.
