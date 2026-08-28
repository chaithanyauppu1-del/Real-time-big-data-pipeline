import os
import sys
import threading
import webbrowser
from flask import Flask, render_template, jsonify, request, send_file

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from stream.stream import StreamSimulator
from processing.processor import DataProcessor

app = Flask(__name__)

# Initialize Core Services
stream_simulator = StreamSimulator(dataset_path='data/dataset.csv', batch_size=5, delay=1.0)
data_processor = DataProcessor(max_history=1000)

# Register callback so stream feeds processor
def handle_stream_batch(batch):
    data_processor.process_batch(batch)

# Auto-start streaming background simulator
stream_simulator.start(callback=handle_stream_batch)

def open_browser():
    """Automatically opens the default web browser to the local dashboard URL."""
    webbrowser.open_new("http://127.0.0.1:5000/")

@app.route('/')
def index():
    """Renders the main Business Intelligence Dashboard."""
    return render_template('index.html')

@app.route('/api/status', methods=['GET'])
def get_status():
    """Returns streaming engine status."""
    return jsonify(stream_simulator.get_status())

@app.route('/api/stream/control', methods=['POST'])
def stream_control():
    """API endpoint to start, stop, pause, resume, or adjust streaming parameters."""
    data = request.get_json() or {}
    action = data.get('action')
    
    if action == 'start':
        data_processor.reset_session()
        stream_simulator.start(callback=handle_stream_batch)
    elif action == 'stop':
        stream_simulator.stop()
    elif action == 'pause':
        stream_simulator.pause()
    elif action == 'resume':
        data_processor.reset_session()
        stream_simulator.resume()
    elif action == 'reset':
        stream_simulator.reset()
        data_processor.reset()
    elif action == 'set_speed':
        speed = float(data.get('speed', 1.0))
        stream_simulator.set_speed(speed)
    elif action == 'set_batch_size':
        size = int(data.get('batch_size', 5))
        stream_simulator.set_batch_size(size)
    else:
        return jsonify({"status": "error", "message": f"Unknown action: {action}"}), 400
        
    return jsonify({"status": "success", "stream_state": stream_simulator.get_status()})

@app.route('/api/metrics', methods=['GET'])
def get_metrics():
    """Returns real-time BI Key Performance Indicators."""
    kpis = data_processor.get_kpis()
    stream_info = stream_simulator.get_status()
    kpis['stream_status'] = stream_info
    
    # Dataset scale metadata (dynamic from loaded dataset file)
    kpis['total_dataset_records'] = stream_simulator.total_records
    kpis['processing_mode'] = "Simulated Micro-Batch Stream"
    kpis['dataset_path'] = stream_simulator.dataset_path
    
    is_active = stream_info.get('is_running') and not stream_info.get('is_paused')
    kpis['is_active_streaming'] = is_active
    if not is_active:
        kpis['records_per_sec'] = 0.0
        
    return jsonify(kpis)

@app.route('/api/charts', methods=['GET'])
def get_charts():
    """Returns aggregated dynamic datasets for Chart.js rendering."""
    history = list(data_processor.processed_history)
    recent = history[-30:] if history else []
    
    # Clean micro-batch sequence labels for X-axis visual clarity
    batch_labels = [f"Micro-Batch {i+1}" for i in range(len(recent))]
    timestamps = [r['FormattedDate'] for r in recent]
    revenues = [round(r['TotalRevenue'], 2) for r in recent]
    quantities = [r['Quantity'] for r in recent]
    anom_scores = [r['anomaly_score'] for r in recent]
    invoices = [r.get('InvoiceNo', '') for r in recent]
    descriptions = [r.get('Description', '') for r in recent]
    
    country_dist = data_processor.get_revenue_by_country(top_n=5)
    
    return jsonify({
        "batch_labels": batch_labels,
        "timestamps": timestamps,
        "revenues": revenues,
        "quantities": quantities,
        "anomaly_scores": anom_scores,
        "invoices": invoices,
        "descriptions": descriptions,
        "country_distribution": country_dist
    })

@app.route('/api/transactions', methods=['GET'])
def get_transactions():
    """Returns the most recent N processed transaction records."""
    limit = int(request.args.get('limit', 50))
    recent = data_processor.get_recent_transactions(limit=limit)
    return jsonify(recent)

@app.route('/api/anomalies', methods=['GET'])
def get_anomalies():
    """Returns flagged anomalies from the recent processing buffer."""
    limit = int(request.args.get('limit', 50))
    all_history = list(data_processor.processed_history)
    anomalies = [r for r in reversed(all_history) if r.get('is_anomaly', False)][:limit]
    return jsonify(anomalies)

@app.route('/api/download/csv', methods=['GET'])
def download_csv():
    """Allows downloading the generated processed transactions CSV file."""
    csv_path = data_processor.csv_output_path
    if os.path.exists(csv_path):
        return send_file(csv_path, as_attachment=True, download_name="processed_transactions.csv", mimetype="text/csv")
    return jsonify({"status": "error", "message": "CSV file has not been generated yet."}), 404

if __name__ == '__main__':
    print("Starting Flask Real-Time Analytics Pipeline Dashboard on http://127.0.0.1:5000")
    
    app.debug = True
    is_worker = (os.environ.get('WERKZEUG_RUN_MAIN') == 'true') if app.debug else True
    
    if is_worker:
        threading.Timer(1.25, open_browser).start()
        
    try:
        app.run(host='0.0.0.0', port=5000, debug=True)
    finally:
        if is_worker:
            stream_simulator.stop()

