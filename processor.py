import sys
import os
import time
import json
import sqlite3
from collections import deque
import pandas as pd
import numpy as np

# Ensure parent directory is in python path for models import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from models.anomaly_detector import AnomalyDetector

class DataProcessor:
    """
    Processes incoming micro-batches of real-time transactions.
    Performs data cleaning, feature engineering, Isolation Forest anomaly scoring, and maintains running BI metrics.
    Persists all transactions and cumulative metrics to an SQLite database for seamless restart survival.
    """
    def __init__(self, db_path='data/pipeline_storage.db', csv_output_path='data/processed_transactions.csv', max_history=1000):
        self.db_path = db_path
        self.csv_output_path = csv_output_path
        self.max_history = max_history
        self.processed_history = deque(maxlen=max_history)
        self.anomaly_detector = AnomalyDetector()
        
        # Cumulative KPIs
        self.total_transactions = 0
        self.total_revenue = 0.0
        self.total_units_sold = 0
        self.total_anomalies = 0
        
        # Performance & Latency Tracking
        self.session_start_time = time.time()
        self.session_records_processed = 0
        self.total_records_processed = 0
        self.last_records_per_sec = 0.0
        self.last_batch_latency_ms = 0.0
        self.latency_history = deque(maxlen=100)
        
        # Revenue by Country dictionary
        self.country_revenue = {}
        
        self._init_db()
        self._load_persisted_data()

    def _get_db_connection(self):
        """Returns a thread-safe sqlite3 connection."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Initializes SQLite schema for persistent storage."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    InvoiceNo TEXT,
                    StockCode TEXT,
                    Description TEXT,
                    Quantity INTEGER,
                    UnitPrice REAL,
                    TotalRevenue REAL,
                    CustomerID TEXT,
                    Country TEXT,
                    IsCancellation INTEGER,
                    FormattedDate TEXT,
                    Hour INTEGER,
                    DayOfWeek TEXT,
                    is_anomaly INTEGER,
                    anomaly_score REAL,
                    anomaly_type TEXT,
                    tx_type TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS kpis (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            try:
                cursor.execute('ALTER TABLE transactions ADD COLUMN tx_type TEXT')
            except Exception:
                pass
            cursor.execute("UPDATE transactions SET anomaly_type = 'Isolation Forest Outlier' WHERE anomaly_type = 'Statistical Outlier'")
            cursor.execute("UPDATE transactions SET tx_type = CASE WHEN Quantity > 0 THEN 'Sale' ELSE 'Return' END WHERE tx_type IS NULL OR tx_type = ''")
            conn.commit()

    def _load_persisted_data(self):
        """Restores past transactions and cumulative KPI counters from SQLite."""
        try:
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Check total row count in database
                cursor.execute("SELECT COUNT(*) FROM transactions")
                db_tx_count = cursor.fetchone()[0]
                
                # Load stored KPIs
                cursor.execute("SELECT key, value FROM kpis")
                kpi_map = {row['key']: row['value'] for row in cursor.fetchall()}
                
                self.total_transactions = max(int(kpi_map.get('total_transactions', 0)), db_tx_count)
                self.total_revenue = float(kpi_map.get('total_revenue', 0.0))
                self.total_units_sold = int(kpi_map.get('total_units_sold', 0))
                self.total_anomalies = int(kpi_map.get('total_anomalies', 0))
                self.total_records_processed = max(int(kpi_map.get('total_records_processed', 0)), db_tx_count)
                
                if 'country_revenue' in kpi_map:
                    self.country_revenue = json.loads(kpi_map['country_revenue'])
                    
                # Load last max_history transactions into deque
                cursor.execute("SELECT * FROM transactions ORDER BY id DESC LIMIT ?", (self.max_history,))
                rows = cursor.fetchall()
                
                # Reverse to maintain chronological order in deque
                for r in reversed(rows):
                    rec = dict(r)
                    rec['IsCancellation'] = bool(rec['IsCancellation'])
                    rec['is_anomaly'] = bool(rec['is_anomaly'])
                    if rec.get('anomaly_type') == 'Statistical Outlier':
                        rec['anomaly_type'] = 'Isolation Forest Outlier'
                    if not rec.get('tx_type'):
                        rec['tx_type'] = 'Sale' if int(rec.get('Quantity', 0)) > 0 else 'Return'
                    self.processed_history.append(rec)
                    
                print(f"[DataProcessor] Restored {len(self.processed_history)} recent transactions & KPIs from SQLite database (Total TX: {self.total_transactions}).")
        except Exception as e:
            print(f"[DataProcessor] Error restoring persisted data: {e}")

    def _save_kpis_to_db(self):
        """Saves cumulative KPIs to SQLite database."""
        try:
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                kpi_data = [
                    ('total_transactions', str(self.total_transactions)),
                    ('total_revenue', str(self.total_revenue)),
                    ('total_units_sold', str(self.total_units_sold)),
                    ('total_anomalies', str(self.total_anomalies)),
                    ('total_records_processed', str(self.total_records_processed)),
                    ('country_revenue', json.dumps(self.country_revenue))
                ]
                cursor.executemany("INSERT OR REPLACE INTO kpis (key, value) VALUES (?, ?)", kpi_data)
                conn.commit()
        except Exception as e:
            print(f"[DataProcessor] Error saving KPIs to SQLite: {e}")

    def process_batch(self, raw_batch):
        """
        Cleans, engineers features, scores anomalies, updates metrics, and persists to SQLite.
        Returns the list of processed records.
        """
        if not raw_batch:
            return []
            
        start_time = time.perf_counter()
        df = pd.DataFrame(raw_batch)
        
        # 1. Data Cleansing & Validation
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0).astype(int)
        df['UnitPrice'] = pd.to_numeric(df['UnitPrice'], errors='coerce').fillna(0.0).astype(float)
        df['Description'] = df['Description'].fillna('Unknown Product').astype(str)
        df['CustomerID'] = df['CustomerID'].fillna('Unregistered').astype(str)
        df['Country'] = df['Country'].fillna('Unknown').astype(str)
        
        # Remove exact duplicate rows in the micro-batch
        df = df.drop_duplicates().reset_index(drop=True)
        
        # 2. Feature Engineering
        # Calculate transaction value (revenue)
        df['TotalRevenue'] = df['Quantity'] * df['UnitPrice']
        
        # Flag cancellation transactions (starts with 'C' or negative quantity)
        df['IsCancellation'] = df['InvoiceNo'].astype(str).str.startswith('C') | (df['Quantity'] < 0)
        
        # Datetime features
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
        df['Hour'] = df['InvoiceDate'].dt.hour.fillna(12).astype(int)
        df['DayOfWeek'] = df['InvoiceDate'].dt.day_name().fillna('Unknown')
        df['FormattedDate'] = df['InvoiceDate'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
        
        # 3. Anomaly Detection via Isolation Forest & Rules
        anomaly_results = self.anomaly_detector.predict_batch(df)
        
        # Convert back to dictionary records
        processed_records = df.to_dict(orient='records')
        
        # 4. Update BI Metrics & History Buffer
        db_rows = []
        for idx, rec in enumerate(processed_records):
            self.total_transactions += 1
            self.total_records_processed += 1
            self.session_records_processed += 1
            revenue = float(rec['TotalRevenue'])
            qty = int(rec['Quantity'])
            country = rec['Country']
            
            self.total_revenue += revenue
            if qty > 0:
                self.total_units_sold += qty
                
            self.country_revenue[country] = self.country_revenue.get(country, 0.0) + revenue
            
            # Attach transaction type (Sale vs Return) and anomaly scoring
            rec['tx_type'] = 'Sale' if qty > 0 else 'Return'
            anom_info = anomaly_results[idx]
            rec['is_anomaly'] = anom_info['is_anomaly']
            rec['anomaly_score'] = anom_info['anomaly_score']
            rec['anomaly_type'] = anom_info['anomaly_type']
            
            if rec['is_anomaly']:
                self.total_anomalies += 1
            
            self.processed_history.append(rec)
            
            db_rows.append((
                str(rec.get('InvoiceNo', '')),
                str(rec.get('StockCode', '')),
                str(rec.get('Description', '')),
                int(rec['Quantity']),
                float(rec['UnitPrice']),
                float(rec['TotalRevenue']),
                str(rec.get('CustomerID', '')),
                str(rec['Country']),
                1 if rec['IsCancellation'] else 0,
                str(rec['FormattedDate']),
                int(rec['Hour']),
                str(rec['DayOfWeek']),
                1 if rec['is_anomaly'] else 0,
                float(rec['anomaly_score']),
                str(rec['anomaly_type']),
                str(rec['tx_type'])
            ))

        # Insert records into SQLite
        try:
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany('''
                    INSERT INTO transactions (
                        InvoiceNo, StockCode, Description, Quantity, UnitPrice, TotalRevenue,
                        CustomerID, Country, IsCancellation, FormattedDate, Hour, DayOfWeek,
                        is_anomaly, anomaly_score, anomaly_type, tx_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', db_rows)
                conn.commit()
        except Exception as e:
            print(f"[DataProcessor] Error persisting batch to SQLite: {e}")
            
        self._append_to_csv(processed_records)
        self._save_kpis_to_db()
        
        # Measure processing latency
        end_time = time.perf_counter()
        batch_latency_ms = round((end_time - start_time) * 1000, 2)
        self.last_batch_latency_ms = batch_latency_ms
        self.latency_history.append(batch_latency_ms)
        
        return processed_records

    def _append_to_csv(self, processed_records):
        """Appends processed micro-batch records to a CSV output file."""
        if not processed_records:
            return
        try:
            os.makedirs(os.path.dirname(self.csv_output_path), exist_ok=True)
            file_exists = os.path.exists(self.csv_output_path) and os.path.getsize(self.csv_output_path) > 0
            
            df_batch = pd.DataFrame(processed_records)
            columns = [
                'InvoiceNo', 'StockCode', 'Description', 'Quantity', 'UnitPrice', 
                'TotalRevenue', 'CustomerID', 'Country', 'IsCancellation', 'tx_type',
                'FormattedDate', 'Hour', 'DayOfWeek', 'is_anomaly', 
                'anomaly_score', 'anomaly_type'
            ]
            for col in columns:
                if col not in df_batch.columns:
                    df_batch[col] = ''
            df_batch = df_batch[columns]
            
            df_batch.to_csv(self.csv_output_path, mode='a', header=not file_exists, index=False)
        except Exception as e:
            print(f"[DataProcessor] Error appending batch to CSV ({self.csv_output_path}): {e}")

    def reset_session(self):
        """Resets active throughput session counter and start timestamp."""
        self.session_start_time = time.time()
        self.session_records_processed = 0

    def reset(self):
        """Resets in-memory metrics, clears SQLite storage, and removes generated CSV file."""
        self.total_transactions = 0
        self.total_revenue = 0.0
        self.total_units_sold = 0
        self.total_anomalies = 0
        self.total_records_processed = 0
        self.session_records_processed = 0
        self.last_records_per_sec = 0.0
        self.last_batch_latency_ms = 0.0
        self.latency_history.clear()
        self.country_revenue.clear()
        self.processed_history.clear()
        self.session_start_time = time.time()
        
        try:
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM transactions")
                cursor.execute("DELETE FROM kpis")
                conn.commit()
                print("[DataProcessor] Cleared SQLite transactions and KPIs.")
        except Exception as e:
            print(f"[DataProcessor] Error resetting SQLite storage: {e}")
            
        try:
            if os.path.exists(self.csv_output_path):
                os.remove(self.csv_output_path)
                print(f"[DataProcessor] Cleared CSV output file: {self.csv_output_path}")
        except Exception as e:
            print(f"[DataProcessor] Error resetting CSV file: {e}")

    def get_kpis(self):
        """Returns current BI Key Performance Indicators."""
        avg_value = (self.total_revenue / self.total_transactions) if self.total_transactions > 0 else 0.0
        elapsed_seconds = max(0.1, time.time() - self.session_start_time)
        records_per_sec = round(self.session_records_processed / elapsed_seconds, 2)
        
        if records_per_sec > 0:
            self.last_records_per_sec = records_per_sec
            
        avg_latency_ms = round(sum(self.latency_history) / len(self.latency_history), 2) if self.latency_history else 0.0
        
        # Recent Rolling Window metrics (last 50 transactions)
        recent_items = list(self.processed_history)[-50:]
        recent_count = len(recent_items)
        recent_revenue = sum(r['TotalRevenue'] for r in recent_items) if recent_items else 0.0
        
        return {
            "total_transactions": self.total_transactions,
            "total_records_processed": self.total_records_processed,
            "total_revenue": round(self.total_revenue, 2),
            "avg_transaction_value": round(avg_value, 2),
            "total_units_sold": self.total_units_sold,
            "total_anomalies": self.total_anomalies,
            "records_per_sec": records_per_sec,
            "last_records_per_sec": self.last_records_per_sec,
            "avg_latency_ms": avg_latency_ms,
            "last_batch_latency_ms": self.last_batch_latency_ms,
            "recent_rolling_count": recent_count,
            "recent_rolling_revenue": round(recent_revenue, 2)
        }

    def get_recent_transactions(self, limit=20):
        """Returns the N most recent processed transactions."""
        items = list(self.processed_history)[-limit:]
        return list(reversed(items))

    def get_revenue_by_country(self, top_n=5):
        """Returns top N countries by revenue and groups remaining as Others."""
        sorted_countries = sorted(self.country_revenue.items(), key=lambda x: x[1], reverse=True)
        top_countries = sorted_countries[:top_n]
        others_rev = sum(val for _, val in sorted_countries[top_n:])
        
        result = {k: round(v, 2) for k, v in top_countries}
        if others_rev > 0:
            result['Others'] = round(others_rev, 2)
        return result

    def get_revenue_timeline(self, limit=30):
        """Returns timestamps and revenue for line charts."""
        items = list(self.processed_history)[-limit:]
        timestamps = [item['FormattedDate'] for item in items]
        revenues = [round(item['TotalRevenue'], 2) for item in items]
        return {"timestamps": timestamps, "revenues": revenues}

if __name__ == "__main__":
    # Standalone sanity test for DataProcessor
    print("Testing DataProcessor with SQLite persistence...")
    processor = DataProcessor()
    
    sample_batch = [
        {"InvoiceNo": "536365", "StockCode": "85123A", "Description": "WHITE HANGING HEART", "Quantity": 6, "InvoiceDate": "2010-12-01 08:26:00", "UnitPrice": 2.55, "CustomerID": "17850", "Country": "United Kingdom"},
        {"InvoiceNo": "536365", "StockCode": "71053", "Description": None, "Quantity": 2, "InvoiceDate": "2010-12-01 08:26:00", "UnitPrice": 3.39, "CustomerID": None, "Country": "United Kingdom"},
        {"InvoiceNo": "C536379", "StockCode": "D", "Description": "Discount", "Quantity": -1, "InvoiceDate": "2010-12-01 09:41:00", "UnitPrice": 27.50, "CustomerID": "14527", "Country": "United Kingdom"}
    ]
    
    processed = processor.process_batch(sample_batch)
    print(f"Processed {len(processed)} records successfully.")
    print("KPIs:", processor.get_kpis())
    print("Revenue by Country:", processor.get_revenue_by_country())
