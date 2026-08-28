import threading
import time
import json
import pandas as pd
import os

class StreamSimulator:
    """
    Simulates a real-time data stream by replaying historical e-commerce transactions
    chronologically in configurable micro-batches. Persists stream pointer state to disk.
    """
    def __init__(self, dataset_path='data/dataset.csv', state_path='data/stream_state.json', batch_size=5, delay=1.0):
        self.dataset_path = dataset_path
        self.state_path = state_path
        self.batch_size = batch_size
        self.delay = delay
        
        self.df = None
        self.total_records = 0
        self.current_index = 0
        
        self.is_running = False
        self.is_paused = False
        self._thread = None
        self._lock = threading.Lock()
        
        # Buffer for streamed raw batches
        self.buffer = []
        
        self._load_dataset()
        self._load_state()

    def _load_dataset(self):
        """Loads and verifies dataset chronologically."""
        if not os.path.exists(self.dataset_path):
            raise FileNotFoundError(f"Dataset file not found at {self.dataset_path}. Run Phase 1 first.")
            
        self.df = pd.read_csv(self.dataset_path)
        self.total_records = len(self.df)
        print(f"[StreamSimulator] Loaded {self.total_records} records from {self.dataset_path}")

    def _load_state(self):
        """Loads persisted stream pointer from disk if present."""
        if os.path.exists(self.state_path):
            try:
                with open(self.state_path, 'r') as f:
                    data = json.load(f)
                    self.current_index = int(data.get('current_index', 0))
                    self.is_paused = bool(data.get('is_paused', False))
                    print(f"[StreamSimulator] Resumed stream pointer at index {self.current_index} (paused: {self.is_paused})")
            except Exception as e:
                print(f"[StreamSimulator] Error loading state, starting from index 0: {e}")
                self.current_index = 0
                self.is_paused = False

    def _save_state(self):
        """Saves current stream pointer to disk."""
        try:
            os.makedirs(os.path.dirname(self.state_path), exist_ok=True)
            with open(self.state_path, 'w') as f:
                json.dump({
                    "current_index": self.current_index,
                    "is_paused": self.is_paused
                }, f)
        except Exception as e:
            print(f"[StreamSimulator] Error saving state: {e}")

    def start(self, callback=None):
        """Starts the real-time simulation background thread."""
        with self._lock:
            if self.is_running:
                print("[StreamSimulator] Stream is already running.")
                return
            self.is_running = True
            
        self._thread = threading.Thread(target=self._stream_loop, args=(callback,), daemon=True)
        self._thread.start()
        print(f"[StreamSimulator] Streaming background thread started (paused: {self.is_paused}).")

    def stop(self):
        """Stops the streaming thread."""
        with self._lock:
            self.is_running = False
        self._save_state()
        print("[StreamSimulator] Stream stopped.")

    def pause(self):
        """Pauses data streaming."""
        with self._lock:
            self.is_paused = True
        self._save_state()
        print("[StreamSimulator] Stream paused.")

    def resume(self):
        """Resumes data streaming."""
        with self._lock:
            self.is_paused = False
        self._save_state()
        print("[StreamSimulator] Stream resumed.")

    def reset(self):
        """Resets stream pointer back to start."""
        with self._lock:
            self.current_index = 0
            self.is_paused = False
            self.buffer.clear()
        self._save_state()
        print("[StreamSimulator] Stream reset to beginning.")

    def set_speed(self, delay):
        """Adjusts delay (seconds) between batch emissions."""
        with self._lock:
            self.delay = max(0.1, float(delay))
        print(f"[StreamSimulator] Stream delay updated to {self.delay}s")

    def set_batch_size(self, size):
        """Adjusts number of records per batch."""
        with self._lock:
            self.batch_size = max(1, int(size))
        print(f"[StreamSimulator] Batch size updated to {self.batch_size}")

    def fetch_next_batch(self):
        """Fetches the next chunk of records without thread loop."""
        with self._lock:
            if self.current_index >= self.total_records:
                # Reset to beginning for continuous streaming demonstration
                self.current_index = 0
                
            start = self.current_index
            end = min(start + self.batch_size, self.total_records)
            batch = self.df.iloc[start:end].to_dict(orient='records')
            self.current_index = end
            self._save_state()
            return batch

    def _stream_loop(self, callback=None):
        """Worker loop that continuously fetches and emits batches."""
        while self.is_running:
            if self.is_paused:
                time.sleep(0.5)
                continue
                
            batch = self.fetch_next_batch()
            if batch:
                with self._lock:
                    self.buffer.extend(batch)
                if callback:
                    try:
                        callback(batch)
                    except Exception as e:
                        print(f"[StreamSimulator] Error in batch callback: {e}")
                        
            time.sleep(self.delay)

    def get_status(self):
        """Returns current stream state metrics."""
        with self._lock:
            progress_pct = round((self.current_index / self.total_records) * 100, 2) if self.total_records > 0 else 0
            return {
                "is_running": self.is_running,
                "is_paused": self.is_paused,
                "current_index": self.current_index,
                "total_records": self.total_records,
                "progress_percent": progress_pct,
                "batch_size": self.batch_size,
                "delay_seconds": self.delay
            }

if __name__ == "__main__":
    # Quick standalone sanity test
    print("Testing StreamSimulator...")
    simulator = StreamSimulator(batch_size=5, delay=0.5)
    
    received_batches = []
    def on_batch_received(batch):
        received_batches.append(batch)
        print(f"-> [Received Batch] {len(batch)} items | Sample Invoice: {batch[0]['InvoiceNo']} | Date: {batch[0]['InvoiceDate']}")

    simulator.start(callback=on_batch_received)
    time.sleep(2.5)
    simulator.stop()
    
    status = simulator.get_status()
    print(f"Test completed. Total batches processed: {len(received_batches)}")
    print("Stream Status:", status)
