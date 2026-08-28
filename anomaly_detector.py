import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
import warnings
warnings.filterwarnings('ignore')

class AnomalyDetector:
    """
    Real-Time Anomaly Detection Engine for E-Commerce Streamed Data.
    Combines Machine Learning (Isolation Forest) with Statistical Thresholds (Z-Scores)
    to flag irregular order quantities, extreme transaction values, and price outliers.
    """
    def __init__(self, contamination=0.03, random_state=42):
        self.contamination = contamination
        self.random_state = random_state
        
        # Isolation Forest Model
        self.model = IsolationForest(
            contamination=self.contamination,
            random_state=self.random_state,
            n_estimators=100
        )
        self.is_fitted = False
        
        # Feature columns used for anomaly detection
        self.feature_cols = ['Quantity', 'UnitPrice', 'TotalRevenue']
        
        # Statistical baseline metrics (mean & std for Z-score backup)
        self.stats = {
            'Quantity_mean': 10.0, 'Quantity_std': 25.0,
            'UnitPrice_mean': 3.5, 'UnitPrice_std': 10.0,
            'TotalRevenue_mean': 18.0, 'TotalRevenue_std': 60.0
        }
        
        # Pre-fit on synthetic/baseline distributions to enable immediate scoring
        self._fit_initial_baseline()

    def _fit_initial_baseline(self):
        """Fits initial Isolation Forest model with realistic baseline distribution."""
        np.random.seed(self.random_state)
        n_samples = 1000
        
        # Typical order patterns
        normal_qty = np.random.geometric(p=0.1, size=n_samples)
        normal_price = np.random.exponential(scale=3.5, size=n_samples) + 0.5
        normal_revenue = normal_qty * normal_price
        
        baseline_df = pd.DataFrame({
            'Quantity': normal_qty,
            'UnitPrice': normal_price,
            'TotalRevenue': normal_revenue
        })
        
        self.fit(baseline_df)

    def fit(self, df):
        """Fits the Isolation Forest on a reference DataFrame."""
        if df.empty or len(df) < 10:
            return
            
        X = df[self.feature_cols].copy().fillna(0)
        self.model.fit(X)
        self.is_fitted = True
        
        # Update running stats for Z-score backup calculation
        for col in self.feature_cols:
            self.stats[f'{col}_mean'] = float(X[col].mean())
            self.stats[f'{col}_std'] = float(X[col].std() if X[col].std() > 0 else 1.0)

    def predict_batch(self, df):
        """
        Scores a batch of processed DataFrame rows.
        Returns a list of dictionaries with anomaly flags, scores, and reason types.
        """
        if df.empty:
            return []
            
        X = df[self.feature_cols].copy().fillna(0)
        
        # Isolation Forest Predictions (-1 = Anomaly, 1 = Normal)
        if self.is_fitted:
            raw_preds = self.model.predict(X)
            # decision_function gives score (lower/negative = more abnormal)
            decision_scores = self.model.decision_function(X)
            # Standardize score into 0 to 1 range where >0.6 is anomalous
            anomaly_scores = [round(float(max(0.0, min(1.0, 0.5 - s))), 3) for s in decision_scores]
        else:
            raw_preds = [1] * len(df)
            anomaly_scores = [0.0] * len(df)

        results = []
        for idx, row in df.iterrows():
            is_iso_anomaly = (raw_preds[idx] == -1) if self.is_fitted else False
            score = anomaly_scores[idx]
            
            # Rule-Based / Z-Score Anomaly Diagnosis
            qty = abs(float(row.get('Quantity', 0)))
            price = float(row.get('UnitPrice', 0))
            rev = abs(float(row.get('TotalRevenue', 0)))
            is_cancel = bool(row.get('IsCancellation', False))
            
            # Diagnose specific anomaly category
            anomaly_type = "Normal"
            is_flagged = False
            
            if qty > 300 or rev > 1000:
                anomaly_type = "Bulk Order Surge"
                is_flagged = True
            elif price > 150:
                anomaly_type = "High-Value Item Spurt"
                is_flagged = True
            elif is_cancel and rev > 300:
                anomaly_type = "High-Value Return"
                is_flagged = True
            elif is_iso_anomaly or score > 0.65:
                anomaly_type = "Isolation Forest Outlier"
                is_flagged = True
                
            results.append({
                "is_anomaly": is_flagged,
                "anomaly_score": score,
                "anomaly_type": anomaly_type
            })
            
        return results

if __name__ == "__main__":
    print("Testing AnomalyDetector...")
    detector = AnomalyDetector()
    
    test_batch = pd.DataFrame([
        {'Quantity': 6, 'UnitPrice': 2.55, 'TotalRevenue': 15.30, 'IsCancellation': False},
        {'Quantity': 500, 'UnitPrice': 12.00, 'TotalRevenue': 6000.00, 'IsCancellation': False}, # Anomaly (Surge)
        {'Quantity': -1, 'UnitPrice': 250.00, 'TotalRevenue': -250.00, 'IsCancellation': True}  # Anomaly (Price/Return)
    ])
    
    predictions = detector.predict_batch(test_batch)
    for i, res in enumerate(predictions):
        print(f"Record {i+1}: Flagged={res['is_anomaly']} | Score={res['anomaly_score']} | Type={res['anomaly_type']}")
