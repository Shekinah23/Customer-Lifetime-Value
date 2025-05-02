import sqlite3
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from datetime import datetime, timedelta
import joblib

class TransactionPatternAnalyzer:
    def __init__(self, db_path='banking_data.db'):
        self.db_path = db_path
        self.scaler = StandardScaler()
        self.kmeans = None
        self.isolation_forest = None
        
    def load_transactions(self, days_back=365):
        """Load transaction data from database"""
        conn = sqlite3.connect(self.db_path)
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        query = """
        SELECT 
            t.transaction_id,
            t.client_id,
            t.transaction_date,
            t.amount,
            t.transaction_type,
            t.channel,
            c.ACNTS_AC_TYPE,
            c.ACNTS_PROD_CODE
        FROM transactions t
        JOIN clients c ON t.client_id = c.ACNTS_CLIENT_NUM
        WHERE t.transaction_date >= ?
        """
        
        df = pd.read_sql_query(query, conn, params=[cutoff_date])
        conn.close()
        
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        return df
    
    def calculate_client_metrics(self, df):
        """Calculate various transaction metrics per client"""
        metrics = {}
        
        # Group by client
        grouped = df.groupby('client_id')
        
        # Transaction frequency
        metrics['avg_monthly_transactions'] = grouped.size() / 12
        
        # Amount metrics
        metrics['avg_transaction_amount'] = grouped['amount'].mean()
        metrics['total_transaction_amount'] = grouped['amount'].sum()
        metrics['transaction_amount_std'] = grouped['amount'].std().fillna(0)
        
        # Channel preferences
        channel_counts = df.groupby(['client_id', 'channel']).size().unstack(fill_value=0)
        for channel in channel_counts.columns:
            metrics[f'channel_{channel}_ratio'] = channel_counts[channel] / grouped.size()
        
        # Time patterns
        df['hour'] = df['transaction_date'].dt.hour
        df['day_of_week'] = df['transaction_date'].dt.dayofweek
        
        metrics['peak_transaction_hour'] = df.groupby('client_id')['hour'].agg(lambda x: x.value_counts().index[0])
        metrics['weekend_ratio'] = df[df['day_of_week'].isin([5,6])].groupby('client_id').size() / grouped.size()
        
        # Combine all metrics
        metrics_df = pd.DataFrame(metrics)
        metrics_df = metrics_df.fillna(0)
        
        return metrics_df
    
    def identify_customer_segments(self, metrics_df, n_clusters=5):
        """Identify customer segments based on transaction patterns"""
        # Scale features
        features_scaled = self.scaler.fit_transform(metrics_df)
        
        # Train KMeans
        self.kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        segments = self.kmeans.fit_predict(features_scaled)
        
        # Analyze segments
        segment_analysis = []
        for i in range(n_clusters):
            segment_metrics = metrics_df[segments == i].mean()
            segment_size = (segments == i).sum()
            segment_analysis.append({
                'segment_id': i,
                'size': segment_size,
                'percentage': segment_size / len(segments) * 100,
                'avg_monthly_transactions': segment_metrics['avg_monthly_transactions'],
                'avg_transaction_amount': segment_metrics['avg_transaction_amount'],
                'primary_channel': metrics_df.filter(like='channel_').columns[
                    segment_metrics.filter(like='channel_').argmax()
                ].replace('channel_', '')
            })
        
        return pd.DataFrame(segment_analysis), segments
    
    def detect_anomalies(self, metrics_df, contamination=0.1):
        """Detect anomalous transaction patterns"""
        # Train isolation forest
        self.isolation_forest = IsolationForest(
            contamination=contamination,
            random_state=42
        )
        
        # Fit and predict
        anomalies = self.isolation_forest.fit_predict(self.scaler.transform(metrics_df))
        
        # Convert predictions to boolean (True for anomalies)
        return anomalies == -1
    
    def analyze_trends(self, df, window='30D'):
        """Analyze transaction trends over time"""
        # Resample data by window
        daily_stats = df.set_index('transaction_date').resample(window).agg({
            'amount': ['count', 'mean', 'sum'],
            'channel': lambda x: x.value_counts().index[0] if len(x) > 0 else None
        })
        
        daily_stats.columns = ['transaction_count', 'avg_amount', 'total_amount', 'primary_channel']
        
        # Calculate growth rates
        daily_stats['transaction_growth'] = daily_stats['transaction_count'].pct_change()
        daily_stats['amount_growth'] = daily_stats['total_amount'].pct_change()
        
        return daily_stats
    
    def generate_insights(self, df, metrics_df, segments, anomalies):
        """Generate insights from transaction patterns"""
        insights = []
        
        # Segment distribution
        segment_dist = pd.Series(segments).value_counts(normalize=True)
        largest_segment = segment_dist.index[0]
        insights.append({
            'category': 'Customer Segments',
            'insight': f'Largest customer segment (Segment {largest_segment}) comprises {segment_dist[largest_segment]:.1%} of customers',
            'importance': 'High'
        })
        
        # Channel preferences
        channel_cols = metrics_df.filter(like='channel_').columns
        primary_channel = channel_cols[metrics_df[channel_cols].mean().argmax()].replace('channel_', '').replace('_ratio', '')
        insights.append({
            'category': 'Channel Usage',
            'insight': f'Most preferred transaction channel is {primary_channel}',
            'importance': 'Medium'
        })
        
        # Anomaly detection
        anomaly_rate = anomalies.mean()
        insights.append({
            'category': 'Risk Analysis',
            'insight': f'{anomaly_rate:.1%} of customers show anomalous transaction patterns',
            'importance': 'High' if anomaly_rate > 0.15 else 'Medium'
        })
        
        # Time patterns
        df['hour'] = df['transaction_date'].dt.hour
        peak_hour = df['hour'].mode()[0]
        insights.append({
            'category': 'Timing Patterns',
            'insight': f'Peak transaction hour is {peak_hour}:00',
            'importance': 'Medium'
        })
        
        # Amount patterns
        avg_amount = df['amount'].mean()
        amount_std = df['amount'].std()
        insights.append({
            'category': 'Transaction Amounts',
            'insight': f'Average transaction amount is {avg_amount:.2f} (±{amount_std:.2f})',
            'importance': 'Medium'
        })
        
        return pd.DataFrame(insights)
    
    def save_models(self, path='data_processor/models'):
        """Save trained models"""
        joblib.dump(self.scaler, f'{path}/transaction_scaler.joblib')
        joblib.dump(self.kmeans, f'{path}/transaction_kmeans.joblib')
        joblib.dump(self.isolation_forest, f'{path}/transaction_isolation_forest.joblib')
    
    def load_models(self, path='data_processor/models'):
        """Load trained models"""
        self.scaler = joblib.load(f'{path}/transaction_scaler.joblib')
        self.kmeans = joblib.load(f'{path}/transaction_kmeans.joblib')
        self.isolation_forest = joblib.load(f'{path}/transaction_isolation_forest.joblib')

def main():
    # Initialize analyzer
    analyzer = TransactionPatternAnalyzer()
    
    # Load and process data
    print("Loading transaction data...")
    df = analyzer.load_transactions()
    
    print("Calculating client metrics...")
    metrics_df = analyzer.calculate_client_metrics(df)
    
    print("Identifying customer segments...")
    segment_analysis, segments = analyzer.identify_customer_segments(metrics_df)
    print("\nCustomer Segments:")
    print(segment_analysis)
    
    print("\nDetecting anomalies...")
    anomalies = analyzer.detect_anomalies(metrics_df)
    print(f"Found {anomalies.sum()} anomalous patterns")
    
    print("\nAnalyzing trends...")
    trends = analyzer.analyze_trends(df)
    print(trends.tail())
    
    print("\nGenerating insights...")
    insights = analyzer.generate_insights(df, metrics_df, segments, anomalies)
    print("\nKey Insights:")
    print(insights)
    
    print("\nSaving models...")
    analyzer.save_models()

if __name__ == "__main__":
    main()
