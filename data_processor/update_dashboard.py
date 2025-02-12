import pandas as pd
from datetime import datetime, timedelta
import numpy as np

def generate_chart_data():
    """Generate data for business analysis charts"""
    # Customer Segments data
    segments_data = {
        'labels': ['High Value', 'Medium Value', 'Low Value', 'At Risk'],
        'data': [30, 40, 20, 10]
    }

    # Revenue Trends data (last 6 months)
    revenue_data = {
        'labels': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
        'data': [5000, 5500, 6000, 5800, 6200, 6500]
    }

    # CLV by Channel data
    channel_data = {
        'labels': ['Direct', 'Referral', 'Social', 'Email'],
        'data': [4500, 3800, 3200, 4000]
    }

    # Retention Analysis data
    retention_data = {
        'labels': ['3m', '6m', '9m', '12m'],
        'data': [95, 85, 78, 72]
    }

    return {
        'segments': segments_data,
        'revenue': revenue_data,
        'channels': channel_data,
        'retention': retention_data
    }

def get_dashboard_data():
    """Get formatted dashboard data for the template"""
    # Generate simulated metrics
    metrics = {
        'avg_clv': float(3500.0),
        'clv_cac_ratio': float(6.25),
        'retention_rate': float(85.0),
        'predicted_growth': float(np.random.normal(8, 2)),
        'churn_rate': float(10.0),  # Default value for churn rate
        'churn_prediction': 'Low',    # Default value for churn prediction
        'clv_trend': float(4000.0),   # Default value for CLV trend
        'cac_breakdown': float(500.0), # Default value for CAC breakdown
        'revenue_per_customer': float(700.0) # Default value for revenue per customer
    }
    
    # Get chart data
    chart_data = generate_chart_data()
    
    return {
        'metrics': metrics,
        'chart_data': chart_data
    }

if __name__ == "__main__":
    get_dashboard_data()
