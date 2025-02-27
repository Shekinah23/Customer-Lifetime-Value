import json
import sqlite3
from datetime import datetime, timedelta

class RetentionManager:
    def __init__(self, db_connection):
        self.conn = db_connection
        self.cursor = self.conn.cursor()
        
    def evaluate_condition(self, condition_str, client_data):
        """Evaluate a single condition against client data"""
        try:
            # Parse operator and value from condition string
            if '<' in condition_str:
                op = '<'
                value = float(condition_str.split('<')[1])
            elif '>' in condition_str:
                op = '>'
                value = float(condition_str.split('>')[1])
            elif condition_str.lower() == 'true':
                return True
            elif condition_str.lower() == 'false':
                return False
            else:
                value = float(condition_str)
                op = '='
            
            # Get client value
            client_value = float(client_data) if isinstance(client_data, (int, float)) else 0
            
            # Compare using appropriate operator
            if op == '<':
                return client_value < value
            elif op == '>':
                return client_value > value
            else:
                return client_value == value
                
        except (ValueError, TypeError):
            return False
            
    def check_action_conditions(self, conditions, client_metrics):
        """Check if all conditions are met for an action"""
        try:
            conditions_dict = json.loads(conditions)
            for key, condition in conditions_dict.items():
                if key in client_metrics:
                    if not self.evaluate_condition(condition, client_metrics[key]):
                        return False
            return True
        except json.JSONDecodeError:
            return False
            
    def get_client_metrics(self, client):
        """Extract relevant metrics from client data"""
        days_since = None
        if client['ACNTS_LAST_TRAN_DATE']:
            last_tran_date = datetime.strptime(client['ACNTS_LAST_TRAN_DATE'], '%Y-%m-%d')
            days_since = (datetime.now() - last_tran_date).days
            
        digital_score = (
            (client['ACNTS_ATM_OPERN'] or 0) / 20 +
            (client['ACNTS_INET_OPERN'] or 0) / 30 * 1.5 +
            (client['ACNTS_SMS_OPERN'] or 0) / 25 * 1.2
        ) / 3.7 * 100
        
        product_count = (client['ACNTS_SALARY_ACNT'] or 0) + (client['ACNTS_CR_CARDS_ALLOWED'] or 0)
        
        # Calculate activity score
        activity_score = 0
        if days_since is not None:
            recency_score = max(0, 100 - (days_since / 30) * 20)
            activity_score = (digital_score * 0.4 + recency_score * 0.6)
        
        return {
            'days_since_last_transaction': days_since,
            'digital_usage_score': digital_score,
            'product_count': product_count,
            'activity_score': activity_score,
            'current_clv': client.get('clv', 0),
            'product_type': 'premium' if client['ACNTS_PROD_CODE'] == 3102 else 'standard',
            'salary_account': client['ACNTS_SALARY_ACNT'] == 1,
            'credit_card': client['ACNTS_CR_CARDS_ALLOWED'] == 1
        }
        
    def generate_actions(self, client):
        """Generate appropriate retention actions for a client"""
        # Get current metrics
        metrics = self.get_client_metrics(client)
        
        # Get all action types
        self.cursor.execute("SELECT * FROM retention_action_types")
        action_types = self.cursor.fetchall()
        
        # Check each action type against client metrics
        for action_type in action_types:
            if self.check_action_conditions(action_type['conditions'], metrics):
                # Check if similar action already exists and is still pending
                self.cursor.execute("""
                    SELECT id FROM retention_actions 
                    WHERE client_id = ? AND type = ? AND status = 'pending'
                    AND action_date > date('now', '-30 days')
                """, (client['ACNTS_CLIENT_NUM'], action_type['type']))
                
                if not self.cursor.fetchone():
                    # Create new action
                    self.cursor.execute("""
                        INSERT INTO retention_actions 
                        (client_id, type, description, action_date, priority)
                        VALUES (?, ?, ?, date('now', '+7 days'), ?)
                    """, (
                        client['ACNTS_CLIENT_NUM'],
                        action_type['type'],
                        action_type['description_template'],
                        action_type['priority']
                    ))
                    
        self.conn.commit()
        
    def get_client_actions(self, client_id):
        """Get all retention actions for a client"""
        self.cursor.execute("""
            SELECT * FROM retention_actions 
            WHERE client_id = ?
            ORDER BY 
                CASE status 
                    WHEN 'pending' THEN 1
                    WHEN 'in_progress' THEN 2
                    ELSE 3
                END,
                priority DESC,
                action_date ASC
        """, (client_id,))
        return self.cursor.fetchall()
