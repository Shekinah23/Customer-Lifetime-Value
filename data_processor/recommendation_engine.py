import sqlite3
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity
from datetime import datetime, timedelta
import joblib
from transaction_analyzer import TransactionPatternAnalyzer

class RecommendationEngine:
    def __init__(self, db_path='banking_data.db'):
        self.db_path = db_path
        self.scaler = StandardScaler()
        self.transaction_analyzer = TransactionPatternAnalyzer(db_path)
        self.product_similarities = None
        self.user_product_matrix = None
        self.product_features = None
        
    def load_data(self):
        """Load necessary data from database"""
        conn = sqlite3.connect(self.db_path)
        
        # Load product data
        products_query = """
        SELECT DISTINCT
            p.product_id,
            p.product_name,
            p.product_type,
            p.product_category,
            p.min_balance,
            p.interest_rate,
            p.annual_fee
        FROM products p
        """
        self.products_df = pd.read_sql_query(products_query, conn)
        
        # Load client-product relationships
        client_products_query = """
        SELECT 
            c.ACNTS_CLIENT_NUM as client_id,
            c.ACNTS_PROD_CODE as product_id,
            c.ACNTS_OPENING_DATE,
            c.ACNTS_AC_TYPE,
            c.ACNTS_SALARY_ACNT,
            c.ACNTS_CR_CARDS_ALLOWED
        FROM clients c
        WHERE c.ACNTS_CLIENT_NUM IS NOT NULL
        """
        self.client_products_df = pd.read_sql_query(client_products_query, conn)
        
        conn.close()
        
    def prepare_product_features(self):
        """Prepare product feature matrix"""
        # Create dummy variables for categorical features
        product_type_dummies = pd.get_dummies(self.products_df['product_type'], prefix='type')
        product_category_dummies = pd.get_dummies(self.products_df['product_category'], prefix='category')
        
        # Combine numerical and categorical features
        numerical_features = self.products_df[['min_balance', 'interest_rate', 'annual_fee']]
        self.product_features = pd.concat([
            numerical_features,
            product_type_dummies,
            product_category_dummies
        ], axis=1)
        
        # Scale features
        self.product_features = pd.DataFrame(
            self.scaler.fit_transform(self.product_features),
            columns=self.product_features.columns,
            index=self.products_df['product_id']
        )
        
    def calculate_product_similarities(self):
        """Calculate similarity between products"""
        self.product_similarities = pd.DataFrame(
            cosine_similarity(self.product_features),
            index=self.products_df['product_id'],
            columns=self.products_df['product_id']
        )
        
    def create_user_product_matrix(self):
        """Create user-product interaction matrix"""
        # Create matrix of user-product interactions
        self.user_product_matrix = pd.crosstab(
            self.client_products_df['client_id'],
            self.client_products_df['product_id']
        )
        
    def get_similar_products(self, product_id, n=5):
        """Get n most similar products to a given product"""
        if self.product_similarities is None:
            self.prepare_product_features()
            self.calculate_product_similarities()
            
        similar_scores = self.product_similarities[product_id].sort_values(ascending=False)
        similar_products = similar_scores[1:n+1]  # Exclude the product itself
        
        return similar_products
        
    def get_user_segment(self, client_id):
        """Get user segment information"""
        # Load transaction data for the client
        df = self.transaction_analyzer.load_transactions()
        client_df = df[df['client_id'] == client_id]
        
        if len(client_df) == 0:
            return None
            
        # Calculate metrics
        metrics_df = self.transaction_analyzer.calculate_client_metrics(client_df)
        
        # Load pre-trained models
        self.transaction_analyzer.load_models()
        
        # Get segment
        _, segments = self.transaction_analyzer.identify_customer_segments(metrics_df)
        return segments[0]  # Return segment for the client
        
    def get_product_recommendations(self, client_id, n_recommendations=5):
        """Generate product recommendations for a client"""
        if self.user_product_matrix is None:
            self.create_user_product_matrix()
            self.prepare_product_features()
            self.calculate_product_similarities()
        
        # Get current products
        current_products = set(self.client_products_df[
            self.client_products_df['client_id'] == client_id
        ]['product_id'].values)
        
        if not current_products:
            return self._get_popular_products(n_recommendations)
        
        # Calculate recommendation scores
        scores = {}
        for prod in current_products:
            similar_products = self.get_similar_products(prod, n=n_recommendations)
            for similar_prod, score in similar_products.items():
                if similar_prod not in current_products:
                    scores[similar_prod] = max(score, scores.get(similar_prod, 0))
        
        # Sort and filter recommendations
        recommendations = pd.Series(scores).sort_values(ascending=False)
        recommendations = recommendations[:n_recommendations]
        
        # Get product details
        recommended_products = self.products_df[
            self.products_df['product_id'].isin(recommendations.index)
        ].copy()
        
        # Add confidence scores
        recommended_products['confidence_score'] = recommended_products['product_id'].map(recommendations)
        
        return recommended_products.sort_values('confidence_score', ascending=False)
    
    def _get_popular_products(self, n=5):
        """Get most popular products as fallback recommendations"""
        product_counts = self.client_products_df['product_id'].value_counts()
        popular_products = self.products_df[
            self.products_df['product_id'].isin(product_counts.head(n).index)
        ].copy()
        popular_products['confidence_score'] = popular_products['product_id'].map(
            product_counts / product_counts.max()
        )
        return popular_products.sort_values('confidence_score', ascending=False)
    
    def get_cross_selling_opportunities(self, client_id):
        """Identify cross-selling opportunities"""
        # Get client's segment
        segment = self.get_user_segment(client_id)
        
        # Get client's current products
        current_products = set(self.client_products_df[
            self.client_products_df['client_id'] == client_id
        ]['product_id'].values)
        
        # Get client details
        client_details = self.client_products_df[
            self.client_products_df['client_id'] == client_id
        ].iloc[0]
        
        opportunities = []
        
        # Check for credit card opportunity
        if not client_details['ACNTS_CR_CARDS_ALLOWED'] and segment in [0, 1]:  # High-value segments
            opportunities.append({
                'product_type': 'Credit Card',
                'reason': 'High-value customer without credit card',
                'priority': 'High'
            })
        
        # Check for salary account opportunity
        if not client_details['ACNTS_SALARY_ACNT']:
            opportunities.append({
                'product_type': 'Salary Account',
                'reason': 'Customer without salary account',
                'priority': 'Medium'
            })
        
        # Get recommended products
        recommendations = self.get_product_recommendations(client_id)
        
        # Add high-confidence recommendations
        for _, rec in recommendations.iterrows():
            if rec['confidence_score'] > 0.8:
                opportunities.append({
                    'product_type': rec['product_type'],
                    'reason': f'Strong match based on customer profile',
                    'priority': 'High' if rec['confidence_score'] > 0.9 else 'Medium'
                })
        
        return pd.DataFrame(opportunities)
    
    def save_models(self, path='data_processor/models'):
        """Save trained models and matrices"""
        joblib.dump(self.scaler, f'{path}/recommendation_scaler.joblib')
        joblib.dump(self.product_features, f'{path}/product_features.joblib')
        joblib.dump(self.product_similarities, f'{path}/product_similarities.joblib')
        joblib.dump(self.user_product_matrix, f'{path}/user_product_matrix.joblib')
    
    def load_models(self, path='data_processor/models'):
        """Load trained models and matrices"""
        self.scaler = joblib.load(f'{path}/recommendation_scaler.joblib')
        self.product_features = joblib.load(f'{path}/product_features.joblib')
        self.product_similarities = joblib.load(f'{path}/product_similarities.joblib')
        self.user_product_matrix = joblib.load(f'{path}/user_product_matrix.joblib')

def main():
    # Initialize recommendation engine
    engine = RecommendationEngine()
    
    print("Loading data...")
    engine.load_data()
    
    print("Preparing product features...")
    engine.prepare_product_features()
    
    print("Calculating product similarities...")
    engine.calculate_product_similarities()
    
    print("Creating user-product matrix...")
    engine.create_user_product_matrix()
    
    # Example: Get recommendations for a client
    client_id = engine.client_products_df['client_id'].iloc[0]
    print(f"\nGenerating recommendations for client {client_id}...")
    recommendations = engine.get_product_recommendations(client_id)
    print("\nTop Recommendations:")
    print(recommendations[['product_name', 'product_type', 'confidence_score']])
    
    print("\nIdentifying cross-selling opportunities...")
    opportunities = engine.get_cross_selling_opportunities(client_id)
    print("\nCross-selling Opportunities:")
    print(opportunities)
    
    print("\nSaving models...")
    engine.save_models()

if __name__ == "__main__":
    main()
