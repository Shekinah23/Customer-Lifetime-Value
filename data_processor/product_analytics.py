import os
import sqlite3
from typing import Dict, List, Any
from datetime import datetime, timedelta

def get_db_connection():
    """Get database connection with timeout and cache"""
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'banking_data.db')
    conn = sqlite3.connect(db_path, timeout=30)  # Add timeout
    conn.row_factory = sqlite3.Row
    # Enable WAL mode for better concurrent access
    conn.execute('PRAGMA journal_mode=WAL')
    # Add index optimization
    conn.execute('PRAGMA temp_store=MEMORY')
    conn.execute('PRAGMA synchronous=NORMAL')
    return conn

def calculate_product_performance(product_code: str = None) -> Dict[str, float]:
    """Calculate product performance metrics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if required tables exist
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name IN ('products', 'clients', 'transactions')
        """)
        existing_tables = {row['name'] for row in cursor.fetchall()}
        required_tables = {'products', 'clients', 'transactions'}
        
        if not all(table in existing_tables for table in required_tables):
            # Return sample data if tables don't exist
            return {
                'total_sales': 1250000.00,
                'avg_clv': 2500.00,
                'revenue': 850000.00,
            }
        
        # Base query for all products or specific product
        where_clause = f"WHERE p.PRODUCT_CODE = '{product_code}'" if product_code else ""
        
        # Calculate total sales and revenue
        sales_query = f"""
            WITH AccountMetrics AS (
                SELECT 
                    c.ACNTS_PROD_CODE,
                    c.ACNTS_ACCOUNT_NUMBER,
                    julianday('now') - julianday(c.ACNTS_OPENING_DATE) as account_age_days,
                    CASE 
                        WHEN c.ACNTS_AC_TYPE = 1 THEN 'Premium'
                        WHEN c.ACNTS_AC_TYPE = 2 THEN 'Business'
                        WHEN c.ACNTS_AC_TYPE = 3 THEN 'Retail'
                        ELSE 'Other'
                    END as segment
                FROM clients c
                WHERE c.ACNTS_OPENING_DATE IS NOT NULL
                {where_clause}
            )
            SELECT 
                COUNT(DISTINCT ACNTS_ACCOUNT_NUMBER) as total_accounts,
                AVG(account_age_days) as avg_age_days,
                COUNT(CASE WHEN segment = 'Premium' THEN 1 END) as premium_accounts,
                COUNT(CASE WHEN segment = 'Business' THEN 1 END) as business_accounts,
                COUNT(CASE WHEN segment = 'Retail' THEN 1 END) as retail_accounts
            FROM AccountMetrics
        """
        
        cursor.execute(sales_query)
        metrics = cursor.fetchone()
        
        # Calculate metrics
        total_accounts = metrics['total_accounts'] or 0
        avg_age_days = metrics['avg_age_days'] or 0
        avg_age_years = avg_age_days / 365.0
        
        # Calculate segment-based metrics
        premium_accounts = metrics['premium_accounts'] or 0
        business_accounts = metrics['business_accounts'] or 0
        retail_accounts = metrics['retail_accounts'] or 0
        
        # Calculate CLV based on segment and age
        premium_clv = premium_accounts * 5000 * (1 + avg_age_years * 0.1)  # Premium accounts have higher value
        business_clv = business_accounts * 3000 * (1 + avg_age_years * 0.08)  # Business accounts have medium value
        retail_clv = retail_accounts * 1000 * (1 + avg_age_years * 0.05)  # Retail accounts have base value
        
        total_clv = premium_clv + business_clv + retail_clv
        avg_clv = total_clv / total_accounts if total_accounts > 0 else 0
        
        # Calculate revenue (estimated annual revenue based on account types)
        premium_revenue = premium_accounts * 2000  # Premium accounts generate more revenue
        business_revenue = business_accounts * 1500  # Business accounts generate medium revenue
        retail_revenue = retail_accounts * 500  # Retail accounts generate base revenue
        total_revenue = premium_revenue + business_revenue + retail_revenue
        
        return {
            'total_sales': total_accounts,
            'avg_clv': avg_clv,
            'revenue': total_revenue,
            'segment_distribution': {
                'Premium': premium_accounts,
                'Business': business_accounts,
                'Retail': retail_accounts
            }
        }
    except Exception as e:
        print(f"Error calculating product performance: {str(e)}")
        # Return sample data in case of any error
        return {
            'total_sales': 1250000.00,
            'avg_clv': 2500.00,
            'revenue': 850000.00,
        }

def get_revenue_by_segment() -> Dict[str, List]:
    """Get revenue breakdown by customer segment"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if required tables exist
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='clients'
        """)
        if not cursor.fetchone():
            # Return sample data if table doesn't exist
            return {
                'labels': ['Premium', 'Business', 'Retail'],
                'data': [40, 35, 25]
            }
        
        # Get segment distribution with revenue calculation
        segment_query = """
            SELECT 
                CASE 
                    WHEN c.ACNTS_AC_TYPE = 1 THEN 'Premium'
                    WHEN c.ACNTS_AC_TYPE = 2 THEN 'Business'
                    WHEN c.ACNTS_AC_TYPE = 3 THEN 'Retail'
                    ELSE 'Other'
                END as segment,
                COUNT(DISTINCT c.ACNTS_ACCOUNT_NUMBER) as account_count,
                COUNT(DISTINCT c.ACNTS_ACCOUNT_NUMBER) * 
                CASE 
                    WHEN c.ACNTS_AC_TYPE = 1 THEN 2000  -- Premium revenue per account
                    WHEN c.ACNTS_AC_TYPE = 2 THEN 1500  -- Business revenue per account
                    WHEN c.ACNTS_AC_TYPE = 3 THEN 500   -- Retail revenue per account
                    ELSE 250
                END as estimated_revenue
            FROM clients c
            GROUP BY 
                CASE 
                    WHEN c.ACNTS_AC_TYPE = 1 THEN 'Premium'
                    WHEN c.ACNTS_AC_TYPE = 2 THEN 'Business'
                    WHEN c.ACNTS_AC_TYPE = 3 THEN 'Retail'
                    ELSE 'Other'
                END
            HAVING segment != 'Other'
            ORDER BY estimated_revenue DESC
        """
        
        cursor.execute(segment_query)
        segments = cursor.fetchall()
        conn.close()
        
        if not segments:
            return {
                'labels': ['Premium', 'Business', 'Retail'],
                'data': [40, 35, 25]
            }
        
        # Calculate percentages based on revenue
        total_revenue = sum(seg['estimated_revenue'] for seg in segments)
        data = [round((seg['estimated_revenue'] / total_revenue * 100), 1) if total_revenue > 0 else 0 for seg in segments]
        
        return {
            'labels': [seg['segment'] for seg in segments],
            'data': data
        }
        
    except Exception as e:
        print(f"Error getting revenue by segment: {str(e)}")
        # Return sample data in case of any error
        return {
            'labels': ['Premium', 'Business', 'Retail'],
            'data': [40, 35, 25]
        }

def analyze_product_lifecycle(product_code: str = None) -> List[Dict[str, Any]]:
    """Analyze product lifecycle stage with comprehensive analysis"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check tables with single query
        cursor.execute("""
            SELECT COUNT(*) as table_count 
            FROM sqlite_master 
            WHERE type='table' AND name IN ('products', 'clients')
        """)
        if cursor.fetchone()['table_count'] < 2:
            return get_default_lifecycle_stages("Introduction")
        
        if product_code:
            # Get product info and age
            cursor.execute("""
                SELECT 
                    p.PRODUCT_CODE,
                    p.PRODUCT_NAME, 
                    p.PRODUCT_GROUP_CODE,
                    p.PRODUCT_CLASS,
                    p.PRODUCT_FOR_DEPOSITS,
                    p.PRODUCT_FOR_LOANS,
                    MIN(c.ACNTS_OPENING_DATE) as first_account_date,
                    COUNT(DISTINCT c.ACNTS_ACCOUNT_NUMBER) as total_accounts
                FROM products p
                LEFT JOIN clients c ON p.PRODUCT_CODE = c.ACNTS_PROD_CODE
                WHERE p.PRODUCT_CODE = ?
                GROUP BY p.PRODUCT_CODE
            """, (product_code,))
            prod_info = cursor.fetchone()
            
            if prod_info:
                # Calculate product age in months
                if prod_info['first_account_date']:
                    cursor.execute("SELECT julianday('now') - julianday(?) as age_days", 
                                 (prod_info['first_account_date'],))
                    age_months = cursor.fetchone()['age_days'] / 30.44  # Average days per month
                else:
                    age_months = 0
                
                # New products (less than 6 months) start in Introduction
                if age_months < 6:
                    return get_default_lifecycle_stages("Introduction")
                
                # Products with very few accounts might be in Introduction or Decline
                if prod_info['total_accounts'] < 50:
                    if age_months < 12:
                        return get_default_lifecycle_stages("Introduction")
                    else:
                        return get_default_lifecycle_stages("Decline")
        
        # Get trend data for growth analysis
        trend_query = f"""
WITH RECURSIVE dates(date) AS (
  SELECT date('now', '-12 months')
  UNION ALL
  SELECT date(date, '+1 month')
  FROM dates
  WHERE date < date('now')
),
monthly_stats AS (
    SELECT 
        strftime('%Y-%m', c.ACNTS_OPENING_DATE) as month,
        COUNT(*) as total_accounts,
        SUM(CASE WHEN c.ACNTS_AC_TYPE = 1 THEN 1 ELSE 0 END) as premium_accounts,
        SUM(CASE WHEN c.ACNTS_AC_TYPE = 2 THEN 1 ELSE 0 END) as business_accounts,
        SUM(CASE WHEN c.ACNTS_AC_TYPE = 3 THEN 1 ELSE 0 END) as retail_accounts
    FROM clients c
    WHERE c.ACNTS_OPENING_DATE IS NOT NULL
    {f"AND c.ACNTS_PROD_CODE = '{product_code}'" if product_code else ""}
    GROUP BY strftime('%Y-%m', c.ACNTS_OPENING_DATE)
)
SELECT 
    strftime('%Y-%m', dates.date) as month,
    COALESCE(ms.total_accounts, 0) as total_accounts,
    COALESCE(ms.premium_accounts, 0) as premium_accounts,
    COALESCE(ms.business_accounts, 0) as business_accounts,
    COALESCE(ms.retail_accounts, 0) as retail_accounts
FROM dates
LEFT JOIN monthly_stats ms ON strftime('%Y-%m', dates.date) = ms.month
ORDER BY dates.date DESC
LIMIT 12"""
        
        cursor.execute(trend_query)
        trends = cursor.fetchall()
        conn.close()
        
        if len(trends) > 1:
            # Calculate total accounts and segment distribution
            total_accounts = sum(t['total_accounts'] for t in trends)
            if total_accounts == 0:
                return get_default_lifecycle_stages("Introduction")
            
            # Split into quarters for more granular analysis
            q1 = trends[:3]
            q2 = trends[3:6]
            q3 = trends[6:9]
            q4 = trends[9:]
            
            # Calculate quarterly averages
            q1_avg = sum(t['total_accounts'] for t in q1) / len(q1)
            q2_avg = sum(t['total_accounts'] for t in q2) / len(q2)
            q3_avg = sum(t['total_accounts'] for t in q3) / len(q3)
            q4_avg = sum(t['total_accounts'] for t in q4) / len(q4)
            
            # Calculate year-over-year growth
            current_half = sum(t['total_accounts'] for t in trends[:6])
            previous_half = sum(t['total_accounts'] for t in trends[6:])
            yoy_growth = ((current_half - previous_half) / previous_half * 100) if previous_half > 0 else 0
            
            # Calculate monthly growth rates
            monthly_growth_rates = []
            for i in range(len(trends) - 1):
                current = trends[i]['total_accounts']
                previous = trends[i + 1]['total_accounts']
                if previous > 0:
                    growth = ((current - previous) / previous * 100)
                    monthly_growth_rates.append(growth)
            
            # Calculate average monthly growth rate
            avg_monthly_growth = sum(monthly_growth_rates) / len(monthly_growth_rates) if monthly_growth_rates else 0
            
            # Calculate premium ratio
            total_premium = sum(t['premium_accounts'] for t in trends[:3])  # Last 3 months
            total_accounts_recent = sum(t['total_accounts'] for t in trends[:3])
            premium_ratio = (total_premium / total_accounts_recent) if total_accounts_recent > 0 else 0
            
            # Calculate business ratio
            total_business = sum(t['business_accounts'] for t in trends[:3])
            business_ratio = (total_business / total_accounts_recent) if total_accounts_recent > 0 else 0
            
            # Get the most recent month's total
            latest_month_total = trends[0]['total_accounts'] if trends else 0
            
            # Determine stage based on multiple factors
            if latest_month_total < 50:
                stage = "Introduction"  # Very low volume
            elif yoy_growth < -20 or (yoy_growth < -10 and latest_month_total < 100):
                stage = "Decline"  # Significant decline or low volume with negative growth
            elif yoy_growth > 30 or (avg_monthly_growth > 5 and premium_ratio > 0.2):
                stage = "Growth"  # High growth or good growth with premium customers
            elif abs(yoy_growth) <= 15 and (premium_ratio > 0.3 or business_ratio > 0.4):
                stage = "Maturity"  # Stable with strong customer base
            elif yoy_growth > 10 or avg_monthly_growth > 2:
                stage = "Growth"  # Moderate but consistent growth
            elif yoy_growth < -5 and avg_monthly_growth < 0:
                stage = "Decline"  # Consistent decline
            else:
                stage = "Maturity"  # Default for stable products
            
            return get_default_lifecycle_stages(stage)
        
        return get_default_lifecycle_stages("Introduction")
            
    except Exception as e:
        print(f"Error analyzing lifecycle: {str(e)}")
        return get_default_lifecycle_stages("Introduction")  # Default to Introduction on error

def get_default_lifecycle_stages(current_stage: str) -> List[Dict[str, Any]]:
    """Helper function to get default lifecycle stages with specified current stage"""
    try:
        # Return all lifecycle stages with current stage marked
        return [
        {
            'name': 'Introduction',
            'description': 'New products gaining market traction',
            'current': current_stage == 'Introduction'
        },
        {
            'name': 'Growth',
            'description': 'Rapidly increasing customer adoption and revenue',
            'current': current_stage == 'Growth'
        },
        {
            'name': 'Maturity',
            'description': 'Stable market share and consistent revenue',
            'current': current_stage == 'Maturity'
        },
        {
            'name': 'Decline',
            'description': 'Decreasing market share and revenue',
            'current': current_stage == 'Decline'
        }
        ]
    except Exception as e:
        print(f"Error getting default lifecycle stages: {str(e)}")
        # Return a safe default in case of error
        return [
            {
                'name': 'Introduction',
                'description': 'New products gaining market traction',
                'current': True
            },
            {
                'name': 'Growth',
                'description': 'Rapidly increasing customer adoption and revenue',
                'current': False
            },
            {
                'name': 'Maturity',
                'description': 'Stable market share and consistent revenue',
                'current': False
            },
            {
                'name': 'Decline',
                'description': 'Decreasing market share and revenue',
                'current': False
            }
        ]

def get_bundling_recommendations() -> List[Dict[str, Any]]:
    """Generate product bundling recommendations based on purchase patterns"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if required tables exist
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name IN ('products', 'clients')
        """)
        existing_tables = {row['name'] for row in cursor.fetchall()}
        required_tables = {'products', 'clients'}
        
        if not all(table in existing_tables for table in required_tables):
            # Return default recommendations if tables don't exist
            return get_default_bundle_recommendations()
        
        # Find products frequently used together by the same client, segmented by account type
        bundle_query = """
            WITH ClientSegments AS (
                SELECT 
                    c.ACNTS_CLIENT_NUM,
                    c.ACNTS_AC_TYPE,
                    CASE 
                        WHEN c.ACNTS_AC_TYPE = 1 THEN 'Premium'
                        WHEN c.ACNTS_AC_TYPE = 2 THEN 'Business'
                        WHEN c.ACNTS_AC_TYPE = 3 THEN 'Retail'
                        ELSE 'Other'
                    END as segment
                FROM clients c
                GROUP BY c.ACNTS_CLIENT_NUM
            ),
            ProductPairs AS (
                SELECT 
                    c1.ACNTS_PROD_CODE as prod1,
                    c2.ACNTS_PROD_CODE as prod2,
                    cs.segment,
                    COUNT(*) as pair_count,
                    COUNT(*) * 100.0 / (
                        SELECT COUNT(DISTINCT c3.ACNTS_CLIENT_NUM) 
                        FROM clients c3 
                        JOIN ClientSegments cs3 ON cs3.ACNTS_CLIENT_NUM = c3.ACNTS_CLIENT_NUM 
                        WHERE cs3.segment = cs.segment
                    ) as adoption_rate
                FROM clients c1
                JOIN clients c2 ON 
                    c1.ACNTS_CLIENT_NUM = c2.ACNTS_CLIENT_NUM AND
                    c1.ACNTS_PROD_CODE < c2.ACNTS_PROD_CODE
                JOIN ClientSegments cs ON cs.ACNTS_CLIENT_NUM = c1.ACNTS_CLIENT_NUM
                GROUP BY c1.ACNTS_PROD_CODE, c2.ACNTS_PROD_CODE, cs.segment
                HAVING pair_count >= 5
            )
            SELECT 
                p1.PRODUCT_NAME as prod1_name,
                p2.PRODUCT_NAME as prod2_name,
                pp.segment,
                pp.pair_count,
                pp.adoption_rate,
                CASE pp.segment
                    WHEN 'Premium' THEN 35
                    WHEN 'Business' THEN 30
                    WHEN 'Retail' THEN 20
                    ELSE 15
                END as base_revenue_increase
            FROM ProductPairs pp
            JOIN products p1 ON pp.prod1 = p1.PRODUCT_CODE
            JOIN products p2 ON pp.prod2 = p2.PRODUCT_CODE
            ORDER BY pp.segment, pp.pair_count DESC
        """
        
        cursor.execute(bundle_query)
        product_pairs = cursor.fetchall()
        
        conn.close()
        
        # Create segment-specific bundle recommendations
        bundles = []
        if product_pairs:
            # Group pairs by segment
            segment_pairs = {}
            for pair in product_pairs:
                segment = pair['segment']
                if segment not in segment_pairs:
                    segment_pairs[segment] = []
                segment_pairs[segment].append(pair)
            
            # Create bundles for each segment
            for segment, pairs in segment_pairs.items():
                top_pairs = pairs[:3]  # Take top 3 pairs for each segment
                
                # Calculate weighted revenue increase based on adoption rates
                avg_adoption = sum(p['adoption_rate'] for p in top_pairs) / len(top_pairs)
                revenue_increase = top_pairs[0]['base_revenue_increase'] * (1 + avg_adoption / 100)
                
                bundle_name = f"{segment} Banking Bundle"
                if segment == 'Premium':
                    description = 'High-value combination of premium banking products with exclusive benefits'
                elif segment == 'Business':
                    description = 'Comprehensive business banking solution with integrated services'
                elif segment == 'Retail':
                    description = 'Essential banking products tailored for personal banking needs'
                else:
                    description = 'Customized banking package with complementary products'
                
                # Create list of products with their details
                bundle_products = []
                for pair in top_pairs:
                    bundle_products.extend([
                        {
                            'name': pair['prod1_name'],
                            'adoption_rate': round(pair['adoption_rate'], 1)
                        },
                        {
                            'name': pair['prod2_name'],
                            'adoption_rate': round(pair['adoption_rate'], 1)
                        }
                    ])
                
                bundles.append({
                    'name': bundle_name,
                    'description': description,
                    'products': bundle_products,
                    'revenue_increase': round(revenue_increase, 1),
                    'adoption_rate': round(avg_adoption, 1),
                    'pair_count': sum(p['pair_count'] for p in top_pairs)
                })
        
        return bundles if bundles else get_default_bundle_recommendations()
    except Exception as e:
        print(f"Error getting bundle recommendations: {str(e)}")
        return get_default_bundle_recommendations()

def get_default_bundle_recommendations() -> List[Dict[str, Any]]:
    """Helper function to get default bundle recommendations"""
    return [
        {
            'name': 'Premium Banking Bundle',
            'description': 'High-value combination of savings and investment products',
            'products': [
                {'name': 'Premium Savings', 'adoption_rate': 45.5},
                {'name': 'Investment Account', 'adoption_rate': 35.2},
                {'name': 'Wealth Management', 'adoption_rate': 28.7}
            ],
            'revenue_increase': 25,
            'pair_count': 150
        },
        {
            'name': 'Business Essentials',
            'description': 'Core banking products for business customers',
            'products': [
                {'name': 'Business Current', 'adoption_rate': 52.3},
                {'name': 'Overdraft', 'adoption_rate': 38.9},
                {'name': 'Business Loans', 'adoption_rate': 31.5}
            ],
            'revenue_increase': 30,
            'pair_count': 120
        },
        {
            'name': 'Retail Banking Bundle',
            'description': 'Essential banking products for retail customers',
            'products': [
                {'name': 'Savings Account', 'adoption_rate': 65.8},
                {'name': 'Debit Card', 'adoption_rate': 58.4},
                {'name': 'Mobile Banking', 'adoption_rate': 42.1}
            ],
            'revenue_increase': 20,
            'pair_count': 200
        }
    ]

def get_sales_trend_data() -> Dict[str, List]:
    """Get sales trend data for the past 6 months"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if transactions table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='transactions'
        """)
        if not cursor.fetchone():
            # Return sample data if table doesn't exist
            return get_default_sales_trend_data()
        
        sales_query = """
            SELECT 
                strftime('%Y-%m', t.ACNTS_LAST_TRAN_DATE) as month,
                COUNT(DISTINCT t.ACCOUNT_NUMBER) as sales
            FROM transactions t
            WHERE t.ACNTS_LAST_TRAN_DATE >= date('now', '-6 months')
            GROUP BY month
            ORDER BY month
        """
        
        cursor.execute(sales_query)
        sales_data = cursor.fetchall()
        
        conn.close()
        
        if not sales_data:
            return get_default_sales_trend_data()
        
        return {
            'labels': [row['month'] for row in sales_data],
            'data': [row['sales'] for row in sales_data]
        }
    except Exception as e:
        print(f"Error getting sales trend data: {str(e)}")
        return get_default_sales_trend_data()

def get_default_sales_trend_data() -> Dict[str, List]:
    """Helper function to get default sales trend data"""
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun']
    return {
        'labels': months,
        'data': [120000, 150000, 180000, 210000, 250000, 340000]
    }
