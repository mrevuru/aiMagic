#!/usr/bin/env python3
"""
DARA Agent Backend - Snowflake Cortex Integration
Handles connections for both Dashboard data and Cortex Agent queries using PAT authentication
"""

import os
import json
from typing import Optional, Dict, Any
import snowflake.connector
from snowflake.connector import DictCursor
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


class DARAConnectionManager:
    """
    Manages separate connections for Dashboard KPI data and Cortex Agent AI queries
    """
    
    def __init__(self):
        self.dashboard_conn = None
        self.cortex_conn = None
        
    # ═══════════════════════════════════════════════════════════
    # DASHBOARD CONNECTION (SSO or Key-Pair)
    # ═══════════════════════════════════════════════════════════
    
    def connect_dashboard_sso(
        self,
        account: str,
        user: str,
        warehouse: str,
        database: str,
        schema: str,
        role: str,
        authenticator: str = 'externalbrowser'
    ) -> snowflake.connector.SnowflakeConnection:
        """
        Connect to Snowflake for dashboard data using SSO authentication
        
        Args:
            account: Snowflake account identifier
            user: User email for SSO
            warehouse: Warehouse for queries
            database: Database name
            schema: Schema name
            role: Role to use
            authenticator: 'externalbrowser' or Okta URL
        
        Returns:
            Snowflake connection object
        """
        print(f"🔐 Connecting to Dashboard (SSO) as {user}...")
        
        self.dashboard_conn = snowflake.connector.connect(
            user=user,
            account=account,
            authenticator=authenticator,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role
        )
        
        print("✅ Dashboard connection established (SSO)")
        return self.dashboard_conn
    
    def connect_dashboard_keypair(
        self,
        account: str,
        user: str,
        private_key_path: str,
        warehouse: str,
        database: str,
        schema: str,
        role: str,
        passphrase: Optional[str] = None
    ) -> snowflake.connector.SnowflakeConnection:
        """
        Connect to Snowflake for dashboard data using key-pair authentication
        
        Args:
            account: Snowflake account identifier
            user: Service account username
            private_key_path: Path to private key file
            warehouse: Warehouse for queries
            database: Database name
            schema: Schema name
            role: Role to use
            passphrase: Optional passphrase for encrypted key
        
        Returns:
            Snowflake connection object
        """
        print(f"🔑 Connecting to Dashboard (Key-Pair) as {user}...")
        
        # Load private key
        with open(private_key_path, "rb") as key_file:
            password = passphrase.encode() if passphrase else None
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=password,
                backend=default_backend()
            )
        
        self.dashboard_conn = snowflake.connector.connect(
            user=user,
            account=account,
            private_key=private_key,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role
        )
        
        print("✅ Dashboard connection established (Key-Pair)")
        return self.dashboard_conn
    
    # ═══════════════════════════════════════════════════════════
    # CORTEX AGENT CONNECTION (PAT Authentication)
    # ═══════════════════════════════════════════════════════════
    
    def connect_cortex_pat(
        self,
        account: str,
        user: str,
        pat: str,  # Personal Access Token
        warehouse: str,
        database: str,
        schema: str,
        role: str
    ) -> snowflake.connector.SnowflakeConnection:
        """
        Connect to Snowflake Cortex using Personal Access Token (PAT) authentication
        
        Args:
            account: Snowflake account identifier
            user: Username associated with PAT
            pat: Personal Access Token (starts with 'sfp_')
            warehouse: Warehouse for Cortex queries
            database: Database name
            schema: Schema name (typically where Cortex functions are available)
            role: Role to use
        
        Returns:
            Snowflake connection object
        
        Example:
            >>> manager = DARAConnectionManager()
            >>> conn = manager.connect_cortex_pat(
            ...     account='xy12345.us-east-1',
            ...     user='CORTEX_USER',
            ...     pat='sfp_abc123...',
            ...     warehouse='CORTEX_WH',
            ...     database='ANALYTICS_DB',
            ...     schema='CORTEX',
            ...     role='CORTEX_ROLE'
            ... )
        """
        print(f"🤖 Connecting to Cortex Agent (PAT) as {user}...")
        
        if not pat.startswith('sfp_'):
            raise ValueError("Invalid PAT format. Token should start with 'sfp_'")
        
        self.cortex_conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=pat,  # PAT is passed as password
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
            authenticator='snowflake_jwt'  # Use JWT authenticator for PAT
        )
        
        print("✅ Cortex Agent connection established (PAT)")
        return self.cortex_conn
    
    # ═══════════════════════════════════════════════════════════
    # DASHBOARD QUERIES
    # ═══════════════════════════════════════════════════════════
    
    def get_dashboard_kpi(self, kpi_name: str) -> Dict[str, Any]:
        """
        Fetch KPI data for the dashboard
        
        Args:
            kpi_name: Name of the KPI to fetch
        
        Returns:
            Dictionary containing KPI data
        """
        if not self.dashboard_conn:
            raise ConnectionError("Dashboard not connected. Call connect_dashboard_* first.")
        
        cursor = self.dashboard_conn.cursor(DictCursor)
        
        # Example queries for different KPIs
        queries = {
            'delinquency': """
                SELECT 
                    COUNT(*) FILTER (WHERE dpd_bucket = '30') as dpd_30_count,
                    COUNT(*) FILTER (WHERE dpd_bucket = '60') as dpd_60_count,
                    COUNT(*) FILTER (WHERE dpd_bucket = '90+') as dpd_90_count,
                    SUM(upb) FILTER (WHERE dpd_bucket = '30') as dpd_30_upb,
                    SUM(upb) FILTER (WHERE dpd_bucket = '60') as dpd_60_upb,
                    SUM(upb) FILTER (WHERE dpd_bucket = '90+') as dpd_90_upb
                FROM loan_portfolio
                WHERE status = 'ACTIVE'
            """,
            'portfolio_overview': """
                SELECT 
                    COUNT(*) as total_loans,
                    SUM(upb) as total_upb,
                    AVG(upb) as avg_balance,
                    COUNT(*) FILTER (WHERE current_status = 'CURRENT') * 100.0 / COUNT(*) as current_pct
                FROM loan_portfolio
                WHERE status = 'ACTIVE'
            """
        }
        
        query = queries.get(kpi_name)
        if not query:
            raise ValueError(f"Unknown KPI: {kpi_name}")
        
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        
        return dict(result) if result else {}
    
    # ═══════════════════════════════════════════════════════════
    # CORTEX AGENT QUERIES
    # ═══════════════════════════════════════════════════════════
    
    def query_cortex(
        self,
        user_question: str,
        model: str = 'snowflake-arctic',
        context: Optional[str] = None
    ) -> str:
        """
        Query Snowflake Cortex LLM with natural language question
        
        Args:
            user_question: Natural language question from user
            model: Cortex model to use
            context: Optional context/system prompt
        
        Returns:
            AI-generated response
        
        Example:
            >>> response = manager.query_cortex(
            ...     "What is the current delinquency rate?",
            ...     model='snowflake-arctic'
            ... )
        """
        if not self.cortex_conn:
            raise ConnectionError("Cortex not connected. Call connect_cortex_pat first.")
        
        cursor = self.cortex_conn.cursor()
        
        # Build Cortex COMPLETE query
        system_prompt = context or """You are DARA Agent, an AI assistant for mortgage servicing analytics. 
You have access to loan portfolio data and can answer questions about delinquency, 
foreclosures, escrow, payments, compliance, and customer service metrics."""
        
        query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT('role', 'system', 'content', '{system_prompt}'),
                OBJECT_CONSTRUCT('role', 'user', 'content', '{user_question}')
            )
        ) as response
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            response_json = json.loads(result[0])
            return response_json['choices'][0]['messages']
        
        return "I couldn't generate a response. Please try again."
    
    def query_cortex_with_data(
        self,
        user_question: str,
        model: str = 'snowflake-arctic',
        data_query: Optional[str] = None
    ) -> str:
        """
        Query Cortex with data from dashboard connection for context-aware responses
        
        Args:
            user_question: Natural language question
            model: Cortex model to use
            data_query: Optional SQL query to fetch relevant data
        
        Returns:
            AI-generated response with data context
        
        Example:
            >>> response = manager.query_cortex_with_data(
            ...     "Explain the current delinquency trends",
            ...     data_query="SELECT * FROM delinquency_summary WHERE date >= DATEADD(month, -3, CURRENT_DATE())"
            ... )
        """
        if not self.dashboard_conn or not self.cortex_conn:
            raise ConnectionError("Both Dashboard and Cortex connections required")
        
        # Fetch data from dashboard
        data_context = ""
        if data_query:
            cursor = self.dashboard_conn.cursor(DictCursor)
            cursor.execute(data_query)
            data = cursor.fetchall()
            cursor.close()
            
            # Convert data to text context
            data_context = f"\n\nHere is the relevant data:\n{json.dumps(data, indent=2, default=str)}"
        
        # Query Cortex with data context
        enhanced_question = f"{user_question}{data_context}"
        return self.query_cortex(enhanced_question, model=model)
    
    def cortex_search(
        self,
        search_service: str,
        query: str,
        columns: list,
        limit: int = 5
    ) -> list:
        """
        Use Cortex Search for semantic search on documentation or data
        
        Args:
            search_service: Name of the Cortex Search service
            query: Search query
            columns: Columns to return
            limit: Number of results
        
        Returns:
            List of search results
        
        Example:
            >>> results = manager.cortex_search(
            ...     search_service='servicing_docs',
            ...     query='foreclosure process',
            ...     columns=['title', 'content', 'score']
            ... )
        """
        if not self.cortex_conn:
            raise ConnectionError("Cortex not connected")
        
        cursor = self.cortex_conn.cursor(DictCursor)
        
        columns_str = ', '.join(columns)
        query_sql = f"""
        SELECT {columns_str}
        FROM TABLE(
            {search_service}.SEARCH(
                '{query}',
                LIMIT => {limit}
            )
        )
        """
        
        cursor.execute(query_sql)
        results = cursor.fetchall()
        cursor.close()
        
        return [dict(row) for row in results]
    
    # ═══════════════════════════════════════════════════════════
    # CONNECTION MANAGEMENT
    # ═══════════════════════════════════════════════════════════
    
    def close_all(self):
        """Close all connections"""
        if self.dashboard_conn:
            self.dashboard_conn.close()
            print("🔒 Dashboard connection closed")
        
        if self.cortex_conn:
            self.cortex_conn.close()
            print("🔒 Cortex connection closed")
    
    def test_dashboard_connection(self) -> bool:
        """Test dashboard connection"""
        if not self.dashboard_conn:
            return False
        
        try:
            cursor = self.dashboard_conn.cursor()
            cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
            result = cursor.fetchone()
            print(f"✓ Dashboard: User={result[0]}, Role={result[1]}, Warehouse={result[2]}")
            cursor.close()
            return True
        except Exception as e:
            print(f"✗ Dashboard connection test failed: {str(e)}")
            return False
    
    def test_cortex_connection(self) -> bool:
        """Test Cortex connection and model availability"""
        if not self.cortex_conn:
            return False
        
        try:
            cursor = self.cortex_conn.cursor()
            
            # Test basic connection
            cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE()")
            result = cursor.fetchone()
            print(f"✓ Cortex: User={result[0]}, Role={result[1]}")
            
            # Test Cortex function availability
            cursor.execute("SELECT SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', 'Hello') as test")
            test_result = cursor.fetchone()
            print(f"✓ Cortex model test: {test_result[0][:50]}...")
            
            cursor.close()
            return True
        except Exception as e:
            print(f"✗ Cortex connection test failed: {str(e)}")
            return False


# ═══════════════════════════════════════════════════════════
# USAGE EXAMPLES
# ═══════════════════════════════════════════════════════════

def example_usage():
    """Example usage of DARA Connection Manager"""
    
    manager = DARAConnectionManager()
    
    # ──────────────────────────────────────────────────────────
    # Example 1: Connect Dashboard with SSO
    # ──────────────────────────────────────────────────────────
    print("\n" + "="*60)
    print("Example 1: Dashboard Connection (SSO)")
    print("="*60)
    
    manager.connect_dashboard_sso(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        warehouse='COMPUTE_WH',
        database='ANALYTICS_DB',
        schema='PUBLIC',
        role='ANALYST_ROLE'
    )
    
    manager.test_dashboard_connection()
    
    # ──────────────────────────────────────────────────────────
    # Example 2: Connect Cortex with PAT
    # ──────────────────────────────────────────────────────────
    print("\n" + "="*60)
    print("Example 2: Cortex Agent Connection (PAT)")
    print("="*60)
    
    manager.connect_cortex_pat(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('CORTEX_USER', 'CORTEX_USER'),
        pat=os.getenv('CORTEX_PAT'),  # Personal Access Token
        warehouse='CORTEX_WH',
        database='ANALYTICS_DB',
        schema='CORTEX',
        role='CORTEX_ROLE'
    )
    
    manager.test_cortex_connection()
    
    # ──────────────────────────────────────────────────────────
    # Example 3: Fetch Dashboard KPI
    # ──────────────────────────────────────────────────────────
    print("\n" + "="*60)
    print("Example 3: Fetch Dashboard KPI")
    print("="*60)
    
    try:
        kpi_data = manager.get_dashboard_kpi('portfolio_overview')
        print(f"Portfolio Overview: {kpi_data}")
    except Exception as e:
        print(f"Error fetching KPI: {str(e)}")
    
    # ──────────────────────────────────────────────────────────
    # Example 4: Query Cortex Agent
    # ──────────────────────────────────────────────────────────
    print("\n" + "="*60)
    print("Example 4: Query Cortex Agent")
    print("="*60)
    
    try:
        response = manager.query_cortex(
            user_question="What are the top 3 strategies to reduce delinquency rates?",
            model='snowflake-arctic'
        )
        print(f"Cortex Response: {response}")
    except Exception as e:
        print(f"Error querying Cortex: {str(e)}")
    
    # ──────────────────────────────────────────────────────────
    # Example 5: Cortex with Data Context
    # ──────────────────────────────────────────────────────────
    print("\n" + "="*60)
    print("Example 5: Cortex with Data Context")
    print("="*60)
    
    try:
        response = manager.query_cortex_with_data(
            user_question="Analyze the delinquency trends and suggest actions",
            model='snowflake-arctic',
            data_query="SELECT * FROM delinquency_summary LIMIT 10"
        )
        print(f"Context-Aware Response: {response}")
    except Exception as e:
        print(f"Error with context query: {str(e)}")
    
    # Clean up
    manager.close_all()


if __name__ == "__main__":
    # Check for required environment variables
    required_vars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'CORTEX_PAT']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("\n⚠️  Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these before running:")
        print("export SNOWFLAKE_ACCOUNT='xy12345.us-east-1'")
        print("export SNOWFLAKE_USER='your.email@company.com'")
        print("export CORTEX_PAT='sfp_xxxxxxxxxxxxxxxxxx'")
    else:
        example_usage()
