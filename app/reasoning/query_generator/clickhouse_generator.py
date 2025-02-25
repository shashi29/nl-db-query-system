"""
ClickHouse query generator for generating SQL queries.
"""
from typing import Any, Dict, List, Optional, Union
import re

from ...config.logging_config import logger
from ...utils.query_utils import add_query_timeout, sanitize_clickhouse_table_name


class ClickHouseQueryGenerator:
    """
    Generator for ClickHouse SQL queries.
    """
    
    @staticmethod
    def generate_query(
        query_plan: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a ClickHouse SQL query from a query plan.
        
        Args:
            query_plan: The query plan.
            
        Returns:
            Dict[str, Any]: Generated SQL query details.
        """
        try:
            # Check if query is present
            if "query" not in query_plan:
                return {"success": False, "error": "Query not specified"}
                
            # Extract the query
            query = query_plan["query"]
            params = query_plan.get("params", {})
            settings = query_plan.get("settings", {})
            
            # Add timeout to query
            query_with_timeout = add_query_timeout(query, is_mongodb=False)
            
            # Determine the query type
            query_type = ClickHouseQueryGenerator._determine_query_type(query_with_timeout)
            
            # Generate the appropriate query
            if query_type == "select":
                return ClickHouseQueryGenerator._generate_select_query(query_with_timeout, params, settings)
                
            elif query_type == "insert":
                return ClickHouseQueryGenerator._generate_insert_query(query_with_timeout, params, settings)
                
            elif query_type == "other":
                return ClickHouseQueryGenerator._generate_other_query(query_with_timeout, params, settings)
                
            else:
                return {"success": False, "error": f"Unsupported query type: {query_type}"}
                
        except Exception as e:
            logger.error(f"Error generating ClickHouse query: {str(e)}")
            return {"success": False, "error": f"Error generating ClickHouse query: {str(e)}"}

    @staticmethod
    def _determine_query_type(query: str) -> str:
        """
        Determine the type of SQL query.
        
        Args:
            query: The SQL query.
            
        Returns:
            str: Query type (select, insert, other).
        """
        # Normalize query for easier checking
        normalized_query = query.strip().upper()
        
        if normalized_query.startswith("SELECT"):
            return "select"
            
        elif normalized_query.startswith("INSERT"):
            return "insert"
            
        else:
            return "other"

    @staticmethod
    def _generate_select_query(
        query: str, 
        params: Dict[str, Any],
        settings: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a ClickHouse SELECT query.
        
        Args:
            query: The SQL query.
            params: Query parameters.
            settings: ClickHouse settings.
            
        Returns:
            Dict[str, Any]: Generated query details.
        """
        # Sanitize table names in the query
        sanitized_query = ClickHouseQueryGenerator._sanitize_table_names(query)
        
        # Identify key components of the query
        from_table = ClickHouseQueryGenerator._extract_from_table(sanitized_query)
        
        # Build the executable query
        executable_query = {
            "query": sanitized_query,
            "params": params,
            "settings": settings
        }
        
        # Create readable query representation
        readable_query = sanitized_query
        
        if params:
            readable_query += f"\n-- With parameters: {str(params)}"
            
        if settings:
            readable_query += f"\n-- With settings: {str(settings)}"
            
        return {
            "success": True,
            "query_type": "select",
            "from_table": from_table,
            "executable_query": executable_query,
            "readable_query": readable_query
        }

    @staticmethod
    def _generate_insert_query(
        query: str, 
        params: Dict[str, Any],
        settings: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a ClickHouse INSERT query.
        
        Args:
            query: The SQL query.
            params: Query parameters.
            settings: ClickHouse settings.
            
        Returns:
            Dict[str, Any]: Generated query details.
        """
        # Sanitize table names in the query
        sanitized_query = ClickHouseQueryGenerator._sanitize_table_names(query)
        
        # Identify key components of the query
        into_table = ClickHouseQueryGenerator._extract_into_table(sanitized_query)
        
        # Build the executable query
        executable_query = {
            "query": sanitized_query,
            "params": params,
            "settings": settings
        }
        
        # Create readable query representation
        readable_query = sanitized_query
        
        if params:
            readable_query += f"\n-- With parameters: {str(params)}"
            
        if settings:
            readable_query += f"\n-- With settings: {str(settings)}"
            
        return {
            "success": True,
            "query_type": "insert",
            "into_table": into_table,
            "executable_query": executable_query,
            "readable_query": readable_query
        }

    @staticmethod
    def _generate_other_query(
        query: str, 
        params: Dict[str, Any],
        settings: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate other types of ClickHouse queries.
        
        Args:
            query: The SQL query.
            params: Query parameters.
            settings: ClickHouse settings.
            
        Returns:
            Dict[str, Any]: Generated query details.
        """
        # Sanitize table names in the query
        sanitized_query = ClickHouseQueryGenerator._sanitize_table_names(query)
        
        # Build the executable query
        executable_query = {
            "query": sanitized_query,
            "params": params,
            "settings": settings
        }
        
        # Create readable query representation
        readable_query = sanitized_query
        
        if params:
            readable_query += f"\n-- With parameters: {str(params)}"
            
        if settings:
            readable_query += f"\n-- With settings: {str(settings)}"
            
        return {
            "success": True,
            "query_type": "other",
            "executable_query": executable_query,
            "readable_query": readable_query
        }

    @staticmethod
    def _sanitize_table_names(query: str) -> str:
        """
        Sanitize table names in a SQL query.
        
        Args:
            query: The SQL query.
            
        Returns:
            str: Query with sanitized table names.
        """
        # This is a simplified implementation
        # In a real-world scenario, you would use a SQL parser
        
        # Find FROM clauses
        from_pattern = r'FROM\s+([a-zA-Z0-9_\.]+)'
        from_matches = re.finditer(from_pattern, query, re.IGNORECASE)
        
        sanitized_query = query
        
        for match in from_matches:
            table_name = match.group(1)
            sanitized_table = sanitize_clickhouse_table_name(table_name)
            
            if table_name != sanitized_table:
                # Replace the table name
                start, end = match.span(1)
                sanitized_query = sanitized_query[:start] + sanitized_table + sanitized_query[end:]
        
        # Find JOIN clauses
        join_pattern = r'JOIN\s+([a-zA-Z0-9_\.]+)'
        join_matches = re.finditer(join_pattern, sanitized_query, re.IGNORECASE)
        
        for match in join_matches:
            table_name = match.group(1)
            sanitized_table = sanitize_clickhouse_table_name(table_name)
            
            if table_name != sanitized_table:
                # Replace the table name
                start, end = match.span(1)
                sanitized_query = sanitized_query[:start] + sanitized_table + sanitized_query[end:]
        
        # Find INTO clauses
        into_pattern = r'INTO\s+([a-zA-Z0-9_\.]+)'
        into_matches = re.finditer(into_pattern, sanitized_query, re.IGNORECASE)
        
        for match in into_matches:
            table_name = match.group(1)
            sanitized_table = sanitize_clickhouse_table_name(table_name)
            
            if table_name != sanitized_table:
                # Replace the table name
                start, end = match.span(1)
                sanitized_query = sanitized_query[:start] + sanitized_table + sanitized_query[end:]
        
        return sanitized_query

    @staticmethod
    def _extract_from_table(query: str) -> Optional[str]:
        """
        Extract the table name from a FROM clause.
        
        Args:
            query: The SQL query.
            
        Returns:
            Optional[str]: Table name, or None if not found.
        """
        # Simple regex to extract table name from FROM clause
        match = re.search(r'FROM\s+([a-zA-Z0-9_\.]+)', query, re.IGNORECASE)
        
        if match:
            return match.group(1)
            
        return None

    @staticmethod
    def _extract_into_table(query: str) -> Optional[str]:
        """
        Extract the table name from an INTO clause.
        
        Args:
            query: The SQL query.
            
        Returns:
            Optional[str]: Table name, or None if not found.
        """
        # Simple regex to extract table name from INTO clause
        match = re.search(r'INTO\s+([a-zA-Z0-9_\.]+)', query, re.IGNORECASE)
        
        if match:
            return match.group(1)
            
        return None