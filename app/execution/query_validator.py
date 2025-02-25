"""
Query validator for validating database queries before execution.
"""
from typing import Any, Dict, List, Optional, Tuple, Union
import re
import json

from ..config.logging_config import logger
from ..config.settings import settings
from ..utils.query_utils import (
    validate_mongodb_query, 
    validate_clickhouse_query, 
    sanitize_mongodb_collection_name, 
    sanitize_clickhouse_table_name
)


class QueryValidator:
    """
    Validator for database queries before execution.
    """
    
    @staticmethod
    def validate(
        executable_query: Dict[str, Any],
        data_source: str
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Validate a query before execution.
        
        Args:
            executable_query: The executable query.
            data_source: The data source (mongodb, clickhouse, federated).
            
        Returns:
            Tuple[bool, str, Dict[str, Any]]: 
                (True, "", sanitized_query) if valid, 
                (False, error_reason, {}) if invalid.
        """
        try:
            if data_source == "mongodb":
                return QueryValidator._validate_mongodb_query(executable_query)
                
            elif data_source == "clickhouse":
                return QueryValidator._validate_clickhouse_query(executable_query)
                
            elif data_source == "federated":
                return QueryValidator._validate_federated_query(executable_query)
                
            else:
                return False, f"Unsupported data source: {data_source}", {}
                
        except Exception as e:
            logger.error(f"Error validating query: {str(e)}")
            return False, f"Validation error: {str(e)}", {}

    @staticmethod
    def _validate_mongodb_query(
        executable_query: Dict[str, Any]
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Validate a MongoDB query.
        
        Args:
            executable_query: The executable MongoDB query.
            
        Returns:
            Tuple[bool, str, Dict[str, Any]]: Validation result.
        """
        # Check required fields
        if "collection" not in executable_query:
            return False, "Collection not specified", {}
            
        if "operation" not in executable_query:
            return False, "Operation not specified", {}
            
        # Get query components
        collection = executable_query["collection"]
        operation = executable_query["operation"]
        
        # Sanitize collection name
        sanitized_collection = sanitize_mongodb_collection_name(collection)
        
        # Check if operation is allowed
        allowed_operations = settings.security.allowed_query_types
        if operation not in allowed_operations:
            return False, f"Operation '{operation}' is not allowed", {}
            
        # Check if write operations are enabled
        if operation in ["insert_one", "insert_many", "update_one", "update_many", "delete_one", "delete_many"]:
            if not settings.security.enable_write_operations:
                return False, "Write operations are disabled", {}
        
        # Validate query based on operation type
        if operation in ["find", "count", "delete_one", "delete_many"]:
            if "filter" not in executable_query:
                return False, "Filter not specified", {}
                
            query_filter = executable_query["filter"]
            is_valid, reason = validate_mongodb_query(query_filter)
            
            if not is_valid:
                return False, reason, {}
                
        elif operation == "aggregate":
            if "pipeline" not in executable_query:
                return False, "Pipeline not specified", {}
                
            pipeline = executable_query["pipeline"]
            is_valid, reason = validate_mongodb_query(pipeline)
            
            if not is_valid:
                return False, reason, {}
                
        elif operation == "insert_one":
            if "document" not in executable_query:
                return False, "Document not specified", {}
                
            document = executable_query["document"]
            is_valid, reason = validate_mongodb_query(document)
            
            if not is_valid:
                return False, reason, {}
                
        elif operation == "insert_many":
            if "documents" not in executable_query:
                return False, "Documents not specified", {}
                
            documents = executable_query["documents"]
            is_valid, reason = validate_mongodb_query(documents)
            
            if not is_valid:
                return False, reason, {}
                
        elif operation in ["update_one", "update_many"]:
            if "filter" not in executable_query:
                return False, "Filter not specified", {}
                
            if "update" not in executable_query:
                return False, "Update not specified", {}
                
            query_filter = executable_query["filter"]
            update = executable_query["update"]
            
            # Validate filter
            is_valid, reason = validate_mongodb_query(query_filter)
            if not is_valid:
                return False, f"Invalid filter: {reason}", {}
                
            # Validate update
            is_valid, reason = validate_mongodb_query(update)
            if not is_valid:
                return False, f"Invalid update: {reason}", {}
        
        # Create sanitized query
        sanitized_query = executable_query.copy()
        sanitized_query["collection"] = sanitized_collection
        
        return True, "", sanitized_query

    @staticmethod
    def _validate_clickhouse_query(
        executable_query: Dict[str, Any]
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Validate a ClickHouse query.
        
        Args:
            executable_query: The executable ClickHouse query.
            
        Returns:
            Tuple[bool, str, Dict[str, Any]]: Validation result.
        """
        # Check required fields
        if "query" not in executable_query:
            return False, "Query not specified", {}
            
        # Get query components
        query = executable_query["query"]
        params = executable_query.get("params", {})
        settings_dict = executable_query.get("settings", {})
        
        # Validate query
        is_valid, reason = validate_clickhouse_query(query)
        if not is_valid:
            return False, reason, {}
            
        # Check for write operations if they're disabled
        query_upper = query.upper()
        write_operations = ["INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP"]
        
        if not settings.security.enable_write_operations:
            for op in write_operations:
                if op in query_upper:
                    return False, f"Write operation ({op}) is not allowed", {}
        
        # Sanitize table names
        sanitized_query = query
        
        # Extract table names using simple regex patterns
        # This is a simplified approach - a real implementation would use a SQL parser
        table_patterns = [
            r'FROM\s+([a-zA-Z0-9_\.]+)',
            r'JOIN\s+([a-zA-Z0-9_\.]+)',
            r'INTO\s+([a-zA-Z0-9_\.]+)'
        ]
        
        for pattern in table_patterns:
            for match in re.finditer(pattern, query, re.IGNORECASE):
                table_name = match.group(1)
                sanitized_name = sanitize_clickhouse_table_name(table_name)
                
                if table_name != sanitized_name:
                    start, end = match.span(1)
                    prefix = sanitized_query[:start]
                    suffix = sanitized_query[end:]
                    sanitized_query = f"{prefix}{sanitized_name}{suffix}"
        
        # Create sanitized query dict
        sanitized_query_dict = {
            "query": sanitized_query,
            "params": params,
            "settings": settings_dict
        }
        
        return True, "", sanitized_query_dict
        
    @staticmethod
    def _validate_federated_query(
        executable_query: Dict[str, Any]
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Validate a federated query.
        
        Args:
            executable_query: The executable federated query.
            
        Returns:
            Tuple[bool, str, Dict[str, Any]]: Validation result.
        """
        # Check required fields
        if "steps" not in executable_query:
            return False, "Steps not specified", {}
            
        steps = executable_query["steps"]
        if not isinstance(steps, list):
            return False, "Steps must be a list", {}
            
        if not steps:
            return False, "No steps specified", {}
            
        # Validate each step
        sanitized_steps = []
        
        for i, step in enumerate(steps):
            # Check step structure
            if "step_type" not in step:
                return False, f"Step {i}: Step type not specified", {}
                
            if "data_source" not in step:
                return False, f"Step {i}: Data source not specified", {}
                
            step_type = step["step_type"]
            data_source = step["data_source"]
            
            # Validate based on data source
            if data_source == "mongodb":
                if "mongodb_query" not in step:
                    return False, f"Step {i}: MongoDB query not specified", {}
                    
                mongodb_query = step["mongodb_query"]
                is_valid, reason, sanitized_query = QueryValidator._validate_mongodb_query(mongodb_query)
                
                if not is_valid:
                    return False, f"Step {i}: {reason}", {}
                    
                # Create sanitized step
                sanitized_step = step.copy()
                sanitized_step["mongodb_query"] = sanitized_query
                sanitized_steps.append(sanitized_step)
                
            elif data_source == "clickhouse":
                if "clickhouse_query" not in step:
                    return False, f"Step {i}: ClickHouse query not specified", {}
                    
                clickhouse_query = step["clickhouse_query"]
                is_valid, reason, sanitized_query = QueryValidator._validate_clickhouse_query(clickhouse_query)
                
                if not is_valid:
                    return False, f"Step {i}: {reason}", {}
                    
                # Create sanitized step
                sanitized_step = step.copy()
                sanitized_step["clickhouse_query"] = sanitized_query
                sanitized_steps.append(sanitized_step)
                
            elif data_source == "memory":
                if "operation" not in step:
                    return False, f"Step {i}: Operation not specified", {}
                    
                if "inputs" not in step:
                    return False, f"Step {i}: Inputs not specified", {}
                    
                # Just copy memory operations as they don't need sanitization
                sanitized_steps.append(step.copy())
                
            else:
                return False, f"Step {i}: Unsupported data source: {data_source}", {}
        
        # Check for a final step
        has_final_step = any(step["step_type"] == "final" for step in steps)
        if not has_final_step:
            return False, "No final step specified", {}
            
        # Create sanitized query
        sanitized_query = {
            "steps": sanitized_steps
        }
        
        return True, "", sanitized_query