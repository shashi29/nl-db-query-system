"""
Plan validator for validating query execution plans.
"""
from typing import Any, Dict, List, Optional, Tuple, Union
import json

from ..config.logging_config import logger
from ..utils.query_utils import validate_mongodb_query, validate_clickhouse_query
from .schema_manager import schema_manager


class PlanValidator:
    """
    Validator for query execution plans.
    """
    
    @staticmethod
    def validate_plan(plan: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate a query execution plan.
        
        Args:
            plan: The query execution plan.
            
        Returns:
            Tuple[bool, str]: (True, "") if valid, (False, error_reason) if invalid.
        """
        try:
            # Check if plan has required fields
            if "data_source" not in plan:
                return False, "Plan missing 'data_source' field"
                
            if "query_type" not in plan:
                return False, "Plan missing 'query_type' field"
                
            # Validate based on data source
            data_source = plan["data_source"]
            
            if data_source == "mongodb":
                return PlanValidator._validate_mongodb_plan(plan)
                
            elif data_source == "clickhouse":
                return PlanValidator._validate_clickhouse_plan(plan)
                
            elif data_source == "federated":
                return PlanValidator._validate_federated_plan(plan)
                
            else:
                return False, f"Unsupported data source: {data_source}"
                
        except Exception as e:
            logger.error(f"Error validating plan: {str(e)}")
            return False, f"Validation error: {str(e)}"

    @staticmethod
    def _validate_mongodb_plan(plan: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate a MongoDB query execution plan.
        
        Args:
            plan: The MongoDB query execution plan.
            
        Returns:
            Tuple[bool, str]: (True, "") if valid, (False, error_reason) if invalid.
        """
        # Check if plan has required MongoDB-specific fields
        if "collection" not in plan:
            return False, "MongoDB plan missing 'collection' field"
            
        if "operation" not in plan:
            return False, "MongoDB plan missing 'operation' field"
            
        if "query" not in plan:
            return False, "MongoDB plan missing 'query' field"
            
        # Validate collection exists
        collection = plan["collection"]
        if collection not in schema_manager.get_mongodb_collections():
            return False, f"Collection '{collection}' does not exist"
            
        # Validate operation type
        operation = plan["operation"]
        valid_operations = [
            "find", "aggregate", "count", 
            "insert_one", "insert_many", 
            "update_one", "update_many", 
            "delete_one", "delete_many"
        ]
        if operation not in valid_operations:
            return False, f"Invalid MongoDB operation: {operation}"
            
        # Validate query
        query = plan["query"]
        if operation in ["find", "count", "delete_one", "delete_many"]:
            if not isinstance(query, dict):
                return False, f"Query for {operation} operation must be a dictionary"
                
        elif operation == "aggregate":
            if not isinstance(query, list):
                return False, f"Query for {operation} operation must be a list"
                
        elif operation in ["update_one", "update_many"]:
            if not isinstance(query, dict):
                return False, f"Query for {operation} operation must be a dictionary"
                
            if "filter" not in query or "update" not in query:
                return False, f"Update operation must include 'filter' and 'update' fields"
                
        # Validate query safety
        is_valid, reason = validate_mongodb_query(query)
        if not is_valid:
            return False, reason
            
        return True, ""
        
    @staticmethod
    def _validate_clickhouse_plan(plan: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate a ClickHouse query execution plan.
        
        Args:
            plan: The ClickHouse query execution plan.
            
        Returns:
            Tuple[bool, str]: (True, "") if valid, (False, error_reason) if invalid.
        """
        # Check if plan has required ClickHouse-specific fields
        if "query" not in plan:
            return False, "ClickHouse plan missing 'query' field"
            
        # Validate query
        query = plan["query"]
        if not isinstance(query, str):
            return False, "ClickHouse query must be a string"
            
        # Check if query is empty
        if not query.strip():
            return False, "ClickHouse query is empty"
            
        # Validate query safety
        is_valid, reason = validate_clickhouse_query(query)
        if not is_valid:
            return False, reason
            
        # Check if query references existing tables
        tables = schema_manager.get_clickhouse_tables()
        
        # Simple check for table references - a more robust parser would be used in production
        for table in tables:
            if f"FROM {table}" in query.upper() or f"JOIN {table}" in query.upper():
                return True, ""
                
        # If no tables were found, check if it might be a valid query without explicit table references
        if "SELECT 1" in query.upper() or "SHOW TABLES" in query.upper():
            return True, ""
            
        return False, "Query does not reference any existing tables"
        
    @staticmethod
    def _validate_federated_plan(plan: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate a federated query execution plan.
        
        Args:
            plan: The federated query execution plan.
            
        Returns:
            Tuple[bool, str]: (True, "") if valid, (False, error_reason) if invalid.
        """
        # Check if plan has required federated-specific fields
        if "steps" not in plan:
            return False, "Federated plan missing 'steps' field"
            
        steps = plan["steps"]
        if not isinstance(steps, list):
            return False, "Federated plan 'steps' must be a list"
            
        if not steps:
            return False, "Federated plan has no steps"
            
        # Validate each step
        for i, step in enumerate(steps):
            if "step_type" not in step:
                return False, f"Step {i} missing 'step_type' field"
                
            if "data_source" not in step:
                return False, f"Step {i} missing 'data_source' field"
                
            # Validate step type
            step_type = step["step_type"]
            valid_step_types = ["query", "transform", "join", "union", "final"]
            if step_type not in valid_step_types:
                return False, f"Invalid step type in step {i}: {step_type}"
                
            # Validate data source
            data_source = step["data_source"]
            if data_source not in ["mongodb", "clickhouse", "memory"]:
                return False, f"Invalid data source in step {i}: {data_source}"
                
            # Validate based on data source
            if data_source == "mongodb":
                if "mongodb_plan" not in step:
                    return False, f"Step {i} missing 'mongodb_plan' field"
                    
                is_valid, reason = PlanValidator._validate_mongodb_plan(step["mongodb_plan"])
                if not is_valid:
                    return False, f"Invalid MongoDB plan in step {i}: {reason}"
                    
            elif data_source == "clickhouse":
                if "clickhouse_plan" not in step:
                    return False, f"Step {i} missing 'clickhouse_plan' field"
                    
                is_valid, reason = PlanValidator._validate_clickhouse_plan(step["clickhouse_plan"])
                if not is_valid:
                    return False, f"Invalid ClickHouse plan in step {i}: {reason}"
                    
        # Validate that the final step exists
        has_final_step = any(step["step_type"] == "final" for step in steps)
        if not has_final_step:
            return False, "Federated plan missing a 'final' step"
            
        return True, ""