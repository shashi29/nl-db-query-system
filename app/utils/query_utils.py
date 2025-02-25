"""
Query utilities for validating and manipulating database queries.
"""
import re
import json
from typing import Any, Dict, List, Optional, Tuple, Union

from ..config.settings import settings
from ..config.logging_config import logger


def validate_mongodb_query(query: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate a MongoDB query for safety and correctness.
    
    Args:
        query: The MongoDB query as a dictionary.
        
    Returns:
        Tuple[bool, str]: (True, "") if valid, (False, error_reason) if invalid.
    """
    # Check if query is too large
    query_str = json.dumps(query)
    if len(query_str) > settings.security.max_query_size:
        return False, f"Query exceeds maximum size of {settings.security.max_query_size} characters"
    
    # Check for dangerous operations
    dangerous_ops = ["$where", "$function", "$eval", "mapReduce"]
    for op in dangerous_ops:
        if _contains_key(query, op):
            return False, f"Query contains dangerous operation: {op}"
    
    # Check for system collection access
    if _is_system_collection_access(query):
        return False, "Query attempts to access system collections"
    
    # Check for JavaScript execution (potential injection)
    if _contains_javascript(query):
        return False, "Query contains JavaScript code execution"
    
    return True, ""


def validate_clickhouse_query(query: str) -> Tuple[bool, str]:
    """
    Validate a ClickHouse SQL query for safety and correctness.
    
    Args:
        query: The SQL query string.
        
    Returns:
        Tuple[bool, str]: (True, "") if valid, (False, error_reason) if invalid.
    """
    # Check if query is too large
    if len(query) > settings.security.max_query_size:
        return False, f"Query exceeds maximum size of {settings.security.max_query_size} characters"
    
    # Check for dangerous operations
    dangerous_ops = [
        (r'\bDROP\b', "DROP operation"), 
        (r'\bTRUNCATE\b', "TRUNCATE operation"),
        (r'\bALTER\b', "ALTER operation"),
        (r'\bGRANT\b', "GRANT operation"),
        (r'\bREVOKE\b', "REVOKE operation"),
        (r'\bSYSTEM\b', "SYSTEM command"),
        (r'\bSHUTDOWN\b', "SHUTDOWN operation"),
        (r'\bKILL\b', "KILL operation"),
        (r'\bOUTFILE\b', "OUTFILE operation")
    ]
    
    query_upper = query.upper()
    for pattern, reason in dangerous_ops:
        if re.search(pattern, query_upper):
            return False, f"Query contains dangerous operation: {reason}"
    
    # If write operations are disabled, check for write operations
    if not settings.security.enable_write_operations:
        write_ops = [r'\bINSERT\b', r'\bUPDATE\b', r'\bDELETE\b', r'\bCREATE\b']
        for pattern in write_ops:
            if re.search(pattern, query_upper):
                return False, "Write operations are disabled"
    
    # Check for multi-statement queries (potential for injection)
    if ";" in query and not query.strip().endswith(";"):
        statements = [s.strip() for s in query.split(";") if s.strip()]
        if len(statements) > 1:
            return False, "Multi-statement queries are not allowed"
    
    # Check for potential SQL injection via comments
    if "--" in query or "/*" in query:
        return False, "Query contains comment syntax that may be used for SQL injection"
    
    return True, ""


def sanitize_clickhouse_table_name(table_name: str) -> str:
    """
    Sanitize a ClickHouse table name.
    
    Args:
        table_name: The table name to sanitize.
        
    Returns:
        str: Sanitized table name.
    """
    # Only allow alphanumeric characters and underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '', table_name)
    
    # Prevent access to system tables
    if sanitized.lower().startswith(('system', '_system')):
        sanitized = "user_" + sanitized
        
    return sanitized


def sanitize_mongodb_collection_name(collection_name: str) -> str:
    """
    Sanitize a MongoDB collection name.
    
    Args:
        collection_name: The collection name to sanitize.
        
    Returns:
        str: Sanitized collection name.
    """
    # Only allow alphanumeric characters and underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '', collection_name)
    
    # Prevent access to system collections
    if sanitized.lower().startswith(('system.', 'admin.')):
        sanitized = "user_" + sanitized
        
    return sanitized


def add_query_timeout(query: Union[Dict[str, Any], str], is_mongodb: bool = True) -> Union[Dict[str, Any], str]:
    """
    Add timeout constraints to a query.
    
    Args:
        query: The query to modify.
        is_mongodb: True if MongoDB query, False if ClickHouse.
        
    Returns:
        Union[Dict[str, Any], str]: Modified query with timeout constraints.
    """
    timeout_seconds = settings.security.query_timeout_seconds
    
    if is_mongodb:
        # For MongoDB, add maxTimeMS option
        if isinstance(query, dict):
            # If it's a find-like query
            return {"$maxTimeMS": timeout_seconds * 1000, **query}
        else:
            # If it's already a string for some reason
            return query
    else:
        # For ClickHouse, add SETTINGS max_execution_time
        if isinstance(query, str):
            # Check if SETTINGS already exists
            if "SETTINGS" in query.upper():
                return query + f", max_execution_time={timeout_seconds}"
            else:
                return query + f" SETTINGS max_execution_time={timeout_seconds}"
        else:
            # If it's not a string
            return query


def _contains_key(obj: Any, key: str) -> bool:
    """
    Recursively check if a dictionary contains a specific key.
    
    Args:
        obj: Object to check (dict, list, or scalar).
        key: Key to look for.
        
    Returns:
        bool: True if the key is found, False otherwise.
    """
    if isinstance(obj, dict):
        if key in obj:
            return True
        return any(_contains_key(v, key) for v in obj.values())
    elif isinstance(obj, list):
        return any(_contains_key(item, key) for item in obj)
    else:
        return False


def _is_system_collection_access(query: Dict[str, Any]) -> bool:
    """
    Check if a query tries to access system collections.
    
    Args:
        query: MongoDB query to check.
        
    Returns:
        bool: True if accessing system collections, False otherwise.
    """
    # Convert query to string to check for system collection patterns
    query_str = json.dumps(query)
    system_patterns = [
        r'system\.', r'admin\.', r'config\.', r'local\.'
    ]
    
    for pattern in system_patterns:
        if re.search(pattern, query_str):
            return True
    
    return False


def _contains_javascript(query: Dict[str, Any]) -> bool:
    """
    Check if a query contains JavaScript code.
    
    Args:
        query: MongoDB query to check.
        
    Returns:
        bool: True if contains JavaScript, False otherwise.
    """
    # Convert query to string to check for JavaScript patterns
    query_str = json.dumps(query)
    js_patterns = [
        r'function\s*\(', r'=>', r'new\s+', r'this\.', 
        r'prototype', r'constructor', r'\$eval', r'\$where'
    ]
    
    for pattern in js_patterns:
        if re.search(pattern, query_str):
            return True
    
    return False