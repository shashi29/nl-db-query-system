"""
Preprocessing utilities for natural language queries.
Handles preprocessing of user queries before sending to OpenAI.
"""
import re
from typing import Dict, List, Optional, Set, Tuple

from ..config.logging_config import logger


def preprocess_query(query: str) -> str:
    """
    Preprocess a natural language query for better results.
    
    Args:
        query: The raw user query.
        
    Returns:
        str: Processed query.
    """
    # Convert to lowercase for consistent processing
    processed = query.lower()
    
    # Remove multiple spaces
    processed = re.sub(r'\s+', ' ', processed)
    
    # Remove punctuation that doesn't affect meaning
    processed = re.sub(r'[^\w\s?!.]', ' ', processed)
    
    # Normalize database terminology
    processed = normalize_database_terms(processed)
    
    # Remove filler words
    filler_words = {
        "please", "could you", "would you", "can you", "i want", "i need",
        "i would like", "give me", "show me", "tell me", "find", "search for"
    }
    for word in filler_words:
        processed = processed.replace(word, "")
    
    # Clean up extra spaces again
    processed = re.sub(r'\s+', ' ', processed).strip()
    
    logger.debug(f"Preprocessed query: '{query}' -> '{processed}'")
    return processed


def normalize_database_terms(text: str) -> str:
    """
    Normalize database terminology in the query.
    
    Args:
        text: The text to normalize.
        
    Returns:
        str: Text with normalized database terms.
    """
    # Dictionary of common terms and their normalized versions
    term_mapping = {
        "mongo db": "mongodb",
        "click house": "clickhouse",
        "records": "documents",
        "rows": "documents",
        "entries": "documents",
        "fields": "fields",
        "columns": "fields",
        "attributes": "fields",
        "properties": "fields",
        "relations": "relationships",
        "relationships": "relationships",
        "sort by": "sort",
        "order by": "sort",
        "group by": "group",
        "aggregate by": "aggregate",
        "join": "join"
    }
    
    # Replace terms
    for original, normalized in term_mapping.items():
        pattern = r'\b' + re.escape(original) + r'\b'
        text = re.sub(pattern, normalized, text)
    
    return text


def extract_db_references(query: str) -> Tuple[List[str], List[str]]:
    """
    Extract references to databases, collections, and tables from a query.
    
    Args:
        query: The user query.
        
    Returns:
        Tuple[List[str], List[str]]: Lists of MongoDB collections and ClickHouse tables mentioned.
    """
    # Simple extraction using regex patterns
    # In a real implementation, this would be more sophisticated
    mongodb_collections = []
    clickhouse_tables = []
    
    # Extract collection/table names
    # This is a simplified version - would need to be improved with actual schema information
    collection_pattern = r'(in|from|to|update|delete from|insert into)\s+([a-zA-Z0-9_]+)'
    matches = re.findall(collection_pattern, query.lower())
    
    for _, name in matches:
        # Determine if it's a MongoDB collection or ClickHouse table
        # This is oversimplified; in reality would check against actual schema
        if "time" in name or "event" in name or name.endswith("s"):
            clickhouse_tables.append(name)
        else:
            mongodb_collections.append(name)
    
    # Deduplicate
    mongodb_collections = list(set(mongodb_collections))
    clickhouse_tables = list(set(clickhouse_tables))
    
    logger.debug(f"Extracted DB references: MongoDB={mongodb_collections}, ClickHouse={clickhouse_tables}")
    return mongodb_collections, clickhouse_tables


def extract_operation_type(query: str) -> str:
    """
    Extract the type of database operation from the query.
    
    Args:
        query: The user query.
        
    Returns:
        str: Operation type (find, update, delete, aggregate, etc.)
    """
    # Check for operation keywords
    if re.search(r'\b(find|get|show|display|list|search|select|query)\b', query.lower()):
        return "find"
    elif re.search(r'\b(count|how many|number of)\b', query.lower()):
        return "count"
    elif re.search(r'\b(average|avg|mean|sum|total|max|min|compute|calculate)\b', query.lower()):
        return "aggregate"
    elif re.search(r'\b(insert|add|create|new)\b', query.lower()):
        return "insert"
    elif re.search(r'\b(update|change|modify|set)\b', query.lower()):
        return "update"
    elif re.search(r'\b(delete|remove|drop)\b', query.lower()):
        return "delete"
    else:
        # Default to find operation
        return "find"


def extract_field_references(query: str) -> List[str]:
    """
    Extract field/column names referenced in the query.
    
    Args:
        query: The user query.
        
    Returns:
        List[str]: Field/column names mentioned.
    """
    # Common field name patterns
    field_patterns = [
        r'field\s+([a-zA-Z0-9_]+)',
        r'column\s+([a-zA-Z0-9_]+)',
        r'([a-zA-Z0-9_]+)\s+field',
        r'([a-zA-Z0-9_]+)\s+column',
        r'([a-zA-Z0-9_]+)\s+is',
        r'([a-zA-Z0-9_]+)\s+equals',
        r'([a-zA-Z0-9_]+)\s+contains',
        r'([a-zA-Z0-9_]+)\s+greater than',
        r'([a-zA-Z0-9_]+)\s+less than',
    ]
    
    fields = []
    for pattern in field_patterns:
        matches = re.findall(pattern, query.lower())
        fields.extend(matches)
    
    # Filter out common words that might be mistaken for fields
    stopwords = {
        "the", "and", "or", "in", "where", "from", "that", "with", "for", 
        "have", "this", "not", "but", "all", "what", "when", "who", "which"
    }
    fields = [field for field in fields if field not in stopwords]
    
    return list(set(fields))


def check_dangerous_patterns(query: str) -> Tuple[bool, str]:
    """
    Check for potentially dangerous patterns in the query.
    
    Args:
        query: The user query.
        
    Returns:
        Tuple[bool, str]: (True, reason) if dangerous pattern found, (False, "") otherwise.
    """
    dangerous_patterns = [
        (r'\bdrop\b', "Detected 'drop' command"),
        (r'\bdelete\b.*\bwhere\b', "Detected 'delete where' pattern"),
        (r'\btruncate\b', "Detected 'truncate' command"),
        (r'\bsystem\b', "Detected 'system' reference"),
        (r'\bshutdown\b', "Detected 'shutdown' command"),
        (r';.*--', "Detected potential SQL injection pattern"),
        (r'\$where\b', "Detected potential MongoDB injection"),
        (r'\$ne\b', "Detected potential MongoDB injection ($ne operator)"),
        (r'\$exists\b', "Detected potential MongoDB injection ($exists operator)"),
        (r'password', "Detected 'password' in query - might be accessing sensitive data")
    ]
    
    for pattern, reason in dangerous_patterns:
        if re.search(pattern, query.lower()):
            logger.warning(f"Dangerous pattern detected in query: '{query}'. Reason: {reason}")
            return True, reason
    
    return False, ""