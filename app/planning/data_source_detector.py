"""
Data source detector for determining which database to use.
"""
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import re

from ..config.logging_config import logger
from ..utils.preprocessing import extract_db_references, extract_operation_type, extract_field_references
from .schema_manager import schema_manager


class DataSourceDetector:
    """
    Detector for determining which database(s) to use for a query.
    """
    
    @staticmethod
    async def detect_data_source(query: str) -> Dict[str, Any]:
        """
        Detect which database(s) should be used for the query.
        
        Args:
            query: The natural language query.
            
        Returns:
            Dict[str, Any]: Data source information.
        """
        # Extract explicit references to collections/tables
        mongodb_refs, clickhouse_refs = extract_db_references(query)
        
        # Extract operation type
        operation_type = extract_operation_type(query)
        
        # Extract field references
        fields = extract_field_references(query)
        
        # Score different data sources
        mongodb_score = DataSourceDetector._score_mongodb(query, mongodb_refs, operation_type, fields)
        clickhouse_score = DataSourceDetector._score_clickhouse(query, clickhouse_refs, operation_type, fields)
        
        # Determine data source based on scores
        if mongodb_score > 0 and clickhouse_score > 0:
            # Both databases might be needed (federated query)
            if mongodb_score >= clickhouse_score:
                primary = "mongodb"
                secondary = "clickhouse"
            else:
                primary = "clickhouse"
                secondary = "mongodb"
                
            data_source = "federated"
            
        elif mongodb_score > 0:
            # Use MongoDB
            primary = "mongodb"
            secondary = None
            data_source = "mongodb"
            
        elif clickhouse_score > 0:
            # Use ClickHouse
            primary = "clickhouse"
            secondary = None
            data_source = "clickhouse"
            
        else:
            # Default to MongoDB if we can't determine
            primary = "mongodb"
            secondary = None
            data_source = "mongodb"
        
        # Log detection result
        logger.debug(f"Data source detection: {data_source} (MongoDB score: {mongodb_score}, ClickHouse score: {clickhouse_score})")
        
        # Return detection result
        return {
            "data_source": data_source,
            "primary": primary,
            "secondary": secondary,
            "mongodb_score": mongodb_score,
            "clickhouse_score": clickhouse_score,
            "mongodb_references": mongodb_refs,
            "clickhouse_references": clickhouse_refs,
            "operation_type": operation_type,
            "field_references": fields
        }

    @staticmethod
    def _score_mongodb(
        query: str, 
        mongodb_refs: List[str], 
        operation_type: str, 
        fields: List[str]
    ) -> float:
        """
        Score MongoDB as a potential data source.
        
        Args:
            query: The query text.
            mongodb_refs: MongoDB collection references.
            operation_type: The operation type.
            fields: Field references.
            
        Returns:
            float: Score for MongoDB (0-10).
        """
        score = 0.0
        
        # If MongoDB collections are explicitly referenced
        if mongodb_refs:
            score += 5.0
            
            # Check if referenced collections actually exist
            mongodb_collections = schema_manager.get_mongodb_collections()
            for ref in mongodb_refs:
                if ref in mongodb_collections:
                    score += 2.0
        
        # Operation type scoring
        if operation_type in ["find", "insert", "update", "delete"]:
            score += 1.0
        elif operation_type == "aggregate":
            score += 0.5  # MongoDB can aggregate, but ClickHouse might be better
        
        # Look for MongoDB-specific terms
        mongodb_terms = [
            "document", "collection", "mongo", "mongodb", "nosql", 
            "embedded", "subdocument", "object id", "bson"
        ]
        for term in mongodb_terms:
            if term in query.lower():
                score += 0.5
        
        # Look for references to MongoDB operators
        mongodb_operators = [
            "$match", "$group", "$sort", "$project", "$lookup", 
            "$unwind", "$in", "$or", "$and", "$elemMatch"
        ]
        for op in mongodb_operators:
            if op in query.lower():
                score += 0.5
        
        # Field matching
        if fields:
            # Check if fields exist in MongoDB collections
            for collection in schema_manager.get_mongodb_collections():
                schema = schema_manager.get_mongodb_schema(collection)
                for field in fields:
                    if field in schema:
                        score += 0.5
        
        return min(score, 10.0)  # Cap score at 10

    @staticmethod
    def _score_clickhouse(
        query: str, 
        clickhouse_refs: List[str], 
        operation_type: str, 
        fields: List[str]
    ) -> float:
        """
        Score ClickHouse as a potential data source.
        
        Args:
            query: The query text.
            clickhouse_refs: ClickHouse table references.
            operation_type: The operation type.
            fields: Field references.
            
        Returns:
            float: Score for ClickHouse (0-10).
        """
        score = 0.0
        
        # If ClickHouse tables are explicitly referenced
        if clickhouse_refs:
            score += 5.0
            
            # Check if referenced tables actually exist
            clickhouse_tables = schema_manager.get_clickhouse_tables()
            for ref in clickhouse_refs:
                if ref in clickhouse_tables:
                    score += 2.0
        
        # Operation type scoring
        if operation_type in ["aggregate", "count"]:
            score += 2.0  # ClickHouse excels at aggregations
        elif operation_type == "find":
            score += 1.0
        
        # Look for analytics/time-series indicators
        analytics_terms = [
            "count", "sum", "average", "avg", "min", "max", 
            "group by", "order by", "aggregate", "analytics", 
            "time series", "timeseries", "trend", "historical",
            "clickhouse", "over time", "window", "period", "interval"
        ]
        for term in analytics_terms:
            if term in query.lower():
                score += 0.5
        
        # Look for SQL-like syntax
        sql_patterns = [
            r'\bselect\b', r'\bfrom\b', r'\bwhere\b', r'\bgroup\b',
            r'\bhaving\b', r'\binner join\b', r'\bleft join\b'
        ]
        for pattern in sql_patterns:
            if re.search(pattern, query.lower()):
                score += 0.5
        
        # Field matching
        if fields:
            # Check if fields exist in ClickHouse tables
            for table in schema_manager.get_clickhouse_tables():
                schema = schema_manager.get_clickhouse_schema(table)
                for field in fields:
                    if field in schema:
                        score += 0.5
        
        # Check for time-related fields which are common in ClickHouse
        time_related_fields = [f for f in fields if "time" in f.lower() or "date" in f.lower()]
        if time_related_fields:
            score += 1.0
        
        return min(score, 10.0)  # Cap score at 10


# Create global data source detector instance
data_source_detector = DataSourceDetector()