"""
Context builder for OpenAI query processing.
Builds context about the database schemas and query requirements.
"""
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import json

from ..config.logging_config import logger
from ..config.settings import settings
from .schema_manager import schema_manager


class ContextBuilder:
    """
    Builder for creating context information for OpenAI.
    """
    
    @staticmethod
    def build_mongodb_context(
        collection_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Build context information for MongoDB collections.
        
        Args:
            collection_names: Specific collection names to include.
                If None, include all collections.
                
        Returns:
            Dict[str, Any]: MongoDB context information.
        """
        if collection_names is None:
            collection_names = schema_manager.get_mongodb_collections()
        
        collections_info = {}
        for collection in collection_names:
            schema = schema_manager.get_mongodb_schema(collection)
            if schema:
                collections_info[collection] = {
                    "fields": schema,
                    "description": f"Collection storing {collection} data"
                }
        
        return {
            "database_type": "MongoDB",
            "database_name": settings.mongodb.database,
            "collections": collections_info
        }

    @staticmethod
    def build_clickhouse_context(
        table_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Build context information for ClickHouse tables.
        
        Args:
            table_names: Specific table names to include.
                If None, include all tables.
                
        Returns:
            Dict[str, Any]: ClickHouse context information.
        """
        if table_names is None:
            table_names = schema_manager.get_clickhouse_tables()
        
        tables_info = {}
        for table in table_names:
            schema = schema_manager.get_clickhouse_schema(table)
            if schema:
                tables_info[table] = {
                    "fields": schema,
                    "description": f"Table storing {table} data"
                }
        
        return {
            "database_type": "ClickHouse",
            "database_name": settings.clickhouse.database,
            "tables": tables_info
        }

    @staticmethod
    def build_context(
        data_source_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Build combined context based on detected data source.
        
        Args:
            data_source_info: Data source detection information.
                
        Returns:
            Dict[str, Any]: Combined context information.
        """
        context = {
            "data_source": data_source_info["data_source"],
            "primary_data_source": data_source_info["primary"],
            "operation_type": data_source_info["operation_type"],
        }
        
        # Add MongoDB context if needed
        if data_source_info["primary"] == "mongodb" or data_source_info["secondary"] == "mongodb":
            mongodb_context = ContextBuilder.build_mongodb_context(
                data_source_info.get("mongodb_references")
            )
            context["mongodb"] = mongodb_context
        
        # Add ClickHouse context if needed
        if data_source_info["primary"] == "clickhouse" or data_source_info["secondary"] == "clickhouse":
            clickhouse_context = ContextBuilder.build_clickhouse_context(
                data_source_info.get("clickhouse_references")
            )
            context["clickhouse"] = clickhouse_context
        
        # Add examples based on operation type
        context["examples"] = ContextBuilder._get_examples(
            data_source_info["operation_type"],
            data_source_info["primary"]
        )
        
        # Add usage guidelines
        context["usage_guidelines"] = ContextBuilder._get_usage_guidelines(
            data_source_info["primary"]
        )
        
        return context

    @staticmethod
    def _get_examples(operation_type: str, data_source: str) -> List[Dict[str, Any]]:
        """
        Get examples for the specified operation type and data source.
        
        Args:
            operation_type: Type of operation.
            data_source: Primary data source.
            
        Returns:
            List[Dict[str, Any]]: Examples.
        """
        examples = []
        
        if data_source == "mongodb":
            if operation_type == "find":
                examples.append({
                    "description": "Find all documents that match criteria",
                    "natural_language": "Find all customers from New York who have spent more than $1000",
                    "query": {
                        "collection": "customers",
                        "operation": "find",
                        "query": {
                            "address.state": "New York",
                            "total_spent": {"$gt": 1000}
                        }
                    }
                })
                
            elif operation_type == "aggregate":
                examples.append({
                    "description": "Aggregate data by customer state",
                    "natural_language": "Calculate the average order value by state",
                    "query": {
                        "collection": "orders",
                        "operation": "aggregate",
                        "query": [
                            {"$group": {
                                "_id": "$customer.state",
                                "average_order_value": {"$avg": "$total"}
                            }}
                        ]
                    }
                })
                
            elif operation_type == "count":
                examples.append({
                    "description": "Count documents matching criteria",
                    "natural_language": "How many orders were placed in the last month?",
                    "query": {
                        "collection": "orders",
                        "operation": "count",
                        "query": {
                            "order_date": {
                                "$gte": {"$date": "2023-01-01T00:00:00Z"},
                                "$lt": {"$date": "2023-02-01T00:00:00Z"}
                            }
                        }
                    }
                })
                
        elif data_source == "clickhouse":
            if operation_type == "find":
                examples.append({
                    "description": "Select rows that match criteria",
                    "natural_language": "Find all events from user 12345 in the last week",
                    "query": "SELECT * FROM events WHERE user_id = 12345 AND event_time >= now() - INTERVAL 1 WEEK"
                })
                
            elif operation_type == "aggregate":
                examples.append({
                    "description": "Aggregate data by time intervals",
                    "natural_language": "Calculate the hourly page view count for the last 24 hours",
                    "query": """
                        SELECT 
                            toStartOfHour(event_time) AS hour, 
                            count() AS views 
                        FROM page_views 
                        WHERE event_time >= now() - INTERVAL 1 DAY 
                        GROUP BY hour 
                        ORDER BY hour
                    """
                })
                
            elif operation_type == "count":
                examples.append({
                    "description": "Count rows matching criteria",
                    "natural_language": "How many unique users visited our site yesterday?",
                    "query": """
                        SELECT 
                            count(DISTINCT user_id) AS unique_users 
                        FROM visits 
                        WHERE visit_date = yesterday()
                    """
                })
        
        return examples

    @staticmethod
    def _get_usage_guidelines(data_source: str) -> Dict[str, Any]:
        """
        Get usage guidelines for the specified data source.
        
        Args:
            data_source: Primary data source.
            
        Returns:
            Dict[str, Any]: Usage guidelines.
        """
        if data_source == "mongodb":
            return {
                "query_format": "JSON object format for MongoDB queries",
                "operation_types": [
                    "find - For retrieving documents that match criteria",
                    "aggregate - For data aggregation and transformation",
                    "count - For counting documents",
                    "insert_one - For inserting a single document",
                    "insert_many - For inserting multiple documents",
                    "update_one - For updating a single document",
                    "update_many - For updating multiple documents",
                    "delete_one - For deleting a single document",
                    "delete_many - For deleting multiple documents"
                ],
                "limitations": [
                    "Avoid using $where operator which is a security risk",
                    "Keep queries efficient by using proper indexes",
                    "Do not use JavaScript execution for queries",
                    "Be aware of the document size limit (16MB)"
                ]
            }
        elif data_source == "clickhouse":
            return {
                "query_format": "SQL syntax for ClickHouse queries",
                "operation_types": [
                    "SELECT - For retrieving data",
                    "INSERT - For adding new data",
                    "ALTER - For modifying table structure",
                    "CREATE - For creating new tables",
                    "DROP - For deleting tables",
                    "OPTIMIZE - For optimizing tables"
                ],
                "limitations": [
                    "ClickHouse is optimized for read-heavy workloads, not frequent updates",
                    "Use ORDER BY and GROUP BY efficiently for better performance",
                    "Be mindful of memory usage for large joins",
                    "Data modification operations are not as flexible as in MongoDB"
                ]
            }
        else:
            return {}


# Create global context builder instance
context_builder = ContextBuilder()