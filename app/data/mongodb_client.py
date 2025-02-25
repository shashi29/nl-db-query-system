"""
MongoDB client module for interacting with MongoDB databases.
Provides connection management and query execution.
"""
import json
from typing import Any, Dict, List, Optional, Union
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError
from bson import ObjectId
import datetime  # Standard Python datetime module

from ..config.settings import settings
from ..config.logging_config import logger


class MongoDBClient:
    """Client for interacting with MongoDB databases."""
    
    def __init__(self):
        """Initialize the MongoDB client with settings."""
        self.uri = settings.mongodb.uri
        self.db_name = settings.mongodb.database
        self.timeout_ms = settings.mongodb.timeout_ms
        self.client = None
        self.db = None
        self._connected = False

    async def connect(self) -> bool:
        """
        Establish connection to MongoDB.
        
        Returns:
            bool: True if connection successful, False otherwise.
        """
        try:
            # Create client with connection parameters
            self.client = MongoClient(
                self.uri,
                serverSelectionTimeoutMS=self.timeout_ms,
            )
            
            # Check if connection is successful by issuing a server command
            self.client.admin.command('ping')
            
            # Get database reference
            self.db = self.client[self.db_name]
            
            self._connected = True
            logger.info(f"Connected to MongoDB database: {self.db_name}")
            return True
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            self._connected = False
            return False

    async def disconnect(self) -> None:
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()
            self._connected = False
            logger.info("Disconnected from MongoDB")

    async def get_collections(self) -> List[str]:
        """
        Get list of collections in the database.
        
        Returns:
            List[str]: List of collection names.
        """
        if not self._connected and not await self.connect():
            return []
            
        try:
            return self.db.list_collection_names()
        except OperationFailure as e:
            logger.error(f"Failed to get collections: {str(e)}")
            return []

    async def get_schema(self, collection_name: str, sample_size: int = 100) -> Dict[str, Any]:
        """
        Infer schema from a collection by sampling documents.
        
        Args:
            collection_name: Name of the collection.
            sample_size: Number of documents to sample.
            
        Returns:
            Dict[str, Any]: Inferred schema with field names and types.
        """
        if not self._connected and not await self.connect():
            return {}
            
        try:
            collection = self.db[collection_name]
            
            # Get sample documents
            sample_docs = list(collection.find().limit(sample_size))
            
            if not sample_docs:
                return {}
                
            # Infer schema from sample documents
            schema = {}
            for doc in sample_docs:
                for field, value in doc.items():
                    if field not in schema:
                        schema[field] = {
                            "type": _get_bson_type(value),
                            "sample": str(value)[:100] if value is not None else None
                        }
                        
            return schema
            
        except OperationFailure as e:
            logger.error(f"Failed to get schema for collection {collection_name}: {str(e)}")
            return {}

    async def execute_query(
        self, 
        collection_name: str, 
        operation: str, 
        query: Dict[str, Any], 
        options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute a MongoDB query.
        
        Args:
            collection_name: Name of the collection to query.
            operation: Type of operation (find, insert, update, delete, aggregate, count).
            query: Query parameters.
            options: Additional options for the query.
            
        Returns:
            Dict[str, Any]: Result of the query with status and data.
        """
        if not self._connected and not await self.connect():
            return {"success": False, "error": "Database connection failed"}
            
        try:
            collection = self.db[collection_name]
            options = options or {}
            
            # Set default options if not provided
            if 'limit' not in options and operation == 'find':
                options['limit'] = 100
                
            # Execute the appropriate operation
            if operation == 'find':
                cursor = collection.find(query, **options)
                data = list(cursor)
                # Convert ObjectId to string for JSON serialization
                data = json.loads(json.dumps(data, default=str))
                return {"success": True, "data": data, "count": len(data)}
                
            elif operation == 'aggregate':
                cursor = collection.aggregate(query, **options)
                data = list(cursor)
                # Convert ObjectId to string for JSON serialization
                data = json.loads(json.dumps(data, default=str))
                return {"success": True, "data": data, "count": len(data)}
                
            elif operation == 'count':
                count = collection.count_documents(query)
                return {"success": True, "count": count}
                
            elif operation == 'insert_one':
                if not settings.security.enable_write_operations:
                    return {"success": False, "error": "Write operations are disabled"}
                result = collection.insert_one(query)
                return {"success": True, "inserted_id": str(result.inserted_id)}
                
            elif operation == 'insert_many':
                if not settings.security.enable_write_operations:
                    return {"success": False, "error": "Write operations are disabled"}
                result = collection.insert_many(query)
                return {"success": True, "inserted_ids": [str(id) for id in result.inserted_ids]}
                
            elif operation == 'update_one':
                if not settings.security.enable_write_operations:
                    return {"success": False, "error": "Write operations are disabled"}
                filter_doc = query.get("filter", {})
                update_doc = query.get("update", {})
                result = collection.update_one(filter_doc, update_doc, **options)
                return {
                    "success": True, 
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count
                }
                
            elif operation == 'update_many':
                if not settings.security.enable_write_operations:
                    return {"success": False, "error": "Write operations are disabled"}
                filter_doc = query.get("filter", {})
                update_doc = query.get("update", {})
                result = collection.update_many(filter_doc, update_doc, **options)
                return {
                    "success": True, 
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count
                }
                
            elif operation == 'delete_one':
                if not settings.security.enable_write_operations:
                    return {"success": False, "error": "Write operations are disabled"}
                result = collection.delete_one(query)
                return {"success": True, "deleted_count": result.deleted_count}
                
            elif operation == 'delete_many':
                if not settings.security.enable_write_operations:
                    return {"success": False, "error": "Write operations are disabled"}
                result = collection.delete_many(query)
                return {"success": True, "deleted_count": result.deleted_count}
                
            else:
                return {"success": False, "error": f"Unsupported operation: {operation}"}
                
        except OperationFailure as e:
            logger.error(f"MongoDB operation failed: {str(e)}")
            return {"success": False, "error": str(e)}
            
        except Exception as e:
            logger.error(f"Error executing MongoDB query: {str(e)}")
            return {"success": False, "error": str(e)}


def _get_bson_type(value: Any) -> str:
    """
    Get the BSON type name for a value.
    
    Args:
        value: Value to get type for.
        
    Returns:
        str: BSON type name.
    """
    if value is None:
        return "null"
    elif isinstance(value, bool):
        return "bool"
    elif isinstance(value, int):
        return "int"
    elif isinstance(value, float):
        return "double"
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, list):
        return "array"
    elif isinstance(value, dict):
        return "object"
    elif isinstance(value, ObjectId):
        return "objectId"
    elif isinstance(value, datetime.datetime):
        return "datetime"
    else:
        return type(value).__name__


# Create global MongoDB client instance
mongodb_client = MongoDBClient()