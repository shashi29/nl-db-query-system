"""
MongoDB executor for executing MongoDB queries.
"""
from typing import Any, Dict, List, Optional, Union
import time

from ..config.logging_config import logger
from ..data.mongodb_client import mongodb_client
from .query_validator import QueryValidator


class MongoDBExecutor:
    """
    Executor for MongoDB queries.
    """
    
    @staticmethod
    async def execute(
        executable_query: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute a MongoDB query.
        
        Args:
            executable_query: The executable query.
            
        Returns:
            Dict[str, Any]: Query result.
        """
        start_time = time.time()
        
        try:
            # Validate the query
            is_valid, reason, sanitized_query = QueryValidator.validate(
                executable_query, "mongodb"
            )
            
            if not is_valid:
                return {
                    "success": False,
                    "error": reason,
                    "execution_time": time.time() - start_time
                }
                
            # Connect to MongoDB
            connection_success = await mongodb_client.connect()
            if not connection_success:
                return {
                    "success": False,
                    "error": "Failed to connect to MongoDB",
                    "execution_time": time.time() - start_time
                }
                
            # Execute the query
            result = await MongoDBExecutor._execute_query(sanitized_query)
            
            # Add execution time
            result["execution_time"] = time.time() - start_time
            
            return result
            
        except Exception as e:
            logger.error(f"Error executing MongoDB query: {str(e)}")
            return {
                "success": False,
                "error": f"Error executing MongoDB query: {str(e)}",
                "execution_time": time.time() - start_time
            }
        finally:
            # Disconnect from MongoDB
            await mongodb_client.disconnect()

    @staticmethod
    async def _execute_query(
        query: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute the specific type of MongoDB query.
        
        Args:
            query: The sanitized query.
            
        Returns:
            Dict[str, Any]: Query result.
        """
        # Extract query components
        collection = query["collection"]
        operation = query["operation"]
        
        # Execute based on operation type
        if operation == "find":
            filter_doc = query.get("filter", {})
            options = query.get("options", {})
            
            return await mongodb_client.execute_query(
                collection_name=collection,
                operation="find",
                query=filter_doc,
                options=options
            )
            
        elif operation == "aggregate":
            pipeline = query.get("pipeline", [])
            options = query.get("options", {})
            
            return await mongodb_client.execute_query(
                collection_name=collection,
                operation="aggregate",
                query=pipeline,
                options=options
            )
            
        elif operation == "count":
            filter_doc = query.get("filter", {})
            
            return await mongodb_client.execute_query(
                collection_name=collection,
                operation="count",
                query=filter_doc
            )
            
        elif operation == "insert_one":
            document = query.get("document", {})
            
            return await mongodb_client.execute_query(
                collection_name=collection,
                operation="insert_one",
                query=document
            )
            
        elif operation == "insert_many":
            documents = query.get("documents", [])
            
            return await mongodb_client.execute_query(
                collection_name=collection,
                operation="insert_many",
                query=documents
            )
            
        elif operation == "update_one":
            filter_doc = query.get("filter", {})
            update_doc = query.get("update", {})
            options = query.get("options", {})
            
            update_query = {
                "filter": filter_doc,
                "update": update_doc
            }
            
            return await mongodb_client.execute_query(
                collection_name=collection,
                operation="update_one",
                query=update_query,
                options=options
            )
            
        elif operation == "update_many":
            filter_doc = query.get("filter", {})
            update_doc = query.get("update", {})
            options = query.get("options", {})
            
            update_query = {
                "filter": filter_doc,
                "update": update_doc
            }
            
            return await mongodb_client.execute_query(
                collection_name=collection,
                operation="update_many",
                query=update_query,
                options=options
            )
            
        elif operation == "delete_one":
            filter_doc = query.get("filter", {})
            
            return await mongodb_client.execute_query(
                collection_name=collection,
                operation="delete_one",
                query=filter_doc
            )
            
        elif operation == "delete_many":
            filter_doc = query.get("filter", {})
            
            return await mongodb_client.execute_query(
                collection_name=collection,
                operation="delete_many",
                query=filter_doc
            )
            
        else:
            return {
                "success": False,
                "error": f"Unsupported operation: {operation}"
            }


# Create global MongoDB executor instance
mongodb_executor = MongoDBExecutor()