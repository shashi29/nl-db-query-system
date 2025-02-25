"""
MongoDB query generator for generating MongoDB queries.
"""
from typing import Any, Dict, List, Optional, Union
import json
import re

from ...config.logging_config import logger
from ...utils.query_utils import add_query_timeout, sanitize_mongodb_collection_name


class MongoDBQueryGenerator:
    """
    Generator for MongoDB queries.
    """
    
    @staticmethod
    def generate_query(
        query_plan: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a MongoDB query from a query plan.
        
        Args:
            query_plan: The query plan.
            
        Returns:
            Dict[str, Any]: Generated MongoDB query details.
        """
        try:
            # Check if required fields are present
            if "collection" not in query_plan:
                return {"success": False, "error": "Collection not specified"}
                
            if "operation" not in query_plan:
                return {"success": False, "error": "Operation not specified"}
                
            if "query" not in query_plan:
                return {"success": False, "error": "Query not specified"}
                
            # Extract the query components
            collection = sanitize_mongodb_collection_name(query_plan["collection"])
            operation = query_plan["operation"]
            query = query_plan["query"]
            options = query_plan.get("options", {})
            
            # Add timeout to query
            query_with_timeout = add_query_timeout(query, is_mongodb=True)
            
            # Generate the appropriate query
            if operation == "find":
                return MongoDBQueryGenerator._generate_find_query(collection, query_with_timeout, options)
                
            elif operation == "aggregate":
                return MongoDBQueryGenerator._generate_aggregate_query(collection, query_with_timeout, options)
                
            elif operation == "count":
                return MongoDBQueryGenerator._generate_count_query(collection, query_with_timeout)
                
            elif operation == "insert_one":
                return MongoDBQueryGenerator._generate_insert_one_query(collection, query_with_timeout)
                
            elif operation == "insert_many":
                return MongoDBQueryGenerator._generate_insert_many_query(collection, query_with_timeout)
                
            elif operation == "update_one":
                return MongoDBQueryGenerator._generate_update_one_query(collection, query_with_timeout, options)
                
            elif operation == "update_many":
                return MongoDBQueryGenerator._generate_update_many_query(collection, query_with_timeout, options)
                
            elif operation == "delete_one":
                return MongoDBQueryGenerator._generate_delete_one_query(collection, query_with_timeout)
                
            elif operation == "delete_many":
                return MongoDBQueryGenerator._generate_delete_many_query(collection, query_with_timeout)
                
            else:
                return {"success": False, "error": f"Unsupported operation: {operation}"}
                
        except Exception as e:
            logger.error(f"Error generating MongoDB query: {str(e)}")
            return {"success": False, "error": f"Error generating MongoDB query: {str(e)}"}

    @staticmethod
    def _generate_find_query(
        collection: str, 
        query: Dict[str, Any], 
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a MongoDB find query.
        
        Args:
            collection: The collection name.
            query: The query filter.
            options: Additional options.
            
        Returns:
            Dict[str, Any]: Generated query details.
        """
        # Process options
        projection = options.get("projection", None)
        limit = options.get("limit", None)
        skip = options.get("skip", None)
        sort = options.get("sort", None)
        
        # Build the executable query
        executable_query = {
            "collection": collection,
            "operation": "find",
            "filter": query,
            "options": {}
        }
        
        # Add options if specified
        if projection is not None:
            executable_query["options"]["projection"] = projection
            
        if limit is not None:
            executable_query["options"]["limit"] = limit
            
        if skip is not None:
            executable_query["options"]["skip"] = skip
            
        if sort is not None:
            executable_query["options"]["sort"] = sort
            
        # Create readable query representation
        readable_query = f"db.{collection}.find({json.dumps(query, indent=2)}"
        
        if projection:
            readable_query += f", {json.dumps(projection, indent=2)}"
            
        readable_query += ")"
        
        if sort:
            readable_query += f".sort({json.dumps(sort, indent=2)})"
            
        if skip:
            readable_query += f".skip({skip})"
            
        if limit:
            readable_query += f".limit({limit})"
            
        return {
            "success": True,
            "executable_query": executable_query,
            "readable_query": readable_query
        }
        
    @staticmethod
    def _generate_aggregate_query(
        collection: str, 
        pipeline: List[Dict[str, Any]], 
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a MongoDB aggregate query.
        
        Args:
            collection: The collection name.
            pipeline: The aggregation pipeline.
            options: Additional options.
            
        Returns:
            Dict[str, Any]: Generated query details.
        """
        # Build the executable query
        executable_query = {
            "collection": collection,
            "operation": "aggregate",
            "pipeline": pipeline,
            "options": options
        }
        
        # Create readable query representation
        readable_query = f"db.{collection}.aggregate({json.dumps(pipeline, indent=2)}"
        
        if options:
            readable_query += f", {json.dumps(options, indent=2)}"
            
        readable_query += ")"
        
        return {
            "success": True,
            "executable_query": executable_query,
            "readable_query": readable_query
        }
        
    @staticmethod
    def _generate_count_query(
        collection: str, 
        query: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a MongoDB count query.
        
        Args:
            collection: The collection name.
            query: The query filter.
            
        Returns:
            Dict[str, Any]: Generated query details.
        """
        # Build the executable query
        executable_query = {
            "collection": collection,
            "operation": "count",
            "filter": query
        }
        
        # Create readable query representation
        readable_query = f"db.{collection}.countDocuments({json.dumps(query, indent=2)})"
        
        return {
            "success": True,
            "executable_query": executable_query,
            "readable_query": readable_query
        }
        
    @staticmethod
    def _generate_insert_one_query(
        collection: str, 
        document: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a MongoDB insert one query.
        
        Args:
            collection: The collection name.
            document: The document to insert.
            
        Returns:
            Dict[str, Any]: Generated query details.
        """
        # Build the executable query
        executable_query = {
            "collection": collection,
            "operation": "insert_one",
            "document": document
        }
        
        # Create readable query representation
        readable_query = f"db.{collection}.insertOne({json.dumps(document, indent=2)})"
        
        return {
            "success": True,
            "executable_query": executable_query,
            "readable_query": readable_query
        }
        
    @staticmethod
    def _generate_insert_many_query(
        collection: str, 
        documents: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Generate a MongoDB insert many query.
        
        Args:
            collection: The collection name.
            documents: The documents to insert.
            
        Returns:
            Dict[str, Any]: Generated query details.
        """
        # Build the executable query
        executable_query = {
            "collection": collection,
            "operation": "insert_many",
            "documents": documents
        }
        
        # Create readable query representation
        readable_query = f"db.{collection}.insertMany({json.dumps(documents, indent=2)})"
        
        return {
            "success": True,
            "executable_query": executable_query,
            "readable_query": readable_query
        }
        
    @staticmethod
    def _generate_update_one_query(
        collection: str, 
        query: Dict[str, Any],
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a MongoDB update one query.
        
        Args:
            collection: The collection name.
            query: The query containing filter and update.
            options: Additional options.
            
        Returns:
            Dict[str, Any]: Generated query details.
        """
        # Extract filter and update
        filter_doc = query.get("filter", {})
        update_doc = query.get("update", {})
        
        # Build the executable query
        executable_query = {
            "collection": collection,
            "operation": "update_one",
            "filter": filter_doc,
            "update": update_doc,
            "options": options
        }
        
        # Create readable query representation
        readable_query = f"db.{collection}.updateOne({json.dumps(filter_doc, indent=2)}, {json.dumps(update_doc, indent=2)}"
        
        if options:
            readable_query += f", {json.dumps(options, indent=2)}"
            
        readable_query += ")"
        
        return {
            "success": True,
            "executable_query": executable_query,
            "readable_query": readable_query
        }
        
    @staticmethod
    def _generate_update_many_query(
        collection: str, 
        query: Dict[str, Any],
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a MongoDB update many query.
        
        Args:
            collection: The collection name.
            query: The query containing filter and update.
            options: Additional options.
            
        Returns:
            Dict[str, Any]: Generated query details.
        """
        # Extract filter and update
        filter_doc = query.get("filter", {})
        update_doc = query.get("update", {})
        
        # Build the executable query
        executable_query = {
            "collection": collection,
            "operation": "update_many",
            "filter": filter_doc,
            "update": update_doc,
            "options": options
        }
        
        # Create readable query representation
        readable_query = f"db.{collection}.updateMany({json.dumps(filter_doc, indent=2)}, {json.dumps(update_doc, indent=2)}"
        
        if options:
            readable_query += f", {json.dumps(options, indent=2)}"
            
        readable_query += ")"
        
        return {
            "success": True,
            "executable_query": executable_query,
            "readable_query": readable_query
        }
        
    @staticmethod
    def _generate_delete_one_query(
        collection: str, 
        query: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a MongoDB delete one query.
        
        Args:
            collection: The collection name.
            query: The query filter.
            
        Returns:
            Dict[str, Any]: Generated query details.
        """
        # Build the executable query
        executable_query = {
            "collection": collection,
            "operation": "delete_one",
            "filter": query
        }
        
        # Create readable query representation
        readable_query = f"db.{collection}.deleteOne({json.dumps(query, indent=2)})"
        
        return {
            "success": True,
            "executable_query": executable_query,
            "readable_query": readable_query
        }
        
    @staticmethod
    def _generate_delete_many_query(
        collection: str, 
        query: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a MongoDB delete many query.
        
        Args:
            collection: The collection name.
            query: The query filter.
            
        Returns:
            Dict[str, Any]: Generated query details.
        """
        # Build the executable query
        executable_query = {
            "collection": collection,
            "operation": "delete_many",
            "filter": query
        }
        
        # Create readable query representation
        readable_query = f"db.{collection}.deleteMany({json.dumps(query, indent=2)})"
        
        return {
            "success": True,
            "executable_query": executable_query,
            "readable_query": readable_query
        }