"""
Schema manager for maintaining database schema information.
Collects and caches schema information from MongoDB and ClickHouse.
"""
import json
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import asyncio
import os
from pathlib import Path

from ..config.settings import settings, BASE_DIR
from ..config.logging_config import logger
from ..data.mongodb_client import mongodb_client
from ..data.clickhouse_client import clickhouse_client


class SchemaManager:
    """
    Manager for database schema information.
    Collects and provides schema information for MongoDB and ClickHouse.
    """
    
    def __init__(self):
        """Initialize the schema manager."""
        self.mongodb_schemas = {}
        self.clickhouse_schemas = {}
        self.cache_file = BASE_DIR / "cache" / "schema_cache.json"
        self.last_refresh = 0
        self.refresh_interval = 60 * 60  # 1 hour in seconds
        
    async def initialize(self):
        """
        Initialize the schema manager by loading cached schemas or refreshing.
        """
        # Create cache directory if it doesn't exist
        cache_dir = BASE_DIR / "cache"
        cache_dir.mkdir(exist_ok=True)
        
        # Try to load from cache first
        if self._load_from_cache():
            logger.info("Loaded schema information from cache")
        else:
            # If cache loading fails, refresh schemas
            await self.refresh_schemas()

    async def refresh_schemas(self) -> bool:
        """
        Refresh schema information from databases.
        
        Returns:
            bool: True if refresh was successful, False otherwise.
        """
        # try:
        logger.info("Refreshing database schema information")
        
        # Get MongoDB schemas
        mongodb_success = await self._refresh_mongodb_schemas()
        
        # Get ClickHouse schemas
        clickhouse_success = await self._refresh_clickhouse_schemas()
        
        # Update last refresh time
        self.last_refresh = time.time()
        
        # Save to cache
        self._save_to_cache()
        
        return mongodb_success or clickhouse_success
            
        # except Exception as e:
        #     logger.error(f"Error refreshing schemas: {str(e)}")
        #     return False

    async def _refresh_mongodb_schemas(self) -> bool:
        """
        Refresh MongoDB schema information.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        # try:
        # Connect to MongoDB
        connection_success = await mongodb_client.connect()
        if not connection_success:
            logger.warning("Could not connect to MongoDB to refresh schemas")
            return False
            
        # Get collections
        collections = await mongodb_client.get_collections()
        
    
        # Get schema for each collection
        for collection in collections:
            logger.info(f"Getting schema for collection: {collection}")
            # Skip system collections
            if collection.startswith("system."):
                continue
                
            schema = await mongodb_client.get_schema(collection)
            if schema:
                self.mongodb_schemas[collection] = schema
        
        logger.info(f"Refreshed schemas for {len(self.mongodb_schemas)} MongoDB collections")
        return True
            
        # except Exception as e:
        #     logger.error(f"Error refreshing MongoDB schemas: {str(e)}")
        #     return False
        # finally:
        #     # Disconnect from MongoDB
        #     await mongodb_client.disconnect()

    async def _refresh_clickhouse_schemas(self) -> bool:
        """
        Refresh ClickHouse schema information.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            # Connect to ClickHouse
            connection_success = await clickhouse_client.connect()
            if not connection_success:
                logger.warning("Could not connect to ClickHouse to refresh schemas")
                return False
                
            # Get tables
            tables = await clickhouse_client.get_tables()
            
            # Get schema for each table
            for table in tables:
                # Skip system tables
                if table.startswith("system."):
                    continue
                    
                schema = await clickhouse_client.get_schema(table)
                if schema:
                    self.clickhouse_schemas[table] = schema
            
            logger.info(f"Refreshed schemas for {len(self.clickhouse_schemas)} ClickHouse tables")
            return True
            
        except Exception as e:
            logger.error(f"Error refreshing ClickHouse schemas: {str(e)}")
            return False
        finally:
            # Disconnect from ClickHouse
            await clickhouse_client.disconnect()

    def _load_from_cache(self) -> bool:
        """
        Load schema information from cache file.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            if not self.cache_file.exists():
                return False
                
            # Check if cache is too old
            cache_age = time.time() - self.cache_file.stat().st_mtime
            if cache_age > self.refresh_interval:
                logger.info(f"Schema cache is {cache_age:.0f} seconds old, will refresh")
                return False
                
            # Load cache
            with open(self.cache_file, 'r') as f:
                cache_data = json.load(f)
                
            self.mongodb_schemas = cache_data.get("mongodb", {})
            self.clickhouse_schemas = cache_data.get("clickhouse", {})
            self.last_refresh = cache_data.get("last_refresh", 0)
            
            return bool(self.mongodb_schemas or self.clickhouse_schemas)
            
        except Exception as e:
            logger.error(f"Error loading schema cache: {str(e)}")
            return False

    def _save_to_cache(self) -> bool:
        """
        Save schema information to cache file.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            cache_data = {
                "mongodb": self.mongodb_schemas,
                "clickhouse": self.clickhouse_schemas,
                "last_refresh": self.last_refresh
            }
            
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, default=str)
                
            return True
            
        except Exception as e:
            logger.error(f"Error saving schema cache: {str(e)}")
            return False

    def get_mongodb_collections(self) -> List[str]:
        """
        Get list of all MongoDB collections.
        
        Returns:
            List[str]: Collection names.
        """
        return list(self.mongodb_schemas.keys())

    def get_clickhouse_tables(self) -> List[str]:
        """
        Get list of all ClickHouse tables.
        
        Returns:
            List[str]: Table names.
        """
        return list(self.clickhouse_schemas.keys())

    def get_mongodb_schema(self, collection: str) -> Dict[str, Any]:
        """
        Get schema for a MongoDB collection.
        
        Args:
            collection: Collection name.
            
        Returns:
            Dict[str, Any]: Collection schema.
        """
        return self.mongodb_schemas.get(collection, {})

    def get_clickhouse_schema(self, table: str) -> Dict[str, Any]:
        """
        Get schema for a ClickHouse table.
        
        Args:
            table: Table name.
            
        Returns:
            Dict[str, Any]: Table schema.
        """
        return self.clickhouse_schemas.get(table, {})

    def get_all_schemas(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all schema information.
        
        Returns:
            Dict[str, Dict[str, Any]]: Combined schema information for all databases.
        """
        return {
            "mongodb": self.mongodb_schemas,
            "clickhouse": self.clickhouse_schemas
        }

    def find_matching_collections(self, pattern: str) -> List[Tuple[str, str]]:
        """
        Find collections or tables that match a pattern.
        
        Args:
            pattern: Pattern to search for in collection/table names.
            
        Returns:
            List[Tuple[str, str]]: List of (database_type, collection_name) tuples.
        """
        matches = []
        
        # Search MongoDB collections
        for collection in self.mongodb_schemas:
            if pattern.lower() in collection.lower():
                matches.append(("mongodb", collection))
        
        # Search ClickHouse tables
        for table in self.clickhouse_schemas:
            if pattern.lower() in table.lower():
                matches.append(("clickhouse", table))
        
        return matches

    def find_matching_fields(self, field_pattern: str) -> Dict[str, List[str]]:
        """
        Find fields that match a pattern.
        
        Args:
            field_pattern: Pattern to search for in field names.
            
        Returns:
            Dict[str, List[str]]: Dictionary mapping collection names to matching fields.
        """
        matches = {"mongodb": {}, "clickhouse": {}}
        
        # Search MongoDB fields
        for collection, schema in self.mongodb_schemas.items():
            matching_fields = []
            for field in schema:
                if field_pattern.lower() in field.lower():
                    matching_fields.append(field)
            
            if matching_fields:
                matches["mongodb"][collection] = matching_fields
        
        # Search ClickHouse fields
        for table, schema in self.clickhouse_schemas.items():
            matching_fields = []
            for field in schema:
                if field_pattern.lower() in field.lower():
                    matching_fields.append(field)
            
            if matching_fields:
                matches["clickhouse"][table] = matching_fields
        
        return matches

    def get_schema_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the available schemas.
        
        Returns:
            Dict[str, Any]: Schema summary.
        """
        mongodb_collections = len(self.mongodb_schemas)
        clickhouse_tables = len(self.clickhouse_schemas)
        
        total_mongodb_fields = sum(len(schema) for schema in self.mongodb_schemas.values())
        total_clickhouse_fields = sum(len(schema) for schema in self.clickhouse_schemas.values())
        
        return {
            "mongodb": {
                "collections": mongodb_collections,
                "total_fields": total_mongodb_fields,
                "collection_names": list(self.mongodb_schemas.keys())
            },
            "clickhouse": {
                "tables": clickhouse_tables,
                "total_fields": total_clickhouse_fields,
                "table_names": list(self.clickhouse_schemas.keys())
            },
            "last_refresh": self.last_refresh
        }


# Create global schema manager instance
schema_manager = SchemaManager()