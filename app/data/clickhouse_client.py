"""
ClickHouse client module for interacting with ClickHouse databases.
Provides connection management and query execution.
"""
from typing import Any, Dict, List, Optional, Tuple, Union
import json
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError

from ..config.settings import settings
from ..config.logging_config import logger


class ClickHouseClient:
    """Client for interacting with ClickHouse databases."""
    
    def __init__(self):
        """Initialize the ClickHouse client with settings."""
        self.host = settings.clickhouse.host
        self.port = settings.clickhouse.port
        self.user = settings.clickhouse.user
        self.password = settings.clickhouse.password
        self.db_name = settings.clickhouse.database
        self.timeout = settings.clickhouse.timeout
        self.client = None
        self._connected = False

    async def connect(self) -> bool:
        """
        Establish connection to ClickHouse.
        
        Returns:
            bool: True if connection successful, False otherwise.
        """
        try:
            # Create client with connection parameters
            self.client = Client(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.db_name,
                connect_timeout=self.timeout
            )
            
            # Check if connection is successful by issuing a simple query
            self.client.execute("SELECT 1")
            
            self._connected = True
            logger.info(f"Connected to ClickHouse database: {self.db_name}")
            return True
            
        except ClickHouseError as e:
            logger.error(f"Failed to connect to ClickHouse: {str(e)}")
            self._connected = False
            return False

    async def disconnect(self) -> None:
        """Close the ClickHouse connection."""
        if self.client:
            # ClickHouse Python driver handles connection pooling
            # so explicit disconnection is not required
            self._connected = False
            logger.info("Disconnected from ClickHouse")

    async def get_tables(self) -> List[str]:
        """
        Get list of tables in the database.
        
        Returns:
            List[str]: List of table names.
        """
        if not self._connected and not await self.connect():
            return []
            
        try:
            query = f"SHOW TABLES FROM {self.db_name}"
            result = self.client.execute(query)
            # Extract table names from the result (first column)
            return [row[0] for row in result]
        except ClickHouseError as e:
            logger.error(f"Failed to get tables: {str(e)}")
            return []

    async def get_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get schema information for a table.
        
        Args:
            table_name: Name of the table.
            
        Returns:
            Dict[str, Any]: Table schema with column names, types, and other metadata.
        """
        if not self._connected and not await self.connect():
            return {}
            
        try:
            # Get column information
            query = f"DESCRIBE TABLE {self.db_name}.{table_name}"
            columns = self.client.execute(query)
            
            schema = {}
            for col in columns:
                name, type_info, default_type, default_expression, comment, codec_expression, ttl_expression = col
                
                # Get sample data (first row)
                sample_query = f"SELECT {name} FROM {self.db_name}.{table_name} LIMIT 1"
                sample_result = self.client.execute(sample_query)
                sample_value = sample_result[0][0] if sample_result else None
                
                schema[name] = {
                    "type": type_info,
                    "default_type": default_type,
                    "comment": comment,
                    "sample": str(sample_value)[:100] if sample_value is not None else None
                }
                
            return schema
            
        except ClickHouseError as e:
            logger.error(f"Failed to get schema for table {table_name}: {str(e)}")
            return {}

    async def execute_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute a SQL query on ClickHouse.
        
        Args:
            query: SQL query to execute.
            params: Query parameters.
            settings: ClickHouse settings for this query.
            
        Returns:
            Dict[str, Any]: Result of the query with status and data.
        """
        if not self._connected and not await self.connect():
            return {"success": False, "error": "Database connection failed"}
            
        try:
            # Check if it's a SELECT query or other query type
            is_select = query.strip().upper().startswith("SELECT")
            
            # For non-SELECT queries, check if write operations are allowed
            if not is_select and not settings.security.enable_write_operations:
                return {"success": False, "error": "Write operations are disabled"}
            
            # Execute the query
            if is_select:
                # For SELECT queries, return the result rows
                result = self.client.execute(query, params=params or {}, settings=settings or {})
                
                # Get column names from description
                description = self.client.execute(
                    f"SELECT name FROM system.columns WHERE table = '{query.split('FROM')[1].strip().split()[0]}' "
                    f"AND database = '{self.db_name}'"
                )
                column_names = [col[0] for col in description]
                
                # Format results as list of dicts
                formatted_result = []
                for row in result:
                    formatted_result.append(dict(zip(column_names, row)))
                
                return {
                    "success": True, 
                    "data": formatted_result,
                    "count": len(formatted_result),
                    "query_id": self.client.last_query.query_id
                }
            else:
                # For non-SELECT queries, execute and return affected rows
                result = self.client.execute(query, params=params or {}, settings=settings or {})
                
                return {
                    "success": True,
                    "affected_rows": len(result) if result else 0,
                    "query_id": self.client.last_query.query_id
                }
                
        except ClickHouseError as e:
            logger.error(f"ClickHouse query failed: {str(e)}")
            return {"success": False, "error": str(e)}
            
        except Exception as e:
            logger.error(f"Error executing ClickHouse query: {str(e)}")
            return {"success": False, "error": str(e)}

    async def execute_with_streaming(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute a SQL query with streaming results for large datasets.
        
        Args:
            query: SQL query to execute.
            params: Query parameters.
            settings: ClickHouse settings for this query.
            
        Returns:
            Dict[str, Any]: Status information about the query execution.
            The actual data is streamed in chunks.
        """
        if not self._connected and not await self.connect():
            return {"success": False, "error": "Database connection failed"}
            
        try:
            # Check if it's a SELECT query
            if not query.strip().upper().startswith("SELECT"):
                return {"success": False, "error": "Streaming is only supported for SELECT queries"}
            
            # Get column names from query
            table_name = query.split('FROM')[1].strip().split()[0]
            column_query = f"SELECT name FROM system.columns WHERE table = '{table_name}' AND database = '{self.db_name}'"
            description = self.client.execute(column_query)
            column_names = [col[0] for col in description]
            
            # Execute query with streaming
            settings = settings or {}
            settings['max_block_size'] = settings.get('max_block_size', 100000)  # Chunk size
            
            generator = self.client.execute_iter(query, params=params or {}, settings=settings)
            
            # Return query metadata
            return {
                "success": True,
                "columns": column_names,
                "query_id": self.client.last_query.query_id,
                "generator": generator  # Client code will need to handle the generator
            }
                
        except ClickHouseError as e:
            logger.error(f"ClickHouse streaming query failed: {str(e)}")
            return {"success": False, "error": str(e)}
            
        except Exception as e:
            logger.error(f"Error executing ClickHouse streaming query: {str(e)}")
            return {"success": False, "error": str(e)}
            
    async def get_query_progress(self, query_id: str) -> Dict[str, Any]:
        """
        Get progress information for a running query.
        
        Args:
            query_id: ID of the query to check.
            
        Returns:
            Dict[str, Any]: Query progress information.
        """
        if not self._connected and not await self.connect():
            return {"success": False, "error": "Database connection failed"}
            
        try:
            query = f"SELECT * FROM system.processes WHERE query_id = '{query_id}'"
            result = self.client.execute(query)
            
            if not result:
                return {"success": True, "running": False, "message": "Query not found or completed"}
                
            process_info = dict(zip(
                ['user', 'query_id', 'query', 'elapsed', 'memory_usage', 'read_rows', 'written_rows'],
                [result[0][0], result[0][1], result[0][7], result[0][12], result[0][18], result[0][19], result[0][20]]
            ))
            
            return {
                "success": True,
                "running": True,
                "progress": process_info
            }
                
        except ClickHouseError as e:
            logger.error(f"Failed to get query progress: {str(e)}")
            return {"success": False, "error": str(e)}


# Create global ClickHouse client instance
clickhouse_client = ClickHouseClient()