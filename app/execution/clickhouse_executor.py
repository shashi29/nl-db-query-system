"""
ClickHouse executor for executing ClickHouse queries.
"""
from typing import Any, Dict, List, Optional, Union
import time

from ..config.logging_config import logger
from ..data.clickhouse_client import clickhouse_client
from .query_validator import QueryValidator


class ClickHouseExecutor:
    """
    Executor for ClickHouse queries.
    """
    
    @staticmethod
    async def execute(
        executable_query: Dict[str, Any],
        use_streaming: bool = False
    ) -> Dict[str, Any]:
        """
        Execute a ClickHouse query.
        
        Args:
            executable_query: The executable query.
            use_streaming: Whether to use streaming for large result sets.
            
        Returns:
            Dict[str, Any]: Query result.
        """
        start_time = time.time()
        
        try:
            # Validate the query
            is_valid, reason, sanitized_query = QueryValidator.validate(
                executable_query, "clickhouse"
            )
            
            if not is_valid:
                return {
                    "success": False,
                    "error": reason,
                    "execution_time": time.time() - start_time
                }
                
            # Connect to ClickHouse
            connection_success = await clickhouse_client.connect()
            if not connection_success:
                return {
                    "success": False,
                    "error": "Failed to connect to ClickHouse",
                    "execution_time": time.time() - start_time
                }
                
            # Extract query components
            query = sanitized_query["query"]
            params = sanitized_query.get("params", {})
            settings_dict = sanitized_query.get("settings", {})
            
            # Execute the query
            if use_streaming:
                result = await clickhouse_client.execute_with_streaming(
                    query=query,
                    params=params,
                    settings=settings_dict
                )
            else:
                result = await clickhouse_client.execute_query(
                    query=query,
                    params=params,
                    settings=settings_dict
                )
            
            # Add execution time
            result["execution_time"] = time.time() - start_time
            
            return result
            
        except Exception as e:
            logger.error(f"Error executing ClickHouse query: {str(e)}")
            return {
                "success": False,
                "error": f"Error executing ClickHouse query: {str(e)}",
                "execution_time": time.time() - start_time
            }
        finally:
            # Disconnect from ClickHouse
            await clickhouse_client.disconnect()


# Create global ClickHouse executor instance
clickhouse_executor = ClickHouseExecutor()