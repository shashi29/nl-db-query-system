"""
Parallel executor for executing multiple queries in parallel.
"""
from typing import Any, Dict, List, Optional, Union
import time
import asyncio

from ..config.logging_config import logger
from .mongodb_executor import MongoDBExecutor
from .clickhouse_executor import ClickHouseExecutor


class ParallelExecutor:
    """
    Executor for parallel query execution.
    """
    
    @staticmethod
    async def execute(
        queries: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Execute multiple queries in parallel.
        
        Args:
            queries: List of queries to execute.
                Each query should have 'data_source' and 'query' fields.
            
        Returns:
            Dict[str, Any]: Combined query results.
        """
        start_time = time.time()
        
        if not queries:
            return {
                "success": False,
                "error": "No queries to execute",
                "execution_time": 0
            }
            
        try:
            # Prepare tasks for each query
            tasks = []
            
            for i, query_info in enumerate(queries):
                if "data_source" not in query_info:
                    return {
                        "success": False,
                        "error": f"Query {i} missing 'data_source' field",
                        "execution_time": time.time() - start_time
                    }
                    
                if "query" not in query_info:
                    return {
                        "success": False,
                        "error": f"Query {i} missing 'query' field",
                        "execution_time": time.time() - start_time
                    }
                    
                data_source = query_info["data_source"]
                query = query_info["query"]
                
                if data_source == "mongodb":
                    task = asyncio.create_task(
                        MongoDBExecutor.execute(query)
                    )
                elif data_source == "clickhouse":
                    task = asyncio.create_task(
                        ClickHouseExecutor.execute(query)
                    )
                else:
                    return {
                        "success": False,
                        "error": f"Unsupported data source: {data_source}",
                        "execution_time": time.time() - start_time
                    }
                    
                tasks.append((i, data_source, task))
            
            # Wait for all tasks to complete
            results = []
            
            for i, data_source, task in tasks:
                try:
                    result = await task
                    results.append({
                        "query_index": i,
                        "data_source": data_source,
                        "result": result
                    })
                except Exception as e:
                    logger.error(f"Error executing query {i}: {str(e)}")
                    results.append({
                        "query_index": i,
                        "data_source": data_source,
                        "success": False,
                        "error": f"Error executing query: {str(e)}"
                    })
            
            # Check if all queries were successful
            all_success = all(
                (r.get("result", {}).get("success", False) if "result" in r else r.get("success", False))
                for r in results
            )
            
            return {
                "success": all_success,
                "results": results,
                "execution_time": time.time() - start_time
            }
            
        except Exception as e:
            logger.error(f"Error in parallel execution: {str(e)}")
            return {
                "success": False,
                "error": f"Error in parallel execution: {str(e)}",
                "execution_time": time.time() - start_time
            }


# Create global parallel executor instance
parallel_executor = ParallelExecutor()