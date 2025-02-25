"""
Main executor module for coordinating query execution.
"""
from typing import Any, Dict, List, Optional, Union
import time
import json

from ..config.logging_config import logger
from ..utils.result_formatter import format_query_result, generate_summary, extract_insights
from .mongodb_executor import mongodb_executor
from .clickhouse_executor import clickhouse_executor
from .parallel_executor import parallel_executor
from .result_aggregator import result_aggregator
from .query_validator import QueryValidator


class Executor:
    """
    Main executor for coordinating query execution.
    """
    
    @staticmethod
    async def execute_query(
        execution_plan: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute a query plan.
        
        Args:
            execution_plan: The query execution plan.
            
        Returns:
            Dict[str, Any]: Query result.
        """
        start_time = time.time()
        
        try:
            # Check if the execution plan has required fields
            if "data_source" not in execution_plan:
                return {
                    "success": False,
                    "error": "Execution plan missing 'data_source' field",
                    "execution_time": time.time() - start_time
                }
                
            # Get data source
            data_source = execution_plan["data_source"]
            
            # Execute based on data source
            if data_source == "mongodb":
                result = await Executor._execute_mongodb_query(execution_plan)
                
            elif data_source == "clickhouse":
                result = await Executor._execute_clickhouse_query(execution_plan)
                
            elif data_source == "federated":
                result = await Executor._execute_federated_query(execution_plan)
                
            else:
                return {
                    "success": False,
                    "error": f"Unsupported data source: {data_source}",
                    "execution_time": time.time() - start_time
                }
                
            # Add execution time
            result["execution_time"] = time.time() - start_time
            
            # Extract insights if successful and contains data
            if result.get("success", False) and "data" in result:
                insights = extract_insights(result["data"], execution_plan)
                if insights:
                    result["insights"] = insights
                    
            # Generate summary
            summary = generate_summary(result)
            if summary:
                result["summary"] = summary
                
            return result
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return {
                "success": False,
                "error": f"Error executing query: {str(e)}",
                "execution_time": time.time() - start_time
            }

    @staticmethod
    async def _execute_mongodb_query(
        execution_plan: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute a MongoDB query.
        
        Args:
            execution_plan: The MongoDB query execution plan.
            
        Returns:
            Dict[str, Any]: Query result.
        """
        # Check if query is present
        if "query" not in execution_plan:
            return {
                "success": False,
                "error": "MongoDB query not specified"
            }
            
        # Execute the query
        result = await mongodb_executor.execute(execution_plan["query"])
        
        # Format the result if requested
        format_type = execution_plan.get("format", "json")
        if result.get("success", False) and "data" in result:
            result = format_query_result(result, format_type)
            
        return result

    @staticmethod
    async def _execute_clickhouse_query(
        execution_plan: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute a ClickHouse query.
        
        Args:
            execution_plan: The ClickHouse query execution plan.
            
        Returns:
            Dict[str, Any]: Query result.
        """
        # Check if query is present
        if "query" not in execution_plan:
            return {
                "success": False,
                "error": "ClickHouse query not specified"
            }
            
        # Determine if streaming should be used
        use_streaming = execution_plan.get("use_streaming", False)
        
        # Execute the query
        result = await clickhouse_executor.execute(
            execution_plan["query"], 
            use_streaming=use_streaming
        )
        
        # Format the result if requested
        format_type = execution_plan.get("format", "json")
        if result.get("success", False) and "data" in result:
            result = format_query_result(result, format_type)
            
        return result

    @staticmethod
    async def _execute_federated_query(
        execution_plan: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute a federated query plan.
        
        Args:
            execution_plan: The federated query execution plan.
            
        Returns:
            Dict[str, Any]: Query result.
        """
        # Check if steps are present
        if "steps" not in execution_plan:
            return {
                "success": False,
                "error": "Federated query steps not specified"
            }
            
        steps = execution_plan["steps"]
        if not isinstance(steps, list) or not steps:
            return {
                "success": False,
                "error": "Invalid federated query steps"
            }
            
        # Validate the federated query
        is_valid, reason, sanitized_query = QueryValidator.validate(
            execution_plan, "federated"
        )
        
        if not is_valid:
            return {
                "success": False,
                "error": reason
            }
            
        # Execute each step in order
        step_results = {}
        
        for step in sanitized_query["steps"]:
            step_index = step["step_index"]
            step_type = step["step_type"]
            data_source = step["data_source"]
            output_var = step["output_var"]
            
            # Execute based on data source
            if data_source == "mongodb":
                if "mongodb_query" not in step:
                    return {
                        "success": False,
                        "error": f"Step {step_index}: MongoDB query not specified"
                    }
                    
                step_result = await mongodb_executor.execute(step["mongodb_query"])
                
            elif data_source == "clickhouse":
                if "clickhouse_query" not in step:
                    return {
                        "success": False,
                        "error": f"Step {step_index}: ClickHouse query not specified"
                    }
                    
                step_result = await clickhouse_executor.execute(step["clickhouse_query"])
                
            elif data_source == "memory":
                if "operation" not in step:
                    return {
                        "success": False,
                        "error": f"Step {step_index}: Memory operation not specified"
                    }
                    
                if "inputs" not in step:
                    return {
                        "success": False,
                        "error": f"Step {step_index}: Memory inputs not specified"
                    }
                    
                # Get input results
                input_results = []
                for input_var in step["inputs"]:
                    if input_var not in step_results:
                        return {
                            "success": False,
                            "error": f"Step {step_index}: Input '{input_var}' not found"
                        }
                        
                    input_results.append(step_results[input_var])
                    
                # Execute memory operation
                operation = step["operation"]
                parameters = step.get("parameters", {})
                
                step_result = result_aggregator.aggregate(
                    input_results, operation, parameters
                )
                
            else:
                return {
                    "success": False,
                    "error": f"Step {step_index}: Unsupported data source: {data_source}"
                }
                
            # Check if step was successful
            if not step_result.get("success", False):
                return {
                    "success": False,
                    "error": f"Step {step_index} failed: {step_result.get('error', 'Unknown error')}"
                }
                
            # Store step result
            step_results[output_var] = step_result
            
            # If this is the final step, return its result
            if step_type == "final":
                # Format the result if requested
                format_type = execution_plan.get("format", "json")
                if "data" in step_result:
                    formatted_result = format_query_result(step_result, format_type)
                    return formatted_result
                else:
                    return step_result
        
        # If we get here, no final step was found
        return {
            "success": False,
            "error": "No final step found in federated query"
        }


# Create global executor instance
executor = Executor()