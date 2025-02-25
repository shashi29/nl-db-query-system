"""
Federated query generator for generating multi-database queries.
"""
from typing import Any, Dict, List, Optional, Union
import json

from ...config.logging_config import logger
from .mongodb_generator import MongoDBQueryGenerator
from .clickhouse_generator import ClickHouseQueryGenerator


class FederatedQueryGenerator:
    """
    Generator for federated queries across multiple databases.
    """
    
    @staticmethod
    def generate_query(
        query_plan: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate a federated query plan from a query plan.
        
        Args:
            query_plan: The query plan.
            
        Returns:
            Dict[str, Any]: Generated federated query details.
        """
        try:
            # Check if required fields are present
            if "steps" not in query_plan:
                return {"success": False, "error": "Steps not specified in federated query plan"}
                
            steps = query_plan["steps"]
            if not isinstance(steps, list):
                return {"success": False, "error": "Steps must be a list"}
                
            if not steps:
                return {"success": False, "error": "Federated query plan has no steps"}
                
            # Process each step
            processed_steps = []
            
            for i, step in enumerate(steps):
                processed_step = FederatedQueryGenerator._process_step(step, i)
                
                if not processed_step["success"]:
                    return {
                        "success": False,
                        "error": f"Error processing step {i}: {processed_step['error']}",
                        "processed_steps": processed_steps
                    }
                    
                processed_steps.append(processed_step)
                
            # Validate that the steps form a valid pipeline
            is_valid, reason = FederatedQueryGenerator._validate_pipeline(processed_steps)
            if not is_valid:
                return {
                    "success": False,
                    "error": f"Invalid query pipeline: {reason}",
                    "processed_steps": processed_steps
                }
                
            # Build the executable query
            executable_query = {
                "steps": [step["executable_step"] for step in processed_steps]
            }
            
            # Create readable query representation
            readable_query = "Federated Query Plan:\n\n"
            
            for i, step in enumerate(processed_steps):
                readable_query += f"Step {i+1}: {step['step_type']} ({step['data_source']})\n"
                readable_query += f"{step['readable_query']}\n\n"
                
            return {
                "success": True,
                "executable_query": executable_query,
                "readable_query": readable_query,
                "processed_steps": processed_steps
            }
                
        except Exception as e:
            logger.error(f"Error generating federated query: {str(e)}")
            return {"success": False, "error": f"Error generating federated query: {str(e)}"}

    @staticmethod
    def _process_step(
        step: Dict[str, Any], 
        step_index: int
    ) -> Dict[str, Any]:
        """
        Process a step in the federated query plan.
        
        Args:
            step: The step to process.
            step_index: Index of the step.
            
        Returns:
            Dict[str, Any]: Processed step details.
        """
        try:
            # Check if required fields are present
            if "step_type" not in step:
                return {"success": False, "error": "Step type not specified"}
                
            if "data_source" not in step:
                return {"success": False, "error": "Data source not specified"}
                
            step_type = step["step_type"]
            data_source = step["data_source"]
            
            # Process based on data source
            if data_source == "mongodb":
                if "mongodb_plan" not in step:
                    return {"success": False, "error": "MongoDB plan not specified for MongoDB step"}
                    
                mongodb_plan = step["mongodb_plan"]
                mongodb_result = MongoDBQueryGenerator.generate_query(mongodb_plan)
                
                if not mongodb_result["success"]:
                    return {"success": False, "error": mongodb_result["error"]}
                    
                executable_step = {
                    "step_index": step_index,
                    "step_type": step_type,
                    "data_source": data_source,
                    "mongodb_query": mongodb_result["executable_query"],
                    "output_var": step.get("output_var", f"step_{step_index}_output")
                }
                
                return {
                    "success": True,
                    "step_type": step_type,
                    "data_source": data_source,
                    "executable_step": executable_step,
                    "readable_query": mongodb_result["readable_query"]
                }
                
            elif data_source == "clickhouse":
                if "clickhouse_plan" not in step:
                    return {"success": False, "error": "ClickHouse plan not specified for ClickHouse step"}
                    
                clickhouse_plan = step["clickhouse_plan"]
                clickhouse_result = ClickHouseQueryGenerator.generate_query(clickhouse_plan)
                
                if not clickhouse_result["success"]:
                    return {"success": False, "error": clickhouse_result["error"]}
                    
                executable_step = {
                    "step_index": step_index,
                    "step_type": step_type,
                    "data_source": data_source,
                    "clickhouse_query": clickhouse_result["executable_query"],
                    "output_var": step.get("output_var", f"step_{step_index}_output")
                }
                
                return {
                    "success": True,
                    "step_type": step_type,
                    "data_source": data_source,
                    "executable_step": executable_step,
                    "readable_query": clickhouse_result["readable_query"]
                }
                
            elif data_source == "memory":
                # Memory operations (transformations, joins, etc.)
                if "operation" not in step:
                    return {"success": False, "error": "Operation not specified for memory step"}
                    
                operation = step["operation"]
                inputs = step.get("inputs", [])
                
                executable_step = {
                    "step_index": step_index,
                    "step_type": step_type,
                    "data_source": data_source,
                    "operation": operation,
                    "inputs": inputs,
                    "output_var": step.get("output_var", f"step_{step_index}_output"),
                    "parameters": step.get("parameters", {})
                }
                
                # Create readable representation
                operation_description = FederatedQueryGenerator._describe_memory_operation(operation, inputs, step.get("parameters", {}))
                
                return {
                    "success": True,
                    "step_type": step_type,
                    "data_source": data_source,
                    "executable_step": executable_step,
                    "readable_query": operation_description
                }
                
            else:
                return {"success": False, "error": f"Unsupported data source: {data_source}"}
                
        except Exception as e:
            logger.error(f"Error processing federated query step: {str(e)}")
            return {"success": False, "error": f"Error processing step: {str(e)}"}

    @staticmethod
    def _validate_pipeline(steps: List[Dict[str, Any]]) -> Tuple[bool, str]:
        """
        Validate that the steps form a valid pipeline.
        
        Args:
            steps: The processed steps.
            
        Returns:
            Tuple[bool, str]: (True, "") if valid, (False, reason) if invalid.
        """
        # Check that the pipeline has a final step
        has_final_step = False
        for step in steps:
            if step["step_type"] == "final":
                has_final_step = True
                break
                
        if not has_final_step:
            return False, "Pipeline has no final step"
            
        # Check that all referenced inputs exist
        available_outputs = set()
        
        for i, step in enumerate(steps):
            # Add this step's output to available outputs
            output_var = step["executable_step"]["output_var"]
            available_outputs.add(output_var)
            
            # Check if this step references non-existent inputs
            if step["data_source"] == "memory":
                inputs = step["executable_step"]["inputs"]
                
                for input_var in inputs:
                    if input_var not in available_outputs:
                        return False, f"Step {i} references non-existent input: {input_var}"
                        
        return True, ""

    @staticmethod
    def _describe_memory_operation(
        operation: str, 
        inputs: List[str],
        parameters: Dict[str, Any]
    ) -> str:
        """
        Create a readable description of a memory operation.
        
        Args:
            operation: The operation type.
            inputs: Input variables.
            parameters: Operation parameters.
            
        Returns:
            str: Readable description.
        """
        if operation == "join":
            return f"Join data from {inputs[0]} and {inputs[1]} on {parameters.get('join_key', 'unknown key')}"
            
        elif operation == "filter":
            return f"Filter data from {inputs[0]} where {parameters.get('condition', 'condition')}"
            
        elif operation == "map":
            return f"Transform each item in {inputs[0]} using {parameters.get('mapping', 'mapping function')}"
            
        elif operation == "sort":
            return f"Sort data from {inputs[0]} by {parameters.get('sort_key', 'key')} in {parameters.get('order', 'ascending')} order"
            
        elif operation == "group":
            return f"Group data from {inputs[0]} by {parameters.get('group_key', 'key')} and apply {parameters.get('aggregation', 'aggregation')}"
            
        elif operation == "limit":
            return f"Limit data from {inputs[0]} to {parameters.get('count', 'N')} items"
            
        elif operation == "project":
            fields = parameters.get('fields', [])
            fields_str = ', '.join(fields) if fields else 'all fields'
            return f"Select fields ({fields_str}) from {inputs[0]}"
            
        else:
            return f"Apply {operation} operation to {', '.join(inputs)}"