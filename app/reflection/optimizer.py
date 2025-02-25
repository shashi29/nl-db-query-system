"""
Query optimizer for optimizing query plans.
"""
from typing import Any, Dict, List, Optional, Tuple, Union
import copy
import json

from ..config.logging_config import logger
from .performance_analyzer import performance_analyzer


class Optimizer:
    """
    Optimizer for query plans.
    """
    
    @staticmethod
    def optimize_query(
        query_plan: Dict[str, Any],
        performance_analysis: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Optimize a query plan based on performance analysis.
        
        Args:
            query_plan: The query plan to optimize.
            performance_analysis: Optional performance analysis.
            
        Returns:
            Dict[str, Any]: Optimized query plan.
        """
        # Clone the query plan to avoid modifying the original
        optimized_plan = copy.deepcopy(query_plan)
        
        try:
            # Get data source
            data_source = query_plan.get("data_source", "")
            
            # Apply optimizations based on data source
            if data_source == "mongodb":
                optimized_plan = Optimizer._optimize_mongodb_query(
                    optimized_plan, performance_analysis
                )
                
            elif data_source == "clickhouse":
                optimized_plan = Optimizer._optimize_clickhouse_query(
                    optimized_plan, performance_analysis
                )
                
            elif data_source == "federated":
                optimized_plan = Optimizer._optimize_federated_query(
                    optimized_plan, performance_analysis
                )
                
            # Add optimization metadata
            optimized_plan["is_optimized"] = True
            optimized_plan["optimization_strategy"] = Optimizer._get_optimization_strategy(
                data_source, performance_analysis
            )
            
            return optimized_plan
            
        except Exception as e:
            logger.error(f"Error optimizing query: {str(e)}")
            
            # Return original plan with error info
            query_plan["optimization_error"] = str(e)
            return query_plan

    @staticmethod
    def _optimize_mongodb_query(
        query_plan: Dict[str, Any],
        performance_analysis: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Optimize a MongoDB query plan.
        
        Args:
            query_plan: The MongoDB query plan.
            performance_analysis: Optional performance analysis.
            
        Returns:
            Dict[str, Any]: Optimized MongoDB query plan.
        """
        # Check if query field exists
        if "query" not in query_plan:
            return query_plan
            
        query = query_plan["query"]
        
        # Apply projection optimization
        query = Optimizer._optimize_mongodb_projection(query)
        
        # Apply limit optimization
        query = Optimizer._optimize_mongodb_limit(query)
        
        # Apply index hint optimization
        query = Optimizer._optimize_mongodb_index_hint(query)
        
        # Update query in plan
        query_plan["query"] = query
        
        return query_plan

    @staticmethod
    def _optimize_mongodb_projection(query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Optimize MongoDB query by adding projection.
        
        Args:
            query: The MongoDB query.
            
        Returns:
            Dict[str, Any]: Optimized query.
        """
        # Clone query to avoid modifying the original
        optimized_query = copy.deepcopy(query)
        
        # Check if it's a find operation
        if "operation" in optimized_query and optimized_query["operation"] == "find":
            # Check if options exists
            if "options" not in optimized_query:
                optimized_query["options"] = {}
                
            # Add projection if not present
            if "projection" not in optimized_query["options"]:
                # If we don't have specific field information,
                # exclude _id as a minimal optimization
                optimized_query["options"]["projection"] = {"_id": 0}
        
        return optimized_query

    @staticmethod
    def _optimize_mongodb_limit(query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Optimize MongoDB query by adding limit.
        
        Args:
            query: The MongoDB query.
            
        Returns:
            Dict[str, Any]: Optimized query.
        """
        # Clone query to avoid modifying the original
        optimized_query = copy.deepcopy(query)
        
        # Check if it's a find operation
        if "operation" in optimized_query and optimized_query["operation"] == "find":
            # Check if options exists
            if "options" not in optimized_query:
                optimized_query["options"] = {}
                
            # Add limit if not present
            if "limit" not in optimized_query["options"]:
                # Default to a reasonable limit
                optimized_query["options"]["limit"] = 100
        
        return optimized_query

    @staticmethod
    def _optimize_mongodb_index_hint(query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Optimize MongoDB query by adding index hint.
        
        Args:
            query: The MongoDB query.
            
        Returns:
            Dict[str, Any]: Optimized query.
        """
        # Clone query to avoid modifying the original
        optimized_query = copy.deepcopy(query)
        
        # Check if it's a find operation
        if "operation" in optimized_query and optimized_query["operation"] == "find":
            # Check if filter exists
            if "filter" not in optimized_query:
                return optimized_query
                
            filter_doc = optimized_query["filter"]
            
            # Check if options exists
            if "options" not in optimized_query:
                optimized_query["options"] = {}
                
            # Add hint if not present and we can detect a good index
            if "hint" not in optimized_query["options"] and filter_doc:
                # Simple heuristic: use the first field in the filter as hint
                # In a real application, this would be more sophisticated
                first_field = next(iter(filter_doc.keys()), None)
                
                if first_field and first_field != "$and" and first_field != "$or":
                    optimized_query["options"]["hint"] = {first_field: 1}
        
        return optimized_query

    @staticmethod
    def _optimize_clickhouse_query(
        query_plan: Dict[str, Any],
        performance_analysis: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Optimize a ClickHouse query plan.
        
        Args:
            query_plan: The ClickHouse query plan.
            performance_analysis: Optional performance analysis.
            
        Returns:
            Dict[str, Any]: Optimized ClickHouse query plan.
        """
        # Check if query field exists
        if "query" not in query_plan:
            return query_plan
            
        query = query_plan["query"]
        
        # Apply LIMIT optimization
        query = Optimizer._optimize_clickhouse_limit(query)
        
        # Apply settings optimization
        query_plan = Optimizer._optimize_clickhouse_settings(query_plan)
        
        # Update query in plan
        query_plan["query"] = query
        
        return query_plan

    @staticmethod
    def _optimize_clickhouse_limit(query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Optimize ClickHouse query by adding LIMIT.
        
        Args:
            query: The ClickHouse query.
            
        Returns:
            Dict[str, Any]: Optimized query.
        """
        # Check if it's a string query
        if not isinstance(query, str):
            return query
            
        # Check if it's a SELECT query
        if not query.strip().upper().startswith("SELECT"):
            return query
            
        # Check if LIMIT is already present
        if "LIMIT" in query.upper():
            return query
            
        # Add LIMIT clause
        optimized_query = f"{query} LIMIT 100"
        
        return optimized_query

    @staticmethod
    def _optimize_clickhouse_settings(query_plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Optimize ClickHouse query by adding performance settings.
        
        Args:
            query_plan: The ClickHouse query plan.
            
        Returns:
            Dict[str, Any]: Optimized query plan.
        """
        # Clone query plan to avoid modifying the original
        optimized_plan = copy.deepcopy(query_plan)
        
        # Check if query is a dict
        if "query" not in optimized_plan or not isinstance(optimized_plan["query"], dict):
            return optimized_plan
            
        # Check if settings exists
        if "settings" not in optimized_plan["query"]:
            optimized_plan["query"]["settings"] = {}
            
        # Add performance settings
        settings = optimized_plan["query"]["settings"]
        
        # Add max_threads setting if not present
        if "max_threads" not in settings:
            settings["max_threads"] = 4
            
        # Add max_memory_usage setting if not present
        if "max_memory_usage" not in settings:
            settings["max_memory_usage"] = 10000000000  # 10GB
            
        # Add use_uncompressed_cache setting if not present
        if "use_uncompressed_cache" not in settings:
            settings["use_uncompressed_cache"] = 1
        
        return optimized_plan

    @staticmethod
    def _optimize_federated_query(
        query_plan: Dict[str, Any],
        performance_analysis: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Optimize a federated query plan.
        
        Args:
            query_plan: The federated query plan.
            performance_analysis: Optional performance analysis.
            
        Returns:
            Dict[str, Any]: Optimized federated query plan.
        """
        # Check if steps field exists
        if "steps" not in query_plan:
            return query_plan
            
        # Clone query plan to avoid modifying the original
        optimized_plan = copy.deepcopy(query_plan)
        steps = optimized_plan["steps"]
        
        # Optimize each step
        for i, step in enumerate(steps):
            data_source = step.get("data_source", "")
            
            if data_source == "mongodb":
                if "mongodb_query" in step:
                    step["mongodb_query"] = Optimizer._optimize_mongodb_query(
                        {"query": step["mongodb_query"]}, performance_analysis
                    )["query"]
                    
            elif data_source == "clickhouse":
                if "clickhouse_query" in step:
                    step["clickhouse_query"] = Optimizer._optimize_clickhouse_query(
                        {"query": step["clickhouse_query"]}, performance_analysis
                    )["query"]
                    
            elif data_source == "memory":
                # Optimize memory operations
                step = Optimizer._optimize_memory_operation(step)
                
            # Update step
            steps[i] = step
        
        # Optimize step order
        steps = Optimizer._optimize_step_order(steps)
        
        # Update steps in plan
        optimized_plan["steps"] = steps
        
        return optimized_plan

    @staticmethod
    def _optimize_memory_operation(step: Dict[str, Any]) -> Dict[str, Any]:
        """
        Optimize a memory operation step.
        
        Args:
            step: The memory operation step.
            
        Returns:
            Dict[str, Any]: Optimized step.
        """
        # Clone step to avoid modifying the original
        optimized_step = copy.deepcopy(step)
        
        # Check if operation exists
        if "operation" not in optimized_step:
            return optimized_step
            
        operation = optimized_step["operation"]
        
        # Optimize based on operation type
        if operation == "join":
            # Ensure join has parameters
            if "parameters" not in optimized_step:
                optimized_step["parameters"] = {}
                
            # Set more efficient join parameters
            parameters = optimized_step["parameters"]
            
            # Use hash join by default
            if "method" not in parameters:
                parameters["method"] = "hash"
                
        elif operation == "sort":
            # Ensure sort has parameters
            if "parameters" not in optimized_step:
                optimized_step["parameters"] = {}
                
            # Limit sorting to 1000 items by default
            parameters = optimized_step["parameters"]
            
            if "limit" not in parameters:
                parameters["limit"] = 1000
                
        elif operation == "group":
            # Ensure group has parameters
            if "parameters" not in optimized_step:
                optimized_step["parameters"] = {}
                
            # Use optimized grouping
            parameters = optimized_step["parameters"]
            
            if "optimize" not in parameters:
                parameters["optimize"] = True
        
        return optimized_step

    @staticmethod
    def _optimize_step_order(steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Optimize the order of steps in a federated query.
        
        Args:
            steps: The query steps.
            
        Returns:
            List[Dict[str, Any]]: Optimized steps.
        """
        # This is a simplified implementation
        # In a real application, this would be more sophisticated
        optimized_steps = copy.deepcopy(steps)
        
        # Find filter steps and move them earlier in the pipeline when possible
        filter_steps = []
        other_steps = []
        final_step = None
        
        # Separate filter steps from other steps
        for step in optimized_steps:
            step_type = step.get("step_type", "")
            data_source = step.get("data_source", "")
            
            if step_type == "final":
                final_step = step
            elif data_source == "memory" and step.get("operation") == "filter":
                filter_steps.append(step)
            else:
                other_steps.append(step)
        
        # Reorder steps: first filters, then other steps, finally the final step
        optimized_steps = filter_steps + other_steps
        
        # Append final step if it exists
        if final_step:
            optimized_steps.append(final_step)
        
        return optimized_steps

    @staticmethod
    def _get_optimization_strategy(
        data_source: str,
        performance_analysis: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        """
        Get the optimization strategies applied.
        
        Args:
            data_source: The data source.
            performance_analysis: Optional performance analysis.
            
        Returns:
            List[str]: Applied optimization strategies.
        """
        strategies = []
        
        if data_source == "mongodb":
            strategies = [
                "Added projection to reduce data transfer",
                "Added limit to restrict result size",
                "Added index hint to improve query performance"
            ]
        elif data_source == "clickhouse":
            strategies = [
                "Added LIMIT clause to restrict result size",
                "Optimized query settings for better performance"
            ]
        elif data_source == "federated":
            strategies = [
                "Optimized individual database queries",
                "Reordered pipeline steps for efficiency",
                "Optimized memory operations"
            ]
        
        # If we have performance analysis, add bottleneck-specific strategies
        if performance_analysis and "bottlenecks" in performance_analysis:
            bottlenecks = performance_analysis["bottlenecks"]
            
            for bottleneck in bottlenecks:
                phase = bottleneck.get("phase", "")
                
                if phase == "execution":
                    strategies.append("Added optimizations targeting execution bottlenecks")
                elif phase == "aggregation":
                    strategies.append("Added optimizations targeting aggregation bottlenecks")
        
        return strategies


# Create global optimizer instance
optimizer = Optimizer()