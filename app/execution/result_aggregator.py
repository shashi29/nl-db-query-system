"""
Result aggregator for combining and processing query results.
"""
from typing import Any, Dict, List, Optional, Union
import time
import pandas as pd

from ..config.logging_config import logger


class ResultAggregator:
    """
    Aggregator for combining and processing query results.
    """
    
    @staticmethod
    def aggregate(
        results: List[Dict[str, Any]],
        operation: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Aggregate results from multiple queries.
        
        Args:
            results: List of query results to aggregate.
            operation: Type of aggregation operation.
            parameters: Additional parameters for the operation.
            
        Returns:
            Dict[str, Any]: Aggregated result.
        """
        start_time = time.time()
        parameters = parameters or {}
        
        try:
            # Convert results to DataFrames
            dataframes = []
            
            for result in results:
                if not result.get("success", False):
                    continue
                    
                data = result.get("data", [])
                if not data:
                    continue
                    
                try:
                    df = pd.DataFrame(data)
                    dataframes.append(df)
                except Exception as e:
                    logger.error(f"Error converting result to DataFrame: {str(e)}")
            
            if not dataframes:
                return {
                    "success": False,
                    "error": "No valid data to aggregate",
                    "aggregation_time": time.time() - start_time
                }
                
            # Apply the specified operation
            if operation == "join":
                return ResultAggregator._join_results(dataframes, parameters, start_time)
                
            elif operation == "union":
                return ResultAggregator._union_results(dataframes, parameters, start_time)
                
            elif operation == "transform":
                return ResultAggregator._transform_results(dataframes, parameters, start_time)
                
            elif operation == "filter":
                return ResultAggregator._filter_results(dataframes, parameters, start_time)
                
            elif operation == "sort":
                return ResultAggregator._sort_results(dataframes, parameters, start_time)
                
            elif operation == "limit":
                return ResultAggregator._limit_results(dataframes, parameters, start_time)
                
            elif operation == "group":
                return ResultAggregator._group_results(dataframes, parameters, start_time)
                
            else:
                return {
                    "success": False,
                    "error": f"Unsupported aggregation operation: {operation}",
                    "aggregation_time": time.time() - start_time
                }
                
        except Exception as e:
            logger.error(f"Error aggregating results: {str(e)}")
            return {
                "success": False,
                "error": f"Error aggregating results: {str(e)}",
                "aggregation_time": time.time() - start_time
            }

    @staticmethod
    def _join_results(
        dataframes: List[pd.DataFrame],
        parameters: Dict[str, Any],
        start_time: float
    ) -> Dict[str, Any]:
        """
        Join results from multiple dataframes.
        
        Args:
            dataframes: List of dataframes to join.
            parameters: Join parameters.
            start_time: Start time for timing.
            
        Returns:
            Dict[str, Any]: Joined result.
        """
        if len(dataframes) < 2:
            return {
                "success": False,
                "error": "Need at least two dataframes for a join",
                "aggregation_time": time.time() - start_time
            }
            
        try:
            # Extract join parameters
            left_df = dataframes[0]
            right_df = dataframes[1]
            
            left_on = parameters.get("left_on", None)
            right_on = parameters.get("right_on", left_on)
            how = parameters.get("how", "inner")
            suffixes = parameters.get("suffixes", ("_x", "_y"))
            
            if not left_on or not right_on:
                return {
                    "success": False,
                    "error": "Join columns not specified",
                    "aggregation_time": time.time() - start_time
                }
                
            # Perform the join
            result_df = pd.merge(
                left_df,
                right_df,
                left_on=left_on,
                right_on=right_on,
                how=how,
                suffixes=suffixes
            )
            
            # Convert back to list of dicts
            result_data = result_df.to_dict("records")
            
            return {
                "success": True,
                "data": result_data,
                "count": len(result_data),
                "aggregation_time": time.time() - start_time
            }
            
        except Exception as e:
            logger.error(f"Error joining results: {str(e)}")
            return {
                "success": False,
                "error": f"Error joining results: {str(e)}",
                "aggregation_time": time.time() - start_time
            }

    @staticmethod
    def _union_results(
        dataframes: List[pd.DataFrame],
        parameters: Dict[str, Any],
        start_time: float
    ) -> Dict[str, Any]:
        """
        Union (concatenate) results from multiple dataframes.
        
        Args:
            dataframes: List of dataframes to union.
            parameters: Union parameters.
            start_time: Start time for timing.
            
        Returns:
            Dict[str, Any]: Unioned result.
        """
        try:
            # Extract union parameters
            ignore_index = parameters.get("ignore_index", True)
            
            # Perform the union
            result_df = pd.concat(dataframes, ignore_index=ignore_index)
            
            # Convert back to list of dicts
            result_data = result_df.to_dict("records")
            
            return {
                "success": True,
                "data": result_data,
                "count": len(result_data),
                "aggregation_time": time.time() - start_time
            }
            
        except Exception as e:
            logger.error(f"Error unioning results: {str(e)}")
            return {
                "success": False,
                "error": f"Error unioning results: {str(e)}",
                "aggregation_time": time.time() - start_time
            }

    @staticmethod
    def _transform_results(
        dataframes: List[pd.DataFrame],
        parameters: Dict[str, Any],
        start_time: float
    ) -> Dict[str, Any]:
        """
        Transform results using specified operations.
        
        Args:
            dataframes: List of dataframes to transform.
            parameters: Transform parameters.
            start_time: Start time for timing.
            
        Returns:
            Dict[str, Any]: Transformed result.
        """
        if not dataframes:
            return {
                "success": False,
                "error": "No dataframes to transform",
                "aggregation_time": time.time() - start_time
            }
            
        try:
            # Use the first dataframe
            df = dataframes[0]
            
            # Extract transform parameters
            transformations = parameters.get("transformations", [])
            
            if not transformations:
                return {
                    "success": False,
                    "error": "No transformations specified",
                    "aggregation_time": time.time() - start_time
                }
                
            # Apply transformations
            for transform in transformations:
                transform_type = transform.get("type", None)
                
                if transform_type == "select_columns":
                    columns = transform.get("columns", [])
                    if columns:
                        df = df[columns]
                        
                elif transform_type == "rename_columns":
                    rename_map = transform.get("rename_map", {})
                    if rename_map:
                        df = df.rename(columns=rename_map)
                        
                elif transform_type == "add_column":
                    column_name = transform.get("column_name", "")
                    expression = transform.get("expression", "")
                    
                    if column_name and expression:
                        # Simple expressions only - in a real app, would need safety checks
                        df[column_name] = df.eval(expression)
                        
                elif transform_type == "drop_columns":
                    columns = transform.get("columns", [])
                    if columns:
                        df = df.drop(columns=columns)
                        
                elif transform_type == "fill_na":
                    value = transform.get("value", None)
                    columns = transform.get("columns", None)
                    
                    if columns:
                        df[columns] = df[columns].fillna(value)
                    else:
                        df = df.fillna(value)
            
            # Convert back to list of dicts
            result_data = df.to_dict("records")
            
            return {
                "success": True,
                "data": result_data,
                "count": len(result_data),
                "aggregation_time": time.time() - start_time
            }
            
        except Exception as e:
            logger.error(f"Error transforming results: {str(e)}")
            return {
                "success": False,
                "error": f"Error transforming results: {str(e)}",
                "aggregation_time": time.time() - start_time
            }

    @staticmethod
    def _filter_results(
        dataframes: List[pd.DataFrame],
        parameters: Dict[str, Any],
        start_time: float
    ) -> Dict[str, Any]:
        """
        Filter results based on conditions.
        
        Args:
            dataframes: List of dataframes to filter.
            parameters: Filter parameters.
            start_time: Start time for timing.
            
        Returns:
            Dict[str, Any]: Filtered result.
        """
        if not dataframes:
            return {
                "success": False,
                "error": "No dataframes to filter",
                "aggregation_time": time.time() - start_time
            }
            
        try:
            # Use the first dataframe
            df = dataframes[0]
            
            # Extract filter parameters
            condition = parameters.get("condition", "")
            
            if not condition:
                return {
                    "success": False,
                    "error": "No filter condition specified",
                    "aggregation_time": time.time() - start_time
                }
                
            # Apply filter
            filtered_df = df.query(condition)
            
            # Convert back to list of dicts
            result_data = filtered_df.to_dict("records")
            
            return {
                "success": True,
                "data": result_data,
                "count": len(result_data),
                "aggregation_time": time.time() - start_time
            }
            
        except Exception as e:
            logger.error(f"Error filtering results: {str(e)}")
            return {
                "success": False,
                "error": f"Error filtering results: {str(e)}",
                "aggregation_time": time.time() - start_time
            }

    @staticmethod
    def _sort_results(
        dataframes: List[pd.DataFrame],
        parameters: Dict[str, Any],
        start_time: float
    ) -> Dict[str, Any]:
        """
        Sort results based on columns.
        
        Args:
            dataframes: List of dataframes to sort.
            parameters: Sort parameters.
            start_time: Start time for timing.
            
        Returns:
            Dict[str, Any]: Sorted result.
        """
        if not dataframes:
            return {
                "success": False,
                "error": "No dataframes to sort",
                "aggregation_time": time.time() - start_time
            }
            
        try:
            # Use the first dataframe
            df = dataframes[0]
            
            # Extract sort parameters
            by = parameters.get("by", None)
            ascending = parameters.get("ascending", True)
            
            if not by:
                return {
                    "success": False,
                    "error": "No sort columns specified",
                    "aggregation_time": time.time() - start_time
                }
                
            # Apply sort
            sorted_df = df.sort_values(by=by, ascending=ascending)
            
            # Convert back to list of dicts
            result_data = sorted_df.to_dict("records")
            
            return {
                "success": True,
                "data": result_data,
                "count": len(result_data),
                "aggregation_time": time.time() - start_time
            }
            
        except Exception as e:
            logger.error(f"Error sorting results: {str(e)}")
            return {
                "success": False,
                "error": f"Error sorting results: {str(e)}",
                "aggregation_time": time.time() - start_time
            }
            
            
result_aggregator = ResultAggregator()