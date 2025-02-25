"""
Success evaluator for evaluating query results.
"""
from typing import Any, Dict, List, Optional, Union
import time

from ..config.logging_config import logger


class Evaluator:
    """
    Evaluator for query results.
    """
    
    @staticmethod
    def evaluate(
        result: Dict[str, Any],
        expected_result: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Evaluate a query result.
        
        Args:
            result: The query result to evaluate.
            expected_result: Optional expected result for comparison.
            
        Returns:
            Dict[str, Any]: Evaluation result.
        """
        evaluation = {
            "success": False,
            "evaluation_time": time.time()
        }
        
        try:
            # Check if result is successful
            if not result.get("success", False):
                evaluation["reason"] = "Query execution failed"
                evaluation["error"] = result.get("error", "Unknown error")
                return evaluation
                
            # Check if result contains data when expected
            if "data" not in result and "count" not in result:
                evaluation["reason"] = "Result doesn't contain data or count"
                return evaluation
                
            # If there's an expected result, compare with it
            if expected_result:
                return Evaluator._compare_with_expected(result, expected_result)
                
            # Basic evaluation if no expected result
            evaluation["success"] = True
            
            # Check if result has data
            if "data" in result:
                data = result["data"]
                count = len(data) if isinstance(data, list) else 0
                
                evaluation["data_count"] = count
                
                if count == 0:
                    evaluation["warning"] = "Query returned no data"
                    
            # Check execution time if available
            if "execution_time" in result:
                execution_time = result["execution_time"]
                evaluation["execution_time"] = execution_time
                
                # Warn if execution time is high
                if execution_time > 5.0:  # 5 seconds threshold
                    evaluation["performance_warning"] = "Query execution time is high"
                    
            return evaluation
            
        except Exception as e:
            logger.error(f"Error evaluating result: {str(e)}")
            evaluation["reason"] = f"Error evaluating result: {str(e)}"
            return evaluation

    @staticmethod
    def _compare_with_expected(
        result: Dict[str, Any],
        expected_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Compare actual result with expected result.
        
        Args:
            result: The actual query result.
            expected_result: The expected result.
            
        Returns:
            Dict[str, Any]: Comparison result.
        """
        evaluation = {
            "success": False,
            "evaluation_time": time.time()
        }
        
        # Check data presence
        if "data" in expected_result and "data" not in result:
            evaluation["reason"] = "Expected data not found in result"
            return evaluation
            
        # Compare data counts if both have data
        if "data" in expected_result and "data" in result:
            expected_data = expected_result["data"]
            actual_data = result["data"]
            
            expected_count = len(expected_data) if isinstance(expected_data, list) else 0
            actual_count = len(actual_data) if isinstance(actual_data, list) else 0
            
            evaluation["expected_count"] = expected_count
            evaluation["actual_count"] = actual_count
            
            # Check if counts match
            if expected_count != actual_count:
                evaluation["reason"] = f"Data count mismatch: expected {expected_count}, got {actual_count}"
                return evaluation
                
            # For small result sets, check if data matches exactly
            if expected_count <= 10 and actual_count <= 10:
                if expected_data != actual_data:
                    evaluation["reason"] = "Data content mismatch"
                    return evaluation
                    
        # If no issues found, consider it successful
        evaluation["success"] = True
        return evaluation


# Create global evaluator instance
evaluator = Evaluator()