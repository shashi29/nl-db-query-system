"""
Performance analyzer for analyzing query performance.
"""
from typing import Any, Dict, List, Optional, Union
import time
import json

from ..config.logging_config import logger


class PerformanceAnalyzer:
    """
    Analyzer for query performance.
    """
    
    @staticmethod
    def analyze_performance(
        result: Dict[str, Any],
        query_plan: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Analyze the performance of a query.
        
        Args:
            result: The query result.
            query_plan: The query plan.
            
        Returns:
            Dict[str, Any]: Performance analysis.
        """
        analysis = {
            "timestamp": time.time(),
            "analysis_version": "1.0"
        }
        
        try:
            # Extract timing information
            timings = PerformanceAnalyzer._extract_timings(result, query_plan)
            analysis["timings"] = timings
            
            # Calculate total time
            total_time = sum(timing["duration"] for timing in timings)
            analysis["total_time"] = total_time
            
            # Determine bottlenecks
            bottlenecks = PerformanceAnalyzer._identify_bottlenecks(timings, total_time)
            if bottlenecks:
                analysis["bottlenecks"] = bottlenecks
                
            # Generate recommendations
            recommendations = PerformanceAnalyzer._generate_recommendations(
                result, query_plan, timings, bottlenecks
            )
            if recommendations:
                analysis["recommendations"] = recommendations
                
            # Add performance rating
            analysis["performance_rating"] = PerformanceAnalyzer._rate_performance(
                total_time, result.get("data_size", 0), bottlenecks
            )
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing performance: {str(e)}")
            return {
                "error": f"Error analyzing performance: {str(e)}",
                "timestamp": time.time()
            }

    @staticmethod
    def _extract_timings(
        result: Dict[str, Any],
        query_plan: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Extract timing information from result and plan.
        
        Args:
            result: The query result.
            query_plan: The query plan.
            
        Returns:
            List[Dict[str, Any]]: Timing information.
        """
        timings = []
        
        # Extract planning time
        if "planning_time" in query_plan:
            timings.append({
                "phase": "planning",
                "duration": query_plan["planning_time"],
                "percentage": 0  # Will be calculated later
            })
            
        # Extract reasoning time
        if "generation_time" in query_plan:
            timings.append({
                "phase": "reasoning",
                "duration": query_plan["generation_time"],
                "percentage": 0  # Will be calculated later
            })
            
        # Extract execution time
        if "execution_time" in result:
            timings.append({
                "phase": "execution",
                "duration": result["execution_time"],
                "percentage": 0  # Will be calculated later
            })
            
        # Extract aggregation time
        if "aggregation_time" in result:
            timings.append({
                "phase": "aggregation",
                "duration": result["aggregation_time"],
                "percentage": 0  # Will be calculated later
            })
            
        # Calculate percentages
        total_time = sum(timing["duration"] for timing in timings)
        if total_time > 0:
            for timing in timings:
                timing["percentage"] = (timing["duration"] / total_time) * 100
                
        return timings

    @staticmethod
    def _identify_bottlenecks(
        timings: List[Dict[str, Any]],
        total_time: float
    ) -> List[Dict[str, Any]]:
        """
        Identify performance bottlenecks.
        
        Args:
            timings: Timing information.
            total_time: Total execution time.
            
        Returns:
            List[Dict[str, Any]]: Identified bottlenecks.
        """
        bottlenecks = []
        
        # Thresholds for bottleneck identification
        time_threshold = 1.0  # 1 second
        percentage_threshold = 50.0  # 50% of total time
        
        for timing in timings:
            phase = timing["phase"]
            duration = timing["duration"]
            percentage = timing["percentage"]
            
            if duration > time_threshold and percentage > percentage_threshold:
                bottlenecks.append({
                    "phase": phase,
                    "duration": duration,
                    "percentage": percentage,
                    "severity": "high" if percentage > 80.0 else "medium"
                })
                
        return bottlenecks

    @staticmethod
    def _generate_recommendations(
        result: Dict[str, Any],
        query_plan: Dict[str, Any],
        timings: List[Dict[str, Any]],
        bottlenecks: List[Dict[str, Any]]
    ) -> List[str]:
        """
        Generate performance recommendations.
        
        Args:
            result: The query result.
            query_plan: The query plan.
            timings: Timing information.
            bottlenecks: Identified bottlenecks.
            
        Returns:
            List[str]: Performance recommendations.
        """
        recommendations = []
        
        # Get data source
        data_source = query_plan.get("data_source", "")
        
        # Check for bottlenecks
        for bottleneck in bottlenecks:
            phase = bottleneck["phase"]
            
            if phase == "planning":
                recommendations.append(
                    "Consider caching schema information to reduce planning time."
                )
                
            elif phase == "reasoning":
                recommendations.append(
                    "Use more specific queries with explicit field references to reduce reasoning time."
                )
                
            elif phase == "execution":
                if data_source == "mongodb":
                    recommendations.append(
                        "Add appropriate indexes to MongoDB collections for faster query execution."
                    )
                    recommendations.append(
                        "Use projection to limit the fields returned by the query."
                    )
                    
                elif data_source == "clickhouse":
                    recommendations.append(
                        "Ensure ClickHouse tables are properly indexed with primary and sort keys."
                    )
                    recommendations.append(
                        "Use specific column selections instead of SELECT * for better performance."
                    )
                    
                elif data_source == "federated":
                    recommendations.append(
                        "Minimize data movement between databases in federated queries."
                    )
                    recommendations.append(
                        "Filter data as early as possible in the query pipeline."
                    )
                    
            elif phase == "aggregation":
                recommendations.append(
                    "Perform aggregations in the database rather than in memory when possible."
                )
                recommendations.append(
                    "Limit the data size before performing memory-intensive operations."
                )
        
        # Check result size
        if "data" in result and isinstance(result["data"], list):
            data_size = len(result["data"])
            
            if data_size > 1000:
                recommendations.append(
                    f"Query returned {data_size} records. Consider using pagination or limits."
                )
                
        # Remove duplicates
        unique_recommendations = list(set(recommendations))
        
        return unique_recommendations

    @staticmethod
    def _rate_performance(
        total_time: float,
        data_size: int,
        bottlenecks: List[Dict[str, Any]]
    ) -> str:
        """
        Rate the overall performance.
        
        Args:
            total_time: Total execution time.
            data_size: Size of the result data.
            bottlenecks: Identified bottlenecks.
            
        Returns:
            str: Performance rating.
        """
        # Start with a neutral rating
        rating = "neutral"
        
        # Adjust based on execution time
        if total_time < 0.1:
            rating = "excellent"
        elif total_time < 0.5:
            rating = "good"
        elif total_time < 2.0:
            rating = "acceptable"
        elif total_time < 5.0:
            rating = "slow"
        else:
            rating = "very_slow"
            
        # Adjust for data size
        if data_size > 0:
            time_per_record = total_time / data_size
            
            if time_per_record < 0.0001:  # Less than 0.1ms per record
                if rating != "very_slow":
                    rating = "excellent"
            elif time_per_record < 0.001:  # Less than 1ms per record
                if rating not in ["very_slow", "slow"]:
                    rating = "good"
                    
        # Downgrade for severe bottlenecks
        for bottleneck in bottlenecks:
            if bottleneck["severity"] == "high":
                if rating in ["excellent", "good"]:
                    rating = "acceptable"
                elif rating == "acceptable":
                    rating = "slow"
                    
        return rating


# Create global performance analyzer instance
performance_analyzer = PerformanceAnalyzer()