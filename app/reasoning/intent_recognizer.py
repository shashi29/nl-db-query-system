"""
Intent recognizer for identifying query intent.
"""
from typing import Any, Dict, List, Optional, Tuple, Union
import re

from ..config.logging_config import logger
from ..utils.preprocessing import extract_operation_type


class IntentRecognizer:
    """
    Recognizer for identifying query intent from natural language.
    """
    
    @staticmethod
    def recognize_intent(query: str) -> Dict[str, Any]:
        """
        Recognize the intent of a natural language query.
        
        Args:
            query: The natural language query.
            
        Returns:
            Dict[str, Any]: Recognized intent information.
        """
        try:
            # Determine primary operation type
            operation_type = extract_operation_type(query)
            
            # Check for time series indicators
            is_time_series = IntentRecognizer._check_time_series(query)
            
            # Check for aggregation indicators
            is_aggregation = IntentRecognizer._check_aggregation(query)
            
            # Check for visualization indicators
            visualization_type = IntentRecognizer._check_visualization(query)
            
            # Check for comparison indicators
            is_comparison = IntentRecognizer._check_comparison(query)
            
            # Check for trend indicators
            is_trend = IntentRecognizer._check_trend(query)
            
            # Check for export indicators
            export_format = IntentRecognizer._check_export(query)
            
            # Build intent information
            intent = {
                "operation_type": operation_type,
                "is_time_series": is_time_series,
                "is_aggregation": is_aggregation,
                "visualization_type": visualization_type,
                "is_comparison": is_comparison,
                "is_trend": is_trend,
                "export_format": export_format
            }
            
            logger.debug(f"Recognized intent: {intent}")
            return intent
            
        except Exception as e:
            logger.error(f"Error recognizing intent: {str(e)}")
            return {
                "operation_type": "find",  # Default to find
                "error": f"Error recognizing intent: {str(e)}"
            }

    @staticmethod
    def _check_time_series(query: str) -> bool:
        """
        Check if the query involves time series data.
        
        Args:
            query: The query text.
            
        Returns:
            bool: True if time series related, False otherwise.
        """
        time_series_patterns = [
            r'\b(time series|timeseries|over time|by day|by month|by year|by hour|by week)\b',
            r'\b(daily|monthly|yearly|weekly|hourly|quarterly)\b',
            r'\b(trend|historical|history|evolution|progression)\b',
            r'\b(from date|to date|date range|time range|period)\b'
        ]
        
        for pattern in time_series_patterns:
            if re.search(pattern, query.lower()):
                return True
                
        return False

    @staticmethod
    def _check_aggregation(query: str) -> bool:
        """
        Check if the query involves data aggregation.
        
        Args:
            query: The query text.
            
        Returns:
            bool: True if aggregation related, False otherwise.
        """
        aggregation_patterns = [
            r'\b(average|avg|mean|sum|total|count|min|max|median)\b',
            r'\b(group by|aggregate|summarize|statistics)\b',
            r'\b(distribution|frequency|histogram|percentile)\b'
        ]
        
        for pattern in aggregation_patterns:
            if re.search(pattern, query.lower()):
                return True
                
        return False

    @staticmethod
    def _check_visualization(query: str) -> Optional[str]:
        """
        Check if the query involves data visualization.
        
        Args:
            query: The query text.
            
        Returns:
            Optional[str]: Visualization type if found, None otherwise.
        """
        visualization_patterns = [
            (r'\b(visualize|visualization|visual|display|show|plot|graph)\b', None),
            (r'\b(chart|diagram)\b', None),
            (r'\b(line chart|line graph|line plot)\b', "line"),
            (r'\b(bar chart|bar graph|histogram)\b', "bar"),
            (r'\b(pie chart|pie graph|pie)\b', "pie"),
            (r'\b(scatter plot|scatter chart|scatter)\b', "scatter"),
            (r'\b(heatmap|heat map)\b', "heatmap"),
            (r'\b(table|tabular|grid)\b', "table")
        ]
        
        for pattern, viz_type in visualization_patterns:
            if re.search(pattern, query.lower()):
                if viz_type:
                    return viz_type
                else:
                    # If we just found a generic visualization term, keep looking for specific types
                    for _, specific_type in visualization_patterns:
                        if specific_type and re.search(r'\b' + specific_type + r'\b', query.lower()):
                            return specific_type
                    # If no specific type found, return a default
                    return "auto"
                    
        return None

    @staticmethod
    def _check_comparison(query: str) -> bool:
        """
        Check if the query involves comparison.
        
        Args:
            query: The query text.
            
        Returns:
            bool: True if comparison related, False otherwise.
        """
        comparison_patterns = [
            r'\b(compare|comparison|versus|vs|against)\b',
            r'\b(difference|different|similarities|similar)\b',
            r'\b(higher than|lower than|greater than|less than)\b',
            r'\b(increase|decrease|growth|decline)\b'
        ]
        
        for pattern in comparison_patterns:
            if re.search(pattern, query.lower()):
                return True
                
        return False

    @staticmethod
    def _check_trend(query: str) -> bool:
        """
        Check if the query involves trend analysis.
        
        Args:
            query: The query text.
            
        Returns:
            bool: True if trend related, False otherwise.
        """
        trend_patterns = [
            r'\b(trend|trending|tendency|pattern)\b',
            r'\b(increase|decrease|growth|decline|rise|fall)\b',
            r'\b(forecast|predict|projection|future)\b',
            r'\b(seasonality|seasonal|cyclical|cycle)\b'
        ]
        
        for pattern in trend_patterns:
            if re.search(pattern, query.lower()):
                return True
                
        return False

    @staticmethod
    def _check_export(query: str) -> Optional[str]:
        """
        Check if the query involves exporting data.
        
        Args:
            query: The query text.
            
        Returns:
            Optional[str]: Export format if found, None otherwise.
        """
        export_patterns = [
            (r'\b(export|save|download|extract)\b', None),
            (r'\b(csv|comma separated values)\b', "csv"),
            (r'\b(excel|xlsx|xls)\b', "excel"),
            (r'\b(json|jason)\b', "json"),
            (r'\b(pdf|document)\b', "pdf"),
            (r'\b(html|webpage)\b', "html")
        ]
        
        for pattern, format_type in export_patterns:
            if re.search(pattern, query.lower()):
                if format_type:
                    return format_type
                else:
                    # If we just found a generic export term, keep looking for specific formats
                    for _, specific_format in export_patterns:
                        if specific_format and re.search(r'\b' + specific_format + r'\b', query.lower()):
                            return specific_format
                    # If no specific format found, return a default
                    return "csv"
                    
        return None


# Create global intent recognizer instance
intent_recognizer = IntentRecognizer()