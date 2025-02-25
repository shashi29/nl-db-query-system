"""
Utilities for formatting query results for presentation.
"""
import json
from typing import Any, Dict, List, Optional, Union
import pandas as pd

from ..config.logging_config import logger


def format_query_result(
    result: Dict[str, Any], 
    format_type: str = "json",
    max_rows: int = 100
) -> Dict[str, Any]:
    """
    Format query results for presentation.
    
    Args:
        result: The raw query result.
        format_type: The desired output format (json, table, csv).
        max_rows: Maximum number of rows to include.
        
    Returns:
        Dict[str, Any]: Formatted result.
    """
    if not result.get("success", False):
        # If the query failed, just return the error
        return result
    
    # Extract data if present
    data = result.get("data", [])
    
    # Limit the number of rows
    if isinstance(data, list) and len(data) > max_rows:
        data = data[:max_rows]
        result["truncated"] = True
        result["total_rows"] = len(result.get("data", []))
        result["shown_rows"] = max_rows
    
    # Format based on the requested type
    if format_type == "json":
        result["formatted_data"] = _format_as_json(data)
    elif format_type == "table":
        result["formatted_data"] = _format_as_table(data)
    elif format_type == "csv":
        result["formatted_data"] = _format_as_csv(data)
    else:
        # Default to JSON
        result["formatted_data"] = _format_as_json(data)
    
    return result


def _format_as_json(data: Union[List[Dict[str, Any]], Dict[str, Any]]) -> str:
    """
    Format data as pretty-printed JSON.
    
    Args:
        data: The data to format.
        
    Returns:
        str: JSON-formatted string.
    """
    try:
        return json.dumps(data, indent=2, default=str)
    except Exception as e:
        logger.error(f"Error formatting data as JSON: {str(e)}")
        return json.dumps({"error": "Failed to format data as JSON"})


def _format_as_table(data: Union[List[Dict[str, Any]], Dict[str, Any]]) -> str:
    """
    Format data as an ASCII table.
    
    Args:
        data: The data to format.
        
    Returns:
        str: Table-formatted string.
    """
    try:
        if not data:
            return "No data to display"
            
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Format as table
        table = df.to_string(index=False)
        return table
    except Exception as e:
        logger.error(f"Error formatting data as table: {str(e)}")
        return "Failed to format data as table"


def _format_as_csv(data: Union[List[Dict[str, Any]], Dict[str, Any]]) -> str:
    """
    Format data as CSV.
    
    Args:
        data: The data to format.
        
    Returns:
        str: CSV-formatted string.
    """
    try:
        if not data:
            return "No data to display"
            
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Format as CSV
        csv = df.to_csv(index=False)
        return csv
    except Exception as e:
        logger.error(f"Error formatting data as CSV: {str(e)}")
        return "Failed to format data as CSV"


def generate_summary(result: Dict[str, Any]) -> str:
    """
    Generate a human-readable summary of the query result.
    
    Args:
        result: The query result.
        
    Returns:
        str: Summary description.
    """
    if not result.get("success", False):
        return f"Query failed: {result.get('error', 'Unknown error')}"
    
    # Get data if present
    data = result.get("data", [])
    count = result.get("count", len(data) if isinstance(data, list) else 0)
    
    # Different summaries for different operations
    if "inserted_id" in result:
        return f"Successfully inserted 1 document with ID: {result['inserted_id']}"
    elif "inserted_ids" in result:
        return f"Successfully inserted {len(result['inserted_ids'])} documents"
    elif "matched_count" in result and "modified_count" in result:
        return f"Query matched {result['matched_count']} documents and modified {result['modified_count']} documents"
    elif "deleted_count" in result:
        return f"Successfully deleted {result['deleted_count']} documents"
    elif count == 0:
        return "The query returned no results"
    else:
        return f"Query returned {count} results"


def extract_insights(
    data: List[Dict[str, Any]], 
    query_context: Dict[str, Any]
) -> List[str]:
    """
    Extract basic insights from the query results.
    
    Args:
        data: The query result data.
        query_context: Context information about the query.
        
    Returns:
        List[str]: List of insights.
    """
    insights = []
    
    try:
        if not data:
            insights.append("No data available for analysis")
            return insights
            
        # Convert to DataFrame for analysis
        df = pd.DataFrame(data)
        
        # Get numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        
        # If we have numeric columns, calculate basic statistics
        if numeric_cols:
            for col in numeric_cols:
                # Skip columns with all NaN values
                if df[col].isna().all():
                    continue
                    
                # Basic statistics
                mean_val = df[col].mean()
                min_val = df[col].min()
                max_val = df[col].max()
                
                insights.append(f"Average {col}: {mean_val:.2f}")
                insights.append(f"Range of {col}: {min_val:.2f} to {max_val:.2f}")
                
                # Check for outliers (very simple approach)
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                outliers = df[(df[col] < q1 - 1.5 * iqr) | (df[col] > q3 + 1.5 * iqr)]
                
                if not outliers.empty:
                    insights.append(f"Found {len(outliers)} potential outliers in {col}")
        
        # Count distribution for categorical columns (limit to top 5 categories)
        categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
        
        if categorical_cols:
            for col in categorical_cols[:3]:  # Limit to first 3 categorical columns
                # Skip columns with too many unique values
                if df[col].nunique() > 10:
                    continue
                    
                value_counts = df[col].value_counts().head(5)
                top_value = value_counts.index[0]
                top_count = value_counts.iloc[0]
                
                insights.append(f"Most common {col}: {top_value} ({top_count} occurrences)")
                
        # Time series analysis if query context suggests time series data
        if query_context.get("is_time_series", False):
            time_cols = [col for col in df.columns if "time" in col.lower() or "date" in col.lower()]
            
            if time_cols:
                # Use the first time column
                time_col = time_cols[0]
                
                # Convert to datetime if not already
                try:
                    df[time_col] = pd.to_datetime(df[time_col])
                    
                    # Get first and last dates
                    first_date = df[time_col].min()
                    last_date = df[time_col].max()
                    
                    insights.append(f"Data spans from {first_date} to {last_date}")
                    
                    # Check data frequency if more than one row
                    if len(df) > 1:
                        df = df.sort_values(by=time_col)
                        time_diffs = df[time_col].diff().dropna()
                        
                        if not time_diffs.empty:
                            mode_diff = time_diffs.mode().iloc[0]
                            insights.append(f"Data points appear to be approximately {mode_diff} apart")
                    
                except Exception as e:
                    logger.debug(f"Could not perform time series analysis: {str(e)}")
        
    except Exception as e:
        logger.error(f"Error extracting insights: {str(e)}")
        insights.append("Could not generate insights from the data")
    
    return insights