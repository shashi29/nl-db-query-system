"""
Filter analyzer for analyzing and structuring filter conditions.
"""
from typing import Any, Dict, List, Optional, Tuple, Union
import re

from ..config.logging_config import logger


class FilterAnalyzer:
    """
    Analyzer for structuring filter conditions from extracted entities.
    """
    
    @staticmethod
    def analyze_filters(
        entities: Dict[str, Any],
        data_source: str
    ) -> Dict[str, Any]:
        """
        Analyze and structure filter conditions from entities.
        
        Args:
            entities: Extracted entities.
            data_source: Primary data source (mongodb, clickhouse).
            
        Returns:
            Dict[str, Any]: Structured filter information.
        """
        try:
            # Extract filter-related entities
            fields = entities.get("fields", [])
            mapped_fields = entities.get("mapped_fields", {})
            date_values = entities.get("date_values", [])
            numeric_values = entities.get("numeric_values", [])
            string_values = entities.get("string_values", [])
            comparisons = entities.get("comparisons", [])
            logical_operators = entities.get("logical_operators", [])
            
            # Determine if we have enough information for filters
            has_fields = bool(fields)
            has_values = bool(date_values or numeric_values or string_values)
            has_comparisons = bool(comparisons)
            
            if not (has_fields and has_values):
                return {"has_filters": False}
            
            # Structure filters based on data source
            if data_source == "mongodb":
                filter_structure = FilterAnalyzer._structure_mongodb_filters(entities)
            elif data_source == "clickhouse":
                filter_structure = FilterAnalyzer._structure_clickhouse_filters(entities)
            else:
                filter_structure = {"has_filters": False}
            
            return filter_structure
            
        except Exception as e:
            logger.error(f"Error analyzing filters: {str(e)}")
            return {
                "has_filters": False,
                "error": f"Error analyzing filters: {str(e)}"
            }

    @staticmethod
    def _structure_mongodb_filters(
        entities: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Structure MongoDB filter conditions from entities.
        
        Args:
            entities: Extracted entities.
            
        Returns:
            Dict[str, Any]: Structured MongoDB filter.
        """
        # Extract filter-related entities
        fields = entities.get("fields", [])
        mapped_fields = entities.get("mapped_fields", {}).get("mongodb", [])
        date_values = entities.get("date_values", [])
        numeric_values = entities.get("numeric_values", [])
        string_values = entities.get("string_values", [])
        comparisons = entities.get("comparisons", [])
        logical_operators = entities.get("logical_operators", [])
        
        # Initialize filter structure
        filter_doc = {}
        conditions = []
        
        # Map fields to their best matches in the schema
        field_mappings = {}
        for field in fields:
            best_match = None
            
            # Look for an exact match first
            for mapped_field in mapped_fields:
                if mapped_field["field"] == field and mapped_field["match_type"] == "exact":
                    best_match = mapped_field
                    break
                    
            # If no exact match, use the first partial match
            if not best_match:
                for mapped_field in mapped_fields:
                    if mapped_field["match_type"] == "partial":
                        best_match = mapped_field
                        break
                        
            if best_match:
                field_mappings[field] = best_match
        
        # Process date conditions
        for date_entity in date_values:
            date_value = date_entity["value"]
            
            # Find related field (simple heuristic - look for date/time fields)
            date_field = None
            for field in field_mappings:
                if "date" in field.lower() or "time" in field.lower() or "created" in field.lower():
                    date_field = field_mappings[field]["field"]
                    break
                    
            if date_field and date_value:
                # Find related comparison
                comparison_op = FilterAnalyzer._find_related_comparison(comparisons, date_entity["original_text"])
                
                # Default to equals if no comparison found
                mongo_op = {"$eq": date_value}
                
                if comparison_op:
                    if comparison_op == "gt":
                        mongo_op = {"$gt": date_value}
                    elif comparison_op == "lt":
                        mongo_op = {"$lt": date_value}
                    elif comparison_op == "gte":
                        mongo_op = {"$gte": date_value}
                    elif comparison_op == "lte":
                        mongo_op = {"$lte": date_value}
                    elif comparison_op == "ne":
                        mongo_op = {"$ne": date_value}
                        
                # If date range entities were extracted, create range condition
                if date_entity.get("type") == "relative_range":
                    # Determine the date range based on the unit
                    unit = date_entity.get("range_unit", "")
                    value = date_entity.get("range_value", 1)
                    
                    if "last" in date_entity.get("original_text", "").lower():
                        # For "last X days/months/etc", use $gte condition
                        mongo_op = {"$gte": date_value}
                    elif "next" in date_entity.get("original_text", "").lower():
                        # For "next X days/months/etc", use $lte condition
                        mongo_op = {"$lte": date_value}
                
                conditions.append({date_field: mongo_op})
        
        # Process numeric conditions
        for numeric_entity in numeric_values:
            numeric_value = numeric_entity["value"]
            
            # Find related field
            numeric_field = None
            for field in field_mappings:
                # Try to match the numeric value with the field
                if FilterAnalyzer._text_proximity(field, numeric_entity["original_text"]) <= 10:
                    numeric_field = field_mappings[field]["field"]
                    break
                    
            if not numeric_field:
                # If no field found based on proximity, use the first field that looks numeric
                for field in field_mappings:
                    if "amount" in field.lower() or "count" in field.lower() or "value" in field.lower() or "price" in field.lower():
                        numeric_field = field_mappings[field]["field"]
                        break
                        
            if numeric_field and numeric_value is not None:
                # Find related comparison
                comparison_op = FilterAnalyzer._find_related_comparison(comparisons, numeric_entity["original_text"])
                
                # Default to equals if no comparison found
                mongo_op = {"$eq": numeric_value}
                
                if comparison_op:
                    if comparison_op == "gt":
                        mongo_op = {"$gt": numeric_value}
                    elif comparison_op == "lt":
                        mongo_op = {"$lt": numeric_value}
                    elif comparison_op == "gte":
                        mongo_op = {"$gte": numeric_value}
                    elif comparison_op == "lte":
                        mongo_op = {"$lte": numeric_value}
                    elif comparison_op == "ne":
                        mongo_op = {"$ne": numeric_value}
                        
                conditions.append({numeric_field: mongo_op})
        
        # Process string conditions
        for string_entity in string_values:
            string_value = string_entity["value"]
            
            # Find related field
            string_field = None
            for field in field_mappings:
                # Try to match the string value with the field
                if FilterAnalyzer._text_proximity(field, string_entity["original_text"]) <= 10:
                    string_field = field_mappings[field]["field"]
                    break
                    
            if not string_field:
                # If no field found based on proximity, use the first field that looks like a string
                for field in field_mappings:
                    if "name" in field.lower() or "title" in field.lower() or "description" in field.lower() or "status" in field.lower():
                        string_field = field_mappings[field]["field"]
                        break
                        
            if string_field and string_value:
                # Find related comparison
                comparison_op = FilterAnalyzer._find_related_comparison(comparisons, string_entity["original_text"])
                
                # Default to equals if no comparison found
                mongo_op = {"$eq": string_value}
                
                if comparison_op:
                    if comparison_op == "ne":
                        mongo_op = {"$ne": string_value}
                    elif comparison_op == "contains":
                        mongo_op = {"$regex": string_value, "$options": "i"}
                    elif comparison_op == "starts_with":
                        mongo_op = {"$regex": f"^{string_value}", "$options": "i"}
                    elif comparison_op == "ends_with":
                        mongo_op = {"$regex": f"{string_value}$", "$options": "i"}
                        
                conditions.append({string_field: mongo_op})
        
        # Combine conditions based on logical operators
        if conditions:
            if len(conditions) == 1:
                # Single condition
                filter_doc = conditions[0]
            else:
                # Multiple conditions - determine if AND or OR
                has_or = any(op["operator"] == "or" for op in logical_operators)
                
                if has_or:
                    filter_doc = {"$or": conditions}
                else:
                    # Default to AND
                    filter_doc = {"$and": conditions}
        
        return {
            "has_filters": bool(filter_doc),
            "filter": filter_doc
        }

    @staticmethod
    def _structure_clickhouse_filters(
        entities: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Structure ClickHouse WHERE conditions from entities.
        
        Args:
            entities: Extracted entities.
            
        Returns:
            Dict[str, Any]: Structured ClickHouse filter.
        """
        # Extract filter-related entities
        fields = entities.get("fields", [])
        mapped_fields = entities.get("mapped_fields", {}).get("clickhouse", [])
        date_values = entities.get("date_values", [])
        numeric_values = entities.get("numeric_values", [])
        string_values = entities.get("string_values", [])
        comparisons = entities.get("comparisons", [])
        logical_operators = entities.get("logical_operators", [])
        
        # Initialize filter structure
        where_clauses = []
        
        # Map fields to their best matches in the schema
        field_mappings = {}
        for field in fields:
            best_match = None
            
            # Look for an exact match first
            for mapped_field in mapped_fields:
                if mapped_field["field"] == field and mapped_field["match_type"] == "exact":
                    best_match = mapped_field
                    break
                    
            # If no exact match, use the first partial match
            if not best_match:
                for mapped_field in mapped_fields:
                    if mapped_field["match_type"] == "partial":
                        best_match = mapped_field
                        break
                        
            if best_match:
                field_mappings[field] = best_match
        
        # Process date conditions
        for date_entity in date_values:
            date_value = date_entity["value"]
            
            # Find related field (simple heuristic - look for date/time fields)
            date_field = None
            for field in field_mappings:
                if "date" in field.lower() or "time" in field.lower() or "created" in field.lower():
                    date_field = field_mappings[field]["field"]
                    break
                    
            if date_field and date_value:
                # Find related comparison
                comparison_op = FilterAnalyzer._find_related_comparison(comparisons, date_entity["original_text"])
                
                # Default to equals if no comparison found
                sql_op = f"{date_field} = '{date_value}'"
                
                if comparison_op:
                    if comparison_op == "gt":
                        sql_op = f"{date_field} > '{date_value}'"
                    elif comparison_op == "lt":
                        sql_op = f"{date_field} < '{date_value}'"
                    elif comparison_op == "gte":
                        sql_op = f"{date_field} >= '{date_value}'"
                    elif comparison_op == "lte":
                        sql_op = f"{date_field} <= '{date_value}'"
                    elif comparison_op == "ne":
                        sql_op = f"{date_field} != '{date_value}'"
                        
                # If date range entities were extracted, create range condition
                if date_entity.get("type") == "relative_range":
                    # Determine the date range based on the unit
                    unit = date_entity.get("range_unit", "")
                    value = date_entity.get("range_value", 1)
                    
                    if "last" in date_entity.get("original_text", "").lower():
                        # For "last X days/months/etc", use >= condition
                        sql_op = f"{date_field} >= '{date_value}'"
                    elif "next" in date_entity.get("original_text", "").lower():
                        # For "next X days/months/etc", use <= condition
                        sql_op = f"{date_field} <= '{date_value}'"
                
                where_clauses.append(sql_op)
        
        # Process numeric conditions
        for numeric_entity in numeric_values:
            numeric_value = numeric_entity["value"]
            
            # Find related field
            numeric_field = None
            for field in field_mappings:
                # Try to match the numeric value with the field
                if FilterAnalyzer._text_proximity(field, numeric_entity["original_text"]) <= 10:
                    numeric_field = field_mappings[field]["field"]
                    break
                    
            if not numeric_field:
                # If no field found based on proximity, use the first field that looks numeric
                for field in field_mappings:
                    if "amount" in field.lower() or "count" in field.lower() or "value" in field.lower() or "price" in field.lower():
                        numeric_field = field_mappings[field]["field"]
                        break
                        
            if numeric_field and numeric_value is not None:
                # Find related comparison
                comparison_op = FilterAnalyzer._find_related_comparison(comparisons, numeric_entity["original_text"])
                
                # Default to equals if no comparison found
                sql_op = f"{numeric_field} = {numeric_value}"
                
                if comparison_op:
                    if comparison_op == "gt":
                        sql_op = f"{numeric_field} > {numeric_value}"
                    elif comparison_op == "lt":
                        sql_op = f"{numeric_field} < {numeric_value}"
                    elif comparison_op == "gte":
                        sql_op = f"{numeric_field} >= {numeric_value}"
                    elif comparison_op == "lte":
                        sql_op = f"{numeric_field} <= {numeric_value}"
                    elif comparison_op == "ne":
                        sql_op = f"{numeric_field} != {numeric_value}"
                        
                where_clauses.append(sql_op)
        
        # Process string conditions
        for string_entity in string_values:
            string_value = string_entity["value"]
            
            # Find related field
            string_field = None
            for field in field_mappings:
                # Try to match the string value with the field
                if FilterAnalyzer._text_proximity(field, string_entity["original_text"]) <= 10:
                    string_field = field_mappings[field]["field"]
                    break
                    
            if not string_field:
                # If no field found based on proximity, use the first field that looks like a string
                for field in field_mappings:
                    if "name" in field.lower() or "title" in field.lower() or "description" in field.lower() or "status" in field.lower():
                        string_field = field_mappings[field]["field"]
                        break
                        
            if string_field and string_value:
                # Find related comparison
                comparison_op = FilterAnalyzer._find_related_comparison(comparisons, string_entity["original_text"])
                
                # Default to equals if no comparison found
                sql_op = f"{string_field} = '{string_value}'"
                
                if comparison_op:
                    if comparison_op == "ne":
                        sql_op = f"{string_field} != '{string_value}'"
                    elif comparison_op == "contains":
                        sql_op = f"{string_field} LIKE '%{string_value}%'"
                    elif comparison_op == "starts_with":
                        sql_op = f"{string_field} LIKE '{string_value}%'"
                    elif comparison_op == "ends_with":
                        sql_op = f"{string_field} LIKE '%{string_value}'"
                        
                where_clauses.append(sql_op)
        
        # Combine WHERE clauses based on logical operators
        if where_clauses:
            # Determine the logical operator (AND/OR)
            has_or = any(op["operator"] == "or" for op in logical_operators)
            
            if has_or:
                where_clause = " OR ".join(where_clauses)
            else:
                # Default to AND
                where_clause = " AND ".join(where_clauses)
        else:
            where_clause = ""
        
        return {
            "has_filters": bool(where_clauses),
            "where_clause": where_clause,
            "where_parts": where_clauses
        }

    @staticmethod
    def _find_related_comparison(
        comparisons: List[Dict[str, Any]],
        text: str
    ) -> Optional[str]:
        """
        Find the comparison operator most closely related to a text.
        
        Args:
            comparisons: List of comparison operators.
            text: The text to find comparisons for.
            
        Returns:
            Optional[str]: Most relevant comparison operator, or None.
        """
        if not comparisons:
            return None
            
        # Find the comparison closest to the text (simplified approach)
        min_distance = float('inf')
        closest_comparison = None
        
        for comparison in comparisons:
            distance = FilterAnalyzer._text_proximity(comparison["original_text"], text)
            
            if distance < min_distance:
                min_distance = distance
                closest_comparison = comparison
        
        # Only use the comparison if it's reasonably close
        if min_distance <= 30:  # Arbitrary threshold
            return closest_comparison["operator"]
            
        return None

    @staticmethod
    def _text_proximity(text1: str, text2: str) -> int:
        """
        Calculate a simple proximity score between two text fragments.
        Lower score means closer proximity.
        
        Args:
            text1: First text.
            text2: Second text.
            
        Returns:
            int: Proximity score (lower is closer).
        """
        # Convert to lowercase for comparison
        text1 = text1.lower()
        text2 = text2.lower()
        
        # Check if one contains the other
        if text1 in text2:
            return 0
        if text2 in text1:
            return 0
            
        # Otherwise, use a simple word-based proximity
        words1 = set(text1.split())
        words2 = set(text2.split())
        
        # Count common words
        common_words = words1.intersection(words2)
        
        if common_words:
            return min(10, 10 - len(common_words))
            
        # If no common words, return a high score
        return 100


# Create global filter analyzer instance
filter_analyzer = FilterAnalyzer()