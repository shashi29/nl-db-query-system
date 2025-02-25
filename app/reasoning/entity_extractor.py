 """
Entity extractor for extracting entities from natural language queries.
"""
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import re
import datetime
from dateutil.relativedelta import relativedelta

from ..config.logging_config import logger
from ..utils.preprocessing import extract_field_references
from .intent_recognizer import IntentRecognizer


class EntityExtractor:
    """
    Extractor for identifying entities and values in natural language queries.
    """
    
    @staticmethod
    def extract_entities(
        query: str, 
        schema_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Extract entities from a natural language query.
        
        Args:
            query: The natural language query.
            schema_info: Schema information for context.
            
        Returns:
            Dict[str, Any]: Extracted entities.
        """
        try:
            # Extract mentioned fields
            fields = extract_field_references(query)
            
            # Extract date/time values
            date_values = EntityExtractor._extract_dates(query)
            
            # Extract numeric values
            numeric_values = EntityExtractor._extract_numeric_values(query)
            
            # Extract string values
            string_values = EntityExtractor._extract_string_values(query)
            
            # Extract comparison operators
            comparisons = EntityExtractor._extract_comparisons(query)
            
            # Extract logical operators
            logical_operators = EntityExtractor._extract_logical_operators(query)
            
            # Extract sort information
            sort_info = EntityExtractor._extract_sort_info(query)
            
            # Extract limit information
            limit_info = EntityExtractor._extract_limit_info(query)
            
            # Extract aggregation information
            aggregation_info = EntityExtractor._extract_aggregation_info(query)
            
            # Build entity information
            entities = {
                "fields": fields,
                "date_values": date_values,
                "numeric_values": numeric_values,
                "string_values": string_values,
                "comparisons": comparisons,
                "logical_operators": logical_operators,
                "sort_info": sort_info,
                "limit_info": limit_info,
                "aggregation_info": aggregation_info
            }
            
            # Try to map extracted fields to schema fields
            mapped_fields = EntityExtractor._map_fields_to_schema(fields, schema_info)
            if mapped_fields:
                entities["mapped_fields"] = mapped_fields
            
            logger.debug(f"Extracted entities: {entities}")
            return entities
            
        except Exception as e:
            logger.error(f"Error extracting entities: {str(e)}")
            return {
                "error": f"Error extracting entities: {str(e)}"
            }

    @staticmethod
    def _extract_dates(query: str) -> List[Dict[str, Any]]:
        """
        Extract date/time values from the query.
        
        Args:
            query: The query text.
            
        Returns:
            List[Dict[str, Any]]: Extracted date information.
        """
        date_info = []
        
        # Current date/time for relative date calculation
        now = datetime.datetime.now()
        
        # Extract absolute dates (YYYY-MM-DD format)
        absolute_date_pattern = r'(\d{4}-\d{1,2}-\d{1,2})'
        for match in re.finditer(absolute_date_pattern, query):
            try:
                date_str = match.group(1)
                parsed_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
                date_info.append({
                    "type": "absolute",
                    "value": parsed_date.isoformat(),
                    "original_text": date_str
                })
            except ValueError:
                # Skip invalid dates
                pass
        
        # Extract relative dates
        relative_patterns = [
            # Days
            (r'today', lambda: now.date()),
            (r'yesterday', lambda: (now - datetime.timedelta(days=1)).date()),
            (r'tomorrow', lambda: (now + datetime.timedelta(days=1)).date()),
            
            # Weeks
            (r'this week', lambda: (now - datetime.timedelta(days=now.weekday())).date()),
            (r'last week', lambda: (now - datetime.timedelta(days=now.weekday() + 7)).date()),
            (r'next week', lambda: (now - datetime.timedelta(days=now.weekday() - 7)).date()),
            
            # Months
            (r'this month', lambda: now.replace(day=1).date()),
            (r'last month', lambda: (now.replace(day=1) - datetime.timedelta(days=1)).replace(day=1).date()),
            (r'next month', lambda: (now.replace(day=28) + datetime.timedelta(days=4)).replace(day=1).date()),
            
            # Years
            (r'this year', lambda: now.replace(month=1, day=1).date()),
            (r'last year', lambda: now.replace(year=now.year-1, month=1, day=1).date()),
            (r'next year', lambda: now.replace(year=now.year+1, month=1, day=1).date()),
        ]
        
        for pattern, date_func in relative_patterns:
            if re.search(r'\b' + pattern + r'\b', query, re.IGNORECASE):
                date_value = date_func()
                date_info.append({
                    "type": "relative",
                    "value": date_value.isoformat(),
                    "original_text": pattern
                })
        
        # Extract relative time ranges
        time_range_patterns = [
            # Last X days/weeks/months/years
            (r'last\s+(\d+)\s+days?', lambda x: (now - datetime.timedelta(days=int(x))).date()),
            (r'last\s+(\d+)\s+weeks?', lambda x: (now - datetime.timedelta(weeks=int(x))).date()),
            (r'last\s+(\d+)\s+months?', lambda x: (now - relativedelta(months=int(x))).date()),
            (r'last\s+(\d+)\s+years?', lambda x: (now - relativedelta(years=int(x))).date()),
            
            # Next X days/weeks/months/years
            (r'next\s+(\d+)\s+days?', lambda x: (now + datetime.timedelta(days=int(x))).date()),
            (r'next\s+(\d+)\s+weeks?', lambda x: (now + datetime.timedelta(weeks=int(x))).date()),
            (r'next\s+(\d+)\s+months?', lambda x: (now + relativedelta(months=int(x))).date()),
            (r'next\s+(\d+)\s+years?', lambda x: (now + relativedelta(years=int(x))).date()),
        ]
        
        for pattern, date_func in time_range_patterns:
            matches = re.finditer(pattern, query, re.IGNORECASE)
            for match in matches:
                value = match.group(1)
                date_value = date_func(value)
                date_info.append({
                    "type": "relative_range",
                    "value": date_value.isoformat(),
                    "range_value": int(value),
                    "range_unit": match.group(0).split()[-1],
                    "original_text": match.group(0)
                })
        
        return date_info

    @staticmethod
    def _extract_numeric_values(query: str) -> List[Dict[str, Any]]:
        """
        Extract numeric values from the query.
        
        Args:
            query: The query text.
            
        Returns:
            List[Dict[str, Any]]: Extracted numeric information.
        """
        numeric_info = []
        
        # Extract integers
        int_pattern = r'\b(\d+)\b'
        for match in re.finditer(int_pattern, query):
            # Skip if it's part of a date
            date_pattern = r'\d{4}-\d{1,2}-\d{1,2}'
            is_date = any(date_match.start() <= match.start() <= date_match.end() 
                          for date_match in re.finditer(date_pattern, query))
            
            if not is_date:
                value = int(match.group(1))
                numeric_info.append({
                    "type": "integer",
                    "value": value,
                    "original_text": match.group(0)
                })
        
        # Extract decimals
        decimal_pattern = r'\b(\d+\.\d+)\b'
        for match in re.finditer(decimal_pattern, query):
            value = float(match.group(1))
            numeric_info.append({
                "type": "decimal",
                "value": value,
                "original_text": match.group(0)
            })
        
        # Extract percentages
        percentage_pattern = r'(\d+(?:\.\d+)?)%'
        for match in re.finditer(percentage_pattern, query):
            value = float(match.group(1))
            numeric_info.append({
                "type": "percentage",
                "value": value / 100.0,  # Convert to decimal
                "original_text": match.group(0)
            })
        
        # Extract currency
        currency_pattern = r'(\$|€|£)(\d+(?:\.\d+)?)'
        for match in re.finditer(currency_pattern, query):
            currency = match.group(1)
            value = float(match.group(2))
            numeric_info.append({
                "type": "currency",
                "value": value,
                "currency": currency,
                "original_text": match.group(0)
            })
        
        return numeric_info

    @staticmethod
    def _extract_string_values(query: str) -> List[Dict[str, Any]]:
        """
        Extract string values from the query.
        
        Args:
            query: The query text.
            
        Returns:
            List[Dict[str, Any]]: Extracted string information.
        """
        string_info = []
        
        # Extract quoted strings
        quoted_pattern = r'"([^"]*)"'
        for match in re.finditer(quoted_pattern, query):
            value = match.group(1)
            string_info.append({
                "type": "quoted",
                "value": value,
                "original_text": match.group(0)
            })
        
        # Extract single-quoted strings
        single_quoted_pattern = r"'([^']*)'"
        for match in re.finditer(single_quoted_pattern, query):
            value = match.group(1)
            string_info.append({
                "type": "quoted",
                "value": value,
                "original_text": match.group(0)
            })
        
        # Extract potential enum/category values
        # This is a simplified approach and would need to be enhanced with schema knowledge
        word_pattern = r'\b(true|false|yes|no|high|medium|low|active|inactive|pending|completed|cancelled|canceled|new|open|closed)\b'
        for match in re.finditer(word_pattern, query, re.IGNORECASE):
            value = match.group(1).lower()
            string_info.append({
                "type": "enum",
                "value": value,
                "original_text": match.group(0)
            })
        
        return string_info

    @staticmethod
    def _extract_comparisons(query: str) -> List[Dict[str, Any]]:
        """
        Extract comparison operators from the query.
        
        Args:
            query: The query text.
            
        Returns:
            List[Dict[str, Any]]: Extracted comparison information.
        """
        comparison_info = []
        
        # Define patterns for different comparison types
        comparison_patterns = [
            (r'\b(equal to|equals|is|=)\b', "eq"),
            (r'\b(not equal to|not equals|is not|!=|<>)\b', "ne"),
            (r'\b(greater than|>)\b', "gt"),
            (r'\b(less than|<)\b', "lt"),
            (r'\b(greater than or equal to|>=)\b', "gte"),
            (r'\b(less than or equal to|<=)\b', "lte"),
            (r'\b(between)\b', "between"),
            (r'\b(contains|has|includes)\b', "contains"),
            (r'\b(starts with|begins with)\b', "starts_with"),
            (r'\b(ends with)\b', "ends_with"),
            (r'\b(matches|like)\b', "like"),
            (r'\b(in)\b', "in"),
            (r'\b(not in)\b', "not_in"),
            (r'\b(exists)\b', "exists"),
            (r'\b(not exists|does not exist)\b', "not_exists")
        ]
        
        for pattern, operator in comparison_patterns:
            for match in re.finditer(pattern, query, re.IGNORECASE):
                comparison_info.append({
                    "operator": operator,
                    "original_text": match.group(0),
                    "position": (match.start(), match.end())
                })
        
        return comparison_info

    @staticmethod
    def _extract_logical_operators(query: str) -> List[Dict[str, Any]]:
        """
        Extract logical operators from the query.
        
        Args:
            query: The query text.
            
        Returns:
            List[Dict[str, Any]]: Extracted logical operator information.
        """
        logical_info = []
        
        # Define patterns for different logical operators
        logical_patterns = [
            (r'\b(and|&|\+)\b', "and"),
            (r'\b(or|\|)\b', "or"),
            (r'\b(not|!|-)\b', "not")
        ]
        
        for pattern, operator in logical_patterns:
            for match in re.finditer(pattern, query, re.IGNORECASE):
                logical_info.append({
                    "operator": operator,
                    "original_text": match.group(0),
                    "position": (match.start(), match.end())
                })
        
        return logical_info

    @staticmethod
    def _extract_sort_info(query: str) -> Optional[Dict[str, Any]]:
        """
        Extract sorting information from the query.
        
        Args:
            query: The query text.
            
        Returns:
            Optional[Dict[str, Any]]: Extracted sort information.
        """
        # Check for sort indicators
        sort_pattern = r'\b(sort|order)\s+by\s+([a-zA-Z0-9_]+)\s*(asc|ascending|desc|descending)?\b'
        match = re.search(sort_pattern, query, re.IGNORECASE)
        
        if match:
            field = match.group(2)
            direction = match.group(3) if match.group(3) else "asc"  # Default to ascending
            
            # Normalize direction
            if direction.lower() in ["desc", "descending"]:
                direction = "desc"
            else:
                direction = "asc"
                
            return {
                "field": field,
                "direction": direction,
                "original_text": match.group(0)
            }
            
        return None

    @staticmethod
    def _extract_limit_info(query: str) -> Optional[Dict[str, Any]]:
        """
        Extract limit information from the query.
        
        Args:
            query: The query text.
            
        Returns:
            Optional[Dict[str, Any]]: Extracted limit information.
        """
        # Check for limit indicators
        limit_patterns = [
            r'\b(limit|only|just|top)\s+(\d+)\b',
            r'\b(\d+)\s+(results|rows|documents|records|items)\b'
        ]
        
        for pattern in limit_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                # Extract the limit value
                limit_group = 2 if pattern.startswith(r'\b(limit') else 1
                limit = int(match.group(limit_group))
                
                return {
                    "limit": limit,
                    "original_text": match.group(0)
                }
                
        return None

    @staticmethod
    def _extract_aggregation_info(query: str) -> Optional[Dict[str, Any]]:
        """
        Extract aggregation information from the query.
        
        Args:
            query: The query text.
            
        Returns:
            Optional[Dict[str, Any]]: Extracted aggregation information.
        """
        # Check if the query involves aggregation
        if not IntentRecognizer._check_aggregation(query):
            return None
            
        # Extract aggregation function
        agg_pattern = r'\b(average|avg|mean|sum|total|count|min|max|median)\s+(?:of|for)?\s+([a-zA-Z0-9_]+)?\b'
        match = re.search(agg_pattern, query, re.IGNORECASE)
        
        if match:
            function = match.group(1).lower()
            field = match.group(2) if match.group(2) else None
            
            # Normalize function name
            if function in ["average", "mean"]:
                function = "avg"
            elif function == "total":
                function = "sum"
                
            agg_info = {
                "function": function,
                "original_text": match.group(0)
            }
            
            if field:
                agg_info["field"] = field
                
            # Check for group by
            group_pattern = r'\b(group|grouped)\s+by\s+([a-zA-Z0-9_]+)\b'
            group_match = re.search(group_pattern, query, re.IGNORECASE)
            
            if group_match:
                agg_info["group_by"] = group_match.group(2)
                agg_info["group_original_text"] = group_match.group(0)
                
            return agg_info
            
        return None

    @staticmethod
    def _map_fields_to_schema(
        fields: List[str],
        schema_info: Dict[str, Any]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Map extracted fields to schema fields.
        
        Args:
            fields: Extracted field names.
            schema_info: Schema information.
            
        Returns:
            Dict[str, List[Dict[str, Any]]]: Mapped fields by database.
        """
        if not fields:
            return {}
            
        mapped_fields = {
            "mongodb": [],
            "clickhouse": []
        }
        
        # Check MongoDB schemas
        if "mongodb" in schema_info:
            collections = schema_info["mongodb"].get("collections", {})
            
            for field in fields:
                field_matches = []
                
                for collection_name, collection_info in collections.items():
                    collection_fields = collection_info.get("fields", {})
                    
                    if field in collection_fields:
                        # Exact match
                        field_matches.append({
                            "field": field,
                            "collection": collection_name,
                            "match_type": "exact",
                            "field_info": collection_fields[field]
                        })
                    else:
                        # Check for partial matches
                        for schema_field in collection_fields:
                            if field.lower() in schema_field.lower() or schema_field.lower() in field.lower():
                                field_matches.append({
                                    "field": schema_field,
                                    "collection": collection_name,
                                    "match_type": "partial",
                                    "field_info": collection_fields[schema_field]
                                })
                                
                if field_matches:
                    mapped_fields["mongodb"].extend(field_matches)
        
        # Check ClickHouse schemas
        if "clickhouse" in schema_info:
            tables = schema_info["clickhouse"].get("tables", {})
            
            for field in fields:
                field_matches = []
                
                for table_name, table_info in tables.items():
                    table_fields = table_info.get("fields", {})
                    
                    if field in table_fields:
                        # Exact match
                        field_matches.append({
                            "field": field,
                            "table": table_name,
                            "match_type": "exact",
                            "field_info": table_fields[field]
                        })
                    else:
                        # Check for partial matches
                        for schema_field in table_fields:
                            if field.lower() in schema_field.lower() or schema_field.lower() in field.lower():
                                field_matches.append({
                                    "field": schema_field,
                                    "table": table_name,
                                    "match_type": "partial",
                                    "field_info": table_fields[schema_field]
                                })
                                
                if field_matches:
                    mapped_fields["clickhouse"].extend(field_matches)
        
        # Remove empty entries
        if not mapped_fields["mongodb"]:
            del mapped_fields["mongodb"]
            
        if not mapped_fields["clickhouse"]:
            del mapped_fields["clickhouse"]
            
        return mapped_fields


# Create global entity extractor instance
entity_extractor = EntityExtractor()