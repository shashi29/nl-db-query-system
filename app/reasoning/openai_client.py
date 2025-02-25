"""
OpenAI client for query interpretation and generation.
"""
from typing import Any, Dict, List, Optional, Union
import json
import time
import openai

from ..config.settings import settings
from ..config.logging_config import logger


class OpenAIClient:
    
    def __init__(self):
        self.api_key = settings.openai.api_key
        self.model = settings.openai.model
        self.temperature = settings.openai.temperature
        self.max_tokens = settings.openai.max_tokens
        self.timeout = settings.openai.timeout
        
        # Initialize client directly as AsyncOpenAI
        from openai import AsyncOpenAI
        self.client = AsyncOpenAI(api_key=self.api_key)

    async def generate_query(
        self, 
        query: str, 
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        start_time = time.time()
        
        # try:
        # Prepare the prompt for OpenAI
        prompt = self._build_prompt(query, context)
        
        # Call OpenAI API
        response = await self._call_openai(prompt)
        
        # Parse the response
        parsed_response = self._parse_response(response)
        
        # Add timing information
        parsed_response["generation_time"] = time.time() - start_time
        
        return parsed_response
            
        # except Exception as e:
        #     logger.error(f"Error generating query with OpenAI: {str(e)}")
        #     return {
        #         "success": False,
        #         "error": f"Error generating query: {str(e)}",
        #         "generation_time": time.time() - start_time
        #     }

    def _build_prompt(
        self, 
        query: str, 
        context: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        # Determine data source type
        data_source = "clickhouse"  # context.get("data_source", "mongodb")
        
        # Build system prompt based on data source
        system_content = """
        You are a database query expert. Your task is to convert natural language queries into 
        database queries. Follow these steps carefully:
        
        1. Analyze the query to understand what data the user wants to retrieve.
        2. Identify the appropriate collections or tables to query.
        3. Determine the necessary filtering conditions.
        4. Formulate the query in the correct syntax for the target database.
        5. Explain your reasoning clearly.
        
        Respond with a JSON object containing:
        - reasoning: Your step-by-step reasoning process
        - generated_plan: The complete query plan
        """
        
        # Add data source specific instructions
        if data_source == "mongodb":
            system_content += """
            For MongoDB queries:
            - Use standard MongoDB query operators ($eq, $gt, $lt, etc.)
            - For 'find' operations, provide the query as a JSON object
            - For 'aggregate' operations, provide a pipeline as a JSON array
            """
        elif data_source == "clickhouse":
            system_content += """
            For ClickHouse queries:
            - Use standard SQL syntax
            - Be precise with table and column names
            - Use appropriate ClickHouse SQL functions and features
            """
        elif data_source == "federated":
            system_content += """
            For federated queries:
            - Define clear steps for querying both databases
            - Explain how data will be combined or compared
            - Specify which database handles which part of the query
            """
        
        # Create the prompt messages
        messages = [
            {
                "role": "system",
                "content": system_content
            },
            {
                "role": "user",
                "content": f"""
                Query: {query}
                
                Database Context:
                {json.dumps(context, indent=2)}
                
                Transform this natural language query into the appropriate database query.
                Remember to explain your reasoning step by step.
                """
            }
        ]
        
        return messages

    async def _call_openai(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        # Make the API call with updated syntax and specify JSON response format
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            response_format={"type": "json_object"},
            timeout=self.timeout
        )
        
        return response

    def _parse_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # Get the content from the response
            content = response.choices[0].message.content
            
            # Parse the JSON content directly
            parsed_json = json.loads(content)
            
            # Return structured response
            return {
                "success": True,
                "reasoning": parsed_json.get("reasoning", ""),
                "generated_plan": parsed_json.get("generated_plan", {}),
                "raw_response": content
            }
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON response: {str(e)}")
            return {
                "success": False,
                "error": f"Invalid JSON response: {str(e)}",
                "raw_response": response.choices[0].message.content
            }
        except Exception as e:
            logger.error(f"Error parsing OpenAI response: {str(e)}")
            return {
                "success": False,
                "error": f"Error parsing response: {str(e)}",
                "raw_response": response.choices[0].message.content if hasattr(response, 'choices') else str(response)
            }

    def _extract_structured_info(self, content: str) -> Dict[str, Any]:
        # This method is kept for compatibility but won't be used with direct JSON responses
        try:
            # Look for reasoning section
            reasoning = ""
            reasoning_start = content.find("Reasoning:")
            reasoning_end = content.find("Generated Plan:")
            
            if reasoning_start != -1 and reasoning_end != -1:
                reasoning = content[reasoning_start + len("Reasoning:"):reasoning_end].strip()
            
            # Look for generated plan section
            generated_plan = ""
            plan_start = content.find("Generated Plan:")
            if plan_start != -1:
                generated_plan = content[plan_start + len("Generated Plan:"):].strip()
            
            # Try to parse generated plan as JSON
            try:
                plan_json = json.loads(generated_plan)
                return {
                    "success": True,
                    "reasoning": reasoning,
                    "generated_plan": plan_json,
                    "raw_response": content
                }
            except json.JSONDecodeError:
                return {
                    "success": False,
                    "error": "Could not parse generated plan as JSON",
                    "reasoning": reasoning,
                    "raw_plan_text": generated_plan,
                    "raw_response": content
                }
        
        except Exception as e:
            logger.error(f"Error extracting structured info: {str(e)}")
            return {
                "success": False,
                "error": f"Error extracting structured info: {str(e)}",
                "raw_response": content
            }


# Create global OpenAI client instance
openai_client = OpenAIClient()