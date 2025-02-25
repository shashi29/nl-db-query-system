"""
API interface for the NL-DB-Query-System.
"""
from typing import Any, Dict, List, Optional, Union
from fastapi import FastAPI, HTTPException, Depends, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from ..config.settings import settings
from ..config.logging_config import logger
from ..planning.planner import planner
from ..reasoning.openai_client import openai_client
from ..execution.executor import executor
from ..reflection.evaluator import evaluator
from ..reflection.performance_analyzer import performance_analyzer
from ..reflection.optimizer import optimizer
from ..reflection.feedback_collector import feedback_collector


# Define API models
class NaturalLanguageQuery(BaseModel):
    """Natural language query model."""
    query: str
    format: Optional[str] = "json"
    optimize: Optional[bool] = False
    use_cache: Optional[bool] = True


class FeedbackInput(BaseModel):
    """Feedback input model."""
    rating: Optional[int] = None
    accuracy: Optional[int] = None
    relevance: Optional[int] = None
    performance: Optional[int] = None
    comment: Optional[str] = None
    issues: Optional[List[str]] = None


# Create FastAPI app
app = FastAPI(
    title="NL-DB-Query-System API",
    description="API for natural language database queries",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.api.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Initialize components
@app.on_event("startup")
async def startup_event():
    """Initialize components on startup."""
    # Initialize planner (which initializes schema manager)
    await planner.initialize()
    logger.info("API initialized successfully")


# Define API endpoints
@app.post("/api/query", response_model=Dict[str, Any])
async def process_query(query_request: NaturalLanguageQuery):
    """
    Process a natural language query.
    
    Args:
        query_request: The natural language query request.
        
    Returns:
        Dict[str, Any]: Query result.
    """
    # try:
    # Extract query parameters
    query_text = query_request.query
    format_type = query_request.format
    optimize = query_request.optimize
    use_cache = query_request.use_cache
    
    # Plan the query
    query_plan = await planner.plan_query(query_text)
    
    if not query_plan.get("success", False):
        raise HTTPException(
            status_code=400,
            detail=f"Query planning failed: {query_plan.get('error', 'Unknown error')}"
        )
        
    # Generate database query with OpenAI
    openai_response = await openai_client.generate_query(
        query_text, query_plan["context"]
    )
        
    if not openai_response.get("success", False):
        raise HTTPException(
            status_code=400,
            detail=f"Query generation failed: {openai_response.get('error', 'Unknown error')}"
        )
        
    # Refine the plan with OpenAI's response
    refined_plan = await planner.refine_plan(query_plan, openai_response)
    
    if not refined_plan.get("success", False):
        raise HTTPException(
            status_code=400,
            detail=f"Plan refinement failed: {refined_plan.get('error', 'Unknown error')}"
        )
        
    # Optimize the query if requested
    if optimize:
        refined_plan = optimizer.optimize_query(refined_plan)
        
    # Execute the query
    execution_result = await executor.execute_query(refined_plan["execution_plan"])
    
    # Evaluate the result
    evaluation = evaluator.evaluate(execution_result)
    
    # Analyze performance
    performance = performance_analyzer.analyze_performance(
        execution_result, refined_plan
    )
    
    # Build response
    response = {
        "query": query_text,
        "result": execution_result,
        "evaluation": evaluation,
        "performance": performance,
        "query_id": f"query_{int(execution_result.get('execution_time', 0) * 1000)}"
    }
    
    return response
        
    # except HTTPException:
    #     raise
    # except Exception as e:
    #     logger.error(f"Error processing query: {str(e)}")
    #     raise HTTPException(
    #         status_code=500,
    #         detail=f"Error processing query: {str(e)}"
    #     )


@app.post("/api/feedback/{query_id}", response_model=Dict[str, Any])
async def submit_feedback(query_id: str, feedback: FeedbackInput):
    """
    Submit feedback for a query.
    
    Args:
        query_id: The query ID.
        feedback: The feedback data.
        
    Returns:
        Dict[str, Any]: Feedback submission result.
    """
    try:
        # Validate query ID
        if not query_id:
            raise HTTPException(
                status_code=400,
                detail="Query ID is required"
            )
            
        # Collect feedback
        feedback_result = await feedback_collector.collect_feedback(
            query_id, feedback.dict(exclude_none=True)
        )
        
        if not feedback_result.get("success", False):
            raise HTTPException(
                status_code=400,
                detail=f"Feedback collection failed: {feedback_result.get('error', 'Unknown error')}"
            )
            
        return feedback_result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error submitting feedback: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error submitting feedback: {str(e)}"
        )


@app.get("/api/feedback", response_model=Dict[str, Any])
async def get_feedback_stats():
    """
    Get feedback statistics.
    
    Returns:
        Dict[str, Any]: Feedback statistics.
    """
    try:
        # Get feedback statistics
        stats = feedback_collector.get_feedback_stats()
        
        if not stats.get("success", False):
            raise HTTPException(
                status_code=400,
                detail=f"Failed to get feedback stats: {stats.get('error', 'Unknown error')}"
            )
            
        return stats
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting feedback stats: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting feedback stats: {str(e)}"
        )


@app.get("/api/health", response_model=Dict[str, Any])
async def health_check():
    """
    Health check endpoint.
    
    Returns:
        Dict[str, Any]: Health check result.
    """
    return {
        "status": "ok",
        "version": "1.0.0",
        "environment": settings.environment
    }
