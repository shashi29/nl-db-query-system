"""
Main entry point for the NL-DB-Query-System.
"""
import asyncio
import uvicorn
import click
import json
from typing import Any, Dict, Optional

from .config.settings import settings
from .config.logging_config import logger
from .planning.planner import planner
from .reasoning.openai_client import openai_client
from .execution.executor import executor
from .reflection.evaluator import evaluator
from .reflection.performance_analyzer import performance_analyzer
from .reflection.optimizer import optimizer


@click.group()
def cli():
    """Command line interface for NL-DB-Query-System."""
    pass


@cli.command()
@click.option('--query', '-q', help='Natural language query to execute')
@click.option('--format', '-f', default='json', help='Output format (json, table, csv)')
@click.option('--optimize/--no-optimize', default=False, help='Optimize query before execution')
@click.option('--output', '-o', help='Output file (if not specified, prints to console)')
async def query(query: str, format: str, optimize: bool, output: Optional[str]):
    """
    Execute a natural language query.
    
    Args:
        query: Natural language query to execute.
        format: Output format (json, table, csv).
        optimize: Whether to optimize the query.
        output: Output file (if not specified, prints to console).
    """
    try:
        # Initialize planner
        await planner.initialize()
        
        # Plan the query
        query_plan = await planner.plan_query(query)
        
        if not query_plan.get("success", False):
            logger.error(f"Query planning failed: {query_plan.get('error', 'Unknown error')}")
            return
            
        # Generate database query with OpenAI
        openai_response = await openai_client.generate_query(
            query, 
            query_plan["context"]
        )
        
        if not openai_response.get("success", False):
            logger.error(f"Query generation failed: {openai_response.get('error', 'Unknown error')}")
            return
            
        # Refine the plan with OpenAI's response
        refined_plan = await planner.refine_plan(query_plan, openai_response)
        
        if not refined_plan.get("success", False):
            logger.error(f"Plan refinement failed: {refined_plan.get('error', 'Unknown error')}")
            return
            
        # Optimize query if requested
        if optimize:
            refined_plan = optimizer.optimize_query(refined_plan)
            logger.info("Query optimized")
            
        # Execute the query
        result = await executor.execute_query(refined_plan)
        
        if not result.get("success", False):
            logger.error(f"Query execution failed: {result.get('error', 'Unknown error')}")
            return
            
        # Output the result
        if output:
            with open(output, 'w') as f:
                if format == 'json':
                    json.dump(result, f, indent=2)
                else:
                    f.write(result.get("formatted_data", "No data"))
                    
            logger.info(f"Result written to {output}")
        else:
            if format == 'json':
                print(json.dumps(result, indent=2))
            else:
                print(result.get("formatted_data", "No data"))
                
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")


@cli.command()
@click.option('--host', default=settings.api.host, help='API host')
@click.option('--port', default=settings.api.port, help='API port')
@click.option('--reload/--no-reload', default=settings.api.debug, help='Enable auto-reload')
def api(host: str, port: int, reload: bool):
    """
    Start the API server.
    
    Args:
        host: API host.
        port: API port.
        reload: Whether to enable auto-reload.
    """
    from .interface.api import app
    
    logger.info(f"Starting API server on {host}:{port}")
    uvicorn.run("app.interface.api:app", host=host, port=port, reload=reload)


@cli.command()
async def refresh_schema():
    """Refresh database schema information."""
    from .planning.schema_manager import schema_manager
    
    logger.info("Refreshing schema...")
    success = await schema_manager.refresh_schemas()
    
    if success:
        logger.info("Schema refreshed successfully")
    else:
        logger.error("Failed to refresh schema")


@cli.command()
async def init():
    """Initialize the system."""
    # Initialize planner (which initializes schema manager)
    init_success = await planner.initialize()
    
    if init_success:
        logger.info("System initialized successfully")
    else:
        logger.error("Failed to initialize system")


if __name__ == "__main__":
    # Run CLI in asyncio event loop
    loop = asyncio.get_event_loop()
    loop.run_until_complete(cli())