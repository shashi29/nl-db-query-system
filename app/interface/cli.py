"""
Command line interface for the NL-DB-Query-System.
"""
import click
import json
import asyncio
from typing import Any, Dict, Optional
import textwrap

from ..config.logging_config import logger
from ..planning.planner import planner
from ..reasoning.openai_client import openai_client
from ..execution.executor import executor
from ..reflection.optimizer import optimizer


class CLI:
    """
    Command line interface for NL-DB-Query-System.
    """
    
    @staticmethod
    async def run_query(
        query: str, 
        format: str, 
        optimize: bool, 
        verbose: bool,
        output: Optional[str]
    ) -> None:
        """
        Execute a natural language query.
        
        Args:
            query: Natural language query to execute.
            format: Output format (json, table, csv).
            optimize: Whether to optimize the query.
            verbose: Whether to show verbose output.
            output: Output file (if not specified, prints to console).
        """
        try:
            # Initialize planner
            click.echo("Initializing...")
            await planner.initialize()
            
            # Plan the query
            click.echo("Planning query...")
            query_plan = await planner.plan_query(query)
            
            if not query_plan.get("success", False):
                click.echo(f"Error: Query planning failed: {query_plan.get('error', 'Unknown error')}")
                return
                
            if verbose:
                click.echo("Query plan:")
                click.echo(json.dumps(query_plan, indent=2))
                
            # Generate database query with OpenAI
            click.echo("Generating database query...")
            openai_response = await openai_client.generate_query(
                query, 
                query_plan["context"]
            )
            
            if not openai_response.get("success", False):
                click.echo(f"Error: Query generation failed: {openai_response.get('error', 'Unknown error')}")
                return
                
            if verbose:
                click.echo("Generated query:")
                click.echo(json.dumps(openai_response, indent=2))
                
            # Refine the plan with OpenAI's response
            click.echo("Refining plan...")
            refined_plan = await planner.refine_plan(query_plan, openai_response)
            
            if not refined_plan.get("success", False):
                click.echo(f"Error: Plan refinement failed: {refined_plan.get('error', 'Unknown error')}")
                return
                
            # Optimize query if requested
            if optimize:
                click.echo("Optimizing query...")
                refined_plan = optimizer.optimize_query(refined_plan)
                
                if verbose:
                    click.echo("Optimized plan:")
                    click.echo(json.dumps(refined_plan, indent=2))
                    
            # Execute the query
            click.echo("Executing query...")
            result = await executor.execute_query(refined_plan)
            
            if not result.get("success", False):
                click.echo(f"Error: Query execution failed: {result.get('error', 'Unknown error')}")
                return
                
            # Display summary
            if "summary" in result:
                click.echo("\nSummary:")
                click.echo(result["summary"])
                
            # Display insights
            if "insights" in result:
                click.echo("\nInsights:")
                for insight in result["insights"]:
                    click.echo(f"- {insight}")
                    
            # Output the result
            if output:
                with open(output, 'w') as f:
                    if format == 'json':
                        json.dump(result, f, indent=2)
                    else:
                        f.write(result.get("formatted_data", "No data"))
                        
                click.echo(f"\nResult written to {output}")
            else:
                click.echo("\nResult:")
                if format == 'json':
                    if "data" in result:
                        if len(result["data"]) > 10:
                            # Truncate for display
                            display_data = result["data"][:10]
                            click.echo(f"Showing first 10 of {len(result['data'])} results:")
                            click.echo(json.dumps(display_data, indent=2))
                        else:
                            click.echo(json.dumps(result["data"], indent=2))
                    else:
                        click.echo(json.dumps(result, indent=2))
                else:
                    data = result.get("formatted_data", "No data")
                    click.echo(data)
                    
            # Show performance info
            if "execution_time" in result:
                click.echo(f"\nExecution time: {result['execution_time']:.2f} seconds")
                
            click.echo("\nQuery completed successfully")
                
        except Exception as e:
            click.echo(f"Error executing query: {str(e)}")

    @staticmethod
    async def interactive_mode() -> None:
        """Run the CLI in interactive mode."""
        try:
            click.echo("NL-DB-Query-System Interactive Mode")
            click.echo("Type 'exit' or 'quit' to exit")
            click.echo("Type 'help' for help")
            
            # Initialize planner
            click.echo("Initializing...")
            await planner.initialize()
            click.echo("Initialization complete")
            
            while True:
                # Get query from user
                query = click.prompt("\nEnter query", type=str)
                
                if query.lower() in ['exit', 'quit']:
                    click.echo("Exiting...")
                    break
                    
                if query.lower() == 'help':
                    CLI._show_help()
                    continue
                    
                # Get format preference
                format_options = ['json', 'table', 'csv']
                format_idx = click.prompt(
                    "Select output format",
                    type=click.Choice(['1', '2', '3']),
                    default='1',
                    show_choices=False,
                    prompt_suffix="\n1. JSON\n2. Table\n3. CSV\n> "
                )
                format = format_options[int(format_idx) - 1]
                
                # Get optimization preference
                optimize = click.confirm("Optimize query?", default=False)
                
                # Get verbosity preference
                verbose = click.confirm("Show verbose output?", default=False)
                
                # Execute query
                await CLI.run_query(query, format, optimize, verbose, None)
                
        except click.Abort:
            click.echo("\nOperation aborted")
        except Exception as e:
            click.echo(f"Error in interactive mode: {str(e)}")

    @staticmethod
    def _show_help() -> None:
        """Show help information."""
        help_text = """
        NL-DB-Query-System Interactive Mode Help
        
        Commands:
        - help: Show this help information
        - exit, quit: Exit the interactive mode
        
        Query Examples:
        - "Find all customers from New York who have placed more than 3 orders"
        - "What's the average order value by month for the last year?"
        - "Show me the total sales by product category"
        - "Count the number of events by user in the last 7 days"
        
        Tips:
        - Be specific about the information you want
        - Include timeframes if relevant (e.g., "last month", "last year")
        - Specify aggregations if needed (e.g., "average", "total", "count")
        - Include field names when possible
        """
        
        click.echo(textwrap.dedent(help_text))


@click.group()
def cli():
    """Command line interface for NL-DB-Query-System."""
    pass


@cli.command()
@click.option('--query', '-q', help='Natural language query to execute')
@click.option('--format', '-f', default='json', type=click.Choice(['json', 'table', 'csv']),
              help='Output format (json, table, csv)')
@click.option('--optimize/--no-optimize', default=False, help='Optimize query before execution')
@click.option('--verbose/--no-verbose', default=False, help='Show verbose output')
@click.option('--output', '-o', help='Output file (if not specified, prints to console)')
def query(query: str, format: str, optimize: bool, verbose: bool, output: Optional[str]):
    """Execute a natural language query."""
    asyncio.run(CLI.run_query(query, format, optimize, verbose, output))


@cli.command()
def interactive():
    """Run the CLI in interactive mode."""
    asyncio.run(CLI.interactive_mode())


@cli.command()
async def init():
    """Initialize the system."""
    # Initialize planner (which initializes schema manager)
    click.echo("Initializing...")
    init_success = await planner.initialize()
    
    if init_success:
        click.echo("System initialized successfully")
    else:
        click.echo("Failed to initialize system")


if __name__ == "__main__":
    cli()