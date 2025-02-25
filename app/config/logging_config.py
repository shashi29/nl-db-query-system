"""
Logging configuration for the NL-DB-Query-System.
Sets up loguru logger with appropriate formatting and sinks.
"""
import sys
import os
from pathlib import Path
from loguru import logger

from .settings import settings, BASE_DIR


def setup_logging():
    """
    Configure the logger with appropriate settings.
    - Configures console output
    - Sets up file logging
    - Configures log level based on settings
    """
    # Create logs directory if it doesn't exist
    logs_dir = BASE_DIR / "logs"
    logs_dir.mkdir(exist_ok=True)

    # Clear any existing logger configurations
    logger.remove()

    # Determine log level from settings
    log_level = settings.log_level.upper()

    # Format for console logging
    console_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )

    # Format for file logging
    file_format = (
        "{time:YYYY-MM-DD HH:mm:ss} | "
        "{level: <8} | "
        "{name}:{function}:{line} - "
        "{message}"
    )

    # Add console logger
    logger.add(
        sys.stderr,
        format=console_format,
        level=log_level,
        colorize=True,
    )

    # Add file logger for normal logs
    logger.add(
        logs_dir / "nl_db_query.log",
        format=file_format,
        level=log_level,
        rotation="10 MB",
        compression="zip",
        retention="1 month",
    )

    # Add file logger specifically for errors
    logger.add(
        logs_dir / "error.log",
        format=file_format,
        level="ERROR",
        rotation="10 MB",
        compression="zip",
        retention="1 month",
    )

    # Log startup information
    logger.info(f"Logging initialized with level: {log_level}")
    logger.info(f"Environment: {settings.environment}")
    
    return logger


# Create logger instance
logger = setup_logging()