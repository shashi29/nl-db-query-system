"""
Configuration settings for the NL-DB-Query-System.
Loads environment variables and provides configuration objects.
"""
import os
from pathlib import Path
from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent.parent


class OpenAISettings(BaseModel):
    """OpenAI API configuration settings."""
    api_key: str = Field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))
    model: str = Field(default_factory=lambda: os.getenv("OPENAI_MODEL", "gpt-4"))
    temperature: float = Field(default_factory=lambda: float(os.getenv("OPENAI_TEMPERATURE", "0.2")))
    max_tokens: int = Field(default_factory=lambda: int(os.getenv("OPENAI_MAX_TOKENS", "1000")))
    timeout: int = Field(default_factory=lambda: int(os.getenv("OPENAI_TIMEOUT", "30")))


class MongoDBSettings(BaseModel):
    """MongoDB connection settings."""
    uri: str = Field(default_factory=lambda: os.getenv("MONGODB_URI", "mongodb://localhost:27017"))
    database: str = Field(default_factory=lambda: os.getenv("MONGODB_DATABASE", "default"))
    collections: List[str] = Field(default_factory=lambda: os.getenv("MONGODB_COLLECTIONS", "").split(","))
    timeout_ms: int = Field(default_factory=lambda: int(os.getenv("MONGODB_TIMEOUT_MS", "5000")))


class ClickHouseSettings(BaseModel):
    """ClickHouse connection settings."""
    host: str = Field(default_factory=lambda: os.getenv("CLICKHOUSE_HOST", "localhost"))
    port: int = Field(default_factory=lambda: int(os.getenv("CLICKHOUSE_PORT", "9000")))
    user: str = Field(default_factory=lambda: os.getenv("CLICKHOUSE_USER", "default"))
    password: str = Field(default_factory=lambda: os.getenv("CLICKHOUSE_PASSWORD", ""))
    database: str = Field(default_factory=lambda: os.getenv("CLICKHOUSE_DATABASE", "default"))
    tables: List[str] = Field(default_factory=lambda: os.getenv("CLICKHOUSE_TABLES", "").split(","))
    timeout: int = Field(default_factory=lambda: int(os.getenv("CLICKHOUSE_TIMEOUT", "10")))


class CacheSettings(BaseModel):
    """Cache configuration settings."""
    enabled: bool = Field(default_factory=lambda: os.getenv("CACHE_ENABLED", "True").lower() == "true")
    redis_uri: str = Field(default_factory=lambda: os.getenv("REDIS_URI", "redis://localhost:6379/0"))
    ttl_seconds: int = Field(default_factory=lambda: int(os.getenv("CACHE_TTL_SECONDS", "3600")))


class APISettings(BaseModel):
    """API configuration settings."""
    host: str = Field(default_factory=lambda: os.getenv("API_HOST", "0.0.0.0"))
    port: int = Field(default_factory=lambda: int(os.getenv("API_PORT", "8000")))
    debug: bool = Field(default_factory=lambda: os.getenv("API_DEBUG", "False").lower() == "true")
    cors_origins: List[str] = Field(
        default_factory=lambda: os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")
    )


class SecuritySettings(BaseModel):
    """Security configuration settings."""
    query_timeout_seconds: int = Field(
        default_factory=lambda: int(os.getenv("QUERY_TIMEOUT_SECONDS", "30"))
    )
    max_query_size: int = Field(default_factory=lambda: int(os.getenv("MAX_QUERY_SIZE", "10000")))
    allowed_query_types: List[str] = Field(
        default_factory=lambda: os.getenv(
            "ALLOWED_QUERY_TYPES", "find,aggregate,count"
        ).split(",")
    )
    enable_write_operations: bool = Field(
        default_factory=lambda: os.getenv("ENABLE_WRITE_OPERATIONS", "False").lower() == "true"
    )


class Settings(BaseModel):
    """Global application settings."""
    environment: str = Field(default_factory=lambda: os.getenv("ENVIRONMENT", "development"))
    log_level: str = Field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    openai: OpenAISettings = Field(default_factory=OpenAISettings)
    mongodb: MongoDBSettings = Field(default_factory=MongoDBSettings)
    clickhouse: ClickHouseSettings = Field(default_factory=ClickHouseSettings)
    cache: CacheSettings = Field(default_factory=CacheSettings)
    api: APISettings = Field(default_factory=APISettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)


# Create global settings instance
settings = Settings()