# Natural Language to Database Query System

A system that translates natural language queries into database queries for MongoDB and ClickHouse databases.

## Features

- Natural language to MongoDB query conversion
- Natural language to ClickHouse SQL conversion
- Federated queries across multiple databases
- Performance optimization and analysis
- API and CLI interfaces
- User feedback collection and analysis

## Architecture

The system follows a four-phase architecture:

1. **Planning Phase**: Analyzes the natural language query and determines the appropriate data source(s)
2. **Reasoning Phase**: Uses OpenAI to convert the natural language query to database-specific queries
3. **Execution Phase**: Executes the generated queries against the target databases
4. **Reflection Phase**: Analyzes performance, collects feedback, and optimizes future queries

## Installation

### Prerequisites

- Python 3.8 or higher
- MongoDB server (for MongoDB queries)
- ClickHouse server (for ClickHouse queries)
- OpenAI API key

### Install from Source

```bash
# Clone the repository
git clone https://github.com/yourusername/nl-db-query-system.git
cd nl-db-query-system

# Install the package
pip install -e .
```

### Configuration

Create a `.env` file in the root directory with the following settings:

```bash
# Environment
ENVIRONMENT=development
LOG_LEVEL=INFO

# OpenAI API
OPENAI_API_KEY=your_openai_api_key_here
OPENAI_MODEL=gpt-4
OPENAI_TEMPERATURE=0.2

# MongoDB
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=your_database

# ClickHouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DATABASE=your_database

# API Settings
API_HOST=0.0.0.0
API_PORT=8000
```

## Usage

### CLI

```bash
# Initialize the system
nldbq init

# Execute a query (one-shot mode)
nldbq query --query "Find all customers from New York who have spent more than $1000"

# Interactive mode
nldbq interactive
```

### API

```bash
# Start the API server
nldbq api

# Or with uvicorn directly
uvicorn app.interface.api:app --host 0.0.0.0 --port 8000
```

Then send requests to the API:

```bash
curl -X POST "http://localhost:8000/api/query" \
  -H "Content-Type: application/json" \
  -d '{"query": "Find all customers from New York who have spent more than $1000"}'
```

## Examples

### MongoDB Query

Natural language:
```
Find all customers from New York who have spent more than $1000
```

Generated MongoDB query:
```javascript
db.customers.find({
  "address.state": "New York",
  "total_spent": { $gt: 1000 }
})
```

### ClickHouse Query

Natural language:
```
Calculate the hourly page view count for the last 24 hours
```

Generated ClickHouse query:
```sql
SELECT 
  toStartOfHour(event_time) AS hour, 
  count() AS views 
FROM page_views 
WHERE event_time >= now() - INTERVAL 1 DAY 
GROUP BY hour 
ORDER BY hour
```

### Federated Query

Natural language:
```
Find customers who viewed product X on the website but haven't purchased it yet
```

Generated Federated Query Plan:
```json
{
  "steps": [
    {
      "step_type": "query",
      "data_source": "clickhouse",
      "clickhouse_query": {
        "query": "SELECT user_id FROM page_views WHERE product_id = 'X' GROUP BY user_id"
      },
      "output_var": "viewed_users"
    },
    {
      "step_type": "query",
      "data_source": "mongodb",
      "mongodb_query": {
        "collection": "orders",
        "operation": "find",
        "filter": { "product_id": "X" }
      },
      "output_var": "purchased_users"
    },
    {
      "step_type": "filter",
      "data_source": "memory",
      "inputs": ["viewed_users", "purchased_users"],
      "operation": "filter",
      "parameters": {
        "condition": "viewed_users.user_id not in purchased_users.user_id"
      },
      "output_var": "potential_customers"
    },
    {
      "step_type": "final",
      "data_source": "memory",
      "inputs": ["potential_customers"],
      "output_var": "result"
    }
  ]
}
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## To Run

```
uvicorn app.interface.api:app --host 0.0.0.0 --port 8000 --reload
```