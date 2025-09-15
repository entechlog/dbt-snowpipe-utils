# dbt-snowpipe-utils

Automated Snowflake Snowpipe management with schema inference and evolution support.

## Architecture

```mermaid
graph TD
    subgraph AWS [AWS - Pre-existing]
        S3[Cloud Storage<br/>source=sales/event_name=orders/] 
    
    subgraph SF [Snowflake - Pre-existing]
        STAGES[External Stages<br/>JSON/PARQUET/CSV]
        SCHEMAS[Schemas<br/>SALES, MARKETING]
    end
    
    subgraph PKG [dbt Package]
        CONFIG[Configuration<br/>reference__snowpipe_config]
        MACRO[create_snowpipes<br/>Main Macro]
    end
    
    subgraph CREATED [Created Objects]
        PIPES[Snowpipes<br/>Auto-ingest enabled]
        TABLES[Raw Tables<br/>VARIANT or Individual Columns]
    end
    
    S3 --> STAGES
    STAGES --> PIPES
    PIPES --> TABLES
    CONFIG --> MACRO
    MACRO --> PIPES
    MACRO --> TABLES
    
    classDef preexisting fill:#f9f9f9,stroke:#666
    classDef created fill:#e8f5e8,stroke:#2e7d32
    classDef package fill:#fff3e0,stroke:#e65100
    
    class STAGES,SCHEMAS preexisting
    class PIPES,TABLES created
    class CONFIG,MACRO package
```

## Prerequisites

This package creates Snowpipes and tables but requires existing Snowflake infrastructure:

- **Database and Schemas**: Target database with source schemas (e.g., `RAW_DB.SALES`)
- **External Stages**: Configured with storage integration
- **Warehouse and Roles**: Compute warehouse and admin role with PIPE privileges
- **Data Layout**: Files organized as `source={schema}/event_name={table}/[event_type={suffix}/]`

## Quick Setup

### 1. Install
```yaml
# packages.yml
packages:
  - git: "https://github.com/entechlog/dbt-snowpipe-utils"
    revision: main
```

```bash
dbt deps
```

### 2. Configure Environment
```bash
export SNOWPIPE_DATABASE="RAW_DB"
export SNOWPIPE_SCHEMA="UTIL"
export SNOWPIPE_WAREHOUSE="COMPUTE_WH"
export SNOWPIPE_ADMIN_ROLE="SYSADMIN"
export ENV_CODE="dev"
```

### 3. Configure Pipes
Edit `seeds/reference__snowpipe_config.csv`:
```csv
stage_name,source_name,event_name,event_type,cluster_key_transformation,cluster_key_type,cluster_key,file_pattern,enable_schema_inference,enable_schema_evolution,dev_enable_pipe_flag,dev_pause_pipe_flag,notes
PARQUET_STAGE,SALES,ORDERS,,DATE(timestamp),DATE,event_date,parquet,FALSE,FALSE,TRUE,FALSE,VARIANT mode
JSON_STAGE,SALES,CUSTOMERS,,DATE(created_at),DATE,event_date,json,TRUE,FALSE,TRUE,FALSE,Individual columns
JSON_STAGE,MARKETING,CAMPAIGNS,,DATE(timestamp),DATE,event_date,json,TRUE,TRUE,TRUE,FALSE,With schema evolution
```

Load configuration:
```bash
dbt seed
```

### 4. Run
```bash
# Test first (dry run)
dbt run-operation create_snowpipes --args '{"run_queries": false}'

# Execute
dbt run-operation create_snowpipes --args '{"run_queries": true}'
```

## Storage Modes

**VARIANT Mode** (Default)
- Single `DATA VARIANT` column stores all data as JSON
- Best for flexible, unstructured data
- Set: `enable_schema_inference: FALSE`

**Individual Columns Mode**
- Separate typed columns (user_id, timestamp, etc.)
- Uses Snowflake's `INFER_SCHEMA()` for automatic schema detection
- Better performance and compression
- Set: `enable_schema_inference: TRUE`

**Schema Evolution** (Optional)
- Automatically adds new columns when detected in source files
- Requires individual columns mode
- Set: `enable_schema_evolution: TRUE`

## Change Management

The package handles configuration changes safely:

- **Non-destructive**: Never drops tables or loses data
- **Smart detection**: Only applies necessary changes
- **Column renames**: Uses `RENAME COLUMN` to preserve data
- **Schema changes**: Additive operations only

## Configuration Fields

| Field | Description | Example |
|-------|-------------|---------|
| `source_name` | Schema name | `SALES` |
| `event_name` | Table base name | `ORDERS` |
| `event_type` | Table suffix (optional) | `DAILY` |
| `file_pattern` | File type | `json`, `parquet`, `csv` |
| `enable_schema_inference` | Use individual columns | `TRUE`/`FALSE` |
| `enable_schema_evolution` | Auto-add new columns | `TRUE`/`FALSE` |
| `cluster_key_transformation` | SQL for clustering | `DATE(timestamp)` |
| `cluster_key` | Cluster column name | `event_date` |
| `{env}_enable_pipe_flag` | Enable in environment | `TRUE`/`FALSE` |

## Validation

```sql
-- Check created objects
SHOW PIPES IN SCHEMA RAW_DB.SALES;
SHOW TABLES IN SCHEMA RAW_DB.SALES;

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('RAW_DB.SALES.ORDERS_PIPE');

-- View table structure
DESCRIBE TABLE RAW_DB.SALES.ORDERS;
```

## Troubleshooting

**Permission Error**: Verify role has PIPE creation privileges
```sql
SHOW GRANTS TO ROLE YOUR_ADMIN_ROLE;
```

**Stage Not Found**: Check stage exists and is accessible
```sql
LIST @RAW_DB.UTIL.YOUR_STAGE/source=sales/event_name=orders/;
```

**Pipe Status Issues**: Check pipe execution and error details
```sql
-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('RAW_DB.SALES.ORDERS_PIPE');

-- Check pipe history for errors
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    table_name => 'RAW_DB.SALES.ORDERS',
    start_time => dateadd(hours, -24, current_timestamp())
));
```

**Schema Inference Issues**: Test with sample files
```sql
SELECT * FROM TABLE(INFER_SCHEMA(
    LOCATION => '@YOUR_STAGE/path/', 
    FILE_FORMAT => 'JSON_FORMAT'
));
```