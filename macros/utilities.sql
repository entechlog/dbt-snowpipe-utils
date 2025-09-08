-- =============================================================================
-- EXISTENCE CHECK UTILITIES
-- =============================================================================
{% macro check_pipe_exists(schema_name, pipe_name) %}
    {% set query %}
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.PIPES
        WHERE PIPE_CATALOG = UPPER('{{ var("snowpipe_database") }}')
        AND PIPE_SCHEMA = UPPER('{{ schema_name }}')
        AND PIPE_NAME = UPPER('{{ pipe_name }}');
    {% endset %}
    {%- call statement('pipe_check', fetch_result=True) %}{{ query }}{%- endcall -%}
    {% set result = load_result('pipe_check')['data'][0][0] > 0 %}
    {{ return(result) }}
{% endmacro %}

{% macro check_table_exists(schema_name, table_name, debug_mode=False) %}
    {% if debug_mode %}
        {# Log current session context #}
        {% set session_query %}
            SELECT 
                CURRENT_USER() as current_user,
                CURRENT_ROLE() as current_role,
                CURRENT_DATABASE() as current_database,
                CURRENT_SCHEMA() as current_schema,
                CURRENT_WAREHOUSE() as current_warehouse
        {% endset %}
        {%- call statement('session_context_table', fetch_result=True) %}{{ session_query }}{%- endcall -%}
        {%- set session_result = load_result('session_context_table')['data'] -%}
        {% if session_result|length > 0 %}
            {{ log("Session Context (Table Check) - User: " ~ session_result[0][0] ~ ", Role: " ~ session_result[0][1] ~ ", DB: " ~ session_result[0][2] ~ ", Schema: " ~ session_result[0][3] ~ ", WH: " ~ session_result[0][4], info=True) }}
        {% endif %}
    {% endif %}
    
    {% set query %}
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_CATALOG = UPPER('{{ var("snowpipe_database") }}')
        AND TABLE_SCHEMA = UPPER('{{ schema_name }}')
        AND TABLE_NAME = UPPER('{{ table_name }}');
    {% endset %}
    {% if debug_mode %}
        {{ log("Checking table existence: " ~ table_name ~ " in " ~ var("snowpipe_database") ~ "." ~ schema_name, info=True) }}
    {% endif %}
    {%- call statement('table_check', fetch_result=True) %}{{ query }}{%- endcall -%}
    {% set result = load_result('table_check')['data'][0][0] > 0 %}
    {% if debug_mode %}
        {{ log("Table " ~ table_name ~ " exists: " ~ result, info=True) }}
    {% endif %}
    {{ return(result) }}
{% endmacro %}

-- =============================================================================
-- CURRENT STATE RETRIEVAL UTILITIES
-- =============================================================================
{% macro get_pipe_stage(schema_name, pipe_name) %}
    {% set query %}
        SELECT 
            CASE 
                WHEN definition LIKE '%@%/%' THEN
                    UPPER(TRIM(REGEXP_SUBSTR(definition, '@([^/\\s]+)', 1, 1, 'e', 1)))
                ELSE
                    UPPER(TRIM(REGEXP_SUBSTR(definition, '@([^\\s)]+)', 1, 1, 'e', 1)))
            END as current_stage
        FROM INFORMATION_SCHEMA.PIPES
        WHERE PIPE_CATALOG = UPPER('{{ var("snowpipe_database") }}')
        AND PIPE_SCHEMA = UPPER('{{ schema_name }}')
        AND PIPE_NAME = UPPER('{{ pipe_name }}');
    {% endset %}
    {%- call statement('stage_check', fetch_result=True) %}{{ query }}{%- endcall -%}
    {%- set result = load_result('stage_check')['data'] -%}
    {% set stage_result = result[0][0] if result|length > 0 and result[0][0] else '' %}
    {{ return(stage_result) }}
{% endmacro %}

{% macro get_pipe_file_pattern(schema_name, pipe_name) %}
    {% set query %}
        SELECT COALESCE(
            REGEXP_SUBSTR(definition, 'PATTERN\\s*=\\s*''([^'']+)''', 1, 1, 'ie', 1),
            ''
        ) as current_pattern
        FROM {{ var("snowpipe_database") }}.INFORMATION_SCHEMA.PIPES
        WHERE PIPE_CATALOG = UPPER('{{ var("snowpipe_database") }}')
        AND PIPE_SCHEMA = UPPER('{{ schema_name }}')
        AND PIPE_NAME = UPPER('{{ pipe_name }}');
    {% endset %}
    {%- call statement('pattern_check', fetch_result=True) %}{{ query }}{%- endcall -%}
    {%- set result = load_result('pattern_check')['data'] -%}
    {% set pattern_result = result[0][0] if result|length > 0 and result[0][0] else '' %}
    {{ return(pattern_result) }}
{% endmacro %}

{% macro get_pipe_file_format(schema_name, pipe_name) %}
    {% set query %}
        SELECT COALESCE(
            UPPER(TRIM(REGEXP_SUBSTR(definition, 'FORMAT_NAME\\s*=\\s*''([^'']+)''', 1, 1, 'ie', 1))),
            UPPER(TRIM(REGEXP_SUBSTR(definition, 'FORMAT_NAME\\s*=\\s*([^\\s)]+)', 1, 1, 'ie', 1))),
            ''
        ) as current_file_format
        FROM {{ var("snowpipe_database") }}.INFORMATION_SCHEMA.PIPES
        WHERE PIPE_CATALOG = UPPER('{{ var("snowpipe_database") }}')
        AND PIPE_SCHEMA = UPPER('{{ schema_name }}')
        AND PIPE_NAME = UPPER('{{ pipe_name }}');
    {% endset %}
    {%- call statement('format_check', fetch_result=True) %}{{ query }}{%- endcall -%}
    {%- set result = load_result('format_check')['data'] -%}
    {% set format_result = result[0][0] if result|length > 0 and result[0][0] else '' %}
    {{ return(format_result) }}
{% endmacro %}

{% macro get_table_cluster_key(schema_name, table_name) %}
    {% set query %}
        SELECT COALESCE(CLUSTERING_KEY, '') FROM {{ var("snowpipe_database") }}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_CATALOG = UPPER('{{ var("snowpipe_database") }}')
        AND TABLE_SCHEMA = UPPER('{{ schema_name }}')
        AND TABLE_NAME = UPPER('{{ table_name }}');
    {% endset %}
    {%- call statement('cluster_check', fetch_result=True) %}{{ query }}{%- endcall -%}
    {%- set result = load_result('cluster_check')['data'] -%}
    {% set cluster_result = result[0][0] if result|length > 0 else '' %}
    {{ return(cluster_result) }}
{% endmacro %}

-- =============================================================================
-- PATH AND FORMAT UTILITIES
-- =============================================================================
{% macro get_s3_dir_name(source_name, event_name, event_type) %}
    {% if event_type and event_type|trim != "" %}
        {% set s3_dir = 'source=' ~ source_name ~ '/event_name=' ~ event_name ~ '/event_type=' ~ event_type ~ '/' %}
    {% else %}
        {% set s3_dir = 'source=' ~ source_name ~ '/event_name=' ~ event_name ~ '/' %}
    {% endif %}
    {{ return(s3_dir|lower) }}
{% endmacro %}

{% macro get_file_format_name(file_pattern) %}
    {% if file_pattern|lower == 'json' %}
        {% set format_name = var("snowpipe_json_file_format") %}
    {% elif file_pattern|lower == 'csv' %}
        {% set format_name = var("snowpipe_csv_file_format") %}
    {% else %}
        {% set format_name = var("snowpipe_parquet_file_format") %}
    {% endif %}
    
    {# Return fully qualified format name #}
    {% if '.' in format_name %}
        {{ return(format_name|upper) }}
    {% else %}
        {# If not fully qualified, prepend database.schema #}
        {{ return(var("snowpipe_database") ~ "." ~ var("snowpipe_schema") ~ "." ~ format_name|upper) }}
    {% endif %}
{% endmacro %}

{% macro get_file_pattern(file_pattern) %}
    {% if file_pattern|lower == 'json' %}
        {{ return('.*.json') }}
    {% elif file_pattern|lower == 'csv' %}
        {{ return('.*.csv') }}
    {% else %}
        {{ return('.*.parquet') }}
    {% endif %}
{% endmacro %}

-- =============================================================================
-- ENVIRONMENT UTILITIES
-- =============================================================================
{% macro get_environment_name() %}
    {% set env_code = env_var('ENV_CODE', 'dev')|lower %}
    {% if env_code in ['dev', 'development'] %}
        {{ return(['development', 'dev']) }}
    {% elif env_code in ['stg', 'staging'] %}
        {{ return(['staging', 'stg']) }}
    {% elif env_code in ['prd', 'prod', 'production'] %}
        {{ return(['production', 'prd']) }}
    {% else %}
        {{ return([env_code, env_code]) }}
    {% endif %}
{% endmacro %}

-- =============================================================================
-- VALIDATION UTILITIES
-- =============================================================================
{% macro validate_config(config) %}
    {% set errors = [] %}
    
    {# Updated indices for new config structure #}
    {% set source_name = config[1] %}
    {% set event_name = config[2] %}
    {% set file_pattern = config[7] %}
    {% set enable_schema_inference = config[8] %}
    {% set enable_schema_evolution = config[9] %}
    
    {% if not source_name or source_name|trim == "" %}
        {% do errors.append("source_name is required") %}
    {% endif %}
    
    {% if not event_name or event_name|trim == "" %}
        {% do errors.append("event_name is required") %}
    {% endif %}
    
    {% if not file_pattern or file_pattern|trim == "" %}
        {% do errors.append("file_pattern is required") %}
    {% endif %}
    
    {% if file_pattern|lower not in ['json', 'csv', 'parquet'] %}
        {% do errors.append("file_pattern must be json, csv, or parquet") %}
    {% endif %}
    
    {# Schema evolution validation #}
    {% if enable_schema_evolution and not enable_schema_inference %}
        {% do errors.append("enable_schema_evolution requires enable_schema_inference to be true") %}
    {% endif %}
    
    {{ return(errors) }}
{% endmacro %}

-- =============================================================================
-- PERMISSIONS AND MANAGEMENT UTILITIES
-- =============================================================================
{% macro set_permissions_sql(pipe_name, table_name, pause_pipe_flag) %}
    
    -- Begin permissions and management section
    
    -- Grant permissions to configurable roles
    {% set roles_string = var('snowpipe_monitor_roles', '') %}
    
    {% if roles_string and roles_string|trim != "" %}
        {% set roles_list = roles_string.split(',') %}
        {% for role in roles_list %}
            {% set role_name = role.strip() %}
            GRANT MONITOR ON PIPE {{ pipe_name }} TO ROLE {{ role_name }};
            GRANT SELECT ON TABLE {{ table_name }} TO ROLE {{ role_name }};
        {% endfor %}
    {% endif %}
    
    -- Set pause state based on configuration
    {% if pause_pipe_flag %}
        ALTER PIPE {{ pipe_name }} SET PIPE_EXECUTION_PAUSED = TRUE;
    {% else %}
        SELECT SYSTEM$PIPE_FORCE_RESUME('{{ pipe_name }}');
    {% endif %}
    
    -- End of permissions section
    
{% endmacro %}