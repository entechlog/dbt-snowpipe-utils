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
-- STAGE FILE FORMAT DETECTION UTILITIES (using DESC STAGE)
-- =============================================================================
{% macro get_stage_file_format_info(stage_name) %}
    {% set query %}
        DESC STAGE {{ var("snowpipe_database") }}.{{ var("snowpipe_schema") }}.{{ stage_name }};
    {% endset %}
    {%- call statement('stage_desc_check', fetch_result=True) %}{{ query }}{%- endcall -%}
    {%- set result = load_result('stage_desc_check')['data'] -%}
    
    {% set stage_info = {'has_named_format': false, 'has_inline_format': false, 'format_name': '', 'inline_properties': {}} %}
    
    {% for row in result %}
        {% set parent_property = row[0] %}
        {% set property = row[1] %}
        {% set property_value = row[3] %}
        
        {% if parent_property == 'STAGE_FILE_FORMAT' and property == 'FORMAT_NAME' and property_value and property_value|trim != '' %}
            {% do stage_info.update({'has_named_format': true, 'format_name': property_value}) %}
        {% elif parent_property == 'STAGE_FILE_FORMAT' and property in ['TYPE', 'COMPRESSION', 'NULL_IF', 'TRIM_SPACE', 'BINARY_AS_TEXT', 'REPLACE_INVALID_CHARACTERS', 'USE_LOGICAL_TYPE'] %}
            {% do stage_info.update({'has_inline_format': true}) %}
            {% do stage_info.inline_properties.update({property: property_value}) %}
        {% endif %}
    {% endfor %}
    
    {{ return(stage_info) }}
{% endmacro %}

{% macro check_stage_has_inline_format(stage_name) %}
    {% set stage_info = get_stage_file_format_info(stage_name) %}
    {{ return(stage_info.has_inline_format and not stage_info.has_named_format) }}
{% endmacro %}

{% macro get_stage_file_format(stage_name) %}
    {% set stage_info = get_stage_file_format_info(stage_name) %}
    
    {% if stage_info.has_named_format %}
        {{ return(stage_info.format_name) }}
    {% elif stage_info.has_inline_format %}
        {# Build inline format string from properties #}
        {% set format_parts = [] %}
        {% for prop, value in stage_info.inline_properties.items() %}
            {% if prop == 'TYPE' %}
                {% do format_parts.append('TYPE = ' ~ value) %}
            {% elif prop == 'NULL_IF' and value %}
                {% do format_parts.append('NULL_IF = ' ~ value) %}
            {% elif prop == 'COMPRESSION' and value and value != 'AUTO' %}
                {% do format_parts.append('COMPRESSION = ' ~ value) %}
            {% elif prop == 'TRIM_SPACE' and value|string|lower == 'true' %}
                {% do format_parts.append('TRIM_SPACE = TRUE') %}
            {% elif prop == 'BINARY_AS_TEXT' and value|string|lower == 'true' %}
                {% do format_parts.append('BINARY_AS_TEXT = TRUE') %}
            {% elif prop == 'REPLACE_INVALID_CHARACTERS' and value|string|lower == 'true' %}
                {% do format_parts.append('REPLACE_INVALID_CHARACTERS = TRUE') %}
            {% elif prop == 'USE_LOGICAL_TYPE' and value|string|lower == 'true' %}
                {% do format_parts.append('USE_LOGICAL_TYPE = TRUE') %}
            {% endif %}
        {% endfor %}
        {{ return(format_parts | join(' ')) }}
    {% else %}
        {{ return('') }}
    {% endif %}
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

{% macro get_file_format_clause(file_pattern, stage_name) %}
    {# Extract just the stage name from fully qualified name #}
    {% set stage_name_only = stage_name.split('.')[-1] %}
    
    {# Check if stage exists before trying to describe it #}
    {% set stage_check_query %}
        SELECT COUNT(*) as stage_count
        FROM INFORMATION_SCHEMA.STAGES
        WHERE STAGE_CATALOG = UPPER('{{ var("snowpipe_database") }}')
        AND STAGE_SCHEMA = UPPER('{{ var("snowpipe_schema") }}')
        AND STAGE_NAME = UPPER('{{ stage_name_only }}')
    {% endset %}
    {%- call statement('stage_existence_check_' ~ stage_name_only, fetch_result=True) %}{{ stage_check_query }}{%- endcall -%}
    {%- set stage_result = load_result('stage_existence_check_' ~ stage_name_only)['data'] -%}
    {% set stage_exists = (stage_result[0][0] > 0) if stage_result|length > 0 else false %}
    
    {% if not stage_exists %}
        {# Stage doesn't exist, use default format based on pattern #}
        {% if file_pattern|lower == 'json' %}
            {{ return('FILE_FORMAT = (TYPE = \'JSON\')') }}
        {% elif file_pattern|lower == 'csv' %}
            {{ return('FILE_FORMAT = (TYPE = \'CSV\')') }}
        {% else %}
            {{ return('FILE_FORMAT = (TYPE = \'PARQUET\')') }}
        {% endif %}
    {% endif %}
    
    {% set has_inline_format = check_stage_has_inline_format(stage_name_only) %}
    
    {% if has_inline_format %}
        {# For stages with inline format, use simple TYPE based on file_pattern #}
        {% if file_pattern|lower == 'json' %}
            {{ return('FILE_FORMAT = (TYPE = \'JSON\')') }}
        {% elif file_pattern|lower == 'csv' %}
            {{ return('FILE_FORMAT = (TYPE = \'CSV\')') }}
        {% else %}
            {{ return('FILE_FORMAT = (TYPE = \'PARQUET\')') }}
        {% endif %}
    {% else %}
        {# Check if stage has a named format #}
        {% set stage_info = get_stage_file_format_info(stage_name_only) %}
        {% if stage_info.has_named_format %}
            {# Use the stage's named format #}
            {{ return('FILE_FORMAT = (FORMAT_NAME = \'' ~ stage_info.format_name ~ '\')') }}
        {% else %}
            {# Use default format based on pattern #}
            {% if file_pattern|lower == 'json' %}
                {{ return('FILE_FORMAT = (TYPE = \'JSON\')') }}
            {% elif file_pattern|lower == 'csv' %}
                {{ return('FILE_FORMAT = (TYPE = \'CSV\')') }}
            {% else %}
                {{ return('FILE_FORMAT = (TYPE = \'PARQUET\')') }}
            {% endif %}
        {% endif %}
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
            GRANT MONITOR ON PIPE {{ pipe_name }} TO ROLE "{{ role_name }}";
            GRANT SELECT ON TABLE {{ table_name }} TO ROLE "{{ role_name }}";
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