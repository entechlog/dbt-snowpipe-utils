{% macro create_single_snowpipe(config, run_queries=False, debug_mode=False) %}
    {# Extract configuration values - updated indices for new columns #}
    {% set stage_base = config[0] %}
    {% set source_name = config[1] %}
    {% set event_name = config[2] %}
    {% set event_type = config[3] %}
    {% set cluster_key_transformation = config[4] %}
    {% set cluster_key_type = config[5] %}
    {% set cluster_key = config[6] %}
    {% set file_pattern = config[7] %}
    {% set enable_schema_inference = config[8] %}
    {% set enable_schema_evolution = config[9] %}
    {% set pause_pipe_flag = config[10] %}
    
    {# Determine table mode based on schema flags #}
    {% set use_individual_columns = (enable_schema_inference or enable_schema_evolution) %}
    
    {# Validate configuration #}
    {% set validation_errors = validate_config(config) %}
    {% if validation_errors|length > 0 %}
        {% if debug_mode %}
            {{ log("VALIDATION FAILED for " ~ event_name ~ ": " ~ validation_errors | join(", "), info=True) }}
        {% endif %}
        {{ return({'success': false, 'action': 'validation_failed'}) }}
    {% endif %}
    
    {# Build names and paths #}
    {% set pipe_name = (event_name ~ ('_' ~ event_type if event_type else '') ~ '_PIPE') | upper %}
    {% set table_name = (event_name ~ ('_' ~ event_type if event_type else '')) | upper %}
    {% set s3_dir_name = get_s3_dir_name(source_name, event_name, event_type) %}
    
    {# Build fully qualified names #}
    {% set full_pipe_name = var("snowpipe_database") ~ "." ~ source_name ~ "." ~ pipe_name %}
    {% set full_table_name = var("snowpipe_database") ~ "." ~ source_name ~ "." ~ table_name %}
    
    {# Determine stage name #}
    {% if stage_base and stage_base|trim != "" %}
        {% set stage_name = stage_base %}
    {% else %}
        {% if file_pattern|lower == 'json' %}
            {% set stage_name = var("snowpipe_json_stage") %}
        {% elif file_pattern|lower == 'csv' %}
            {% set stage_name = var("snowpipe_csv_stage") %}
        {% else %}
            {% set stage_name = var("snowpipe_parquet_stage") %}
        {% endif %}
    {% endif %}
    {% set full_stage_name = var("snowpipe_database") ~ "." ~ var("snowpipe_schema") ~ "." ~ stage_name %}
    
    {# Debug mode logging #}
    {% if debug_mode %}
        {{ log("Processing " ~ full_pipe_name ~ " (Source: " ~ source_name ~ ", Pattern: " ~ file_pattern ~ ")", info=True) }}
        
        {# Log initial session context #}
        {% set session_query %}
            SELECT 
                CURRENT_USER() as current_user,
                CURRENT_ROLE() as current_role,
                CURRENT_DATABASE() as current_database,
                CURRENT_SCHEMA() as current_schema,
                CURRENT_WAREHOUSE() as current_warehouse
        {% endset %}
        {%- call statement('initial_session_context', fetch_result=True) %}{{ session_query }}{%- endcall -%}
        {%- set session_result = load_result('initial_session_context')['data'] -%}
        {% if session_result|length > 0 %}
            {{ log("Initial Session Context - User: " ~ session_result[0][0] ~ ", Role: " ~ session_result[0][1] ~ ", DB: " ~ session_result[0][2] ~ ", Schema: " ~ session_result[0][3] ~ ", WH: " ~ session_result[0][4], info=True) }}
        {% endif %}
    {% endif %}
    
    {# Check current state - but first switch to target database context #}
    {% if execute %}
        {% set switch_context_sql %}
            USE DATABASE {{ var("snowpipe_database") }};
        {% endset %}
        {% do run_query(switch_context_sql) %}
        {% if debug_mode %}
            {{ log("Switched to database: " ~ var("snowpipe_database"), info=True) }}
        {% endif %}
    {% endif %}
    
    {% set pipe_exists = check_pipe_exists(source_name, pipe_name) %}
    {% set table_exists = check_table_exists(source_name, table_name, debug_mode) %}
    
    {# Get current states for summary #}
    {% set current_cluster = '' %}
    {% set current_stage = '' %}
    {% set current_pattern = '' %}
    {% set current_format = '' %}
    {% set current_paused = false %}
    
    {% if table_exists %}
        {% set current_cluster = get_table_cluster_key(source_name, table_name) %}
    {% endif %}
    
    {% if pipe_exists %}
        {% set current_stage = get_pipe_stage(source_name, pipe_name) %}
        {% set current_pattern = get_pipe_file_pattern(source_name, pipe_name) %}
        {% set current_format = get_pipe_file_format(source_name, pipe_name) %}
        {% set current_paused = get_pipe_pause_state(source_name, pipe_name) %}
    {% endif %}
    
    {# Enhanced change detection with proper logic flow #}
    {% set requires_pipe_recreation = false %}
    {% set requires_table_creation = false %}
    {% set change_reasons = [] %}
    {% set stage_change_flag = false %}
    {% set cluster_change_flag = false %}
    {% set cluster_transformation_change_flag = false %}
    {% set file_pattern_change_flag = false %}
    {% set file_format_change_flag = false %}
    {% set table_change_flag = false %}
    {% set pause_state_change_flag = false %}
    
    {# Check for missing resources first #}
    {% if not table_exists %}
        {% set requires_table_creation = true %}
        {% set table_change_flag = true %}
        {% do change_reasons.append("Table missing") %}
        {% if debug_mode %}
            {{ log("Table does not exist - requires creation", info=True) }}
        {% endif %}
    {% endif %}
    
    {% if not pipe_exists %}
        {% set requires_pipe_recreation = true %}
        {% do change_reasons.append("Pipe missing") %}
        {% if debug_mode %}
            {{ log("Pipe does not exist - requires creation", info=True) }}
        {% endif %}
    {% endif %}
    
    {# Only check for changes if both resources exist #}
    {% if table_exists and pipe_exists %}
        {% if debug_mode %}
            {{ log("Both table and pipe exist - checking for configuration changes", info=True) }}
        {% endif %}
        
        {# Check if table has required metadata columns #}
        {% set metadata_check_query %}
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_CATALOG = UPPER('{{ var("snowpipe_database") }}')
            AND TABLE_SCHEMA = UPPER('{{ source_name }}')
            AND TABLE_NAME = UPPER('{{ table_name }}')
            AND COLUMN_NAME IN ('FILE_NAME', 'FILE_ROW_NUMBER', 'FILE_CONTENT_KEY', 'FILE_LAST_MODIFIED_TIMESTAMP', 'LOADED_TIMESTAMP')
        {% endset %}
        {%- call statement('metadata_columns_check', fetch_result=True) %}{{ metadata_check_query }}{%- endcall -%}
        {%- set metadata_result = load_result('metadata_columns_check')['data'] -%}
        {% set metadata_columns_count = metadata_result[0][0] if metadata_result|length > 0 else 0 %}
        
        {% if metadata_columns_count < 5 %}
            {% set requires_table_creation = true %}
            {% set table_change_flag = true %}
            {% do change_reasons.append("Missing metadata columns") %}
            {% if debug_mode %}
                {{ log("Table exists but missing metadata columns (" ~ metadata_columns_count ~ "/5 found)", info=True) }}
            {% endif %}
        {% endif %}
        
        {# Check for pipe configuration changes #}
        {% set expected_stage = full_stage_name|upper %}
        {% if current_stage != expected_stage %}
            {% set requires_pipe_recreation = true %}
            {% set stage_change_flag = true %}
            {% do change_reasons.append("Stage changed") %}
            {% if debug_mode %}
                {{ log("Stage change detected: '" ~ current_stage ~ "' -> '" ~ expected_stage ~ "'", info=True) }}
            {% endif %}
        {% endif %}
        
        {% set expected_pattern = get_file_pattern(file_pattern) %}
        {% if current_pattern != expected_pattern %}
            {% set requires_pipe_recreation = true %}
            {% set file_pattern_change_flag = true %}
            {% do change_reasons.append("File pattern changed") %}
            {% if debug_mode %}
                {{ log("File pattern change detected: '" ~ current_pattern ~ "' -> '" ~ expected_pattern ~ "'", info=True) }}
            {% endif %}
        {% endif %}
        
        {# File format changes detection #}
        {% set expected_format = get_file_format_clause(file_pattern, stage_name) %}
        {% if current_format and current_format != expected_format %}
            {% set requires_pipe_recreation = true %}
            {% set file_format_change_flag = true %}
            {% do change_reasons.append("File format changed") %}
            {% if debug_mode %}
                {{ log("File format change detected: '" ~ current_format ~ "' -> '" ~ expected_format ~ "'", info=True) }}
            {% endif %}
        {% endif %}
        
        {# Check clustering changes #}
        {% set expected_cluster = ('LINEAR("' ~ cluster_key|upper ~ '")') if (cluster_key and cluster_key|trim != "") else '' %}
        
        {% if debug_mode %}
            {{ log("Cluster comparison:", info=True) }}
            {{ log("  Current:  '" ~ current_cluster|trim ~ "'", info=True) }}
            {{ log("  Expected: '" ~ expected_cluster|trim ~ "'", info=True) }}
            {{ log("  Match: " ~ (current_cluster|trim|upper == expected_cluster|trim|upper), info=True) }}
        {% endif %}
        
        {% if current_cluster|trim|upper != expected_cluster|trim|upper %}
            {% set requires_table_creation = true %}
            {% set cluster_change_flag = true %}
            {% do change_reasons.append("Clustering changed") %}
            {% if debug_mode %}
                {{ log("Clustering change detected - will recreate table", info=True) }}
            {% endif %}
        {% endif %}
        
        {# Check cluster transformation changes #}
        {% if cluster_key and cluster_key|trim != "" and cluster_key_transformation and cluster_key_transformation|trim != "" %}
            {% set current_transformation = get_pipe_cluster_transformation(source_name, pipe_name, cluster_key) %}
            {% if current_transformation != cluster_key_transformation %}
                {% set requires_pipe_recreation = true %}
                {% set cluster_transformation_change_flag = true %}
                {% do change_reasons.append("Cluster transformation changed") %}
                {% if debug_mode %}
                    {{ log("Cluster transformation change detected: '" ~ current_transformation ~ "' -> '" ~ cluster_key_transformation ~ "'", info=True) }}
                {% endif %}
            {% endif %}
        {% endif %}
        
        {# Check pause state changes - this should not trigger pipe recreation #}
        {% if current_paused != pause_pipe_flag %}
            {% set pause_state_change_flag = true %}
            {% do change_reasons.append("Pause state changed") %}
            {% if debug_mode %}
                {{ log("Pause state change detected: " ~ current_paused ~ " -> " ~ pause_pipe_flag, info=True) }}
            {% endif %}
        {% endif %}
    {% endif %}
    
    {# Log all detected changes #}
    {% if debug_mode %}
        {{ log("Change Detection Summary:", info=True) }}
        {{ log("  stage_change_flag: " ~ stage_change_flag, info=True) }}
        {{ log("  cluster_change_flag: " ~ cluster_change_flag, info=True) }}
        {{ log("  cluster_transformation_change_flag: " ~ cluster_transformation_change_flag, info=True) }}
        {{ log("  file_pattern_change_flag: " ~ file_pattern_change_flag, info=True) }}
        {{ log("  file_format_change_flag: " ~ file_format_change_flag, info=True) }}
        {{ log("  table_change_flag: " ~ table_change_flag, info=True) }}
        {{ log("  pause_state_change_flag: " ~ pause_state_change_flag, info=True) }}
        {{ log("  requires_table_creation: " ~ requires_table_creation, info=True) }}
        {{ log("  requires_pipe_recreation: " ~ requires_pipe_recreation, info=True) }}
        {{ log("  Total change reasons: " ~ change_reasons | join(", "), info=True) }}
    {% endif %}
    
    {# FIXED: Determine action type based on actual changes needed #}
    {% set action_type = 'skipped' %}
    {% set needs_ddl_execution = false %}
    
    {% if not table_exists and not pipe_exists %}
        {% set action_type = 'created' %}
        {% set needs_ddl_execution = true %}
    {% elif not table_exists or not pipe_exists %}
        {% set action_type = 'created' %}
        {% set needs_ddl_execution = true %}
    {% elif requires_table_creation or requires_pipe_recreation %}
        {% set action_type = 'updated' %}
        {% set needs_ddl_execution = true %}
    {% elif pause_state_change_flag %}
        {% set action_type = 'updated' %}
        {% set needs_ddl_execution = true %}
    {% else %}
        {% set action_type = 'skipped' %}
        {% set needs_ddl_execution = false %}
    {% endif %}
    
    {% if debug_mode %}
        {{ log("Final decision:", info=True) }}
        {{ log("  action_type: " ~ action_type, info=True) }}
        {{ log("  needs_ddl_execution: " ~ needs_ddl_execution, info=True) }}
    {% endif %}
    
    {# Generate SQL - only if changes are needed #}
    {% set creation_sql %}
        USE ROLE {{ var("snowpipe_admin_role") }};
        USE DATABASE {{ var("snowpipe_database") }};
        USE SCHEMA {{ source_name }};
        USE WAREHOUSE {{ var("snowpipe_warehouse") }};
        
        {% if needs_ddl_execution %}
            -- {{ full_pipe_name }}: {{ change_reasons | join(', ') if change_reasons|length > 0 else 'Pause state change' }}
            
            {% if requires_table_creation %}
                {{ create_table_sql(
                    table_name, 
                    use_individual_columns,
                    enable_schema_inference,
                    enable_schema_evolution,
                    cluster_key, 
                    cluster_key_type, 
                    file_pattern, 
                    full_stage_name, 
                    s3_dir_name,
                    source_name,
                    event_name,
                    cluster_key_transformation,
                    pipe_name,
                    debug_mode
                ) }}
            {% endif %}
            
            {% if requires_pipe_recreation %}
                {{ create_pipe_sql(
                    pipe_name, 
                    table_name, 
                    use_individual_columns, 
                    cluster_key_transformation, 
                    cluster_key_type,
                    cluster_key, 
                    file_pattern, 
                    full_stage_name, 
                    s3_dir_name, 
                    event_name,
                    source_name
                ) }}
            {% endif %}
            
            {{ set_permissions_sql(full_pipe_name, full_table_name, pause_pipe_flag) }}
            
        {% else %}
            -- {{ full_pipe_name }}: No changes required
            {% if pause_state_change_flag %}
                {% if pause_pipe_flag %}
                    ALTER PIPE IF EXISTS {{ full_pipe_name }} SET PIPE_EXECUTION_PAUSED = TRUE;
                {% else %}
                    SELECT SYSTEM$PIPE_FORCE_RESUME('{{ full_pipe_name }}');
                {% endif %}
            {% else %}
                SELECT 'No changes required for {{ full_pipe_name }}' AS status;
            {% endif %}
        {% endif %}
    {% endset %}
    
    {# Execute if requested #}
    {% if run_queries %}
        {% if needs_ddl_execution %}
            {% if debug_mode %}
                {{ log("Executing changes for " ~ full_pipe_name ~ ": " ~ change_reasons | join(", "), info=True) }}
            {% endif %}
            {% do run_query(creation_sql) %}
        {% else %}
            {% if debug_mode %}
                {{ log("No DDL changes needed for " ~ full_pipe_name, info=True) }}
            {% endif %}
            {# Still execute for pause state changes #}
            {% if pause_state_change_flag %}
                {% do run_query(creation_sql) %}
            {% endif %}
        {% endif %}
        
        {% if debug_mode %}
            {{ log("Completed " ~ full_pipe_name ~ " - Status: " ~ action_type, info=True) }}
        {% endif %}
    {% else %}
        {% if debug_mode %}
            {% if needs_ddl_execution %}
                {{ log("Would execute changes for " ~ full_pipe_name ~ ": " ~ change_reasons | join(", "), info=True) }}
            {% else %}
                {{ log("No changes detected for " ~ full_pipe_name, info=True) }}
            {% endif %}
        {% endif %}
    {% endif %}
    
    {{ return({
        'success': true, 
        'action': action_type, 
        'pipe_name': full_pipe_name, 
        'changes': change_reasons, 
        'table_exists': table_exists, 
        'pipe_exists': pipe_exists,
        'stage_change_flag': stage_change_flag,
        'cluster_change_flag': cluster_change_flag,
        'cluster_transformation_change_flag': cluster_transformation_change_flag,
        'file_pattern_change_flag': file_pattern_change_flag,
        'file_format_change_flag': file_format_change_flag,
        'table_change_flag': table_change_flag,
        'pause_state_change_flag': pause_state_change_flag,
        'needs_ddl_execution': needs_ddl_execution
    }) }}
{% endmacro %}

{# Helper macro to get current pipe pause state #}
{% macro get_pipe_pause_state(schema_name, pipe_name) %}
    {% set query %}
        SELECT 
            CASE 
                WHEN UPPER(definition) LIKE '%PIPE_EXECUTION_PAUSED%=%TRUE%' THEN TRUE
                ELSE FALSE 
            END as is_paused
        FROM {{ var("snowpipe_database") }}.INFORMATION_SCHEMA.PIPES
        WHERE PIPE_CATALOG = UPPER('{{ var("snowpipe_database") }}')
        AND PIPE_SCHEMA = UPPER('{{ schema_name }}')
        AND PIPE_NAME = UPPER('{{ pipe_name }}');
    {% endset %}
    {%- call statement('pause_check', fetch_result=True) %}{{ query }}{%- endcall -%}
    {%- set result = load_result('pause_check')['data'] -%}
    {{ return(result[0][0] if result|length > 0 else false) }}
{% endmacro %}