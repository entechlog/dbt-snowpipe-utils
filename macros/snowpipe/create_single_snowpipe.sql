{% macro create_single_snowpipe(config, run_queries=False, debug_mode=False) %}
    {# Extract configuration values #}
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
    
    {# Processing header - show for all runs #}
    {{ log("", info=True) }}
    {{ log("🔍 PROCESSING: " ~ full_pipe_name, info=True) }}
    
    {# Configuration table - show for all runs with proper width and borders #}
    {{ log("📋 CONFIGURATION:", info=True) }}
    {{ log("┌──────────────────────┬──────────────────────────────────────────────────────────────────────┐", info=True) }}
    {{ log("│ Parameter            │ Value                                                                │", info=True) }}
    {{ log("├──────────────────────┼──────────────────────────────────────────────────────────────────────┤", info=True) }}
    
    {% set config_items = [
        ('Source', source_name),
        ('Event', event_name),
        ('Event Type', event_type if event_type else 'None'),
        ('File Pattern', file_pattern),
        ('Stage', full_stage_name),
        ('Schema Inference', 'ON' if enable_schema_inference else 'OFF'),
        ('Schema Evolution', 'ON' if enable_schema_evolution else 'OFF'),
        ('Table Mode', 'Individual Columns' if use_individual_columns else 'VARIANT'),
        ('Cluster Key', cluster_key if cluster_key else 'None'),
        ('Cluster Transform', cluster_key_transformation if cluster_key_transformation else 'None'),
        ('Pause Flag', 'TRUE' if pause_pipe_flag else 'FALSE')
    ] %}
    
    {% for param, value in config_items %}
        {% set padded_param = (param ~ '                    ')[:20] %}
        {% set padded_value = ((value | string) ~ '                                                                      ')[:68] %}
        {{ log("│ " ~ padded_param ~ " │ " ~ padded_value ~ " │", info=True) }}
    {% endfor %}
    {{ log("└──────────────────────┴──────────────────────────────────────────────────────────────────────┘", info=True) }}
    
    {# Check current state - switch to target database context #}
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
    
    {# Get current states for comparison #}
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
    
    {# Prepare expected values for comparison #}
    {% set expected_stage = full_stage_name|upper %}
    {% set expected_pattern = get_file_pattern(file_pattern) %}
    {% set expected_format = get_file_format_name_for_comparison(file_pattern, stage_name) %}
    {% set expected_cluster = ('LINEAR("' ~ cluster_key|upper ~ '")') if (cluster_key and cluster_key|trim != "") else '' %}
    
    {# For individual columns mode, cluster transformation is not supported #}
    {% if use_individual_columns %}
        {% set expected_transformation = '' %}
    {% else %}
        {% set expected_transformation = cluster_key_transformation if (cluster_key and cluster_key_transformation) else '' %}
    {% endif %}
    
    {% set current_transformation = '' %}
    {% if pipe_exists and cluster_key and not use_individual_columns and cluster_key_transformation %}
        {% set current_transformation = get_pipe_cluster_transformation(source_name, pipe_name, cluster_key) %}
    {% endif %}
    
    {# Comparison table - expanded width for better visibility with increased column sizes #}
    {{ log("", info=True) }}
    {{ log("🔄 DETAILED COMPARISON:", info=True) }}
    {{ log("┌─────────────────────┬──────────────────────────────────────────────────┬─────────────────────────────────────────────────┬─────┐", info=True) }}
    {{ log("│ Parameter           │ Current Value                                    │ Expected Value                                  │Match│", info=True) }}
    {{ log("├─────────────────────┼──────────────────────────────────────────────────┼─────────────────────────────────────────────────┼─────┤", info=True) }}
    
    {% set comparison_items = [
        ('Table Exists', table_exists|string, 'TRUE', table_exists|string == 'True'),
        ('Pipe Exists', pipe_exists|string, 'TRUE', pipe_exists|string == 'True'),
        ('Stage', current_stage if current_stage else 'None', expected_stage if expected_stage else 'None', current_stage == expected_stage),
        ('File Pattern', current_pattern if current_pattern else 'None', expected_pattern if expected_pattern else 'None', current_pattern == expected_pattern),
        ('File Format', current_format if current_format else 'None', expected_format if expected_format else 'None', current_format == expected_format),
        ('Cluster Key', current_cluster if current_cluster else 'None', expected_cluster if expected_cluster else 'None', current_cluster|trim|upper == expected_cluster|trim|upper),
        ('Cluster Transform', current_transformation if current_transformation else 'None', expected_transformation if expected_transformation else 'None', current_transformation == expected_transformation),
        ('Pause State', current_paused|string, pause_pipe_flag|string, current_paused == pause_pipe_flag)
    ] %}
    
    {% for param, current, expected, match in comparison_items %}
        {% set padded_param = (param ~ '                   ')[:19] %}
        {% set padded_current = (current ~ '                                                  ')[:48] %}
        {% set padded_expected = (expected ~ '                                                 ')[:47] %}
        {% set match_text = 'YES' if match else 'NO' %}
        {% set padded_match = (match_text ~ '   ')[:3] %}
        
        {{ log("│ " ~ padded_param ~ " │ " ~ padded_current ~ " │ " ~ padded_expected ~ " │ " ~ padded_match ~ " │", info=True) }}
    {% endfor %}
    {{ log("└─────────────────────┴──────────────────────────────────────────────────┴─────────────────────────────────────────────────┴─────┘", info=True) }}
    
    {# Change detection logic #}
    {% set requires_pipe_recreation = false %}
    {% set requires_table_creation = false %}
    {% set requires_pause_change = false %}
    {% set change_reasons = [] %}
    {% set stage_change_flag = false %}
    {% set cluster_change_flag = false %}
    {% set cluster_transformation_change_flag = false %}
    {% set file_pattern_change_flag = false %}
    {% set file_format_change_flag = false %}
    {% set table_change_flag = false %}
    {% set pause_change_flag = false %}
    
    {# Check if table needs to be created or updated #}
    {% if not table_exists %}
        {% set requires_table_creation = true %}
        {% set table_change_flag = true %}
        {% do change_reasons.append("Table missing") %}
    {% else %}
        {# Table exists - check if it has required metadata columns #}
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
        {% endif %}
    {% endif %}
    
    {# Check if pipe needs to be created or updated #}
    {% if not pipe_exists %}
        {% set requires_pipe_recreation = true %}
        {% do change_reasons.append("Pipe missing") %}
    {% else %}
        {# Pipe exists - check for configuration changes #}
        {% if current_stage != expected_stage %}
            {% set requires_pipe_recreation = true %}
            {% set stage_change_flag = true %}
            {% do change_reasons.append("Stage changed") %}
        {% endif %}
        
        {% if current_pattern != expected_pattern %}
            {% set requires_pipe_recreation = true %}
            {% set file_pattern_change_flag = true %}
            {% do change_reasons.append("File pattern changed") %}
        {% endif %}
        
        {# File format changes detection - compare format names only #}
        {% if expected_format and current_format != expected_format %}
            {% set requires_pipe_recreation = true %}
            {% set file_format_change_flag = true %}
            {% do change_reasons.append("File format changed") %}
        {% endif %}
        
        {# Check clustering changes - MODIFIED LOGIC FOR VARIANT MODE #}
        {% if current_cluster|trim|upper != expected_cluster|trim|upper %}
            {% if use_individual_columns %}
                {# Individual columns mode: clustering changes require table recreation #}
                {% set requires_table_creation = true %}
                {% set cluster_change_flag = true %}
                {% set table_change_flag = true %}
                {% do change_reasons.append("Clustering changed") %}
            {% else %}
                {# VARIANT mode: clustering changes only require table modification, not recreation #}
                {% set cluster_change_flag = true %}
                {% set table_change_flag = true %}
                {% do change_reasons.append("Clustering changed") %}
            {% endif %}
        {% endif %}
        
        {# Check cluster transformation changes - only for VARIANT mode #}
        {% if not use_individual_columns and expected_transformation and current_transformation != expected_transformation %}
            {% set requires_pipe_recreation = true %}
            {% set cluster_transformation_change_flag = true %}
            {% do change_reasons.append("Cluster transformation changed") %}
        {% endif %}
        
        {# Check pause state changes #}
        {% if current_paused != pause_pipe_flag %}
            {% set requires_pause_change = true %}
            {% set pause_change_flag = true %}
            {% do change_reasons.append("Pause state changed") %}
        {% endif %}
    {% endif %}
    
    {# Debug mode decision summary #}
    {% if debug_mode %}
        {{ log("", info=True) }}
        {{ log("⚡ CHANGE DECISION SUMMARY:", info=True) }}
        {{ log("┌─────────────────────────┬─────────┐", info=True) }}
        {{ log("│ Decision                │ Value   │", info=True) }}
        {{ log("├─────────────────────────┼─────────┤", info=True) }}
        
        {% set decision_items = [
            ('requires_table_creation', requires_table_creation|string),
            ('requires_pipe_recreation', requires_pipe_recreation|string),
            ('requires_pause_change', requires_pause_change|string),
            ('change_reasons_count', change_reasons|length|string),
            ('change_reasons', change_reasons|join(", ") if change_reasons else "None")
        ] %}
        
        {% for decision, value in decision_items %}
            {% if decision != 'change_reasons' %}
                {% set padded_decision = (decision ~ '                       ')[:23] %}
                {% set padded_value = (value ~ '       ')[:7] %}
                {{ log("│ " ~ padded_decision ~ " │ " ~ padded_value ~ " │", info=True) }}
            {% endif %}
        {% endfor %}
        {{ log("└─────────────────────────┴─────────┘", info=True) }}
        
        {% if change_reasons %}
            {{ log("🔍 Change Reasons: " ~ change_reasons|join(", "), info=True) }}
        {% endif %}
    {% endif %}
    
    {# Determine if any changes are needed #}
    {% set any_changes_needed = (requires_table_creation or requires_pipe_recreation or requires_pause_change or cluster_change_flag) %}
    
    {# Generate SQL only when changes are actually needed #}
    {% if any_changes_needed %}
        {% set creation_sql %}
            USE ROLE {{ var("snowpipe_admin_role") }};
            USE DATABASE {{ var("snowpipe_database") }};
            USE SCHEMA {{ source_name }};
            USE WAREHOUSE {{ var("snowpipe_warehouse") }};
            
            -- {{ full_pipe_name }}: {{ change_reasons | join(', ') }}
            
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
            {% elif cluster_change_flag %}
                {# Handle cluster key changes without table recreation for VARIANT mode #}
                {{ handle_cluster_key_change(
                    full_table_name,
                    current_cluster,
                    cluster_key,
                    cluster_key_type,
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
            
            {% if requires_table_creation or requires_pipe_recreation %}
                {{ set_permissions_sql(full_pipe_name, full_table_name, pause_pipe_flag) }}
            {% elif requires_pause_change %}
                -- Only pause state changed
                {% if pause_pipe_flag %}
                    ALTER PIPE IF EXISTS {{ full_pipe_name }} SET PIPE_EXECUTION_PAUSED = TRUE;
                {% else %}
                    SELECT SYSTEM$PIPE_FORCE_RESUME('{{ full_pipe_name }}');
                {% endif %}
            {% endif %}
        {% endset %}
    {% else %}
        {% set creation_sql %}
            -- {{ full_pipe_name }}: No changes required
            SELECT 'No changes required for {{ full_pipe_name }}' AS status;
        {% endset %}
    {% endif %}
    
    {# Determine action type #}
    {% set action_type = 'skipped' %}
    {% if not table_exists and not pipe_exists %}
        {% set action_type = 'created' %}
    {% elif any_changes_needed %}
        {% set action_type = 'updated' %}
    {% endif %}
    
    {# Execute SQL only when changes are needed #}
    {% if run_queries and any_changes_needed %}
        {% if debug_mode %}
            {{ log("🚀 Executing changes for " ~ full_pipe_name ~ ": " ~ change_reasons | join(", "), info=True) }}
        {% endif %}
        {% do run_query(creation_sql) %}
        {% if debug_mode %}
            {{ log("✅ Completed " ~ full_pipe_name ~ " - Status: " ~ action_type, info=True) }}
        {% endif %}
    {% elif run_queries and not any_changes_needed %}
        {% if debug_mode %}
            {{ log("⭐ No changes needed for " ~ full_pipe_name ~ " - SKIPPING ALL SQL EXECUTION", info=True) }}
        {% endif %}
    {% elif not run_queries %}
        {% if debug_mode %}
            {% if any_changes_needed %}
                {{ log("📋 Would execute changes for " ~ full_pipe_name ~ ": " ~ change_reasons | join(", "), info=True) }}
            {% else %}
                {{ log("⭐ No changes detected for " ~ full_pipe_name, info=True) }}
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
        'pause_change_flag': pause_change_flag
    }) }}
{% endmacro %}

{# Helper macro to handle cluster key changes without table recreation #}
{% macro handle_cluster_key_change(full_table_name, current_cluster, cluster_key, cluster_key_type, debug_mode=False) %}
    {% if current_cluster and current_cluster != ('LINEAR("' ~ cluster_key|upper ~ '")') %}
        -- Extract old cluster key name from LINEAR("OLD_NAME") format
        {% set old_cluster_key = current_cluster | replace('LINEAR("', '') | replace('")', '') %}
        
        {% if debug_mode %}
            {{ log("Handling cluster key change from " ~ old_cluster_key ~ " to " ~ cluster_key, info=True) }}
        {% endif %}
        
        -- Drop clustering first
        ALTER TABLE {{ full_table_name }} DROP CLUSTERING KEY;
        
        -- Rename the column
        ALTER TABLE {{ full_table_name }} RENAME COLUMN "{{ old_cluster_key }}" TO "{{ cluster_key }}";
        
        -- Re-establish clustering
        ALTER TABLE {{ full_table_name }} CLUSTER BY ("{{ cluster_key }}");
        
    {% elif cluster_key and cluster_key|trim != "" %}
        -- Add new cluster key column if it doesn't exist
        ALTER TABLE {{ full_table_name }} ADD COLUMN IF NOT EXISTS "{{ cluster_key }}" {{ cluster_key_type }} COMMENT 'Clustering key column';
        ALTER TABLE {{ full_table_name }} CLUSTER BY ("{{ cluster_key }}");
    {% endif %}
{% endmacro %}