{% macro create_snowpipes(run_queries=False, debug_mode=False) %}
    {% if execute %}
        {% set start_time = modules.datetime.datetime.now() %}
        
        {# Header with mode indication #}
        {% if run_queries %}
            {{ log("🚀 SNOWPIPE CREATION - EXECUTION MODE", info=True) }}
        {% else %}
            {{ log("📋 SNOWPIPE CREATION - DRY RUN MODE", info=True) }}
        {% endif %}
        {{ log("=" * 80, info=True) }}
        
        {# Get environment configuration #}
        {%- set var_env_code = get_environment_name()[1] -%}
        {{ log("🌍 Environment: " ~ var_env_code.upper() ~ (" | Debug: ON" if debug_mode else " | Debug: OFF"), info=True) }}
        
        {# Build configuration query #}
        {% set config_query %}
            SELECT 
                stage_name, source_name, event_name, event_type, 
                cluster_key_transformation, cluster_key_type, cluster_key, file_pattern,
                COALESCE(enable_schema_inference, FALSE) as enable_schema_inference,
                COALESCE(enable_schema_evolution, FALSE) as enable_schema_evolution,
                CASE 
                    WHEN LOWER('{{ var_env_code }}') = 'dev' THEN COALESCE(LOWER(dev_pause_pipe_flag) = 'true', FALSE)
                    WHEN LOWER('{{ var_env_code }}') = 'stg' THEN COALESCE(LOWER(stg_pause_pipe_flag) = 'true', FALSE)
                    WHEN LOWER('{{ var_env_code }}') = 'prd' THEN COALESCE(LOWER(prd_pause_pipe_flag) = 'true', FALSE)
                    ELSE FALSE
                END AS pause_pipe_flag
            FROM {{ ref('reference__snowpipe_config') }}
            WHERE CASE 
                WHEN LOWER('{{ var_env_code }}') = 'dev' THEN COALESCE(LOWER(dev_enable_pipe_flag) = 'true', FALSE)
                WHEN LOWER('{{ var_env_code }}') = 'stg' THEN COALESCE(LOWER(stg_enable_pipe_flag) = 'true', FALSE)
                WHEN LOWER('{{ var_env_code }}') = 'prd' THEN COALESCE(LOWER(prd_enable_pipe_flag) = 'true', FALSE)
                ELSE FALSE
            END = TRUE
            AND source_name IS NOT NULL 
            AND TRIM(source_name) != ''
            AND event_name IS NOT NULL 
            AND TRIM(event_name) != ''
            ORDER BY source_name, event_name, event_type;
        {% endset %}
        
        {%- call statement('config_stmt', fetch_result=True) %}
            {{ config_query }}
        {%- endcall -%}
        {%- set configs = load_result('config_stmt')['data'] -%}
        
        {% if configs|length == 0 %}
            {{ log("", info=True) }}
            {{ log("⚠️  WARNING: No enabled configurations found for environment: " ~ var_env_code.upper(), info=True) }}
            {{ log("", info=True) }}
            {{ return(none) }}
        {% endif %}
        
        {# ===================== CONFIGURATION SUMMARY ===================== #}
        {{ log("", info=True) }}
        {{ log("📋 CONFIGURATION SUMMARY", info=True) }}
        {{ log("Found " ~ configs|length ~ " pipe configurations to process", info=True) }}
        {{ log("", info=True) }}
        
        {# Configuration table header #}
        {{ log("┌─────────────────────┬──────────────────────┬─────────────┬─────────────┬─────────────┐", info=True) }}
        {{ log("│ Source              │ Event                │ Pattern     │ Schema Inf  │ Schema Evo  │", info=True) }}
        {{ log("├─────────────────────┼──────────────────────┼─────────────┼─────────────┼─────────────┤", info=True) }}
        
        {# Configuration data rows #}
        {% for config in configs %}
            {% set source_name = config[1] | string %}
            {% set event_name = config[2] | string %}
            {% set event_type = config[3] | string if config[3] else '' %}
            {% set file_pattern = config[7] | string %}
            {% set enable_schema_inference = config[8] %}
            {% set enable_schema_evolution = config[9] %}
            
            {% set event_display = event_name ~ (('_' ~ event_type) if event_type else '') %}
            
            {# Simplified padding - guaranteed lengths #}
            {% set padded_source = (source_name ~ '                   ')[:19] %}
            {% set padded_event = (event_display ~ '                    ')[:20] %}
            {% set padded_pattern = (file_pattern ~ '           ')[:11] %}
            {% set inf_text = 'TRUE' if enable_schema_inference else 'FALSE' %}
            {% set evo_text = 'TRUE' if enable_schema_evolution else 'FALSE' %}
            {% set padded_inf = (inf_text ~ '           ')[:11] %}
            {% set padded_evo = (evo_text ~ '           ')[:11] %}
            
            {{ log("│ " ~ padded_source ~ " │ " ~ padded_event ~ " │ " ~ padded_pattern ~ " │ " ~ padded_inf ~ " │ " ~ padded_evo ~ " │", info=True) }}
        {% endfor %}
        
        {# Configuration table footer #}
        {{ log("└─────────────────────┴──────────────────────┴─────────────┴─────────────┴─────────────┘", info=True) }}
        {{ log("", info=True) }}
        {{ log("⚙️  PROCESSING PIPES", info=True) }}
        
        {# Initialize counters #}
        {% set counters = {
            'total': configs|length,
            'processed': 0,
            'created': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0
        } %}
        
        {# Collect results for summary table #}
        {% set results_summary = [] %}
        
        {# Process each configuration #}
        {% for config in configs %}
            {% set result = create_single_snowpipe(config, run_queries, debug_mode) %}
            
            {% set event_name = config[2] | string %}
            {% set event_type = config[3] | string if config[3] else '' %}
            {% set event_display = event_name ~ (('_' ~ event_type) if event_type else '') %}
            
            {# Determine status based on result.action #}
            {% if result.success %}
                {% do counters.update({'processed': counters.processed + 1}) %}
                
                {% if result.action == 'created' %}
                    {% do counters.update({'created': counters.created + 1}) %}
                    {% set table_status = "CREATE" %}
                    {% set pipe_status = "CREATE" %}
                    {% set action_status = "CREATED" %}
                {% elif result.action == 'updated' %}
                    {% do counters.update({'updated': counters.updated + 1}) %}
                    {% set table_status = "UPDATE" %}
                    {% set pipe_status = "UPDATE" %}
                    {% set action_status = "UPDATED" %}
                {% else %}
                    {% do counters.update({'skipped': counters.skipped + 1}) %}
                    {% set table_status = "EXISTS" %}
                    {% set pipe_status = "EXISTS" %}
                    {% set action_status = "SKIPPED" %}
                {% endif %}
            {% else %}
                {% do counters.update({'errors': counters.errors + 1}) %}
                {% set table_status = "ERROR" %}
                {% set pipe_status = "ERROR" %}
                {% set action_status = "FAILED" %}
            {% endif %}
            
            {# Collect data for summary table #}
            {% set stage_flag = 'YES' if result.get('stage_change_flag', false) else 'NO' %}
            {% set cluster_flag = 'YES' if result.get('cluster_change_flag', false) else 'NO' %}
            {% set pattern_flag = 'YES' if result.get('file_pattern_change_flag', false) else 'NO' %}
            {% set format_flag = 'YES' if result.get('file_format_change_flag', false) else 'NO' %}
            {% set table_change_flag_display = 'YES' if result.get('table_change_flag', false) else 'NO' %}
            
            {% do results_summary.append({
                'event_display': event_display,
                'table_status': table_status,
                'pipe_status': pipe_status,
                'action_status': action_status,
                'stage_flag': stage_flag,
                'cluster_flag': cluster_flag,
                'pattern_flag': pattern_flag,
                'format_flag': format_flag,
                'table_change_flag': table_change_flag_display
            }) %}
        {% endfor %}
        
        {# ========================= PROCESSING SUMMARY TABLE ========================== #}
        {{ log("", info=True) }}
        {{ log("📊 PROCESSING SUMMARY", info=True) }}
        
        {# Processing summary table header with expanded event column #}
        {{ log("┌──────────────────────────────┬────────┬────────┬─────────────┬─────────┬─────────┬─────────┬─────────┬───────┐", info=True) }}
        {{ log("│ Event                        │ Table  │ Pipe   │ Action      │ Stage   │ Cluster │ Pattern │ Format  │ Table │", info=True) }}
        {{ log("│                              │        │        │             │ Change  │ Change  │ Change  │ Change  │ Change│", info=True) }}
        {{ log("├──────────────────────────────┼────────┼────────┼─────────────┼─────────┼─────────┼─────────┼─────────┼───────┤", info=True) }}
        
        {% for result_item in results_summary %}
            {% set padded_event = (result_item.event_display ~ '                              ')[:28] %}
            {% set padded_table_status = (result_item.table_status ~ '        ')[:6] %}
            {% set padded_pipe = (result_item.pipe_status ~ '        ')[:6] %}
            {% set padded_action = (result_item.action_status ~ '             ')[:11] %}
            {% set padded_stage = (result_item.stage_flag ~ '         ')[:7] %}
            {% set padded_cluster = (result_item.cluster_flag ~ '         ')[:7] %}
            {% set padded_pattern = (result_item.pattern_flag ~ '         ')[:7] %}
            {% set padded_format = (result_item.format_flag ~ '         ')[:7] %}
            {% set padded_table_change = (result_item.table_change_flag ~ '       ')[:5] %}
            
            {{ log("│ " ~ padded_event ~ " │ " ~ padded_table_status ~ " │ " ~ padded_pipe ~ " │ " ~ padded_action ~ " │ " ~ padded_stage ~ " │ " ~ padded_cluster ~ " │ " ~ padded_pattern ~ " │ " ~ padded_format ~ " │ " ~ padded_table_change ~ " │", info=True) }}
        {% endfor %}
        
        {# Processing summary footer #}
        {{ log("└──────────────────────────────┴────────┴────────┴─────────────┴─────────┴─────────┴─────────┴─────────┴───────┘", info=True) }}
        {{ log("", info=True) }}
        {{ log("💡 Change Flags: Stage = Stage name change, Cluster = Cluster key change", info=True) }}
        {{ log("               Pattern = File pattern change, Format = File format change", info=True) }}
        {{ log("               Table = Table structure/metadata changes", info=True) }}
        
        {# ============================ EXECUTION SUMMARY ============================ #}
        {% set end_time = modules.datetime.datetime.now() %}
        {% set duration = end_time - start_time %}
        
        {{ log("", info=True) }}
        {{ log("📊 EXECUTION SUMMARY", info=True) }}
        
        {# Fixed execution summary table with proper alignment #}
        {{ log("┌─────────────────────────────────┬────────────────────────────────┐", info=True) }}
        {{ log("│ Metric                          │ Value                          │", info=True) }}
        {{ log("├─────────────────────────────────┼────────────────────────────────┤", info=True) }}
        
        {# Summary data rows #}
        {% set summary_items = [
            ('Total Configurations', counters.total),
            ('Created', counters.created),
            ('Updated', counters.updated),
            ('Skipped', counters.skipped),
            ('Errors', counters.errors),
            ('Duration (seconds)', '%.2f' | format(duration.total_seconds()))
        ] %}
        
        {% for metric, value in summary_items %}
            {% set padded_metric = (metric ~ '                               ')[:31] %}
            {% set padded_value = ((value | string) ~ '                              ')[:30] %}
            {{ log("│ " ~ padded_metric ~ " │ " ~ padded_value ~ " │", info=True) }}
        {% endfor %}
        
        {# Fixed execution summary footer #}
        {{ log("└─────────────────────────────────┴────────────────────────────────┘", info=True) }}
        
        {# Status messages #}
        {{ log("", info=True) }}
        {% if counters.errors > 0 %}
            {{ log("⚠️  WARNING: " ~ counters.errors ~ " configurations failed", info=True) }}
        {% endif %}
        
        {% if not run_queries %}
            {{ log("💡 This was a DRY RUN. Use run_queries=true to execute changes.", info=True) }}
        {% else %}
            {% if counters.errors == 0 %}
                {{ log("🎉 All operations completed successfully!", info=True) }}
            {% endif %}
        {% endif %}
        
        {{ log("=" * 80, info=True) }}
        
    {% endif %}
{% endmacro %}