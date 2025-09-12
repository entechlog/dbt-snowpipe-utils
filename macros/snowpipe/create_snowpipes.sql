{% macro create_snowpipes(run_queries=False, debug_mode=False) %}
    {% if execute %}
        {% set start_time = modules.datetime.datetime.now() %}
        
        {# Header with mode indication #}
        {% if run_queries %}
            {{ log("🚀 SNOWPIPE CREATION - EXECUTION MODE", info=True) }}
        {% else %}
            {{ log("🔍 SNOWPIPE CREATION - DRY RUN MODE", info=True) }}
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
        
        {# ===================== CONFIGURATION SUMMARY (HYBRID) ===================== #}
        {{ log("", info=True) }}
        {{ log("📋 CONFIGURATION SUMMARY", info=True) }}
        {{ log("Found " ~ configs|length ~ " pipe configurations to process", info=True) }}
        {{ log("", info=True) }}
        
        {# Unicode header (since this works in debug) #}
        {{ log("┌─────────────────────┬──────────────────────┬─────────────┬─────────────┬─────────────┐", info=True) }}
        {{ log("│ Source              │ Event                │ Pattern     │ Schema Inf  │ Schema Evo  │", info=True) }}
        {{ log("├─────────────────────┼──────────────────────┼─────────────┼─────────────┼─────────────┤", info=True) }}
        
        {# ASCII data rows (more reliable for loops) #}
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
            
            {{ log("| " ~ padded_source ~ " | " ~ padded_event ~ " | " ~ padded_pattern ~ " | " ~ padded_inf ~ " | " ~ padded_evo ~ " |", info=True) }}
        {% endfor %}
        
        {# Unicode footer #}
        {{ log("└─────────────────────┴──────────────────────┴─────────────┴─────────────┴─────────────┘", info=True) }}
        
        {# ========================= PROCESSING PIPES (HYBRID) ========================== #}
        {{ log("", info=True) }}
        {{ log("⚙️  PROCESSING PIPES", info=True) }}
        
        {# Unicode header #}
        {{ log("┌────────────────────────────────┬────────────┬────────────┬───────────────────┐", info=True) }}
        {{ log("│ Event                          │ Table      │ Pipe       │ Action            │", info=True) }}
        {{ log("├────────────────────────────────┼────────────┼────────────┼───────────────────┤", info=True) }}
        
        {# Initialize counters #}
        {% set counters = {
            'total': configs|length,
            'processed': 0,
            'created': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0
        } %}
        
        {# Process each configuration - ASCII data rows #}
        {% for config in configs %}
            {% set result = create_single_snowpipe(config, run_queries, debug_mode) %}
            
            {% set event_name = config[2] | string %}
            {% set event_type = config[3] | string if config[3] else '' %}
            {% set event_display = event_name ~ (('_' ~ event_type) if event_type else '') %}
            
            {# FIXED: Determine actual status based on current state, not intended action #}
            {% if result.success %}
                {% do counters.update({'processed': counters.processed + 1}) %}
                
                {# For dry run mode: show current state, not intended state #}
                {% if not run_queries %}
                    {% set table_status = "EXISTS" if result.table_exists else "MISSING" %}
                    {% set pipe_status = "EXISTS" if result.pipe_exists else "MISSING" %}
                    {% if result.action == 'created' %}
                        {% set action_status = "WOULD CREATE" %}
                        {% do counters.update({'created': counters.created + 1}) %}
                    {% elif result.action == 'updated' %}
                        {% set action_status = "WOULD UPDATE" %}
                        {% do counters.update({'updated': counters.updated + 1}) %}
                    {% else %}
                        {% set action_status = "NO CHANGES" %}
                        {% do counters.update({'skipped': counters.skipped + 1}) %}
                    {% endif %}
                {% else %}
                    {# For execution mode: show post-execution state #}
                    {% if result.action == 'created' %}
                        {% do counters.update({'created': counters.created + 1}) %}
                        {% set table_status = "CREATED" %}
                        {% set pipe_status = "CREATED" %}
                        {% set action_status = "CREATED" %}
                    {% elif result.action == 'updated' %}
                        {% do counters.update({'updated': counters.updated + 1}) %}
                        {% set table_status = "UPDATED" %}
                        {% set pipe_status = "UPDATED" %}
                        {% set action_status = "UPDATED" %}
                    {% else %}
                        {% do counters.update({'skipped': counters.skipped + 1}) %}
                        {% set table_status = "EXISTS" %}
                        {% set pipe_status = "EXISTS" %}
                        {% set action_status = "SKIPPED" %}
                    {% endif %}
                {% endif %}
            {% else %}
                {% do counters.update({'errors': counters.errors + 1}) %}
                {% set table_status = "ERROR" %}
                {% set pipe_status = "ERROR" %}
                {% set action_status = "FAILED" %}
            {% endif %}
            
            {# Simplified padding #}
            {% set padded_event = (event_display ~ '                              ')[:30] %}
            {% set padded_table = (table_status ~ '          ')[:10] %}
            {% set padded_pipe = (pipe_status ~ '          ')[:10] %}
            {% set padded_action = (action_status ~ '                 ')[:17] %}
            
            {{ log("| " ~ padded_event ~ " | " ~ padded_table ~ " | " ~ padded_pipe ~ " | " ~ padded_action ~ " |", info=True) }}
        {% endfor %}
        
        {# Unicode footer #}
        {{ log("└────────────────────────────────┴────────────┴────────────┴───────────────────┴─────────────────────────────────┘", info=True) }}
        
        {# ============================ EXECUTION SUMMARY (HYBRID) ============================ #}
        {% set end_time = modules.datetime.datetime.now() %}
        {% set duration = end_time - start_time %}
        
        {{ log("", info=True) }}
        {{ log("📊 EXECUTION SUMMARY", info=True) }}
        
        {# Unicode header #}
        {{ log("┌─────────────────────────────────┬──────────────────────────────────────┐", info=True) }}
        {{ log("│ Metric                          │ Value                                │", info=True) }}
        {{ log("├─────────────────────────────────┼──────────────────────────────────────┤", info=True) }}
        
        {# ASCII data rows #}
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
            {% set padded_value = ((value | string) ~ '                                    ')[:36] %}
            {{ log("| " ~ padded_metric ~ " | " ~ padded_value ~ " |", info=True) }}
        {% endfor %}
        
        {# Unicode footer #}
        {{ log("└─────────────────────────────────┴──────────────────────────────────────┘", info=True) }}
        
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