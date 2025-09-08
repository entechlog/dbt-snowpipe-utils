{% macro debug_variables() %}
    {% if execute %}
        {{ log("🔍 SNOWPIPE PACKAGE DIAGNOSTIC REPORT", info=True) }}
        {{ log("=" * 80, info=True) }}
        
        {# Switch to pipe admin role for proper permissions #}
        {% set original_role_query %}
            SELECT CURRENT_ROLE() as original_role
        {% endset %}
        {%- call statement('get_original_role', fetch_result=True) %}{{ original_role_query }}{%- endcall -%}
        {%- set original_role = load_result('get_original_role')['data'][0][0] -%}
        
        {% set switch_role_sql %}
            USE ROLE {{ var("snowpipe_admin_role") }};
            USE DATABASE {{ var("snowpipe_database") }};
            USE WAREHOUSE {{ var("snowpipe_warehouse") }};
        {% endset %}
        {% do run_query(switch_role_sql) %}
        {{ log("🔄 Switched to admin role: " ~ var("snowpipe_admin_role"), info=True) }}
        
        {# Environment Variables Table #}
        {{ log("", info=True) }}
        {{ log("📋 ENVIRONMENT CONFIGURATION", info=True) }}
        {{ log("┌─────────────────────────────────┬──────────────────────────────────────┐", info=True) }}
        {{ log("│ Variable                        │ Value                                │", info=True) }}
        {{ log("├─────────────────────────────────┼──────────────────────────────────────┤", info=True) }}
        
        {% set env_vars = [
            ('ENV_CODE', env_var('ENV_CODE', 'NOT_SET')),
            ('PROJ_CODE', env_var('PROJ_CODE', 'NOT_SET')),
            ('SNOWPIPE_DATABASE', var("snowpipe_database", 'NOT_SET')),
            ('SNOWPIPE_SCHEMA', var("snowpipe_schema", 'NOT_SET')),
            ('SNOWPIPE_WAREHOUSE', var("snowpipe_warehouse", 'NOT_SET')),
            ('SNOWPIPE_ADMIN_ROLE', var("snowpipe_admin_role", 'NOT_SET')),
            ('SNOWPIPE_MONITOR_ROLES', var("snowpipe_monitor_roles", 'NOT_SET'))
        ] %}
        
        {% for var_name, var_value in env_vars %}
            {% set padded_name = (var_name ~ ' ' * 31)[:31] %}
            {% set padded_value = (var_value ~ ' ' * 36)[:36] %}
            {{ log("│ " ~ padded_name ~ " │ " ~ padded_value ~ " │", info=True) }}
        {% endfor %}
        {{ log("└─────────────────────────────────┴──────────────────────────────────────┘", info=True) }}
        
        {# Current Session Info #}
        {{ log("", info=True) }}
        {{ log("🏠 CURRENT SESSION CONTEXT", info=True) }}
        {% set session_query %}
            SELECT 
                CURRENT_DATABASE() as current_db,
                CURRENT_SCHEMA() as current_schema,
                CURRENT_ROLE() as current_role,
                CURRENT_WAREHOUSE() as current_warehouse,
                CURRENT_USER() as current_user
        {% endset %}
        {%- call statement('session_info', fetch_result=True) %}{{ session_query }}{%- endcall -%}
        {%- set session_result = load_result('session_info')['data'] -%}
        
        {{ log("┌─────────────────────────────────┬──────────────────────────────────────┐", info=True) }}
        {{ log("│ Session Property                │ Value                                │", info=True) }}
        {{ log("├─────────────────────────────────┼──────────────────────────────────────┤", info=True) }}
        {% if session_result|length > 0 %}
            {% set session_props = [
                ('Database', session_result[0][0]),
                ('Schema', session_result[0][1]),
                ('Role', session_result[0][2]),
                ('Warehouse', session_result[0][3]),
                ('User', session_result[0][4])
            ] %}
            {% for prop_name, prop_value in session_props %}
                {% set padded_name = (prop_name ~ ' ' * 31)[:31] %}
                {% set padded_value = (prop_value ~ ' ' * 36)[:36] %}
                {{ log("│ " ~ padded_name ~ " │ " ~ padded_value ~ " │", info=True) }}
            {% endfor %}
        {% endif %}
        {{ log("└─────────────────────────────────┴──────────────────────────────────────┘", info=True) }}
        
        {# Stages Existence Check #}
        {{ log("", info=True) }}
        {{ log("🏗️  STAGE EXISTENCE VALIDATION", info=True) }}
        {{ log("┌─────────────────────────────────┬──────────────────┬──────────────────┐", info=True) }}
        {{ log("│ Stage Name                      │ Type             │ Status           │", info=True) }}
        {{ log("├─────────────────────────────────┼──────────────────┼──────────────────┤", info=True) }}
        
        {% set stages_to_check = [
            ('JSON', var("snowpipe_json_stage")),
            ('CSV', var("snowpipe_csv_stage")), 
            ('PARQUET', var("snowpipe_parquet_stage"))
        ] %}
        
        {% for stage_type, stage_name in stages_to_check %}
            {% set stage_check_query %}
                SELECT COUNT(*) as stage_count
                FROM INFORMATION_SCHEMA.STAGES
                WHERE STAGE_CATALOG = UPPER('{{ var("snowpipe_database") }}')
                AND STAGE_SCHEMA = UPPER('{{ var("snowpipe_schema") }}')
                AND STAGE_NAME = UPPER('{{ stage_name }}')
            {% endset %}
            
            {%- call statement('stage_existence_' ~ stage_type, fetch_result=True) %}{{ stage_check_query }}{%- endcall -%}
            {%- set stage_result = load_result('stage_existence_' ~ stage_type)['data'] -%}
            {% set stage_exists = (stage_result[0][0] > 0) if stage_result|length > 0 else false %}
            {% set status_icon = "✅ EXISTS" if stage_exists else "❌ MISSING" %}
            
            {% set padded_name = (stage_name ~ ' ' * 31)[:31] %}
            {% set padded_type = (stage_type ~ ' ' * 16)[:16] %}
            {% set padded_status = (status_icon ~ ' ' * 16)[:16] %}
            {{ log("│ " ~ padded_name ~ " │ " ~ padded_type ~ " │ " ~ padded_status ~ " │", info=True) }}
        {% endfor %}
        {{ log("└─────────────────────────────────┴──────────────────┴──────────────────┘", info=True) }}
        
        {# File Formats Existence Check #}
        {{ log("", info=True) }}
        {{ log("📄 FILE FORMAT VALIDATION", info=True) }}
        {{ log("┌─────────────────────────────────┬──────────────────┬──────────────────┐", info=True) }}
        {{ log("│ Format Name                     │ Type             │ Status           │", info=True) }}
        {{ log("├─────────────────────────────────┼──────────────────┼──────────────────┤", info=True) }}
        
        {% set formats_to_check = [
            ('JSON', var("snowpipe_json_file_format")),
            ('CSV', var("snowpipe_csv_file_format")),
            ('PARQUET', var("snowpipe_parquet_file_format"))
        ] %}
        
        {% for format_type, format_name in formats_to_check %}
            {% set format_check_query %}
                SELECT COUNT(*) as format_count
                FROM INFORMATION_SCHEMA.FILE_FORMATS
                WHERE FILE_FORMAT_CATALOG = UPPER('{{ var("snowpipe_database") }}')
                AND FILE_FORMAT_SCHEMA = UPPER('{{ var("snowpipe_schema") }}')
                AND FILE_FORMAT_NAME = UPPER('{{ format_name }}')
            {% endset %}
            
            {%- call statement('format_existence_' ~ format_type, fetch_result=True) %}{{ format_check_query }}{%- endcall -%}
            {%- set format_result = load_result('format_existence_' ~ format_type)['data'] -%}
            {% set format_exists = (format_result[0][0] > 0) if format_result|length > 0 else false %}
            {% set status_icon = "✅ EXISTS" if format_exists else "❌ MISSING" %}
            
            {% set padded_name = (format_name ~ ' ' * 31)[:31] %}
            {% set padded_type = (format_type ~ ' ' * 16)[:16] %}
            {% set padded_status = (status_icon ~ ' ' * 16)[:16] %}
            {{ log("│ " ~ padded_name ~ " │ " ~ padded_type ~ " │ " ~ padded_status ~ " │", info=True) }}
        {% endfor %}
        {{ log("└─────────────────────────────────┴──────────────────┴──────────────────┘", info=True) }}
        
        {# Configuration Table Check #}
        {{ log("", info=True) }}
        {{ log("⚙️  CONFIGURATION VALIDATION", info=True) }}
        {% set config_table_name = var("snowpipe_database") ~ "." ~ var("snowpipe_seed_schema", "seed") ~ ".reference__snowpipe_config" %}
        
        {% set config_check_query %}
            SELECT COUNT(*) as table_count
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = UPPER('{{ var("snowpipe_database") }}')
            AND TABLE_SCHEMA = UPPER('{{ var("snowpipe_seed_schema", "seed") }}')
            AND TABLE_NAME = 'REFERENCE__SNOWPIPE_CONFIG'
        {% endset %}
        
        {%- call statement('config_table_check', fetch_result=True) %}{{ config_check_query }}{%- endcall -%}
        {%- set config_result = load_result('config_table_check')['data'] -%}
        {% set config_exists = (config_result[0][0] > 0) if config_result|length > 0 else false %}
        
        {{ log("┌─────────────────────────────────┬──────────────────────────────────────┐", info=True) }}
        {{ log("│ Configuration Item              │ Status                               │", info=True) }}
        {{ log("├─────────────────────────────────┼──────────────────────────────────────┤", info=True) }}
        
        {% set config_table_status = "✅ EXISTS" if config_exists else "❌ MISSING" %}
        {% set padded_item = ("Config Table" ~ ' ' * 31)[:31] %}
        {% set padded_status = (config_table_status ~ ' ' * 36)[:36] %}
        {{ log("│ " ~ padded_item ~ " │ " ~ padded_status ~ " │", info=True) }}
        
        {% if config_exists %}
            {% set config_count_query %}
                SELECT COUNT(*) as config_count
                FROM {{ ref('reference__snowpipe_config') }}
            {% endset %}
            
            {%- call statement('config_count_check', fetch_result=True) %}{{ config_count_query }}{%- endcall -%}
            {%- set count_result = load_result('config_count_check')['data'] -%}
            {% if count_result|length > 0 %}
                {% set record_count = count_result[0][0] %}
                {% set record_status = "📊 " ~ record_count ~ " records found" %}
                {% set padded_item2 = ("Config Records" ~ ' ' * 31)[:31] %}
                {% set padded_status2 = (record_status ~ ' ' * 36)[:36] %}
                {{ log("│ " ~ padded_item2 ~ " │ " ~ padded_status2 ~ " │", info=True) }}
            {% endif %}
        {% endif %}
        {{ log("└─────────────────────────────────┴──────────────────────────────────────┘", info=True) }}
        
        {# File Format Resolution Test #}
        {{ log("", info=True) }}
        {{ log("🔗 FILE FORMAT RESOLUTION TEST", info=True) }}
        {{ log("┌─────────────────────────────────┬──────────────────────────────────────┐", info=True) }}
        {{ log("│ File Pattern                    │ Resolved Format                      │", info=True) }}
        {{ log("├─────────────────────────────────┼──────────────────────────────────────┤", info=True) }}
        
        {% for pattern in ['json', 'csv', 'parquet'] %}
            {% set format_name = get_file_format_name(pattern) %}
            {% set padded_pattern = (pattern ~ ' ' * 31)[:31] %}
            {% set padded_format = (format_name ~ ' ' * 36)[:36] %}
            {{ log("│ " ~ padded_pattern ~ " │ " ~ padded_format ~ " │", info=True) }}
        {% endfor %}
        {{ log("└─────────────────────────────────┴──────────────────────────────────────┘", info=True) }}
        
        {# Environment Detection #}
        {{ log("", info=True) }}
        {{ log("🌍 ENVIRONMENT DETECTION", info=True) }}
        {% set env_info = get_environment_name() %}
        {{ log("┌─────────────────────────────────┬──────────────────────────────────────┐", info=True) }}
        {{ log("│ Environment Property            │ Value                                │", info=True) }}
        {{ log("├─────────────────────────────────┼──────────────────────────────────────┤", info=True) }}
        {% set padded_name1 = ("Environment Name" ~ ' ' * 31)[:31] %}
        {% set padded_value1 = (env_info[0] ~ ' ' * 36)[:36] %}
        {{ log("│ " ~ padded_name1 ~ " │ " ~ padded_value1 ~ " │", info=True) }}
        {% set padded_name2 = ("Environment Code" ~ ' ' * 31)[:31] %}
        {% set padded_value2 = (env_info[1] ~ ' ' * 36)[:36] %}
        {{ log("│ " ~ padded_name2 ~ " │ " ~ padded_value2 ~ " │", info=True) }}
        {{ log("└─────────────────────────────────┴──────────────────────────────────────┘", info=True) }}
        
        {# Switch back to original role #}
        {% set restore_role_sql %}
            USE ROLE "{{ original_role }}";
        {% endset %}
        {% do run_query(restore_role_sql) %}
        {{ log("", info=True) }}
        {{ log("🔄 Restored original role: " ~ original_role, info=True) }}
        {{ log("", info=True) }}
        {{ log("✅ DIAGNOSTIC REPORT COMPLETE", info=True) }}
        {{ log("=" * 80, info=True) }}
        
    {% endif %}
{% endmacro %}