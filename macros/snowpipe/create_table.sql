{% macro create_table_sql(table_name, use_individual_columns, enable_schema_inference, enable_schema_evolution, cluster_key, cluster_key_type, file_pattern, stage_name, s3_dir_name, source_name, event_name, cluster_key_transformation, pipe_name, debug_mode=False) %}
    
    {# Build fully qualified table name #}
    {% set full_table_name = var("snowpipe_database") ~ "." ~ source_name ~ "." ~ table_name %}
    
    {% if debug_mode %}
        {{ log("Creating/updating table: " ~ full_table_name, info=True) }}
    {% endif %}
    
    {% set table_exists = check_table_exists(source_name, table_name, debug_mode) %}
    {% set is_new_table = not table_exists %}
    
    {% if use_individual_columns %}
        {% if debug_mode %}
            {{ log("Mode: Individual columns | Schema Inference: " ~ ("ON" if enable_schema_inference else "OFF") ~ " | Schema Evolution: " ~ ("ON" if enable_schema_evolution else "OFF"), info=True) }}
        {% endif %}
        
        {# Switch to target database context early #}
        {% if execute %}
            {% set switch_context_sql %}
                USE DATABASE {{ var("snowpipe_database") }};
                USE SCHEMA {{ source_name }};
            {% endset %}
            {% do run_query(switch_context_sql) %}
            {% if debug_mode %}
                {{ log("Switched to database/schema: " ~ var("snowpipe_database") ~ "." ~ source_name, info=True) }}
            {% endif %}
        {% endif %}
        
        {% if not table_exists %}
            -- Create new table with inferred schema + metadata columns
            {% if debug_mode %}
                {{ log("Creating new table with inferred schema + metadata", info=True) }}
            {% endif %}
            
            {# INFER_SCHEMA always requires named file format - create defaults if needed #}
            {% set stage_name_only = stage_name.split('.')[-1] %}
            {% set stage_info = get_stage_file_format_info(stage_name_only) %}
            
            {% if stage_info.has_named_format %}
                {# Use existing named format #}
                CREATE TABLE {{ full_table_name }}
                USING TEMPLATE (
                    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                    FROM TABLE(
                        INFER_SCHEMA(
                            LOCATION => '@{{ stage_name }}/{{ s3_dir_name }}',
                            FILE_FORMAT => '{{ stage_info.format_name }}',
                            IGNORE_CASE => TRUE
                        )
                    )
                )
            {% else %}
                {# Create default named file formats for inference #}
                {% if file_pattern|lower == 'json' %}
                    {% set default_format_name = var("snowpipe_database") ~ "." ~ var("snowpipe_schema") ~ ".DEFAULT_JSON_FORMAT" %}
                    CREATE FILE FORMAT IF NOT EXISTS {{ default_format_name }} TYPE = 'JSON';
                {% elif file_pattern|lower == 'csv' %}
                    {% set default_format_name = var("snowpipe_database") ~ "." ~ var("snowpipe_schema") ~ ".DEFAULT_CSV_FORMAT" %}
                    CREATE FILE FORMAT IF NOT EXISTS {{ default_format_name }} TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1;
                {% else %}
                    {% set default_format_name = var("snowpipe_database") ~ "." ~ var("snowpipe_schema") ~ ".DEFAULT_PARQUET_FORMAT" %}
                    CREATE FILE FORMAT IF NOT EXISTS {{ default_format_name }} TYPE = 'PARQUET';
                {% endif %}
                
                CREATE TABLE {{ full_table_name }}
                USING TEMPLATE (
                    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                    FROM TABLE(
                        INFER_SCHEMA(
                            LOCATION => '@{{ stage_name }}/{{ s3_dir_name }}',
                            FILE_FORMAT => '{{ default_format_name }}',
                            IGNORE_CASE => TRUE
                        )
                    )
                )
            {% endif %}
            {% if enable_schema_evolution %}
            ENABLE_SCHEMA_EVOLUTION = TRUE
            {% endif %};
            
            -- Add metadata columns
            ALTER TABLE {{ full_table_name }} ADD COLUMN FILE_NAME VARCHAR(16777216);
            ALTER TABLE {{ full_table_name }} ADD COLUMN FILE_ROW_NUMBER NUMBER(38,0);
            ALTER TABLE {{ full_table_name }} ADD COLUMN FILE_CONTENT_KEY VARCHAR(16777216);
            ALTER TABLE {{ full_table_name }} ADD COLUMN FILE_LAST_MODIFIED_TIMESTAMP TIMESTAMP_NTZ(9);
            ALTER TABLE {{ full_table_name }} ADD COLUMN LOADED_TIMESTAMP TIMESTAMP_NTZ(9);
            
            -- Add cluster key column if specified and doesn't exist
            {% if cluster_key and cluster_key|trim != "" %}
                ALTER TABLE {{ full_table_name }} ADD COLUMN IF NOT EXISTS "{{ cluster_key }}" {{ cluster_key_type }};
                {% if debug_mode %}
                    {{ log("Adding clustering: " ~ cluster_key, info=True) }}
                {% endif %}
                ALTER TABLE {{ full_table_name }} CLUSTER BY ("{{ cluster_key }}");
            {% endif %}
        {% else %}
            {# Existing table - check if metadata columns exist, add them if missing #}
            {% set metadata_check_query %}
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_CATALOG = UPPER('{{ var("snowpipe_database") }}')
                AND TABLE_SCHEMA = UPPER('{{ source_name }}')
                AND TABLE_NAME = UPPER('{{ table_name }}')
                AND column_name = 'LOADED_TIMESTAMP'
            {% endset %}
            {%- call statement('metadata_check', fetch_result=True) %}{{ metadata_check_query }}{%- endcall -%}
            {%- set has_metadata = load_result('metadata_check')['data'][0][0] > 0 -%}
            
            {# ALWAYS add metadata columns for individual columns mode, regardless of has_metadata check #}
            {% if use_individual_columns %}
                -- Add metadata columns for individual columns mode
                ALTER TABLE {{ full_table_name }} ADD COLUMN IF NOT EXISTS FILE_NAME VARCHAR(16777216);
                ALTER TABLE {{ full_table_name }} ADD COLUMN IF NOT EXISTS FILE_ROW_NUMBER NUMBER(38,0);
                ALTER TABLE {{ full_table_name }} ADD COLUMN IF NOT EXISTS FILE_CONTENT_KEY VARCHAR(16777216);
                ALTER TABLE {{ full_table_name }} ADD COLUMN IF NOT EXISTS FILE_LAST_MODIFIED_TIMESTAMP TIMESTAMP_NTZ(9);
                ALTER TABLE {{ full_table_name }} ADD COLUMN IF NOT EXISTS LOADED_TIMESTAMP TIMESTAMP_NTZ(9);
                {% if cluster_key and cluster_key|trim != "" %}
                ALTER TABLE {{ full_table_name }} ADD COLUMN IF NOT EXISTS "{{ cluster_key }}" {{ cluster_key_type }};
                {% endif %}
            {% elif not has_metadata %}
                -- Add metadata columns for VARIANT mode only if they don't exist
                ALTER TABLE {{ full_table_name }} ADD COLUMN FILE_NAME VARCHAR(16777216);
                ALTER TABLE {{ full_table_name }} ADD COLUMN FILE_ROW_NUMBER NUMBER(38,0);
                ALTER TABLE {{ full_table_name }} ADD COLUMN FILE_CONTENT_KEY VARCHAR(16777216);
                ALTER TABLE {{ full_table_name }} ADD COLUMN FILE_LAST_MODIFIED_TIMESTAMP TIMESTAMP_NTZ(9);
                ALTER TABLE {{ full_table_name }} ADD COLUMN LOADED_TIMESTAMP TIMESTAMP_NTZ(9);
                {% if cluster_key and cluster_key|trim != "" %}
                ALTER TABLE {{ full_table_name }} ADD COLUMN "{{ cluster_key }}" {{ cluster_key_type }};
                {% endif %}
            {% endif %}
            
            -- Update clustering if changed
            {% set current_cluster = get_table_cluster_key(source_name, table_name) %}
            {% set expected_cluster = ('LINEAR("' ~ cluster_key|upper ~ '")') if (cluster_key and cluster_key|trim != "") else '' %}
            
            {% if current_cluster != expected_cluster %}
                {% if cluster_key and cluster_key|trim != "" %}
                    {% if debug_mode %}
                        {{ log("Updating clustering to: " ~ cluster_key, info=True) }}
                    {% endif %}
                    ALTER TABLE {{ full_table_name }} CLUSTER BY ("{{ cluster_key }}");
                {% else %}
                    {% if debug_mode %}
                        {{ log("Removing clustering", info=True) }}
                    {% endif %}
                    ALTER TABLE {{ full_table_name }} DROP CLUSTERING KEY;
                {% endif %}
            {% endif %}
        {% endif %}
        
    {% else %}
        {% if debug_mode %}
            {{ log("Mode: VARIANT column", info=True) }}
        {% endif %}
        
        {% if not table_exists %}
            -- Create new VARIANT table (SAFE: only when table doesn't exist)
            {% if debug_mode %}
                {{ log("Creating new VARIANT table", info=True) }}
            {% endif %}
            CREATE TABLE {{ full_table_name }}
            {% if cluster_key and cluster_key|trim != "" %}
            CLUSTER BY ("{{ cluster_key }}")
            {% endif %}
            (
                FILE_NAME VARCHAR(16777216) COMMENT 'Source file name', 
                FILE_ROW_NUMBER NUMBER(38,0) COMMENT 'Row number in source file',
                FILE_CONTENT_KEY VARCHAR(16777216) COMMENT 'File content hash',
                FILE_LAST_MODIFIED_TIMESTAMP TIMESTAMP_NTZ(9) COMMENT 'File last modified time',
                LOADED_TIMESTAMP TIMESTAMP_NTZ(9) COMMENT 'When record was loaded',
                DATA VARIANT COMMENT 'Full record data in VARIANT format'
                {% if cluster_key and cluster_key|trim != "" %}
                ,"{{ cluster_key }}" {{ cluster_key_type }} COMMENT 'Clustering key column'
                {% endif %}
            );
        {% else %}
            -- Table exists - apply incremental changes safely
            {% if debug_mode %}
                {{ log("VARIANT table exists - applying safe incremental changes", info=True) }}
            {% endif %}
            
            -- Check if DATA column exists (should for VARIANT mode)
            {% set data_column_check_query %}
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_CATALOG = UPPER('{{ var("snowpipe_database") }}')
                AND TABLE_SCHEMA = UPPER('{{ source_name }}')
                AND TABLE_NAME = UPPER('{{ table_name }}')
                AND COLUMN_NAME = 'DATA'
            {% endset %}
            {%- call statement('data_column_check', fetch_result=True) %}{{ data_column_check_query }}{%- endcall -%}
            {%- set has_data_column = load_result('data_column_check')['data'][0][0] > 0 -%}
            
            {% if not has_data_column %}
                -- Add DATA column if missing (converting from individual columns to VARIANT)
                ALTER TABLE {{ full_table_name }} ADD COLUMN DATA VARIANT COMMENT 'Full record data in VARIANT format';
            {% endif %}
            
            -- Ensure metadata columns exist
            ALTER TABLE {{ full_table_name }} ADD COLUMN IF NOT EXISTS FILE_NAME VARCHAR(16777216) COMMENT 'Source file name';
            ALTER TABLE {{ full_table_name }} ADD COLUMN IF NOT EXISTS FILE_ROW_NUMBER NUMBER(38,0) COMMENT 'Row number in source file';
            ALTER TABLE {{ full_table_name }} ADD COLUMN IF NOT EXISTS FILE_CONTENT_KEY VARCHAR(16777216) COMMENT 'File content hash';
            ALTER TABLE {{ full_table_name }} ADD COLUMN IF NOT EXISTS FILE_LAST_MODIFIED_TIMESTAMP TIMESTAMP_NTZ(9) COMMENT 'File last modified time';
            ALTER TABLE {{ full_table_name }} ADD COLUMN IF NOT EXISTS LOADED_TIMESTAMP TIMESTAMP_NTZ(9) COMMENT 'When record was loaded';
            
            -- Handle cluster key column
            {% if cluster_key and cluster_key|trim != "" %}
                ALTER TABLE {{ full_table_name }} ADD COLUMN IF NOT EXISTS "{{ cluster_key }}" {{ cluster_key_type }} COMMENT 'Clustering key column';
            {% endif %}
            
            -- Update clustering if changed
            {% set current_cluster = get_table_cluster_key(source_name, table_name) %}
            {% set expected_cluster = ('LINEAR("' ~ cluster_key|upper ~ '")') if (cluster_key and cluster_key|trim != "") else '' %}
            
            {% if current_cluster != expected_cluster %}
                {% if cluster_key and cluster_key|trim != "" %}
                    {% if debug_mode %}
                        {{ log("Updating clustering to: " ~ cluster_key, info=True) }}
                    {% endif %}
                    ALTER TABLE {{ full_table_name }} CLUSTER BY ("{{ cluster_key }}");
                {% else %}
                    {% if debug_mode %}
                        {{ log("Removing clustering", info=True) }}
                    {% endif %}
                    ALTER TABLE {{ full_table_name }} DROP CLUSTERING KEY;
                {% endif %}
            {% endif %}
        {% endif %}
    {% endif %}
    
    -- Perform initial data load for new tables only
    {% if is_new_table %}
        {% if debug_mode %}
            {{ log("Performing initial data load for new table", info=True) }}
        {% endif %}
        {{ perform_initial_data_copy(table_name, use_individual_columns, stage_name, s3_dir_name, file_pattern, event_name, cluster_key_transformation, cluster_key, source_name, debug_mode) }}
    {% endif %}
    
    {% if debug_mode %}
        {{ log("Table " ~ full_table_name ~ " ready", info=True) }}
    {% endif %}
    
    -- End of table creation
    
{% endmacro %}