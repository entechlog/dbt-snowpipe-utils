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
            
            CREATE TABLE {{ full_table_name }}
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(
                    INFER_SCHEMA(
                        LOCATION => '@{{ stage_name }}/{{ s3_dir_name }}',
                        FILE_FORMAT => '{{ get_file_format_name(file_pattern) }}'
                    )
                )
            )
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
            
            {% if not has_metadata %}
                -- Add metadata columns
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
        
        -- Create or replace VARIANT table
        CREATE OR REPLACE TABLE {{ full_table_name }}
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