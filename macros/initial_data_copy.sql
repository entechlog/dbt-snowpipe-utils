{% macro perform_initial_data_copy(table_name, use_individual_columns, stage_name, s3_dir_name, file_pattern, event_name, cluster_key_transformation, cluster_key, source_name, debug_mode=False) %}
    
    {# Build fully qualified table name #}
    {% set full_table_name = var("snowpipe_database") ~ "." ~ source_name ~ "." ~ table_name %}
    
    {% if debug_mode %}
        {{ log("Performing initial data copy to: " ~ full_table_name, info=True) }}
    {% endif %}
    
    {% if use_individual_columns %}
        -- Individual columns mode: Use MATCH_BY_COLUMN_NAME with proper metadata mapping
        COPY INTO {{ full_table_name }}
        FROM @{{ stage_name }}/{{ s3_dir_name }}
        FILE_FORMAT = (FORMAT_NAME = '{{ get_file_format_name(file_pattern) }}')
        PATTERN = '{{ get_file_pattern(file_pattern) }}'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        INCLUDE_METADATA = (
            FILE_NAME = METADATA$FILENAME,
            FILE_ROW_NUMBER = METADATA$FILE_ROW_NUMBER,
            FILE_CONTENT_KEY = METADATA$FILE_CONTENT_KEY,
            FILE_LAST_MODIFIED_TIMESTAMP = METADATA$FILE_LAST_MODIFIED,
            LOADED_TIMESTAMP = METADATA$START_SCAN_TIME
        );
    {% else %}
        -- VARIANT mode: Use transformation with clustering support
        COPY INTO {{ full_table_name }}
        FROM (
            SELECT 
                metadata$filename AS FILE_NAME,
                metadata$file_row_number AS FILE_ROW_NUMBER,
                metadata$file_content_key AS FILE_CONTENT_KEY,
                metadata$file_last_modified AS FILE_LAST_MODIFIED_TIMESTAMP,
                metadata$start_scan_time AS LOADED_TIMESTAMP,
                $1 AS DATA
                {% if cluster_key and cluster_key|trim != "" and cluster_key_transformation and cluster_key_transformation|trim != "" %}
                ,{{ cluster_key_transformation }} AS "{{ cluster_key }}"
                {% endif %}
            FROM @{{ stage_name }}/{{ s3_dir_name }}
        )
        FILE_FORMAT = (FORMAT_NAME = '{{ get_file_format_name(file_pattern) }}')
        PATTERN = '{{ get_file_pattern(file_pattern) }}';
    {% endif %}
    
    {% if debug_mode %}
        {{ log("Initial data copy completed for: " ~ full_table_name, info=True) }}
    {% endif %}
    
{% endmacro %}