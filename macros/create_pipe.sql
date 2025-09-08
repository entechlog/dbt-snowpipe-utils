{% macro create_pipe_sql(pipe_name, table_name, use_individual_columns, cluster_key_transformation, cluster_key_type, cluster_key, file_pattern, stage_name, s3_dir_name, event_name, source_name) %}
    
    -- Begin pipe creation
    
    {# Build fully qualified names #}
    {% set full_pipe_name = var("snowpipe_database") ~ "." ~ source_name ~ "." ~ pipe_name %}
    {% set full_table_name = var("snowpipe_database") ~ "." ~ source_name ~ "." ~ table_name %}
    {% set file_format = get_file_format_name(file_pattern) %}
    {% set file_pattern_regex = get_file_pattern(file_pattern) %}
    
    -- Drop existing pipe if it exists
    DROP PIPE IF EXISTS {{ full_pipe_name }};
    
    {% set enable_error_integration = var("snowpipe_enable_error_integration", false) %}
    
    -- Create the new pipe
    CREATE PIPE {{ full_pipe_name }}
    AUTO_INGEST = TRUE
    {% if enable_error_integration == true %}
    ERROR_INTEGRATION = {{ var("snowpipe_error_integration") }}
    {% endif %}
    AS 
    {% if use_individual_columns %}
    COPY INTO {{ full_table_name }}
    FROM @{{ stage_name }}/{{ s3_dir_name }}
    FILE_FORMAT = (FORMAT_NAME = '{{ file_format }}')
    PATTERN = '{{ file_pattern_regex }}'
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    INCLUDE_METADATA = (
        FILE_NAME = METADATA$FILENAME,
        FILE_ROW_NUMBER = METADATA$FILE_ROW_NUMBER,
        FILE_CONTENT_KEY = METADATA$FILE_CONTENT_KEY,
        FILE_LAST_MODIFIED_TIMESTAMP = METADATA$FILE_LAST_MODIFIED,
        LOADED_TIMESTAMP = METADATA$START_SCAN_TIME
    );
    {% else %}
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
    FILE_FORMAT = (FORMAT_NAME = '{{ file_format }}')
    PATTERN = '{{ file_pattern_regex }}';
    {% endif %}
    
    
    -- Initially pause the pipe for safety
    ALTER PIPE {{ full_pipe_name }} SET PIPE_EXECUTION_PAUSED = TRUE;
    
    -- End of pipe creation
    
{% endmacro %}