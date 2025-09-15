-- =============================================================================
-- CHANGE DETECTION UTILITIES
-- Detects configuration changes requiring table or pipe recreation
-- =============================================================================

{% macro detect_changes(pipe_exists, table_exists, schema_name, pipe_name, table_name, stage_name, cluster_key, cluster_key_transformation, use_individual_columns, file_pattern) %}
    {% set changes = {
        'requires_action': false,
        'create_table': false,
        'create_pipe': false,
        'recreate_table': false,
        'recreate_pipe': false,
        'reasons': []
    } %}
    
    {# Check for initial creation #}
    {% if not pipe_exists or not table_exists %}
        {% if not table_exists %}
            {% do changes.update({'create_table': true, 'requires_action': true}) %}
            {% do changes.reasons.append("Table missing") %}
        {% endif %}
        {% if not pipe_exists %}
            {% do changes.update({'create_pipe': true, 'requires_action': true}) %}
            {% do changes.reasons.append("Pipe missing") %}
        {% endif %}
    {% else %}
        {# Both exist - check for configuration changes #}
        
        {# Stage Check #}
        {% set current_stage = get_pipe_stage(schema_name, pipe_name) %}
        {% set expected_stage = stage_name|upper %}
        
        {% if current_stage != expected_stage %}
            {% do changes.update({'recreate_pipe': true, 'requires_action': true}) %}
            {% do changes.reasons.append("Stage changed") %}
        {% endif %}
        
        {# Clustering Check #}
        {% set current_cluster = get_table_cluster_key(schema_name, table_name) %}
        {% set expected_cluster = ('LINEAR("' ~ cluster_key|upper ~ '")') if (cluster_key and cluster_key|trim != "") else '' %}
        
        {% if current_cluster != expected_cluster %}
            {% do changes.update({'recreate_table': true, 'recreate_pipe': true, 'requires_action': true}) %}
            {% do changes.reasons.append("Clustering changed") %}
        {% endif %}
        
        {# Table Structure Check #}
        {% set has_data_column = check_column_exists(schema_name, table_name, 'DATA') %}
        {% set metadata_prefix = var("snowpipe_metadata_prefix", "_METADATA") %}
        {% set has_metadata_columns = check_column_exists(schema_name, table_name, metadata_prefix ~ '_FILE_PATH') %}
        
        {% if use_individual_columns and has_data_column and not has_metadata_columns %}
            {% do changes.update({'recreate_table': true, 'recreate_pipe': true, 'requires_action': true}) %}
            {% do changes.reasons.append("Converting to individual columns") %}
        {% elif not use_individual_columns and not has_data_column %}
            {% do changes.update({'recreate_table': true, 'recreate_pipe': true, 'requires_action': true}) %}
            {% do changes.reasons.append("Converting to VARIANT") %}
        {% endif %}
        
        {# Cluster Transformation Check #}
        {% if cluster_key and cluster_key|trim != "" and cluster_key_transformation and cluster_key_transformation|trim != "" %}
            {% set current_transformation = get_pipe_cluster_transformation(schema_name, pipe_name, cluster_key) %}
            
            {% if current_transformation != cluster_key_transformation %}
                {% do changes.update({'recreate_pipe': true, 'requires_action': true}) %}
                {% do changes.reasons.append("Cluster transformation changed") %}
            {% endif %}
        {% endif %}
        
        {# File Pattern Check #}
        {% set current_pattern = get_pipe_file_pattern(schema_name, pipe_name) %}
        {% set expected_pattern = get_file_pattern(file_pattern) %}
        
        {% if current_pattern != expected_pattern %}
            {% do changes.update({'recreate_pipe': true, 'requires_action': true}) %}
            {% do changes.reasons.append("File pattern changed") %}
        {% endif %}
    {% endif %}
    
    {{ return(changes) }}
{% endmacro %}

{# Note: This macro uses check_column_exists which should be implemented as a simple column existence check #}
{% macro check_column_exists(schema_name, table_name, column_name) %}
    {% set query %}
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = UPPER('{{ var("snowpipe_database") }}')
        AND TABLE_SCHEMA = UPPER('{{ schema_name }}')
        AND TABLE_NAME = UPPER('{{ table_name }}')
        AND COLUMN_NAME = UPPER('{{ column_name }}')
    {% endset %}
    {%- call statement('column_check', fetch_result=True) %}{{ query }}{%- endcall -%}
    {% set result = load_result('column_check')['data'][0][0] > 0 %}
    {{ return(result) }}
{% endmacro %}