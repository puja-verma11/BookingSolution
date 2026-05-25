{% macro deduplicate(source_ref, unique_key, order_by='loaded_at') %}

    WITH source AS (
        SELECT * FROM {{ source_ref }}
    ),

    deduped AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ unique_key }}
                ORDER BY {{ order_by }} DESC
            ) AS row_num
        FROM source
    )

    SELECT * FROM deduped

{% endmacro %}
