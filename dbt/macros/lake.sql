{#
  Reference a Delta table by PATH rather than through the metastore.

  dbt-spark's session method builds its own SparkSession in its own process,
  and Hive's Derby metastore resolves relative to the process working
  directory -- which dbt changes to the project dir. The result is that tables
  registered by the Spark jobs are invisible to dbt, with no error beyond
  "table not found". Rather than keep two processes agreeing about a metastore,
  read the storage directly: Delta's `delta.`path`` syntax needs no catalog at
  all, and the path IS the contract.
#}
{% macro lake(relpath) %}
    delta.`{{ var('lake_root') }}/{{ relpath }}`
{%- endmacro %}
