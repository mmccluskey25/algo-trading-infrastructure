{%- macro external_location(relation, config) -%}
  {%- set folder = model.fqn[1] if model.fqn | length >= 2 else 'default' -%}
  {%- set layer_map = {'staging': 'silver', 'intermediate': 'silver', 'marts': 'gold'} -%}
  {%- set layer = layer_map.get(folder, folder) -%}
  {%- set format = config.get('format', 'parquet') -%}
  {%- if config.get('options', {}).get('partition_by') is none -%}
    {{- adapter.external_root() }}/{{ layer }}/{{ relation.identifier }}.{{ format }}
  {%- else -%}
    {{- adapter.external_root() }}/{{ layer }}/{{ relation.identifier }}
  {%- endif -%}
{%- endmacro -%}
