{% for i, item in entries.iterrows() -%}
touch -m -t {{ item.datetime|datetimeformat("%Y%m%d%H%M.%S") }} "{{ item.fullpath }}"
{% endfor %}