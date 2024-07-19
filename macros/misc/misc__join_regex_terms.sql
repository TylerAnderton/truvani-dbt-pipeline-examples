{% macro misc__join_regex_terms(regex_terms) %}
    
    {{ return(regex_terms|join('|')) }}

{% endmacro %}
