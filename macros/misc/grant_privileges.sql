{% macro grant_privileges(database=target.dbname, schema=target.schema, user=target.user) %}
    
    {% set query %}
        grant all privileges on database {{database}} to {{user}};
        grant usage on schema {{schema}} to {{user}};
        grant select on all tables in schema {{schema}} to {{user}};
        grant usage, select on all sequences in schema {{schema}} to {{user}};
    {% endset %}

    {{ log(
        'Granting permissions on all tables & sequences in "' ~ database ~ '"."' ~ schema ~ '" to user ' ~ user ~ '.',
        info=True
    )}}
    {% do run_query(query)%}
    {{ log('Privileges granted.', info=True)}}

{% endmacro %}