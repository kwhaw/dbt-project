{% macro cents_to_dollars(col_name, decimal_places=2 ) %}

round(1.0*{{col_name}} / 100, {{ decimal_places}}) 

{% endmacro %}
