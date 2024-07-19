{% macro utm_mapping__truvani_main_campaigns() %}

{% do return(
    {
        'truvani-main(,|$)':'[truvani] Main Campaign Main',
        'truvani-17-main(,|$)':'[truvani] Main Campaign 17',
        'truvani-main-brownies(,|$)':'[truvani] Main Brownies Campaign',
        'truvani-main-shop(,|$)':'[truvani] Main Shop Campaign',
        'truvani-main-icecream(,|$)':'[truvani] Main Ice Cream Campaign'
    }
) %}
    
{% endmacro %}