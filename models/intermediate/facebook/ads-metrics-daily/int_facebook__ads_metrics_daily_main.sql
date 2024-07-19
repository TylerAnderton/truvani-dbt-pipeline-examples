-- depends_on: {{ ref('stg_facebook_main__ads_insights') }}

{{ facebook__metrics(
    attn_truvani='attn',
    campaign_name_includes=['main campaign name'],
    daily=True,
    agg_level='ad'
) }}