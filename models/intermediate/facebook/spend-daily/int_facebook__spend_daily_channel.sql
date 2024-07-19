with 

fb_he1 as (
	{{ facebook__spend(
        daily=True,
        account='he1'
    ) }}
),

fb_ads as (
	{{ facebook__spend(
        daily=True,
        account='main'
    ) }}
),

final as (

    select 
        report_date,

        round(
            (
                coalesce(fb_he1.spend::numeric, 0) 
                + 
                coalesce(fb_ads.spend::numeric, 0)
            )::numeric,
            2
        ) as spend

    from fb_he1 

    full join fb_ads
        using(report_date)

)

select *
from final
order by report_date desc