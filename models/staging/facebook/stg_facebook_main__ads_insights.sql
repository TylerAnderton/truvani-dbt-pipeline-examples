with

source as (

    select * from {{ source('facebook', 'fb_ads_ads') }}

)

select * 
from source