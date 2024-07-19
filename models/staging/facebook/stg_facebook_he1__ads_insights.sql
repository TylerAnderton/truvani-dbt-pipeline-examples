with

source as (

    select * from {{ source('facebook', 'fb_he1_ads') }}

)

select * 
from source