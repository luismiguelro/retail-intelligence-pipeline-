{{ config(materialized='table') }}

with stg as (

    select * from {{ ref('stg_sales') }}

),

deduplicated as (

    select distinct on (outlet_id)
        outlet_id,
        outlet_type,
        outlet_size,
        outlet_tier,
        outlet_year_opened,
        outlet_age_years
    from stg
    order by outlet_id

),

final as (

    select
        md5(outlet_id)          as outlet_key,
        outlet_id,
        outlet_type,
        outlet_size,
        outlet_tier,
        outlet_year_opened,
        outlet_age_years,
        case
            when outlet_type = 'Grocery Store'      then 1
            when outlet_type = 'Supermarket Type1'  then 2
            when outlet_type = 'Supermarket Type2'  then 3
            when outlet_type = 'Supermarket Type3'  then 4
            else                                         0
        end                     as outlet_type_rank
    from deduplicated

)

select * from final
