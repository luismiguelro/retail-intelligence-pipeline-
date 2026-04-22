{{ config(materialized='table') }}

with stg as (

    select * from {{ ref('stg_sales') }}

),

deduplicated as (

    select distinct on (item_id)
        item_id,
        item_category,
        fat_content,
        item_weight_kg,
        item_mrp
    from stg
    order by item_id

),

final as (

    select
        md5(item_id)            as product_key,
        item_id,
        item_category,
        fat_content,
        item_weight_kg,
        item_mrp,
        case
            when item_mrp < 70  then 'Budget'
            when item_mrp < 140 then 'Mid-Range'
            else                     'Premium'
        end                     as price_tier
    from deduplicated

)

select * from final
