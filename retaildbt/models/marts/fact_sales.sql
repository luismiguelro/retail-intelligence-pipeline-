{{ config(materialized='table') }}

with stg as (

    select * from {{ ref('stg_sales') }}

),

-- Un registro por combinación ítem × outlet (grano del fact)
deduped as (

    select distinct on (item_id, outlet_id)
        item_id,
        outlet_id,
        item_mrp,
        item_shelf_fraction,
        item_weight_kg
    from stg
    order by item_id, outlet_id

),

with_keys as (

    select
        d.item_id,
        d.outlet_id,
        p.product_key,
        o.outlet_key,
        p.price_tier,
        o.outlet_type_rank,
        d.item_mrp,
        d.item_shelf_fraction,
        d.item_weight_kg,
        -- Métrica derivada: potencial de ingreso ponderado por visibilidad en góndola
        round(d.item_mrp * d.item_shelf_fraction, 4)    as weighted_revenue_potential
    from deduped                   d
    inner join {{ ref('dim_product') }} p on p.item_id   = d.item_id
    inner join {{ ref('dim_outlet') }}  o on o.outlet_id = d.outlet_id

)

select
    md5(item_id || '|' || outlet_id)   as fact_key,
    product_key,
    outlet_key,
    item_id,
    outlet_id,
    price_tier,
    outlet_type_rank,
    item_mrp,
    item_shelf_fraction,
    item_weight_kg,
    weighted_revenue_potential
from with_keys
