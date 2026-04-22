{{ config(materialized='view') }}

with source as (

    select * from {{ source('public', 'clean_sales') }}

),

renamed as (

    select
        -- Identificadores
        item_identifier                                     as item_id,
        outlet_identifier                                   as outlet_id,

        -- Atributos del ítem
        item_type                                           as item_category,
        item_fat_content                                    as fat_content,
        round(cast(item_weight     as numeric), 3)          as item_weight_kg,
        round(cast(item_visibility as numeric), 4)          as item_shelf_fraction,
        round(cast(item_mrp        as numeric), 2)          as item_mrp,

        -- Atributos del outlet
        outlet_type,
        outlet_size,
        outlet_location_type                                as outlet_tier,
        outlet_establishment_year                           as outlet_year_opened,

        -- Métricas derivadas simples
        date_part('year', current_date)
            - outlet_establishment_year                     as outlet_age_years

    from source

)

select * from renamed
