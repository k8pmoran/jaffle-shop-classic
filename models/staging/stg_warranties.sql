with source as (
    select * from {{ source('dbo', 'raw_warranties') }}
),

renamed as (

    select
        *
    from source

)

select * from renamed
