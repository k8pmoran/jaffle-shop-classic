with source as (
    select * from {{ source('dbo', 'raw_claims') }}
),

renamed as (

    select
        claim_id,
        warranty_id,
        cast(create_date as date),
        cast(decision_date as date),
        partner_country,
        partner_name_country,
        claim_type,
        process_type,
        status,
        resolution,
        reason_code,
        reason_code_detail,
        resolution_type,
        resolution_agent_name,
        file_location,
        approval_location,
        entity,
        insurance_product as is_insurance_product,
        referral_flag as is_referred
    from source

)

select * from renamed
