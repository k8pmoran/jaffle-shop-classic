with source as (
    select * from {{ source('dbo', 'raw_complaints') }}
),

complaint as (
    select
        complaints_id,
        reference_number,
        merchant_country as source_partner_country,
        merchant_name as source_partner_name,
        warranty_id,
        salesforce_ticket,
        cast(receipt_date as date) as receipt_date,
        complaint_type,
        claims_related,
        claim_id,
        vulnerability_identified,
        description,
        overall_outcome,
        outcome_details,
        cast(resolved_date as date) as resolved_date,
        primary_root_cause,
        secondary_root_cause,
        cost_required,
        type,
    /*
    Commented out the 'cost_amount' field as it is potentially misleading.
    The 'type' field contains multiple cost types separated by a semicolon.
    Only the cost of the first type is reflected in 'cost_amount'.
    Use the 'total_cost' field for the accurate total cost.
    */
        -- cost_amount,
        total_cost,
        derived_working_days_to_resolve as working_days_to_resolve,
        elapsed_days_to_resolve as calendar_days_to_resolve,
        regulatory_process,
        regulated_complaint_type,
        file_name,
        dwid,
        active_flag,
        created,
        created_by,
        updated,
        updated_by

    from source
)
select * from complaint
where active_flag = 'Y'
  
