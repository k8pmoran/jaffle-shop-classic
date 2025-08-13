with
    complaints as (
        select * from {{ref('stg_complaints')}}
        where reference_number <> 'DEFAULT'
    ),

    claims as (
        select * from {{ref('int_claims')}}
    ),

    warranty as (
        select * from {{ref('int_warranties')}}
    ),

    dim_merchant as (
        select distinct
            partner_name,
            partner_country,
            partner_name_country,
            country_code
        from {{ref('stg_dim_merchant')}}
    ),

    partner_name_mapping as (
        select * from {{ref('stg_partner_name_mapping')}}
        where source_system in ('Salesforce', 'Respond')
    ),

    partner_mapping as (
        -- Join complaints with dim_merchant for partner values, use the partner name mapping seed as a fallback
        select 
            cp.*,
            coalesce(dm.partner_name, pm.partner_name) as partner_name,
            coalesce(dm.partner_country, pm.partner_country) as partner_country,
            coalesce(dm.partner_name_country, pm.partner_name_country) as partner_name_country,
            coalesce(dm.country_code, pm.partner_country_code) as partner_country_code
        from complaints as cp
        left join dim_merchant as dm on cp.source_partner_country = dm.partner_country and cp.source_partner_name = dm.partner_name
        left join partner_name_mapping as pm on cp.source_partner_name = pm.source_partner_name and cp.source_partner_country = pm.source_partner_country
    ),

    complaints_transformed as (
        select 
            reference_number,
            salesforce_ticket,
            -- Create source_system field based on reference_number
            case
                when reference_number like 'SF-%' then 'Salesforce'
                else 'Respond'
            end as source_system,
            -- Clean warranty_id field to fix common issues and improve matching with int_warranties
            case
                when warranty_id is null then null
                when warranty_id like 'RecuringPmtDef_%' then warranty_id
                when warranty_id like '[0-9]%' then 'RecuringPmtDef_' + warranty_id
                when warranty_id like '_[0-9]%' then 'RecuringPmtDef' + warranty_id
                else null
            end as warranty_id,
            -- Clean claim_id field to fix common issues and improve matching with int_claims
            case
                when claim_id is null then null
                when claim_id like 'claim_%' then claim_id
                when claim_id like 'laim_%' then 'c' + claim_id
                when claim_id like '[0-9]%' then 'claim_' + claim_id
                when claim_id like '_[0-9]%' then 'claim' + claim_id
                when claim_id like 'Claim %' then replace(claim_id, 'Claim ', '')
                else null
            end as claim_id,
            partner_name,
            partner_country,
            partner_name_country,
            partner_country_code,
            -- Fill in missing values in regulatory_process field for Salesforce data set
            case
                when reference_number like 'SF-%' and partner_country = 'United Kingdom' then 'FCA (UK)'
                when reference_number like 'SF-%' and partner_country is not null then 'MFSA (non-UK)'
                else regulatory_process
            end as regulatory_process,
            receipt_date,
            nullif(resolved_date, '1900-01-01') as resolved_date,
            case
                when resolved_date <> '1900-01-01' then 1
                else 0
            end as is_resolved,
            case
                when resolved_date <> '1900-01-01' then cast(working_days_to_resolve as int)
                when resolved_date = '1900-01-01' then cast({{working_days_between('receipt_date', 'GETDATE()')}} as int)
                else null
            end as working_days_to_resolve,
            case
                when resolved_date <> '1900-01-01' then cast(calendar_days_to_resolve as int)
                when resolved_date = '1900-01-01' then datediff(day, receipt_date, getdate())
                else null
            end as calendar_days_to_resolve,
            complaint_type,
            -- Flag if complaint is upheld or not
            case
                when overall_outcome in ('Upheld', 'Partially Upheld') then 1
                else 0
            end as is_upheld,
            case
                when overall_outcome in ('Not Upheld') then 1
                else 0
            end as is_not_upheld,
            case
                when overall_outcome in ('Withdrawn') then 1
                else 0
            end as is_withdrawn,
            case
                when overall_outcome is null then 'No outcome'
                else overall_outcome
            end as resolution_outcome,
            case
                when claims_related in ('Yes', 'Claims-Other cause', 'Claims-Rejection', 'Others') then 1
                else 0
            end as is_claim_related,
            case
                when claims_related in ('Yes', 'Claims-Other cause', 'Claims-Rejection', 'Others') then 0
                else 1
            end as is_sales_related,
            case
                when claims_related in ('Claims-Other cause', 'Claims-Rejection', 'Others', 'Yes') then 'Claim Related Complaints'
                else 'Non-Claim Related Complaints'
            end as claim_related_or_not_claim_related_complaints,
            primary_root_cause,
            secondary_root_cause,
            -- Flag if cost is required
            case
                when cost_required = 'Yes' then 1
                else 0
            end as is_cost_required,
            -- split values in cost_type field into flags
            case when CHARINDEX('DD Waiver', type) > 0 then 1 else 0 end as is_cost_type_dd_waiver,
            case when CHARINDEX('D&I (Goodwill)', type) > 0 then 1 else 0 end as is_cost_type_goodwill,
            case when CHARINDEX('Refund of Premiums', type) > 0 then 1 else 0 end as is_cost_type_premium_refund,
            case when CHARINDEX('Redress (Financial Loss)', type) > 0 then 1 else 0 end as is_cost_type_redress,
            type as cost_type, -- this field lists multiple cost types separated by ; the flags above are used to split these values out
            total_cost,
            -- Currency codes based on partner_country
            case
                when partner_country = 'Norway' then 'NOK'
                when partner_country = 'Sweden' then 'SEK'
                when partner_country = 'Denmark' then 'DKK'
                when partner_country = 'United Kingdom' then 'GBP'
                when partner_country = 'Hungary' then 'HUF'
                else 'EUR'
            end as currency,
            -- Flag if customer has been identified as vulnerable
            case
                when vulnerability_identified = 'True' then 1
                else 0
            end as is_vulnerable,
            updated
        from partner_mapping
    ),

    final as (
        select
            co.*,
            case
                when co.claim_id is not null and c.claim_id is not null then 1
                else 0
            end as is_claim_id_joined,
            case
                when co.warranty_id is not null and w.warranty_id is not null then 1
                else 0
            end as is_warranty_id_joined
        from complaints_transformed as co
        left join claims as c on co.claim_id = c.claim_id
        left join warranty as w on co.warranty_id = w.warranty_id
    )

select * from final