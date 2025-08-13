select *
from {{ ref('partner_name_mapping') }}
where source_system in ('Salesforce', 'Respond')