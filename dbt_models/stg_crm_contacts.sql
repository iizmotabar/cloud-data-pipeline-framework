-- Staging model: types and deduplicates raw CRM contact records. This is the
-- first and only place null handling and dedup logic for this source should live.

with source as (

    select *
    from {{ source('raw', 'crm_contacts') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by contact_id
            order by _loaded_at desc
        ) as row_num

    from source

),

typed as (

    select
        contact_id,
        cast(email as string) as email,
        cast(company_domain as string) as company_domain,
        cast(created_at as timestamp) as created_at,
        cast(lifecycle_stage as string) as lifecycle_stage,
        cast(lead_score as int64) as lead_score

    from deduplicated
    where row_num = 1

)

select * from typed
