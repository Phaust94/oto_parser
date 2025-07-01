-- add column service

create or replace view otodom.listing_info_full
as
with otodom_full as (
    select
        ...
    from otodom.listing_items as a
    left join otodom.listing_metadata as b
    on (a.listing_id = b.listing_id)
    left join otodom.listing_ai_metadata as c
    on (a.listing_id = c.listing_id)

)
, olx_full as (
    select
        ...
    from otodom.listing_items as a
    left join otodom.listing_metadata as b
    on (a.listing_id = b.listing_id)
    left join otodom.listing_ai_metadata as c
    on (a.listing_id = c.listing_id)
)
, combined as (
    select *, 'otodom' as service from otodom_full
    UNION ALL
    select *, 'olx' as service from olx_full
)
, added_decisions as (
    select
        combined.*,
        ...
    from combined
    left join otodom.decisions as d
    on (a.listing_id = d.listing_id)
    left join otodom.irrelevant_listings as f
    on (a.listing_id = f.listing_id)
)
from added_decisions