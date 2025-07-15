alter table otodom.listing_items_olx
add column slug_external varchar(200) NULL DEFAULT NULL
;

create or replace view otodom.listing_info_full
as
with otodom_full as (
    select
        `a`.`listing_id` AS `listing_id`,
        `a`.`title` AS `title`,
        `a`.`slug` AS `slug`,
        `a`.`rent_price` AS `rent_price`,
        `a`.`administrative_price` AS `administrative_price`,
        (`a`.`rent_price` + coalesce(`a`.`administrative_price`, 0)) AS `total_rent_price`,
        `a`.`area_m2` AS `area_m2`,
        `a`.`n_rooms` AS `n_rooms`,
        `a`.`street` AS `street`,
        `a`.`street_number` AS `street_number`,
        `a`.`district` AS `district`,
        `a`.`district_specific` AS `district_specific`,
        `a`.`created_on` AS `created_on`,
        (case
            when (`a`.`listing_id` is not null) then true
            else false
        end) AS `scraped`,
        `b`.`floor` AS `floor`,
        `b`.`floors_total` AS `floors_total`,
        `b`.`deposit` AS `deposit`,
        `b`.`has_ac` AS `has_ac`,
        `b`.`has_lift` AS `has_lift`,
        `b`.`windows` AS `windows`,
        cast(`b`.`latitude` as float) AS `latitude`,
        cast(`b`.`longitude` as float) AS `longitude`,
        `b`.`description_long` AS `description_long`,
        `c`.`allowed_with_pets` AS `allowed_with_pets`,
        coalesce(`b`.`available_from`, `c`.`availability_date`) AS `availability_date`,
        `c`.`bedroom_number` AS `bedroom_number`,
        `c`.`occasional_lease` AS `occasional_lease`,
        `c`.`kitchen_combined_with_living_room` AS `kitchen_combined_with_living_room`,
        `c`.`updated_at` AS `parsed_on`,
        `b`.`distance_from_center_km` AS `distance_from_center_km`,
        concat('https://www.otodom.pl/pl/oferta/', `a`.`slug`) AS `url`
    from otodom.listing_items as a
    left join otodom.listing_metadata as b
    on (a.listing_id = b.listing_id)
    left join otodom.listing_ai_metadata as c
    on (a.listing_id = c.listing_id)
)
, olx_full as (
    select
        `a`.`listing_id` AS `listing_id`,
        `a`.`title` AS `title`,
        `a`.`slug` AS `slug`,
        `a`.`rent_price` AS `rent_price`,
        `a`.`administrative_price` AS `administrative_price`,
        (`a`.`rent_price` + coalesce(`a`.`administrative_price`, 0)) AS `total_rent_price`,
        `a`.`area_m2` AS `area_m2`,
        `a`.`n_rooms` AS `n_rooms`,
        `c`.`street` AS `street`,
        `c`.`street_number` AS `street_number`,
        `a`.`district` AS `district`,
        `a`.`district` AS `district_specific`,
        NULL AS `created_on`,
        true AS `scraped`,
        `a`.`floor` AS `floor`,
        NULL AS `floors_total`,
        `c`.`deposit` AS `deposit`,
        `c`.`has_ac` AS `has_ac`,
        `a`.`has_lift` AS `has_lift`,
        NULL AS `windows`,
        cast(`a`.`latitude` as float) AS `latitude`,
        cast(`a`.`longitude` as float) AS `longitude`,
        `a`.`description_long` AS `description_long`,
        coalesce(`a`.`allowed_with_pets`, `c`.`allowed_with_pets`) AS `allowed_with_pets`,
        `c`.`availability_date` AS `availability_date`,
        `c`.`bedroom_number` AS `bedroom_number`,
        `c`.`occasional_lease` AS `occasional_lease`,
        `c`.`kitchen_combined_with_living_room` AS `kitchen_combined_with_living_room`,
        `c`.`updated_at` AS `parsed_on`,
        `a`.`distance_from_center_km` AS `distance_from_center_km`,
        concat('https://www.olx.pl/d/oferta/', `a`.`slug`, '.html') AS `url`
    from otodom.listing_items_olx as a
    left join otodom.listing_ai_metadata_olx as c
    on (a.listing_id = c.listing_id)
    where 1=1
    and a.slug_external not in (select slug from otodom.listing_items)
)
, combined as (
    select *, CONVERT('otodom' using utf8mb4) as service from otodom_full
    UNION ALL
    select *, CONVERT('olx' using utf8mb4) as service from olx_full
)
, added_decisions as (
    select
        a.*,
        (case
            when (`f`.`listing_id` is not null) then true
            else false
        end) AS `irrelevant`,
        `d`.`our_decision` AS `our_decision`
    from combined as a
    left join otodom.decisions as d
    on (a.listing_id = d.listing_id and a.service = d.service)
    left join otodom.irrelevant_listings as f
    on (a.listing_id = f.listing_id and a.service = f.service)
)
select *
from added_decisions
;


alter table otodom_krakow.listing_items_olx
add column slug_external varchar(200) NULL DEFAULT NULL
;

create or replace view otodom_krakow.listing_info_full
as
with otodom_full as (
    select
        `a`.`listing_id` AS `listing_id`,
        `a`.`title` AS `title`,
        `a`.`slug` AS `slug`,
        `a`.`rent_price` AS `rent_price`,
        `a`.`administrative_price` AS `administrative_price`,
        (`a`.`rent_price` + coalesce(`a`.`administrative_price`, 0)) AS `total_rent_price`,
        `a`.`area_m2` AS `area_m2`,
        `a`.`n_rooms` AS `n_rooms`,
        `a`.`street` AS `street`,
        `a`.`street_number` AS `street_number`,
        `a`.`district` AS `district`,
        `a`.`district_specific` AS `district_specific`,
        `a`.`created_on` AS `created_on`,
        (case
            when (`a`.`listing_id` is not null) then true
            else false
        end) AS `scraped`,
        `b`.`floor` AS `floor`,
        `b`.`floors_total` AS `floors_total`,
        `b`.`deposit` AS `deposit`,
        `b`.`has_ac` AS `has_ac`,
        `b`.`has_lift` AS `has_lift`,
        `b`.`windows` AS `windows`,
        cast(`b`.`latitude` as float) AS `latitude`,
        cast(`b`.`longitude` as float) AS `longitude`,
        `b`.`description_long` AS `description_long`,
        `c`.`allowed_with_pets` AS `allowed_with_pets`,
        coalesce(`b`.`available_from`, `c`.`availability_date`) AS `availability_date`,
        `c`.`bedroom_number` AS `bedroom_number`,
        `c`.`occasional_lease` AS `occasional_lease`,
        `c`.`kitchen_combined_with_living_room` AS `kitchen_combined_with_living_room`,
        `c`.`updated_at` AS `parsed_on`,
        `b`.`distance_from_center_km` AS `distance_from_center_km`,
        concat('https://www.otodom.pl/pl/oferta/', `a`.`slug`) AS `url`
    from otodom_krakow.listing_items as a
    left join otodom_krakow.listing_metadata as b
    on (a.listing_id = b.listing_id)
    left join otodom_krakow.listing_ai_metadata as c
    on (a.listing_id = c.listing_id)
)
, olx_full as (
    select
        `a`.`listing_id` AS `listing_id`,
        `a`.`title` AS `title`,
        `a`.`slug` AS `slug`,
        `a`.`rent_price` AS `rent_price`,
        `a`.`administrative_price` AS `administrative_price`,
        (`a`.`rent_price` + coalesce(`a`.`administrative_price`, 0)) AS `total_rent_price`,
        `a`.`area_m2` AS `area_m2`,
        `a`.`n_rooms` AS `n_rooms`,
        `c`.`street` AS `street`,
        `c`.`street_number` AS `street_number`,
        `a`.`district` AS `district`,
        `a`.`district` AS `district_specific`,
        NULL AS `created_on`,
        true AS `scraped`,
        `a`.`floor` AS `floor`,
        NULL AS `floors_total`,
        `c`.`deposit` AS `deposit`,
        `c`.`has_ac` AS `has_ac`,
        `a`.`has_lift` AS `has_lift`,
        NULL AS `windows`,
        cast(`a`.`latitude` as float) AS `latitude`,
        cast(`a`.`longitude` as float) AS `longitude`,
        `a`.`description_long` AS `description_long`,
        coalesce(`a`.`allowed_with_pets`, `c`.`allowed_with_pets`) AS `allowed_with_pets`,
        `c`.`availability_date` AS `availability_date`,
        `c`.`bedroom_number` AS `bedroom_number`,
        `c`.`occasional_lease` AS `occasional_lease`,
        `c`.`kitchen_combined_with_living_room` AS `kitchen_combined_with_living_room`,
        `c`.`updated_at` AS `parsed_on`,
        `a`.`distance_from_center_km` AS `distance_from_center_km`,
        concat('https://www.olx.pl/d/oferta/', `a`.`slug`, '.html') AS `url`
    from otodom_krakow.listing_items_olx as a
    left join otodom_krakow.listing_ai_metadata_olx as c
    on (a.listing_id = c.listing_id)
    where 1=1
    and a.slug_external not in (select slug from otodom_krakow.listing_items)
)
, combined as (
    select *, CONVERT('otodom' using utf8mb4) as service from otodom_full
    UNION ALL
    select *, CONVERT('olx' using utf8mb4) as service from olx_full
)
, added_decisions as (
    select
        a.*,
        (case
            when (`f`.`listing_id` is not null) then true
            else false
        end) AS `irrelevant`,
        `d`.`our_decision` AS `our_decision`
    from combined as a
    left join otodom_krakow.decisions as d
    on (a.listing_id = d.listing_id and a.service = d.service)
    left join otodom_krakow.irrelevant_listings as f
    on (a.listing_id = f.listing_id and a.service = f.service)
)
select *
from added_decisions
;