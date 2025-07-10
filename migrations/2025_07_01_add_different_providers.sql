-- add column service

create table otodom.listing_items_2 (
  `listing_id` varchar(100) NOT NULL,
  `title` varchar(255) NOT NULL,
  `slug` varchar(255) NOT NULL,
  `rent_price` decimal(10,2) DEFAULT NULL,
  `administrative_price` decimal(10,2) DEFAULT NULL,
  `area_m2` decimal(10,2) DEFAULT NULL,
  `n_rooms` int DEFAULT NULL,
  `street` varchar(255) DEFAULT NULL,
  `street_number` varchar(50) DEFAULT NULL,
  `district` varchar(255) DEFAULT NULL,
  `district_specific` varchar(255) DEFAULT NULL,
  `created_on` datetime DEFAULT NULL,
  PRIMARY KEY (`listing_id`),
  UNIQUE KEY `slug` (`slug`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
;

insert into otodom.listing_items_2
select
 cast(listing_id as CHAR),
  title,
  slug,
  rent_price,
  administrative_price,
  area_m2,
  n_rooms,
  street,
  street_number,
  district,
  district_specific,
  created_on
from otodom.listing_items
;

rename table otodom.listing_items to otodom.listing_items_bak, otodom.listing_items_2 to otodom.listing_items
;

alter table otodom.listing_metadata
modify listing_id varchar(100)
;

alter table otodom.decisions
modify listing_id varchar(100)
;

alter table otodom.irrelevant_listings
modify listing_id varchar(100)
;

alter table otodom.listing_ai_metadata
modify listing_id varchar(100)
;

alter table otodom.irrelevant_listings
add column service varchar(20) default 'otodoom'
;

CREATE TABLE otodom.listing_items_olx (
  `listing_id` varchar(100) NOT NULL,
  `title` varchar(255) NOT NULL,
  `slug` varchar(255) NOT NULL,
  `rent_price` decimal(10,2) DEFAULT NULL,
  `district` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`listing_id`),
  UNIQUE KEY `slug` (`slug`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
;

CREATE TABLE otodom.listing_metadata_olx (
  `listing_id` varchar(100) NOT NULL,
  `floor` int DEFAULT NULL,
  `description_long` mediumtext NOT NULL,
  `raw_info` longtext NOT NULL,
  `allowed_with_pets` tinyint(1) DEFAULT NULL,
  `n_rooms` int DEFAULT NULL,
  `administrative_price` decimal(10,2) DEFAULT NULL,
  `area_m2` decimal(10,2) DEFAULT NULL,
  PRIMARY KEY (`listing_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
;

CREATE TABLE otodom.listing_ai_metadata_olx (
  `listing_id` varchar(100) NOT NULL,
  `allowed_with_pets` tinyint(1) DEFAULT NULL,
  `availability_date` varchar(50) DEFAULT NULL,
  `bedroom_number` int DEFAULT NULL,
  `occasional_lease` tinyint(1) DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `kitchen_combined_with_living_room` tinyint(1) DEFAULT NULL,
  `deposit` int DEFAULT NULL,
  `has_ac` tinyint(1) DEFAULT NULL,
  `has_lift` tinyint(1) DEFAULT NULL,
  `street` varchar(255) DEFAULT NULL,
  `street_number` varchar(50) DEFAULT NULL,
  `longitude` varchar(30) DEFAULT NULL,
  `latitude` varchar(30) DEFAULT NULL,
  `distance_from_center_km` double NOT NULL DEFAULT '0',
  PRIMARY KEY (`listing_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
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
            when (`b`.`listing_id` is not null) then true
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
        `b`.`administrative_price` AS `administrative_price`,
        (`a`.`rent_price` + coalesce(`b`.`administrative_price`, 0)) AS `total_rent_price`,
        `b`.`area_m2` AS `area_m2`,
        `b`.`n_rooms` AS `n_rooms`,
        `c`.`street` AS `street`,
        `c`.`street_number` AS `street_number`,
        `a`.`district` AS `district`,
        `a`.`district` AS `district_specific`,
        NULL AS `created_on`,
        (case
            when (`b`.`listing_id` is not null) then true
            else false
        end) AS `scraped`,
        `b`.`floor` AS `floor`,
        NULL AS `floors_total`,
        `c`.`deposit` AS `deposit`,
        `c`.`has_ac` AS `has_ac`,
        `c`.`has_lift` AS `has_lift`,
        NULL AS `windows`,
        cast(`c`.`latitude` as float) AS `latitude`,
        cast(`c`.`longitude` as float) AS `longitude`,
        `b`.`description_long` AS `description_long`,
        coalesce(`b`.`allowed_with_pets`, `c`.`allowed_with_pets`) AS `allowed_with_pets`,
        `c`.`availability_date` AS `availability_date`,
        `c`.`bedroom_number` AS `bedroom_number`,
        `c`.`occasional_lease` AS `occasional_lease`,
        `c`.`kitchen_combined_with_living_room` AS `kitchen_combined_with_living_room`,
        `c`.`updated_at` AS `parsed_on`,
        `c`.`distance_from_center_km` AS `distance_from_center_km`,
        concat('https://www.olx.pl/d/oferta/', `a`.`slug`, '.html') AS `url`
    from otodom.listing_items_olx as a
    left join otodom.listing_metadata_olx as b
    on (a.listing_id = b.listing_id)
    left join otodom.listing_ai_metadata_olx as c
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
from added_decisions
;



...the same for Krakow