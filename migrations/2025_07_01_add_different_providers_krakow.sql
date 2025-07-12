create table otodom_krakow.listing_items_2 (
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

insert into otodom_krakow.listing_items_2
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
from otodom_krakow.listing_items
;

rename table otodom_krakow.listing_items to otodom_krakow.listing_items_bak, otodom_krakow.listing_items_2 to otodom_krakow.listing_items
;

alter table otodom_krakow.listing_metadata
modify listing_id varchar(100)
;

alter table otodom_krakow.decisions
modify listing_id varchar(100)
;

alter table otodom_krakow.irrelevant_listings
modify listing_id varchar(100)
;

alter table otodom_krakow.listing_ai_metadata
modify listing_id varchar(100)
;

alter table otodom_krakow.irrelevant_listings
add column service varchar(20) default 'otodom'
;

alter table otodom_krakow.decisions
add column service varchar(20) default 'otodom'
;

DROP TABLE IF EXISTS otodom_krakow.listing_items_olx
;
CREATE TABLE otodom_krakow.listing_items_olx (
  `listing_id` varchar(100) NOT NULL,
  `title` varchar(255) NOT NULL,
  `slug` varchar(255) NOT NULL,
  `rent_price` decimal(10,2) DEFAULT NULL,
  `district` varchar(255) DEFAULT NULL,
  `floor` int DEFAULT NULL,
  `description_long` mediumtext NOT NULL,
  `raw_info` longtext NOT NULL,
  `allowed_with_pets` tinyint(1) DEFAULT NULL,
  `n_rooms` int DEFAULT NULL,
  `administrative_price` decimal(10,2) DEFAULT NULL,
  `area_m2` decimal(10,2) DEFAULT NULL,
  `longitude` varchar(30) DEFAULT NULL,
  `latitude` varchar(30) DEFAULT NULL,
  `distance_from_center_km` double NOT NULL DEFAULT '0',
  `has_lift` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`listing_id`),
  UNIQUE KEY `slug` (`slug`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
;

DROP TABLE IF EXISTS otodom_krakow.listing_metadata_olx;

DROP TABLE IF EXISTS otodom_krakow.listing_ai_metadata_olx;
CREATE TABLE otodom_krakow.listing_ai_metadata_olx (
  `listing_id` varchar(100) NOT NULL,
  `allowed_with_pets` tinyint(1) DEFAULT NULL,
  `availability_date` varchar(50) DEFAULT NULL,
  `bedroom_number` int DEFAULT NULL,
  `occasional_lease` tinyint(1) DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `kitchen_combined_with_living_room` tinyint(1) DEFAULT NULL,
  `deposit` int DEFAULT NULL,
  `has_ac` tinyint(1) DEFAULT NULL,
  `street` varchar(255) DEFAULT NULL,
  `street_number` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`listing_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
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
        concat('https://www.otodom_krakow.pl/pl/oferta/', `a`.`slug`) AS `url`
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
)
, combined as (
    select *, CONVERT('otodom' using utf8mb4) as service from otodom_full
    UNION ALL
    select *, CONVERT('olx' using utf8mb4)as service from olx_full
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
