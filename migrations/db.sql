create database otodom;

CREATE TABLE otodom.listing_items (
    `listing_id` BIGINT PRIMARY KEY, -- Maps to listing_id: int, assuming it's a unique identifier
    `title` VARCHAR(255) NOT NULL, -- Maps to title: str, assuming a title is always present and fits in 255 chars
    `slug` VARCHAR(255) UNIQUE NOT NULL, -- Maps to slug: str, assuming it's unique and always present
    `rent_price` DECIMAL(10, 2) NULL, -- Maps to rent_price: float | None, using DECIMAL for currency, allowing NULL
    `administrative_price` DECIMAL(10, 2) NULL, -- Maps to administrative_price: float | None, using DECIMAL, allowing NULL
    `area_m2` DECIMAL(10, 2) NULL, -- Maps to area_m2: float | None, using DECIMAL for precision, allowing NULL
    `n_rooms` INT NULL, -- Maps to n_rooms: int | None, allowing NULL
    `street` VARCHAR(255) NULL, -- Maps to street: str | None, allowing NULL
    `street_number` VARCHAR(50) NULL, -- Maps to street_number: str | None, allowing NULL (adjust length if needed)
    `district` VARCHAR(255) NULL, -- Maps to district: str | None, allowing NULL
    `district_specific` VARCHAR(255) NULL, -- Maps to district_specific: str | None, allowing NULL
    `created_on` DATETIME NULL -- Maps to created_on: datetime.datetime | None, allowing NULL
);


CREATE TABLE otodom.listing_metadata (
    listing_id INT NOT NULL,
    floor INT NULL,
    floors_total INT NULL,
    deposit INT NULL,
    has_ac TINYINT(1) NOT NULL,
    has_lift TINYINT(1) NOT NULL,
    windows VARCHAR(255) NULL,
    description_long MEDIUMTEXT NOT NULL,
    raw_info LONGTEXT NOT NULL,
    PRIMARY KEY (listing_id)
);


CREATE TABLE otodom.listing_ai_metadata (
    `listing_id` BIGINT PRIMARY KEY,
    allowed_with_pets TINYINT(1) NULL,
    availability_date VARCHAR(50) NULL,
    bedroom_number INT NULL
);


