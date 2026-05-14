\connect retailer_details;

COPY public.retailers
FROM '/docker-entrypoint-initdb.d/data/retailers_data.csv'
DELIMITER ','
CSV HEADER;

\connect transaction_details;

COPY public.farmer_lucky_draw
FROM '/docker-entrypoint-initdb.d/data/lucky_draw.csv'
DELIMITER ','
CSV HEADER;

COPY public.retailer_inventory
FROM '/docker-entrypoint-initdb.d/data/retailers_inventory.csv'
DELIMITER ','
CSV HEADER;

COPY public.retailer_ledger
FROM '/docker-entrypoint-initdb.d/data/retailers_ledger.csv'
DELIMITER ','
CSV HEADER;