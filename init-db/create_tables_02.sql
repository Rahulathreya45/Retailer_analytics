\connect retailer_details;

CREATE TABLE public.retailers (
    id uuid NOT NULL,
    manufacturer_id uuid,
    buyer_account_id uuid,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    erp_code text,
    status text,
    state text,
    district text,
    sub_district text,
    sales_region text,
    zone text,
    mdt_hq text,
    legal_name text,
    first_name text,
    last_name text,
    phone_number text
);

\connect transaction_details;

CREATE TABLE public.farmer_lucky_draw (
    id uuid NOT NULL,
    ticket_number text,
    farmer_name text,
    farmer_mobile text,
    retailer_account_id uuid,
    product_id uuid,
    manufacturer_id uuid,
    quantity numeric,
    inventory_ledger_id uuid,
    notification_status text,
    notification_sent_at timestamp with time zone,
    created_at timestamp with time zone
);

CREATE TABLE public.retailer_inventory (
    id uuid NOT NULL,
    retailer_account_id uuid,
    product_id uuid,
    manufacturer_id uuid,
    stock_balance numeric,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);

CREATE TABLE public.retailer_ledger (
    id uuid NOT NULL,
    inventory_id uuid,
    retailer_account_id uuid,
    product_id uuid,
    manufacturer_id uuid,
    quantity_change numeric,
    balance_after numeric,
    ledger_type text,
    actor_type text,
    actor_id uuid,
    reference_id uuid,
    created_at timestamp with time zone
);