DROP TABLE IF EXISTS public.subscribers_restaurants;
CREATE TABLE IF NOT EXISTS public.subscribers_restaurants (
    id SERIAL NOT NULL,
    client_id VARCHAR NOT NULL,
    restaurant_id VARCHAR NOT NULL,
    CONSTRAINT pk_id PRIMARY KEY (id)
);