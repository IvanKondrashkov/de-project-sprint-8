DROP TABLE IF EXISTS public.subscribers_feedback;
CREATE TABLE IF NOT EXISTS public.subscribers_feedback (
    id SERIAL NOT NULL,
    restaurant_id TEXT NOT NULL,
    adv_campaign_id TEXT NOT NULL,
    adv_campaign_content TEXT NOT NULL,
    adv_campaign_owner TEXT NOT NULL,
    adv_campaign_owner_contact TEXT NOT NULL,
    adv_campaign_datetime_start INT NOT NULL,
    adv_campaign_datetime_end INT NOT NULL,
    datetime_created INT NOT NULL,
    client_id TEXT NOT NULL,
    trigger_datetime_created INT NOT NULL,
    feedback VARCHAR NULL,
    CONSTRAINT id_pk PRIMARY KEY (id)
);