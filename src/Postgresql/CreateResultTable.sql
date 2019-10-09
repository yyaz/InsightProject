DROP TABLE table_final;
CREATE TABLE IF NOT EXISTS table_final
(
   customer_id   varchar(5)   NOT NULL,
   truck_id      varchar(8)   NOT NULL,
   warehouse_id  varchar(5)   NOT NULL,
   wh_lon        float        NOT NULL,
   wh_lat        float        NOT NULL,
   lon           float        NOT NULL,
   lat           float        NOT NULL,
   distance      float        NOT NULL,
   timestamp     char(23)  NOT NULL
);


