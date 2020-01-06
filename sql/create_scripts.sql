DROP TABLE IF EXISTS mergeset;

CREATE TABLE mergeset
(
    id serial,
    opensrp_id uuid,
    openmrs_id uuid,
    opensrpparent_id uuid,
    openmrsparent_id uuid,
    externalid character varying ,
    externalparentid character varying ,
    status character varying ,
    name character varying ,
    name_en character varying ,
    geographiclevel integer,
    type character varying ,
    coordinates character varying ,
    operation character(1) ,
    processed_date timestamp without time zone,
    processed boolean
);


