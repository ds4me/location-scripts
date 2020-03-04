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

DROP TABLE IF EXISTS jurisdiction_master;
CREATE TABLE jurisdiction_master
(
    id uuid,
    externalid character varying ,
    parentid uuid,
    status character varying ,
    name character varying ,
    geographiclevel integer,
    openmrs_id uuid,
    type character varying  ,
    coordinates character varying ,
    externalparentid character varying ,
    openmrsparentid uuid,
    operation character(1) 
);


DROP TABLE IF EXISTS changeset;
CREATE TABLE changeset
(
    id integer,
    externalid character varying ,
    externalparentid character varying ,
    status character varying ,
    name character varying ,
    name_en character varying ,
    geographiclevel integer,
    type character varying ,
    coordinates character varying 
);


DROP TABLE IF EXISTS structure_master;
CREATE TABLE structure_master
(
    id uuid,
    externalid character varying ,
    parentid uuid,
    status character varying ,
    name character varying ,
    geographiclevel integer,
    openmrs_id character varying ,
    type character varying ,
    coordinates character varying
);
