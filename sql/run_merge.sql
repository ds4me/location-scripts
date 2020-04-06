-- clean out main working table
truncate table mergeset;

--update externalparent_id and OpenMRSparent_id fields in masterlist from opensrp ids (the child parent link in OpenSRP)
update jurisdiction_master
set externalparentid = l.externalid, openmrsparentid = l.openmrs_id
from (
	select c.id,  p.externalid, p.openmrs_id
	from jurisdiction_master c
	inner join jurisdiction_master p on c.parentid = p.id
) l
where jurisdiction_master.id = l.id;

-- populate mergeset with existing hierarchy from master list
insert into mergeset (openSRP_Id, openMRS_Id,	openSRPParent_Id,openMRSParent_id ,externalId ,externalParentId ,status ,name ,name_en ,geographicLevel ,type ,coordinates )
select id, openmrs_id, parentid, openmrsparentid,externalid, externalparentid, status, name, '', geographicLevel, type, coordinates
from jurisdiction_master ;

-- insert into mergeset with new records from changeset
insert into mergeset (externalid,externalparentid,status, name, name_en, geographicLevel, type, coordinates , operation)
select 	externalId ,	externalParentId ,	status ,	name ,	name_en ,	geographicLevel ,	type ,	coordinates, 'i'
from changeset where externalid not in (select externalid from jurisdiction_master);

-- update mergeset with changeset changes
update mergeset m
set externalparentid =c.externalparentid  ,status =c.status, name = c.name, name_en =c.name_en, geographicLevel =c.geographicLevel, type =c.type, coordinates =c.coordinates , operation = 'u'
from changeset c
where c.externalid = m.externalid and m.operation is null ;

-- mark issues : external parent ids differ
update 		mergeset  set operation = 'x'
from 		jurisdiction_master ml
where 		ml.externalid = mergeset.externalid
and 		ml.externalparentid <>  mergeset.externalparentid
and 		mergeset.operation is not null;



--flag delta on name
update 		mergeset set d_name = true
from 		jurisdiction_master ml
where 		ml.externalid = mergeset.externalid
and 		ml.name <> mergeset.name
and 		mergeset.operation = 'u';

--flag delta on
update 		mergeset set d_status = true
from 		jurisdiction_master ml
where 		ml.externalid = mergeset.externalid
and 		ml.status <> mergeset.status
and 		mergeset.operation = 'u';

--flag delta on
update 		mergeset set d_externalparentid = true
from 		jurisdiction_master ml
where 		ml.externalid = mergeset.externalid
and 		ml.externalparentid <> mergeset.externalparentid
and 		mergeset.operation = 'u';

--flag delta on
update mergeset set d_type = true
from jurisdiction_master ml
where ml.externalid = mergeset.externalid
and ml.type <> mergeset.type
and mergeset.operation = 'u';

--flag delta on
update mergeset set d_coordinates = true
from jurisdiction_master ml
where ml.externalid = mergeset.externalid
and ml.coordinates <> mergeset.coordinates
and mergeset.operation = 'u';

--flag delta on
update mergeset set d_geographiclevel = true
from jurisdiction_master ml
where ml.externalid = mergeset.externalid
and ml.geographiclevel <> mergeset.geographiclevel
and mergeset.operation = 'u';

--assign uuid for new jurisdictions
update mergeset
set opensrp_id = uuid_generate_v4()
where operation = 'i' and opensrp_id is null;

--update mergeset opensrpparent_id from self
update mergeset
set opensrpparent_id = p.opensrp_id
from mergeset p
where mergeset.externalparentid = p.externalid and mergeset.opensrpparent_id is null;

--mark issues : no opensrp parent id
update mergeset
set operation = 'x'
where opensrpparent_id is null and geographiclevel > 0;

--temporary, don't process changes for those that don't need it so we an upload a list of all foci
-- update mergeset set operation = null
-- where
-- 	d_type = false and
-- 	d_status = false and
-- 	d_externalparentid = false and
-- 	d_name = false and
-- 	d_coordinates = false and
-- 	d_geographiclevel = false and
-- 	SUBSTRING(externalid,1,2) IN ('63','23','34')
