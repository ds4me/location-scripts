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

update structure_master
set externalparentid = l.externalid
from (
	select c.id,  p.externalid
	from structure_master c
	inner join jurisdiction_master p on c.parentid = p.id
) l
where structure_master.id = l.id;

-- populate mergeset with existing hierarchy from master list
insert into mergeset (openSRP_Id, openMRS_Id,	openSRPParent_Id,openMRSParent_id ,externalId ,externalParentId ,status ,name ,name_en ,geographicLevel ,type ,coordinates )
select id, openmrs_id, parentid, openmrsparentid,externalid, externalparentid, status, name, '', geographicLevel, type, coordinates
from jurisdiction_master ;

-- populate mergeset with existing structures from master list
insert into mergeset (openSRP_Id,	openSRPParent_Id ,externalId ,externalParentId ,status ,name ,name_en ,geographicLevel ,type ,coordinates )
select id , parentid ,externalid, externalparentid, status, name, '', geographicLevel, type, coordinates
from structure_master ;

-- insert into mergeset with new records from changeset
insert into mergeset (externalid,externalparentid,status, name, name_en, geographicLevel, type, coordinates , operation)
select 		c.externalId ,	c.externalParentId ,	c.status ,	c.name ,	c.name_en ,	c.geographicLevel ,	c.type ,	c.coordinates, 'i'
from 		changeset c 
left join 	jurisdiction_master j on j. externalid = c.externalid 
where 		j.externalid is null;

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
