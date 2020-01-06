truncate table mergeset;

update jurisdiction_master set externalparentid = l.externalid, openmrsparentid = l.openmrs_id  from (
	select c.id,  p.externalid, p.openmrs_id
	from jurisdiction_master c inner join jurisdiction_master p on c.parentid = p.id
) l where jurisdiction_master.id = l.id;

insert into mergeset (openSRP_Id, openMRS_Id,	openSRPParent_Id,openMRSParent_id ,externalId ,externalParentId ,status ,name ,name_en ,geographicLevel ,type ,coordinates )
select id, openmrs_id, parentid, openmrsparentid,externalid, externalparentid, status, name, '', geographicLevel, type, coordinates 
from jurisdiction_master ;

insert into mergeset (externalid,externalparentid,status, name, name_en, geographicLevel, type, coordinates , operation)
select 	externalId ,	externalParentId ,	status ,	name ,	name_en ,	geographicLevel ,	type ,	coordinates, 'i'
from changeset where externalid not in (select externalid from jurisdiction_master);

update mergeset m set externalparentid =c.externalparentid  ,status =c.status, name = c.name, name_en =c.name_en, geographicLevel =c.geographicLevel, type =c.type, coordinates =c.coordinates , operation = 'u'
from changeset c where c.externalid = m.externalid and m.operation is null ;

update mergeset  set operation = 'x' 
from jurisdiction_master ml 
where ml.externalid = mergeset.externalid 
and ml.externalparentid <>  mergeset.externalparentid
and mergeset.operation is not null;

update mergeset set d_name = true
from jurisdiction_master ml 
where ml.externalid = mergeset.externalid  
and ml.name <> mergeset.name 
and mergeset.operation = 'u';

update mergeset set d_status = true
from jurisdiction_master ml 
where ml.externalid = mergeset.externalid  
and ml.status <> mergeset.status 
and mergeset.operation = 'u';

update mergeset set d_externalparentid = true
from jurisdiction_master ml 
where ml.externalid = mergeset.externalid  
and ml.externalparentid <> mergeset.externalparentid 
and mergeset.operation = 'u';

update mergeset set d_type = true
from jurisdiction_master ml 
where ml.externalid = mergeset.externalid  
and ml.type <> mergeset.type 
and mergeset.operation = 'u';

update mergeset set d_coordinates = true
from jurisdiction_master ml 
where ml.externalid = mergeset.externalid  
and ml.coordinates <> mergeset.coordinates 
and mergeset.operation = 'u';

update mergeset set d_geographiclevel = true
from jurisdiction_master ml 
where ml.externalid = mergeset.externalid  
and ml.geographiclevel <> mergeset.geographiclevel 
and mergeset.operation = 'u';

update mergeset set opensrp_id = uuid_generate_v4() where operation = 'i';

update mergeset set opensrpparent_id = p.opensrp_id 
from mergeset p where mergeset.externalparentid = p.externalid 
and mergeset.opensrpparent_id is null;

update mergeset set name_en = concat(name_en , ' (',externalId,')') where operation is not null;
update mergeset set name = concat(name , ' (',externalId,')') where operation is not null;

update mergeset set operation = 'x' where opensrpparent_id is null and geographiclevel > 0;