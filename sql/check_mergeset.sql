select 		count(*) 
from 		geojson_file;

select 		count(*) 
from 		jurisdiction_master;

select 		count(*) 
from 		changeset;

select * from changeset;
select 		* 
from 		mergeset 
where 		operation is not null;

update 		mergeset 
set 		d_name = null;

update 		mergeset 
set 		d_name = true
from 		jurisdiction_master ml 
where 		ml.externalid = mergeset.externalid  
and 		ml.name <> mergeset.name 
and 		mergeset.operation = 'u';

select 		ml.name, ms.name, ms.d_name, ms.d_type
from 		jurisdiction_master ml 
inner join 	mergeset ms on ms.externalId = ml.externalId
where 		ms.d_name is not null;

select 		ml.name, ms.name, ms.d_name, ms.d_type
from 		jurisdiction_master ml 
inner join 	mergeset ms on ms.externalId = ml.externalId
and 		ml.name <> ms.name 
and 		ms.operation = 'u';


select * from jurisdiction_master where externalId = '0'
select * from mergeset where operation = 'u'
select * from mergeset where openmrs_id is null