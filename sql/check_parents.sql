select c.id, c.externalId, c.parentId, c.externalParentId , p.id  
from 
jurisdiction_master c
left join jurisdiction_master p on p.id = c.parentid
where p.id is null and c.externalId <> '0'
order by c.externalId



select c.externalId, c.id, p.id from 
jurisdiction_master c
left join jurisdiction_master p on p.externalid = left(c.externalid,8)
where c.id in (
select c.id
from 
jurisdiction_master c
left join jurisdiction_master p on p.id = c.parentid
where p.id is null and c.externalId <> '0')
