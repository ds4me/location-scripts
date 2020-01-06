select * from jurisdiction_master

select * from geojson_file
select processed, operation, count(*) from mergeset 
group by processed, operation

select * into mergeset_002 from mergeset

select * from mergeset where operation = 'u'

select j.externalid, j.externalparentid, m.externalparentid  from jurisdiction_master j 
inner join mergeset m on m.externalid = j.externalid
where j.externalid in (
select externalid from mergeset where operation = 'x' )

select * from jurisdiction_master where externalid in (
	
select  m.externalid, j.externalparentid as oldexternalparentid, m.externalparentid as newexternalparentid, jm.id as opensrp_id,  jm.openmrs_id from jurisdiction_master j 
inner join mergeset m on m.externalid = j.externalid
inner join jurisdiction_master jm on jm.externalid = m.externalparentid
where j.externalid in (
select externalid from mergeset where operation = 'x' )


update reveal -  change parent uuid 

update openmrs - change parent uuid






