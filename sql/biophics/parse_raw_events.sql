use db_mhealth

select eventType, count(*) from raw_data_working 
group by eventType
select count(*) from [dbo].[HLD_blood_screening]
/*
eventType	(No column name)
Family Registration	3955
mosquito_collection	6
PAOT	6
blood_screening	3976 vs 3957
larval_dipping	11
case_confirmation	18
Family Member Registration	10417
bednet_distribution	703
*/

select * from raw_data_obs_working where eventId in (
	select eventId from raw_data_working where eventType = 'blood_screening' )
	order by eventId

	create index idx_obs_eventid on raw_data_obs_working (eventid)
	create index idx_e_eventid on raw_data_working (eventid)

create table raw_even
select 
			e.eventId as eventId, 
			e.eventType as eventType, 
			e.providerId as providerId,
			e.entityType as entityType,
			e.teamId as teamId,
			e.team as team,
			e.locationId as locationId,
			e.baseEntityId as baseEntityId,
			e.childLocationId as childLocationId,
			e.dateCreated as dateCreated,
			ed.[taskstatus],
			ed.[locationUUID],
			ed.[appVersionName],
			ed.[taskIdentifier],
			ed.[taskBusinessStatus],
			caseTestingProtocol.val as caseTestingProtocol ,
			forestGoerYesNo.val as forestGoerYesNo,
			personTested.val as personTested,
			testType.val as testType,
			slideNumber.val as slideNumber,
			testMicrosResult.val as testMicrosResult,
			business_status.val as business_status,
			conceptstart.val as conceptstart,
			conceptend.val as conceptend,
			conceptdeviceId.val as conceptdeviceId
			into raw_blood_screening
from		raw_data_working  e
left join	raw_data_details_working ed on e.eventId = ed.eventId
left join	raw_data_obs_working caseTestingProtocol on e.eventId =  caseTestingProtocol.eventId and caseTestingProtocol.fieldCode = 'caseTestingProtocol'
left join	raw_data_obs_working forestGoerYesNo on e.eventId =  forestGoerYesNo.eventId and forestGoerYesNo.fieldCode = 'forestGoerYesNo'
left join	raw_data_obs_working personTested on e.eventId =  personTested.eventId and personTested.fieldCode = 'personTested'
left join	raw_data_obs_working testType on e.eventId =  testType.eventId and testType.fieldCode = 'testType'
left join	raw_data_obs_working slideNumber on e.eventId =  slideNumber.eventId and slideNumber.fieldCode = 'slideNumber'
left join	raw_data_obs_working testMicrosResult on e.eventId =  testMicrosResult.eventId and testMicrosResult.fieldCode = 'testMicrosResult'
left join	raw_data_obs_working business_status on e.eventId =  business_status.eventId and business_status.fieldCode = 'business_status'
left join	raw_data_obs_working conceptstart on e.eventId =  conceptstart.eventId and conceptstart.fieldCode = '163137AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
left join	raw_data_obs_working conceptend on e.eventId =  conceptend.eventId and conceptend.fieldCode = '163138AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
left join	raw_data_obs_working conceptdeviceId on e.eventId =  conceptdeviceId.eventId and conceptdeviceId.fieldCode = '163149AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
where e.eventType = 'blood_screening'

select ou5_eid, e.*
from [dbo].raw_blood_screening e
left join focus_masterlist ml on upper(ml.opensrp_id) = e.locationId

testMicrosResult
business_status




select top 50  JSON_VALUE(event_json, '$.baseEntityId')

select JSON_VALUE(event_json, 'lax $.eventType') , count(*) 

from raw_data group by JSON_VALUE(event_json, 'lax $.eventType')
order by JSON_VALUE(event_json, 'lax $.eventType')
where ISJSON(event_json) = 1


select  raw_data.id, obs.taskstatus, obs.locationUUID, obs.appVersionName, obs.taskIdentifier ,obs.taskBusinessStatus
from raw_data 
cross apply openjson(JSON_query(event_json,'$.details')) WITH (
	taskstatus  varchar(500) '$.taskStatus', 
	locationUUID uniqueidentifier '$.locationUUID',
	appVersionName varchar(500) '$.appVersionName', 
	taskIdentifier uniqueidentifier '$.taskIdentifier', 
	taskBusinessStatus varchar(500) '$.taskBusinessStatus' 
) as obs
where ISJSON(event_json) = 1





select * from raw_data where ISJSON(event_json) = 0


drop table raw_data_working

create table raw_data_working
(
baseEntityId varchar(255),
locationId varchar(255),
eventDate varchar(255),
eventType varchar(255),
formSubmissionId varchar(255),
providerId varchar(255),
entityType varchar(255),
teamId varchar(255),
team varchar(255),
childLocationId varchar(255),
dateCreated varchar(255),
clientApplicationVersion  varchar(255),
clientDatabaseVersion  varchar(255),
type  varchar(255),
id  varchar(255)
)

select * from raw.event

insert into raw_data_working
select	 
		JSON_VALUE(event_json,'$.baseEntityId') baseEntityId,
		JSON_VALUE(event_json,'$.locationId') locationId,
		JSON_VALUE(event_json,'$.eventDate') eventDate,
		JSON_VALUE(event_json,'$.eventType') eventType,
		JSON_VALUE(event_json,'$.formSubmissionId') formSubmissionId,
		JSON_VALUE(event_json,'$.providerId') providerId,
		JSON_VALUE(event_json,'$.entityType') entityType,
		JSON_VALUE(event_json,'$.teamId') teamId,
		JSON_VALUE(event_json,'$.team') team,
		JSON_VALUE(event_json,'$.childLocationId') childLocationId,
		JSON_VALUE(event_json,'$.dateCreated') dateCreated,
		JSON_VALUE(event_json,'$.clientApplicationVersion') clientApplicationVersion,
		JSON_VALUE(event_json,'$.clientDatabaseVersion') clientDatabaseVersion,
		JSON_VALUE(event_json,'$.type') type,
		JSON_VALUE(event_json,'$.id') id
from	raw_data 
where	ISJSON(event_json) = 1 

--missing locations (OA)
select f.ou5_eid,r.locationid, r.eventType, r.providerId, r.team, count(*) from raw_data_working r 
left join [dbo].[focus_masterlist] f on r.locationid = f.opensrp_id
group by f.ou5_eid,r.locationid,r.eventType, r.providerId, r.team

select focus_area_id, locationid,  count(*) from [dbo].[HLD_households]
group by focus_area_id,locationid
order by focus_area_id






--and JSON_VALUE(event_json,'$.locationId') = '744c90b8-a605-492a-931b-d37f79b41937'

select eventType, count(*) from raw_data 

group by eventType

drop table raw_data_values_working;

select locationid, count(*) from raw_data
where locationid = '744c90b8-a605-492a-931b-d37f79b41937'
group by locationid
order by count(*) desc

create table raw_data_values_working (
value_id int IDENTITY,
baseEntityId varchar(50),
eventType varchar(50),
fieldType  varchar(50),
fieldDataType  varchar(50),
fieldCode  varchar(50),
val  varchar(500)
)

truncate table raw.event_obs

insert into raw.event_obs ( [eventId], [fieldType], [fieldDataType], [fieldCode], [val])
select raw_data.id, obs.* 
from raw_data 
cross apply openjson(JSON_query(event_json,'$.obs')) WITH (
	fieldType  varchar(500) '$.fieldType', 
	fieldDataType varchar(500) '$.fieldDataType',
	fieldCode varchar(500) '$.fieldCode',
	vals varchar(500) '$.values[0]'
) as obs
where ISJSON(event_json) = 1






create index idx_raw_data_values_working_beid on raw_data_values_working(baseEntityId, eventType)

select eventType, count(*) from raw_data
group by eventType
/*
bednet_distribution	664
blood_screening	2174
case_confirmation	15
Family Member Registration	7786
Family Registration	2991
larval_dipping	7
mosquito_collection	3
PAOT	5
Register_Structure	1819 */

select count(*) from [dbo].[HLD_households]
select count(*) from  [dbo].[HLD_household_members]



select '[HLD_blood_screening]', count(*) from [dbo].[HLD_blood_screening] union
select '[HLD_household_members]', count(*) from [dbo].[HLD_household_members] union
select '[HLD_households]', count(*) from [dbo].[HLD_households] union
select '[[HLD_index_case_confirmation]]', count(*) from [dbo].[HLD_index_case_confirmation] union
select '[HLD_ITN]', count(*) from [dbo].[HLD_ITN] union
select '[HLD_larvae_dipping]', count(*) from [dbo].[HLD_larvae_dipping] union
select '[HLD_LLIN]', count(*) from [dbo].[HLD_LLIN] union
select '[HLD_mosquito_collection]',  count(*) from [dbo].[HLD_mosquito_collection] union
select '[HLD_potential_area_of_transmission]', count(*) from [dbo].[HLD_potential_area_of_transmission] union
select '[index_case]', count(*) from [dbo].[index_case]




select id, count(*) from [dbo].[HLD_blood_screening]
group by id order by count(*) desc

select count(*) from [dbo].[HLD_households] where house_number like '%HN%'
select count(*) from [dbo].[HLD_household_members]



select * from [dbo].[HLD_households] order by date_created desc 
select * from [dbo].[HLD_household_members] order by date_created desc 
where house_number like '%HN%'

