use db_mhealth 

drop table if exists raw_blood_screening

select 
			e.eventId as eventId, 
			e.eventType as eventType, 
			e.providerId as providerId,
			e.entityType as entityType,
			e.baseEntityId as baseEntityId,
			e.teamId as teamId,
			e.team as team,
			e.locationId as locationId,
			ml.ou5_eid as focusAreaId,
			e.childLocationId as childLocationId,
			e.dateCreated as dateCreated,
			ed.[taskstatus],
			ed.[locationUUID],
			ed.[appVersionName],
			ed.[taskIdentifier],
			ed.[taskBusinessStatus],
			null as planId,
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
from		raw.event  e
left join	raw.event_detail ed on e.eventId = ed.eventId
left join	raw.event_obs caseTestingProtocol on e.eventId =  caseTestingProtocol.eventId and caseTestingProtocol.fieldCode = caseTestingProtocol
left join	raw.event_obs forestGoerYesNo on e.eventId =  forestGoerYesNo.eventId and forestGoerYesNo.fieldCode = forestGoerYesNo
left join	raw.event_obs personTested on e.eventId =  personTested.eventId and personTested.fieldCode = personTested
left join	raw.event_obs testType on e.eventId =  testType.eventId and testType.fieldCode = testType
left join	raw.event_obs slideNumber on e.eventId =  slideNumber.eventId and slideNumber.fieldCode = slideNumber
left join	raw.event_obs testMicrosResult on e.eventId =  testMicrosResult.eventId and testMicrosResult.fieldCode = testMicrosResult
left join	raw.event_obs business_status on e.eventId =  business_status.eventId and business_status.fieldCode = business_status
left join	raw.event_obs conceptstart on e.eventId =  conceptstart.eventId and conceptstart.fieldCode = 163137AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
left join	raw.event_obs conceptend on e.eventId =  conceptend.eventId and conceptend.fieldCode = 163138AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
left join	raw.event_obs conceptdeviceId on e.eventId =  conceptdeviceId.eventId and conceptdeviceId.fieldCode = 163149AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
left join	focus_masterlist ml on upper(ml.opensrp_id) = e.locationId
where e.eventType = blood_screening

select bs.eventid, bs.eventtype,lower(bs.taskIdentifier),lower(bs.locationId), t.identifier
from raw_blood_screening bs
left join raw_data_events_working t on upper(t.identifier) = upper(bs.taskIdentifier)
order by eventtype, locationId

truncate table [raw_json_tasks]
select  * from [dbo].[raw_json_tasks]

drop table json.task
create table json.task (rowid int identity, jsontext varchar(max))
truncate table json.task
select top 1 *  from json.task
drop table  raw.task
create table raw.task 
(
    status varchar(max),
    note varchar(max),
    businessStatus varchar(max),
    code varchar(max),
    executionStartDate varchar(max),
    description  varchar(max),
    [for] uniqueidentifier,
    groupIdentifier uniqueidentifier,
    lastModified varchar(max),
    serverVersion bigint,
    focus uniqueidentifier,
    executionEndDate varchar(max),
    priority int,
    authoredOn varchar(max),
    location uniqueidentifier,
    planIdentifier uniqueidentifier,
    requester varchar(max),
    owner varchar(max),
    reasonReference varchar(max),
    identifier uniqueidentifier
)

insert into  raw.task 
select 
JSON_VALUE(jsontext,'$.status') status ,
JSON_VALUE(jsontext,'$.note') note,
JSON_VALUE(jsontext,'$.businessStatus') businessStatus,
JSON_VALUE(jsontext,'$.code') code,
JSON_VALUE(jsontext,'$.executionStartDate') executionStartDate,
JSON_VALUE(jsontext,'$.description') description,
JSON_VALUE(jsontext,'$.for') [for],
JSON_VALUE(jsontext,'$.groupIdentifier') groupIdentifier,
JSON_VALUE(jsontext,'$.lastModified') lastModified,
JSON_VALUE(jsontext,'$.serverVersion') serverVersion,
JSON_VALUE(jsontext,'$.focus') focus,
JSON_VALUE(jsontext,'$.executionEndDate') executionEndDate,
JSON_VALUE(jsontext,'$.priority') priority,
JSON_VALUE(jsontext,'$.authoredOn') authoredOn,
JSON_VALUE(jsontext,'$.location') location,
JSON_VALUE(jsontext,'$.planIdentifier') planIdentifier,
JSON_VALUE(jsontext,'$.requester') requester,
JSON_VALUE(jsontext,'$.owner') owner,
JSON_VALUE(jsontext,'$.reasonReference') reasonReference,
JSON_VALUE(jsontext,'$.identifier') identifier
from json.task

select * from raw.task






drop table [dbo].[raw_data_details_working]
drop table [dbo].[raw_data_events_working]
drop table [dbo].[raw_data_json_tasks]
drop table [dbo].[raw_data_obs_working]
drop table [dbo].[raw_data_working]
drop table [dbo].[raw_json_plan]
drop table [dbo].[raw_json_tasks]

select * from raw_blood_screening

select * from plans where plans.jurisdiction = bf310b5d-93d0-4f67-b930-0275983948b3

select * from raw_data_events_working where planIdentifier = 5811bd14-bc97-48f7-bd3b-baaf52b895be