use db_mhealth 

select * from raw.event e inner join
raw.event_obs eo
on e.eventId = eo.eventId
where e.eventType = 'bednet_distribution'
order by e.eventId

select eventType, count(*) from raw.event group by eventType 


create procedure populate_bednet_distribution
as
truncate table blood_screening

insert into bednet_distribution ([eventId], [eventType], [providerId], [entityType], [baseEntityId], [teamId], [team], [locationId], [focusAreaId], [childLocationId], [dateCreated], [taskstatus], [locationUUID], [appVersionName], [taskIdentifier], [taskBusinessStatus], [planId], [caseTestingProtocol], [forestGoerYesNo], [personTested], [testType], [slideNumber], [testMicrosResult], [business_status], [conceptstart], [conceptend], [conceptdeviceId])
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
			null as planId/*,
			caseTestingProtocol.val as caseTestingProtocol ,
			forestGoerYesNo.val as forestGoerYesNo,
			personTested.val as personTested,
			testType.val as testType,
			slideNumber.val as slideNumber,
			testMicrosResult.val as testMicrosResult,
			business_status.val as business_status,
			conceptstart.val as conceptstart,
			conceptend.val as conceptend,
			conceptdeviceId.val as conceptdeviceId*/
from		raw.event  e
left join	raw.event_detail ed on e.eventId = ed.eventId
left join	raw.event_obs totPopulation on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs existingLLINs on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs existingLLINsLessThan1yr on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs existingLLINs1yrTo2yr on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs existingLLINs2yrTo3yr on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs existingLLINsGreaterThan3yr on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs existingLLIHNs on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs existingITNs on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs existingITNDipped on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs calcExistingNets on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs calcNumNetsNeeded on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs calcNumNetsToDistribute on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs distributedLLINs  on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs distributedLLIHNs on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs distributedITNs on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs calcPopulationMinusExistingNets on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs calcTotalNetsDistributed on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs calcDistributedRecommendation on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs calcNumNetsToRedip on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs redippedITNs on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs distributedRepellent on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs business_status on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'




left join	raw.event_obs conceptstart on e.eventId =  conceptstart.eventId and conceptstart.fieldCode = '163137AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
left join	raw.event_obs conceptend on e.eventId =  conceptend.eventId and conceptend.fieldCode = '163138AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
left join	raw.event_obs conceptdeviceId on e.eventId =  conceptdeviceId.eventId and conceptdeviceId.fieldCode = '163149AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
left join	focus_masterlist ml on upper(ml.opensrp_id) = e.locationId
where e.eventType = 'bednet_distribution'

GO

create table test (j json)

print @@version

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