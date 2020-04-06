use db_mhealth 

select  distinct fieldcode from raw.event e inner join
raw.event_obs eo
on e.eventId = eo.eventId
where e.eventType = 'case_confirmation'
order by fieldcode

select eventType, count(*) from raw.event group by eventType 

select * from event_base_fields ebf
where ebf.eventType = 'Family Member Registration'
select * from event_base_fields ebf
where ebf.eventType = 'case_confirmation'
select ebf.*,
citizenship.val as citizenship,
dob_unknown.val as dob_unknown,
fam_name.val as fam_name,
first_name.val as first_name,
is_family_head.val as is_family_head,
last_interacted_with.val as last_interacted_with,
occupation.val as occupation,
same_as_fam_name.val as same_as_fam_name,
sleeps_outdoors.val as sleeps_outdoors,
surname.val as surname
from event_base_fields ebf
left join raw.event_obs citizenship on citizenship.eventid = ebf.eventid and citizenship.fieldCode = 'citizenship'
left join raw.event_obs dob_unknown on dob_unknown.eventid = ebf.eventid and dob_unknown.fieldCode = 'dob_unknown'
left join raw.event_obs fam_name on fam_name.eventid = ebf.eventid and fam_name.fieldCode = 'fam_name'
left join raw.event_obs first_name on first_name.eventid = ebf.eventid and first_name.fieldCode = 'first_name'
left join raw.event_obs is_family_head on is_family_head.eventid = ebf.eventid and is_family_head.fieldCode = 'is_family_head'
left join raw.event_obs last_interacted_with on last_interacted_with.eventid = ebf.eventid and last_interacted_with.fieldCode = 'last_interacted_with'
left join raw.event_obs occupation on occupation.eventid = ebf.eventid and occupation.fieldCode = 'occupation'
left join raw.event_obs same_as_fam_name on same_as_fam_name.eventid = ebf.eventid and same_as_fam_name.fieldCode = 'same_as_fam_name'
left join raw.event_obs sleeps_outdoors on sleeps_outdoors.eventid = ebf.eventid and sleeps_outdoors.fieldCode = 'sleeps_outdoors'
left join raw.event_obs surname on surname.eventid = ebf.eventid and surname.fieldCode = 'surname'
where ebf.eventType = 'Family Member Registration'







select * from bednet_distribution
create procedure populate_bednet_distribution
as
truncate table blood_screening

insert into bednet_distribution ([eventId], [eventType], [providerId], [entityType], [baseEntityId], [teamId], [team], [locationId], [focusAreaId], [childLocationId], [dateCreated], [taskstatus], [locationUUID], [appVersionName], [taskIdentifier], [taskBusinessStatus], [planId], [totPopulation], [existingLLINs], [existingLLINsLessThan1yr], [existingLLINs1yrTo2yr], [existingLLINs2yrTo3yr], [existingLLINsGreaterThan3yr], [existingLLIHNs], [existingITNs], [existingITNDipped], [calcExistingNets], [calcNumNetsNeeded], [calcNumNetsToDistribute], [distributedLLINs], [distributedLLIHNs], [distributedITNs], [calcPopulationMinusExistingNets], [calcTotalNetsDistributed], [calcDistributedRecommendation], [calcNumNetsToRedip], [redippedITNs], [distributedRepellent], [business_status], [conceptstart], [conceptend], [conceptdeviceId])
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
[totPopulation] .val as [totPopulation],
[existingLLINs].val as   [existingLLINs] , 
[existingLLINsLessThan1yr].val as [existingLLINsLessThan1yr], 
[existingLLINs1yrTo2yr].val as [existingLLINs1yrTo2yr], 
[existingLLINs2yrTo3yr].val as [existingLLINs2yrTo3yr], 
[existingLLINsGreaterThan3yr].val as [existingLLINsGreaterThan3yr], 
[existingLLIHNs].val as [existingLLIHNs], 
[existingITNs].val as [existingITNs], 
[existingITNDipped].val as [existingITNDipped], 
[calcExistingNets].val as [calcExistingNets], 
[calcNumNetsNeeded].val as [calcNumNetsNeeded], 
[calcNumNetsToDistribute].val as [calcNumNetsToDistribute], 
[distributedLLINs].val as [distributedLLINs], 
[distributedLLIHNs].val as [distributedLLIHNs] , 
[distributedITNs].val as [distributedITNs], 
[calcPopulationMinusExistingNets].val as [calcPopulationMinusExistingNets],
[calcTotalNetsDistributed].val as [calcTotalNetsDistributed], 
[calcDistributedRecommendation].val as [calcDistributedRecommendation] , 
[calcNumNetsToRedip].val as [calcNumNetsToRedip], 
[redippedITNs].val as [redippedITNs], 
[distributedRepellent].val as [distributedRepellent], 
[business_status].val as [business_status] , 
conceptstart.val as conceptstart,
conceptend.val as conceptend,
conceptdeviceId.val as conceptdeviceId
from		raw.event  e
left join	raw.event_detail ed on e.eventId = ed.eventId
left join	raw.event_obs totPopulation on e.eventId = totPopulation.eventId and  totPopulation.fieldCode = 'totPopulation'
left join	raw.event_obs existingLLINs on e.eventId = existingLLINs.eventId and  existingLLINs.fieldCode = 'existingLLINs'
left join	raw.event_obs existingLLINsLessThan1yr on e.eventId = existingLLINsLessThan1yr.eventId and  existingLLINsLessThan1yr.fieldCode = 'existingLLINsLessThan1yr'
left join	raw.event_obs existingLLINs1yrTo2yr on e.eventId = existingLLINs1yrTo2yr.eventId and  existingLLINs1yrTo2yr.fieldCode = 'existingLLINs1yrTo2yr'
left join	raw.event_obs existingLLINs2yrTo3yr on e.eventId = existingLLINs2yrTo3yr.eventId and  existingLLINs2yrTo3yr.fieldCode = 'existingLLINs2yrTo3yr'
left join	raw.event_obs existingLLINsGreaterThan3yr on e.eventId = existingLLINsGreaterThan3yr.eventId and  existingLLINsGreaterThan3yr.fieldCode = 'totPopulation'
left join	raw.event_obs existingLLIHNs on e.eventId = existingLLIHNs.eventId and  existingLLIHNs.fieldCode = 'existingLLIHNs'
left join	raw.event_obs existingITNs on e.eventId = existingITNs.eventId and  existingITNs.fieldCode = 'existingITNs'
left join	raw.event_obs existingITNDipped on e.eventId = existingITNDipped.eventId and  existingITNDipped.fieldCode = 'existingITNDipped'
left join	raw.event_obs calcExistingNets on e.eventId = calcExistingNets.eventId and  calcExistingNets.fieldCode = 'calcExistingNets'
left join	raw.event_obs calcNumNetsNeeded on e.eventId = calcNumNetsNeeded.eventId and  calcNumNetsNeeded.fieldCode = 'calcNumNetsNeeded'
left join	raw.event_obs calcNumNetsToDistribute on e.eventId = calcNumNetsToDistribute.eventId and  calcNumNetsToDistribute.fieldCode = 'calcNumNetsToDistribute'
left join	raw.event_obs distributedLLINs  on e.eventId = distributedLLINs.eventId and  distributedLLINs.fieldCode = 'distributedLLINs'
left join	raw.event_obs distributedLLIHNs on e.eventId = distributedLLIHNs.eventId and  distributedLLIHNs.fieldCode = 'distributedLLIHNs'
left join	raw.event_obs distributedITNs on e.eventId = distributedITNs.eventId and  distributedITNs.fieldCode = 'distributedITNs'
left join	raw.event_obs calcPopulationMinusExistingNets on e.eventId = calcPopulationMinusExistingNets.eventId and  calcPopulationMinusExistingNets.fieldCode = 'calcPopulationMinusExistingNets'
left join	raw.event_obs calcTotalNetsDistributed on e.eventId = calcTotalNetsDistributed.eventId and  calcTotalNetsDistributed.fieldCode = 'calcTotalNetsDistributed'
left join	raw.event_obs calcDistributedRecommendation on e.eventId = calcDistributedRecommendation.eventId and  calcDistributedRecommendation.fieldCode = 'calcDistributedRecommendation'
left join	raw.event_obs calcNumNetsToRedip on e.eventId = calcNumNetsToRedip.eventId and  calcNumNetsToRedip.fieldCode = 'calcNumNetsToRedipvvvvvvvvv'
left join	raw.event_obs redippedITNs on e.eventId = redippedITNs.eventId and  redippedITNs.fieldCode = 'redippedITNs'
left join	raw.event_obs distributedRepellent on e.eventId = distributedRepellent.eventId and  distributedRepellent.fieldCode = 'distributedRepellent'
left join	raw.event_obs business_status on e.eventId = business_status.eventId and  business_status.fieldCode = 'business_status'
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