use db_mhealth;
insert into raw.event ([eventId], [baseEntityId], [locationId], [eventDate], [eventType], [formSubmissionId], [providerId], [entityType], [teamId], [team], [childLocationId], [dateCreated], [clientApplicationVersion], [clientDatabaseVersion], [type])
select	 
		JSON_VALUE(event_json,'$.id') eventid,
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
		JSON_VALUE(event_json,'$.type') type
from	raw_data 
where	ISJSON(event_json) = 1 

insert into raw.event_detail ( [eventId], [taskstatus], [locationUUID], [appVersionName], [taskIdentifier], [taskBusinessStatus])
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

insert into raw.event_obs ([eventId], [fieldType], [fieldDataType], [fieldCode], [val])
select raw_data.id, obs.fieldType, obs.fieldDataType, obs.fieldCode, obs.vals 
from raw_data 
cross apply openjson(JSON_query(event_json,'$.obs')) WITH (
	fieldType  varchar(500) '$.fieldType', 
	fieldDataType varchar(500) '$.fieldDataType',
	fieldCode varchar(500) '$.fieldCode',
	vals varchar(500) '$.values[0]'
) as obs
where ISJSON(event_json) = 1


