use db_mhealth;

drop table if exists raw.event;
create table raw.event
(
row_id int IDENTITY,
eventId  uniqueidentifier,
baseEntityId uniqueidentifier,
locationId uniqueidentifier,
eventDate varchar(255),
eventType varchar(255),
formSubmissionId uniqueidentifier,
providerId varchar(255),
entityType varchar(255),
teamId uniqueidentifier,
team varchar(255),
childLocationId varchar(255),
dateCreated varchar(255),
clientApplicationVersion  varchar(255),
clientDatabaseVersion  varchar(255),
type  varchar(255)
);

drop table if exists raw.event_detail;
create table raw.event_detail (
row_id int IDENTITY,
eventId uniqueidentifier,
taskstatus   varchar(255),
locationUUID uniqueidentifier,
appVersionName varchar(255),
taskIdentifier uniqueidentifier,
taskBusinessStatus varchar(255)
);

drop table if exists raw.event_obs
create table raw.event_obs (
row_id int IDENTITY,
eventId uniqueidentifier,
eventType varchar(50),
fieldType  varchar(50),
fieldDataType  varchar(50),
fieldCode  varchar(50),
val  varchar(500)
);

drop table  raw.task;
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
);


create table json.event
(rowid int identity,
jsontext varchar(max));

create table json.[plan]
(rowid int identity,
jsontext varchar(max));

create table json.jurisdiction
(rowid int identity,
jsontext varchar(max));

create table json.structure
(rowid int identity,
jsontext varchar(max));

create table json.client
(rowid int identity,
jsontext varchar(max));