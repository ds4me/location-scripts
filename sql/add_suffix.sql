-- add externalId suffix
update 		mergeset set name_en = concat(name_en , ' (',externalId,')')
where 		operation is not null;

update 		mergeset set name = concat(name , ' (',externalId,')')
where 		operation is not null;