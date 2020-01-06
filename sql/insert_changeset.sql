insert into changeset ( 
	externalId ,
 	externalParentId ,
	status  ,
	name_en  ,
	name,
	geographicLevel,
	type,
	coordinates
 )
select 
	json_array_elements(file->'features')->'properties'->>'externalId' externalId,
	json_array_elements(file->'features')->'properties'->>'externalParentId' externalParentId,
	'Active' status, --json_array_elements(file->'features')->'properties'->>'status' 
	json_array_elements(file->'features')->'properties'->>'name_en' name_en,
	json_array_elements(file->'features')->'properties'->>'name' "name",
	cast(json_array_elements(file->'features')->'properties'->>'geographicLevel' as int) geographicLevel,
	json_array_elements(file->'features')->'geometry'->>'type' "type",
	json_array_elements(file->'features')->'geometry'->>'coordinates' coordinates
from 
	geojson_file
order by cast(json_array_elements(file->'features')->'properties'->>'geographicLevel' as int);