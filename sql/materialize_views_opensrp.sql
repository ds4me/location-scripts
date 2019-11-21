drop table if exists tmp_locations;
select  json->>'id' id,
        json->'properties'->>'externalId' externalId,
        json->'properties'->>'parentId' parentId,
        json->'properties'->>'status' status,
        json->'properties'->>'name' "name",
        json->'properties'->>'geographicLevel' geographicLevel,
        json->'properties'->>'OpenMRS_Id' OpenMRS_Id,
        json->'geometry'->>'type' "type",
        json->'geometry'->>'coordinates' "coordinates"
        into tmp_locations
from
        core.location;


drop table if exists  tmp_structures;
select  json->>'id' id,
        json->'properties'->>'externalId' externalId,
        json->'properties'->>'parentId' parentId,
        json->'properties'->>'status' status,
        json->'properties'->>'name' "name",
        json->'properties'->>'geographicLevel' geographicLevel,
        json->'properties'->>'OpenMRS_Id' OpenMRS_Id,
        json->'geometry'->>'type' "type",
        json->'geometry'->>'coordinates' "coordinates"
        into tmp_structures
from
        core.structure;


drop table if exists tmp_ouh;
select
            ou1.id p_id, ou1.name p_name, ou1.status p_status,
            ou2.id d_id, ou2.name d_name, ou2.status d_status,
            ou3.id c_id, ou3.name c_name, ou3.status c_status,
            ou4.id sa_id, ou4.name sa_name, ou4.status sa_status,
            s.s_count as structure_count
into tmp_ouh
from        tmp_locations ou4
left join  tmp_locations ou3 on ou4.parentId = ou3.id
left join  tmp_locations ou2 on ou3.parentId = ou2.id
left join  tmp_locations ou1 on ou2.parentId = ou1.id
left join
    (select  s.parentId, count(*) as s_count from tmp_structures s group by  s.parentId) s
        on ou4.id = s.parentId
    order by ou1.name,ou2.name,ou3.name,ou4.name;

\COPY tmp_locations to '~/location.csv' DELIMITER '|' CSV HEADER;
\COPY tmp_structures to '~/structure.csv' DELIMITER '|' CSV HEADER;
\COPY tmp_ouh to '~/ouh.csv' DELIMITER '|' CSV HEADER;
