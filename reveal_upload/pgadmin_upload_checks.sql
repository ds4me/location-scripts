-- If not, make sure the number of changes matches the expected number of changes
SELECT operation, COUNT(*) FROM mergeset GROUP BY operation

-- If there are any 'x' values, check to see why there are errors
SELECT * FROM mergeset WHERE operation = 'x'

-- Check the number of changes by provinces
SELECT SUBSTRING(externalid,1,2), COUNT(*) FROM mergeset WHERE operation IS NOT NULL GROUP BY SUBSTRING(externalid,1,2)

-- Spot check the mergeset table to make sure the correct features are 'i' for insert or 'u' for update
SELECT * FROM mergeset WHERE operation IS NOT NULL

-- After the process completes, make sure everything was processed
SELECT processed, COUNT(*) FROM mergeset WHERE operation IS NOT NULL GROUP BY processed

-- And save the mergeset - CHANGE THE MERGE NUMBER AT THE END BEFORE RUNNING
SELECT * INTO merge_0XX FROM mergeset


/*
Miscellaneous queries
*/

-- If the process fails, check how many changes were processed
SELECT
	SUM(CASE WHEN processed IS NULL THEN 1 ELSE 0 END) AS not_processed,
	SUM(CASE WHEN processed IS NOT NULL THEN 1 ELSE 0 END) AS processed,
	COUNT(*) as total
FROM mergeset
WHERE operation IS NOT NULL

-- Update openmrs and opensrp parent values based on the externalparentid
UPDATE mergeset
SET
	openmrsparent_id = p.openmrs_id,
	opensrpparent_id = p.opensrp_id
FROM mergeset p
WHERE
	mergeset.externalparentid = p.externalid AND
	SUBSTRING(mergeset.externalid,1,2) NOT IN ('63','23','34') AND
	mergeset.geographiclevel = 5

-- Get the entire hierarchy for all level 5 foci
select distinct
	ou1.externalid ou1_id, ou1.name ou1_name, ou1.name_en ou1_local, --ou1.id_opensrp ou1_opensrp,
	ou2.externalid ou2_id, ou2.name ou2_name, ou2.name_en ou2_local, --ou2.id_opensrp ou2_opensrp,
	ou3.externalid ou3_id, ou3.name ou3_name, ou3.name_en ou3_local, --ou3.id_opensrp ou3_opensrp,
	ou4.externalid ou4_id, ou4.name ou4_name, ou4.name_en ou4_local, --ou4.id_opensrp ou4_opensrp,
	ou5.externalid ou5_id, ou5.name ou5_name, ou5.name_en ou5_local --ou5.id_opensrp ou5_opensrp,
from mergeset ou5
left join mergeset ou4 on ou5.externalparentid = ou4.externalid
left join mergeset ou3 on ou4.externalparentid = ou3.externalid
left join mergeset ou2 on ou3.externalparentid = ou2.externalid
left join mergeset ou1 on ou2.externalparentid = ou1.externalid
where ou5.geographiclevel = 5
order by ou5.externalid

SELECT * FROM mergeset WHERE operation IS NOT NULL and processed is NULL

SELECT * FROM mergeset WHERE externalid IN ('6303060501','63030605','630306','6303', '63') ORDER BY externalid
