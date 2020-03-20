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
Miscellaneous commands
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
	
	
SELECT * FROM mergeset WHERE operation IS NOT NULL and processed is NULL

SELECT * FROM mergeset WHERE externalid IN ('6303060501','63030605','630306','6303', '63') ORDER BY externalid