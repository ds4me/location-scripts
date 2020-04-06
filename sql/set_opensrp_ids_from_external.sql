update mergeset
set opensrp_id = cast(externalid as uuid)
where opensrp_id is null;

--update mergeset opensrpparent_id from self
update mergeset
set opensrpparent_id = p.opensrp_id
from mergeset p
where mergeset.externalparentid = p.externalid and mergeset.opensrpparent_id is null;

--mark issues : no opensrp parent id
update mergeset
set operation = 'x'
where opensrpparent_id is null and geographiclevel > 0;