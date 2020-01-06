SELECT geographiclevel, operation, count(*)  from mergeset where operation is not null 
group by geographiclevel, operation