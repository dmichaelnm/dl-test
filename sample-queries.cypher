// object lineage for a column
match (c:COLUMN)-[:IS_USED_BY*]-(d) where c.column = 'calendar_year' 
match (c)-[:IS_COLUMN_OF]-(f)
match (d)-[:IS_COLUMN_OF]-(e)
return f, e

// all used objects by a view
match (v:ENTITY)-[:IS_USED_BY|IS_SUBQUERY_OF*]-(w) where v.entity = 'v_indicator_use' return v, w
