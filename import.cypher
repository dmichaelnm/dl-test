// remove all nodes and relationships
match ()-[r]-() delete r;
match (r) delete r;

// create target schema nodes
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
merge (s:SCHEMA { schema: row.TargetSchema });

// create source schema nodes
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
merge (s:SCHEMA { schema: row.SourceSchema });

// create target entity nodes
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
with row where toInteger(row.TargetEntityLevel) = 0
merge (e:OBJECT:ENTITY { schema: row.TargetSchema, entity: row.TargetEntity, type: row.TargetEntityType, level: row.TargetEntityLevel, alias: coalesce(row.TargetEntityAlias, 'N/A') });

// create source entity nodes
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
with row where toInteger(row.SourceEntityLevel) = 0
merge (e:OBJECT:ENTITY { schema: row.SourceSchema, entity: row.SourceEntity, type: row.SourceEntityType, level: row.SourceEntityLevel, alias: coalesce(row.SourceEntityAlias, 'N/A') });

// create target query nodes
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
with row where row.TargetEntityType = 'subquery'
merge (q:OBJECT:SUBQUERY { schema: row.TargetSchema, entity: row.TargetEntity, alias: coalesce(row.TargetEntityAlias, 'N/A'), level: row.TargetEntityLevel  });

// create source query nodes
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
with row where row.SourceEntityType = 'subquery'
merge (q:OBJECT:SUBQUERY { schema: row.SourceSchema, entity: row.SourceEntity, alias: coalesce(row.SourceEntityAlias, 'N/A'), level: row.SourceEntityLevel  });

// create source constant nodes
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
with row where row.SourceEntityType = 'constant'
merge (q:OBJECT:DUAL { schema: row.SourceSchema, entity: row.SourceEntity, name: 'DUAL', level: row.SourceEntityLevel, alias: coalesce(row.SourceEntityAlias, 'N/A') });

// create target entity column nodes
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
with row where toInteger(row.TargetEntityLevel) = 0
merge (c:COLUMN:ENTITY_COLUMN { schema: row.TargetSchema, entity: row.TargetEntity, column: row.TargetColumn, alias: coalesce(row.TargetEntityAlias, 'N/A'), level: row.TargetEntityLevel });

// create source entity column nodes
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
with row where toInteger(row.SourceEntityLevel) = 0
merge (c:COLUMN:ENTITY_COLUMN { schema: row.SourceSchema, entity: row.SourceEntity, column: row.SourceColumn, alias: coalesce(row.SourceEntityAlias, 'N/A'), level: row.SourceEntityLevel });

// create target query column nodes
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
with row where row.TargetEntityType = 'subquery'
merge (c:COLUMN:QUERY_COLUMN { schema: row.TargetSchema, entity: row.TargetEntity, column: row.TargetColumn, alias: coalesce(row.TargetEntityAlias, 'N/A'), level: row.TargetEntityLevel} );

// create source query column nodes
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
with row where row.SourceEntityType = 'subquery'
merge (c:COLUMN:QUERY_COLUMN { schema: row.SourceSchema, entity: row.SourceEntity, column: row.SourceColumn, alias: coalesce(row.SourceEntityAlias, 'N/A'), level: row.SourceEntityLevel} );

// create source constant column nodes
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
with row where row.SourceEntityType = 'constant'
merge (c:COLUMN:CONST_COLUMN { schema: row.SourceSchema, entity: row.SourceEntity, column: row.SourceColumn, alias: coalesce(row.SourceEntityAlias, 'N/A'), level: row.SourceEntityLevel });

// create parent-child-relationship between entities and schemas
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
with row
match (e:ENTITY) where (e.schema = row.TargetSchema and e.entity = row.TargetEntity) or (e.schema = row.SourceSchema and e.entity = row.SourceEntity)
match (s:SCHEMA) where s.schema = e.schema
merge (e)-[:IS_ENTITY_OF]->(s);

// create relationship between queries and their parents
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
with row
match (s:SUBQUERY) where s.schema = row.SourceSchema and s.entity = row.SourceEntity and s.level = row.SourceEntityLevel and s.alias = coalesce(row.SourceEntityAlias, 'N/A')
match (t:OBJECT) where t.schema = row.TargetSchema and t.entity = row.TargetEntity and t.level = row.TargetEntityLevel and t.alias = coalesce(row.TargetEntityAlias, 'N/A')
merge (s)-[:IS_SUBQUERY_OF]->(t);

// create relationship between constants and their parents
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
with row
match (s:DUAL) where s.schema = row.SourceSchema and s.entity = row.SourceEntity and s.level = row.SourceEntityLevel and s.alias = coalesce(row.SourceEntityAlias, 'N/A')
match (t:OBJECT) where t.schema = row.TargetSchema and t.entity = row.TargetEntity and t.level = row.TargetEntityLevel and t.alias = coalesce(row.TargetEntityAlias, 'N/A')
merge (s)-[:IS_USED_BY]->(t);

// create parent-child-relationship between columns and their parents
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
with row
match (c:COLUMN) where (c.schema = row.TargetSchema and c.entity = row.TargetEntity and c.level = row.TargetEntityLevel and c.column = row.TargetColumn and c.alias = coalesce(row.TargetEntityAlias, 'N/A'))
                    or (c.schema = row.SourceSchema and c.entity = row.SourceEntity and c.level = row.SourceEntityLevel and c.column = row.SourceColumn and c.alias = coalesce(row.SourceEntityAlias, 'N/A'))
match (p:OBJECT) where p.schema = c.schema and p.entity = c.entity and p.level = c.level and p.alias = c.alias
merge (c)-[:IS_COLUMN_OF]->(p);

// create relationship between entities and their usings
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
with row
match (s:ENTITY) where s.schema = row.SourceSchema and s.entity = row.SourceEntity and s.level = row.SourceEntityLevel and s.alias = coalesce(row.SourceEntityAlias, 'N/A')
match (t:OBJECT) where t.schema = row.TargetSchema and t.entity = row.TargetEntity and t.level = row.TargetEntityLevel and t.alias = coalesce(row.TargetEntityAlias, 'N/A')
merge (s)-[:IS_USED_BY {level: 'object'}]->(t);

// create relationship between columns
load csv with headers from 'https://raw.githubusercontent.com/dmichaelnm/dl-test/master/data-lineage.csv' as row
with row
match (s:COLUMN) where s.schema = row.SourceSchema and s.entity = row.SourceEntity and s.column = row.SourceColumn and s.level = row.SourceEntityLevel and s.alias = coalesce(row.SourceEntityAlias, 'N/A')
match (t:COLUMN) where t.schema = row.TargetSchema and t.entity = row.TargetEntity and t.column = row.TargetColumn and t.level = row.TargetEntityLevel and t.alias = coalesce(row.TargetEntityAlias, 'N/A')
merge (s)-[:IS_USED_BY {level: 'column'}]->(t);
