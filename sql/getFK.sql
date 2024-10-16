-- not in used --
SELECT 
    KCU1.TABLE_NAME AS ReferencingTableName,
    KCU1.COLUMN_NAME AS ReferencingColumnName,
    RC.CONSTRAINT_NAME AS ForeignKeyConstraintName,
    KCU2.TABLE_NAME AS ReferencedTableName,
    KCU2.COLUMN_NAME AS ReferencedColumnName
FROM 
    {DATABASE}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC
INNER JOIN 
    {DATABASE}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1 
    ON RC.CONSTRAINT_NAME = KCU1.CONSTRAINT_NAME
INNER JOIN 
    {DATABASE}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU2 
    ON RC.UNIQUE_CONSTRAINT_NAME = KCU2.CONSTRAINT_NAME
ORDER BY 
    ReferencingTableName;
