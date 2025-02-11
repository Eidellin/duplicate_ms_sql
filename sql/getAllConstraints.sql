SELECT A.TABLE_NAME, 
	A.CONSTRAINT_NAME, 
	B.COLUMN_NAME
FROM 
	{DATABASE}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS A, 
	{DATABASE}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE B
WHERE 
    A.CONSTRAINT_NAME = B.CONSTRAINT_NAME
ORDER BY 
	A.TABLE_NAME;
