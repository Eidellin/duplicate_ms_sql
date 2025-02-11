SELECT
    'ALTER TABLE [BPSSamples].' + QUOTENAME(A.TABLE_SCHEMA) + '.' + QUOTENAME(A.TABLE_NAME) +
    ' ADD CONSTRAINT ' + QUOTENAME(A.CONSTRAINT_NAME) +
    CASE
        WHEN A.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN
            ' PRIMARY KEY (' + STRING_AGG(B.COLUMN_NAME, ', ') + ')'
        WHEN A.CONSTRAINT_TYPE = 'UNIQUE' THEN
            ' UNIQUE (' + STRING_AGG(B.COLUMN_NAME, ', ') + ')'
        WHEN A.CONSTRAINT_TYPE = 'FOREIGN KEY' THEN
            ' FOREIGN KEY (' + STRING_AGG(B.COLUMN_NAME, ', ') + ') REFERENCES ' +
            MAX(QUOTENAME(KCU2.TABLE_SCHEMA) + '.' + QUOTENAME(KCU2.TABLE_NAME)) +
            ' (' + STRING_AGG(KCU2.COLUMN_NAME, ', ') + ')'
        WHEN A.CONSTRAINT_TYPE = 'CHECK' THEN
            ' CHECK (' + CHK.CHECK_CLAUSE + ')'
        ELSE ''
    END + ';' AS AlterTableScript
FROM
    BPSSamples.INFORMATION_SCHEMA.TABLE_CONSTRAINTS A
JOIN
    BPSSamples.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE B
    ON A.CONSTRAINT_NAME = B.CONSTRAINT_NAME
LEFT JOIN
    BPSSamples.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
    ON A.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
LEFT JOIN
    BPSSamples.INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2
    ON RC.UNIQUE_CONSTRAINT_NAME = KCU2.CONSTRAINT_NAME
LEFT JOIN
    BPSSamples.INFORMATION_SCHEMA.CHECK_CONSTRAINTS CHK
    ON A.CONSTRAINT_NAME = CHK.CONSTRAINT_NAME
WHERE
    A.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY', 'CHECK')
GROUP BY
    A.TABLE_SCHEMA, A.TABLE_NAME, A.CONSTRAINT_NAME, A.CONSTRAINT_TYPE, CHK.CHECK_CLAUSE
ORDER BY
    CASE 
        WHEN A.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 1
        WHEN A.CONSTRAINT_TYPE = 'UNIQUE' THEN 2
        WHEN A.CONSTRAINT_TYPE = 'CHECK' THEN 3
        WHEN A.CONSTRAINT_TYPE = 'FOREIGN KEY' THEN 4
        ELSE 5
    END,
	A.TABLE_NAME;
