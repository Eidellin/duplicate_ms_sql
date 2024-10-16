SELECT 
    COLUMN_NAME,
    DATA_TYPE + 
        CASE 
            WHEN DATA_TYPE IN ('char', 'varchar', 'nchar', 'nvarchar') 
            THEN 
                CASE 
                    WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN '(max)' 
                    ELSE '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')' 
                END
            WHEN DATA_TYPE IN ('decimal', 'numeric') 
            THEN '(' + CAST(NUMERIC_PRECISION AS VARCHAR) + ',' + CAST(NUMERIC_SCALE AS VARCHAR) + ')' 
            ELSE '' 
        END AS DATA_TYPE,
    CASE
        WHEN IS_NULLABLE = 'YES' THEN 'NULL'
        ELSE 'NOT NULL'
    END AS IS_NULLABLE,
    COLUMN_DEFAULT
FROM 
    {DATABASE}.INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_NAME = '{TABLE}';
