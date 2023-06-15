_stopWords = ['DECLARE', 
'SELECT', 'DELETE', 'UPDATE', 'INSERT', 'MERGE', 'TRUNCATE', 'WITH',
'IF', 'SET',
'DROP', 'CREATE', 'ALTER', 
'PRINT', 
'EXEC',
'DBCC']

#_continueWords = ['(', 'THEN', 'BEGIN', 'END', 'UNION']

_skipLines = ['BEGIN TRY', 'BEGIN TRAN']

_continueWords = ['(', 'THEN', 'END', 'UNION', 'ALL']
