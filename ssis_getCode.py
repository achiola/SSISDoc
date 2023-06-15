import pyodbc
import pandas as pd

conn = pyodbc.connect (    
    'Driver={SQL Server};'
    'Server=agdgdap01,1065;'
    'Database=dwcorp;'
    'Trusted_Connection=yes;'
)

connStr= 'Driver={SQL Server Native Client 10.0};Server=agdgdap01,1065;Database=dwcorp;Trusted_Connection=yes;'


def getSSISCode(id):
    sql = """
    select CONVERT(VARCHAR(MAX),CONVERT(VARBINARY(MAX), packagedata)) as code
    FROM [msdb].[dbo].[sysssispackages] as p
    join [msdb].[dbo].sysssispackagefolders as f
    on p.folderid = f.folderid
    where p.id = '"""+ id +"""'
    """
    df = pd.read_sql_query(sql, conn)
    return df['code'][0]


def getSPCode(spName):
    try:
        sql = """
            SELECT OBJECT_DEFINITION (OBJECT_ID(N'"""+spName+"""')) as code
        """
        df = pd.read_sql_query(sql, conn)
        return df['code'][0]
    except:
        print('getSPCode error', spName)
        return ''

def getSSISPath(id):
    sql = """
with tree as (
select 
*
,0 as nivel
from [msdb].[dbo].sysssispackagefolders 
where folderid in (select folderid from [msdb].[dbo].[sysssispackages] where id = ?)

union all

select
s.*
,f.nivel + 1 as nivel
from [msdb].[dbo].sysssispackagefolders as s 
join tree as f
on s.folderid = f.parentfolderid
)
select foldername, nivel
from tree order by nivel desc
    """
    conn = pyodbc.connect(connStr)
    cursor = conn.cursor()        
    cursor.execute(sql, id)
    path = ''
    while True:
        row = cursor.fetchone()
        if row == None:
            break
        else:
            if row[0] == '':
                path = 'ROOT_msdb\\'
            else:
                path += row[0]+'\\'
    print('-->',path)
    return path
