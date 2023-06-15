from timeit import repeat
import pyodbc
import ssis as s
import ssis_getCode as sgc
import ssis_toDB as sdb
import ssis_alfresco as sal

from io import StringIO 
import sys
import gc
import os as os
connStr= 'Driver={SQL Server Native Client 10.0};Server=agdgdap01,1065;Database=dwcorp;Trusted_Connection=yes;'

class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout



def procesar_msdb():

    alf = sal.alfresco()

    parentFolderId = '00000000-0000-0000-0000-000000000000'
    parentFolderName = 'Root'
    
    conn = pyodbc.connect(connStr)
    cursor = conn.cursor()        

    #sql = "select p.id, p.name FROM [msdb].[dbo].[sysssispackages] as p where p.folderid = '00000000-0000-0000-0000-000000000000'"
    #sql = "select top 50 p.id, p.name, p.verid FROM [msdb].[dbo].[sysssispackages] as p where p.id = 'F2F87831-60A3-4CDF-A3B4-4210A4D872F0'"
    sql = "select p.id, p.name, p.verid FROM [msdb].[dbo].[sysssispackages] as p"
    sql2 = """update [docu].[pkgDefinition] set pkgLog = ? where pkgID = ? and pkgVersion = ?"""
    cursor.execute(sql)
    conn2 = pyodbc.connect(connStr)
    c2 = conn2.cursor()                             
    i = 0
    q = len(cursor.fetchall())
    nombre = 'N/A'
    cursor.execute(sql)
    while True:
        i += 1
        row = cursor.fetchone()
        if i <= q:
            print('Procesando ', i, '/', q, 'Paquete-->', row[0])
        else:
            print('-- Finalizo')
        if row == None:
            break
        else:
            with Capturing() as salida:

                print('Procesando paquete:',row[1], '-', row[0])
                xml = sgc.getSSISCode(row[0])
                obj = s.MySSIS(xml,tipo='variable' )
                path = sgc.getSSISPath(row[0])
                #try:
                sdb.sendPKG(obj, path)
                #except:
                #    print('No se ha enviado a DB')
            try:
                id = obj.pkgProperties['DTSID']
                ver = obj.pkgProperties['VersionGUID']
                nombre = obj.pkgProperties['ObjectName']
                c2.execute(sql2, '\n'.join(salida), id, ver)
                c2.commit()
            except:
                pass
            c2.commit()
        gc.collect()
        print(path)
        folder = ''
        if nombre != 'N/A':
            f = path.split('\\')
            for partes in f:
                folder += '/' + partes

            nombre = nombre + '.dtsx'
            newName = 'temp/'+nombre
            try:
                os.remove(newName)
            except:
                pass
            os.rename('temp/temp.xml', newName)
                    
            alf.sendFile(carpeta = folder, archivo = newName, nombre = nombre)        
            os.remove(newName)
    conn2.close()

def procesar_directorio(ruta_completa):
    alf = sal.alfresco()
    import os
    sql2 = """update [docu].[pkgDefinition] set pkgLog = ? where pkgID = ? and pkgVersion = ?"""
    conn2 = pyodbc.connect(connStr)
    c2 = conn2.cursor()         

    for e in os.walk(ruta_completa):
        # 0 = carpeta actual
        # 1 = subcarpetas
        # 2 = archivos
        print('Procesando-->', ruta_completa)
        for o in e[2]:
            if o[-5:] == '.dtsx':
                print('Procesando paquete:', ruta_completa)
                salida = ''
                #with Capturing() as salida:
                ruta_completa = os.path.join(e[0], o)
                print('Procesando paquete:', ruta_completa)
                obj = s.MySSIS(ruta_completa)
                try:
                    sdb.sendPKG(obj, ruta_completa)
                except:
                    print('No se ha enviado a DB')
                try:
                    id = obj.pkgProperties['DTSID']
                    ver = obj.pkgProperties['VersionGUID']
                    c2.execute(sql2, '\n'.join(salida), id, ver)
                    c2.commit()
                except:
                    pass
                c2.commit()
                del obj
                gc.collect()
                folder = ''
                f = ruta_completa.split('\\')
                for partes in f[1:-1]:
                    folder += '/' + partes
                nombre = f[-1]                    
                folder += '/' + nombre[:-5]
                folder += '/' + ver 
                
                alf.sendFile(carpeta = folder, archivo = ruta_completa, nombre = nombre)
    conn2.close()
