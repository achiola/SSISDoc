import pyodbc
import json

connStr= 'Driver={SQL Server Native Client 10.0};Server=agdgdap01,1065;Database=dwcorp;Trusted_Connection=yes;'

def sendPKG(obj, path ='SinDato'):
    pkgReady = True
    try:
        pkgID = obj.pkgProperties['DTSID']
        pkgVersion = obj.pkgProperties['VersionGUID']
        pkgName = obj.pkgProperties['ObjectName']
        if path == 'SinDato':
            pkgFisicalName = obj.pkgFile
        else:
            pkgFisicalName = path
        pkgFullDict = obj.pkgDict
    except:
        pkgReady = False
    if pkgReady:
        pkg = {
            'pkgID' : pkgID,
            'pkgVersion' : pkgVersion,
            'pkgName' : pkgName,
            'pkgFisicalName' : pkgFisicalName,
            'pkgFullDict' : pkgFullDict
        }

        # Persistimos el pkg (lo mandamos a db)
        existe = pkgExist(pkgID, pkgVersion)
        if existe == 0:
            print('No existe, insertando')
            pkgInsert(pkg)
        elif existe == 1:
            print('Existe y es la misma version')
        elif existe == 2:
            print('Existe y NO es la misma version')
            pkgInsert(pkg)
        # Persistimos el coneciones (lo mandamos a db)
        pkgConnections(pkgID, pkgVersion, obj.pkgConnections)
        pkgVariables(pkgID, pkgVersion, obj.pkgVariables)
        pkgFlow(pkgID, pkgVersion, obj.pkgControlFlow, obj._pkgControlFlowNodesSorted)
    else:
        print('Error recuperando propiedades y persistiendo pkg')

def pkgFlow(pkgID, pkgVersion, flow, flowSorted):
    conn = pyodbc.connect(connStr)
    cursor = conn.cursor()        

    # Eliminamos si existe algo
    sql = """delete from docu.pkgFlow where pkgID = ? and pkgVersion = ?"""
    cursor.execute(sql, pkgID, pkgVersion)

    # Insertamos Flow
    sql = '''
    insert into docu.pkgFlow
    (pkgID,
    pkgVersion,
    pkgFlowId,
    pkgFlowSecueniaID,
    pkgFlowNombre,
    pkgDataFlowNombre,
    pkgFlowTipo,
    pkgFlowOrigen,
    pkgVariableID,
    pkgConnectionID,
    pkgFlowSQLCommand,
    pkgFlowSQL,
    pkgFlowSQLTableWirte,
    pkgFlowSQLTableRead,
    pkgFlowSQLTables,
    pkgFlowSort )
    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    #try:
    
    for y in flow:
        flowSort = flowSorted.index(y)
        i = 0
        for z in flow[y]['SQL']:
            try:
                lWrite = json.dumps(z[2][0])
            except:
                lWrite = ''
            try:
                lRead = json.dumps(z[2][1])
            except:
                lRead = ''
            try:
                lDataFlow= json.dumps(z[4])
            except:
                lDataFlow = ''                    

            if z[2]:
                for t in z[2][1]:
                    if t != '""':
                        cursor.execute(
                            sql,
                            pkgID,
                            pkgVersion,
                            y,
                            i,
                            flow[y]['Nombre'],
                            lDataFlow,
                            flow[y]['Tipo'],
                            flow[y]['Origen'],
                            flow[y]['Variable'],
                            flow[y]['Connection'],
                            z[0],
                            z[1], 
                            lWrite, #tableWrite
                            lRead, #tableread
                            limpiarNombreTabla(t),
                            flowSort
                            )
                        i+=1
                if z[2][0] != '""' and z[2][0] != '':
                    cursor.execute(
                        sql,
                        pkgID,
                        pkgVersion,
                        y,
                        i,
                        flow[y]['Nombre'],
                        lDataFlow,
                        flow[y]['Tipo'],
                        flow[y]['Origen'],
                        flow[y]['Variable'],
                        flow[y]['Connection'],
                        z[0],
                        z[1], 
                        lWrite, #tableWrite
                        lRead, #tableread
                        limpiarNombreTabla(lWrite),
                        flowSort
                        )
                    i+=1     
            else:
                cursor.execute(
                    sql,
                    pkgID,
                    pkgVersion,
                    y,
                    i,
                    flow[y]['Nombre'],
                    lDataFlow,
                    flow[y]['Tipo'],
                    flow[y]['Origen'],
                    flow[y]['Variable'],
                    flow[y]['Connection'],
                    z[0],
                    z[1], 
                    lWrite, #tableWrite
                    lRead, #tableread
                    '',
                    flowSort
                    )                    
            i+=1
            conn.commit()
    #except Exception as e:            
    #    print(e)
    conn.close()    

def pkgVariables(pkgID, pkgVersion, variables):
    conn = pyodbc.connect(connStr)
    cursor = conn.cursor()        

    # Eliminamos si existe algo
    sql = """delete from docu.pkgVariables where pkgID = ? and pkgVersion = ?"""
    cursor.execute(sql, pkgID, pkgVersion)

    # Insertamos variables
    sql = '''
    insert into docu.pkgVariables
    (pkgID, pkgVersion, pkgVariableId, pkgVariableNombre, pkgVariableNameSpace, pkgVariableSQLSecuenceId, pkgVariableSQLCommand, 
    pkgVariableSQL, pkgVariableSQLTableWirte, pkgVariableSQLTableRead)
    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    try:
        for y in variables:
            i = 0
            for z in variables[y]['SQL']:
                cursor.execute(
                    sql, 
                    pkgID, 
                    pkgVersion, 
                    variables[y]['DTSID'],
                    y, #pkgVariableNombre
                    variables[y]['NameSpace'],
                    i, # Secuencia
                    z[0], # Comando
                    z[1], # sql
                    json.dumps(z[2][0]), #tableWrite
                    json.dumps(z[2][1]) #tableread
                    )
                i+=1
                conn.commit()
    except Exception as e:            
        print(e)
    conn.close()    


def pkgConnections(pkgID, pkgVersion, connections):
    conn = pyodbc.connect(connStr)
    cursor = conn.cursor()    
    try:
        # Si existe lo eliminamos
        sql = """delete from docu.pkgConnections where pkgID = ? and pkgVersion = ?"""
        cursor.execute(sql, pkgID, pkgVersion)
        conn.commit()
        sql = """insert into docu.pkgConnections (pkgID, pkgVersion, pkgConnectionID, pkgConnectionNombre, pkgConnectionString) values (?,?,?,?,?)"""
        for c in connections:
            cursor.execute(sql, pkgID, pkgVersion, c, connections[c]['Nombre'],  connections[c]['String'])
            conn.commit()
    except Exception as e:            
        print(e)
    conn.close()

def pkgExist(pkgID, pkgVersion):
    """
    Controla si un pkg existe en la db y si es la misma versión
    Devulve:
    0 == No existe
    1 == Existe y es la misma versión
    2 == Existe y no es la misma versión
    """
    conn = pyodbc.connect(connStr)
    cursor = conn.cursor()    
    sql = """select count(1) from docu.pkgDefinition where pkgId = ? and pkgVersion = ?"""
    cursor.execute(sql, pkgID, pkgVersion)
    q = cursor.fetchone()
    
    if q[0] != 0:
        conn.close()
        return 1
    else:
        sql = """select count(1) from docu.pkgDefinition where pkgId = ? """
        cursor.execute(sql, pkgID)
        q = cursor.fetchone()
        if q[0] != 0:
            conn.close()
            return 2
        else:
            conn.close()
            return 0

def pkgInsert(pkg):
    conn = pyodbc.connect(connStr)    
    cursor = conn.cursor()        
    sql = '''
    insert into docu.pkgDefinition (pkgID, pkgVersion, pkgNombre, pkgLocation, pkgFullDict)
    values ( ?, ?, ?, ?, ? )
    '''
    try:
        cursor.execute(sql, pkg['pkgID'], pkg['pkgVersion'], pkg['pkgName'], pkg['pkgFisicalName'], json.dumps(pkg['pkgFullDict']))
    except:
        print('Falla.. probamos sin definición..')
        try:
            cursor.execute(sql, pkg['pkgID'], pkg['pkgVersion'], pkg['pkgName'], pkg['pkgFisicalName'], '')
        except Exception as e:
            print('pkgName', pkg['pkgName'])
            print('pkgFisicalName', pkg['pkgFisicalName'])
            print(e)
    conn.commit()
    conn.close()

def limpiarNombreTabla(tabla):
    if type(tabla) == list:
        if len(tabla) == 0:
            tabla = ''
        else:
            try:
                tabla = tabla[0]
            except:
                print('Tipo listo, pero no.. -->',tabla)
            
    tablaResultado = tabla.split('.')[-1]
    tablaResultado = tablaResultado.replace('[','')
    tablaResultado = tablaResultado.replace(']','')
    tablaResultado = tablaResultado.replace('"','')
    tablaResultado = tablaResultado.replace(';','')
    tablaResultado = tablaResultado.replace('\'','')

    return tablaResultado