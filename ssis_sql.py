from multiprocessing.resource_sharer import stop
from pyrsistent import l
import ssis_constants as sc


def parseSQLMain( lType, lSql):
    if lType == 'IF':
        resultadoIF = parseIF(lSql)
        resultado = []
    elif lType == 'CREATE':
        resultado = parseCreate(lSql)
    elif lType == 'DROP':
        resultado = parseDrop(lSql)    
    elif lType == 'DELETE':
        resultado = parseDelete(lSql)
    elif lType == 'TRUNCATE':
        resultado = parseTruncate(lSql) 
    elif lType == 'DECLARE':
        resultado = ['','']
    elif lType == 'SET':
        resultado = ['','']
    elif lType == 'SELECT':
        resultado = parseSelect(lSql)        
    elif lType == 'INSERT':
        resultado = parseInsert(lSql)
    elif lType == 'UPDATE':
        resultado = parseUpdate(lSql)        
    elif lType == 'MERGE':
        resultado = parseMerge(lSql)
    elif lType == 'PRINT':
        resultado = ['','']        
    elif lType == 'ALTER':
        resultado = parseAlter(lSql)    
    elif lType == 'DBCC':
        resultado = ['','']
    elif lType == 'WITH':
        resultado = parseWith(lSql)
    elif lType == 'COMENTARIO':    
        resultado = ['','']
    elif lType == 'EXEC':
        resultado = parseExec(lSql)
    else:
        print('--> Comando no implementado:', lType)
        print('SQL-->', lSql)
        resultado = 'N/I'
    return resultado

def parseExec(lSql):
    lINTO = []
    lFROM = []    
    lSqlSplit = lSql.split()
    if lSqlSplit[0] == 'EXEC':  
        descarto =   lSqlSplit.pop(0)
        lINTO = lSqlSplit.pop(0)
    else:
        print('No es un comando EXEC')        

    return [lINTO, lFROM]

def parseWith(lSql):
    lINTO = []
    lFROM = []    
    lSqlSplit = lSql.split()
    fin = False
    
    if lSqlSplit[0] == 'WITH':
        descartamos = lSqlSplit.pop(0)
        while len(lSqlSplit) > 0 and fin == False:
            sentencia = ''
            lINTO.append(lSqlSplit.pop(0)) #tabla temporal que se crea
            if len(lSqlSplit) > 0:
                descartamos = lSqlSplit.pop(0) # debería haber un AS
            if descartamos == 'AS':
                contador = 1
                while contador > 0 and len(lSqlSplit) > 0:
                    abrir = lSqlSplit[0].count('(')
                    cerrar = lSqlSplit[0].count(')')
                    contador += abrir - cerrar 
                    sentencia += ' '+lSqlSplit.pop(0)
                sentencia = sentencia.strip()                    
                if sentencia[0] == "(":
                    sentencia = sentencia[1:]
                if sentencia[-1] == ")":
                    sentencia = sentencia[:-1]
                resultado =  parseSelect(sentencia)     
                try:
                    lFROM.append(resultado[1])
                except:
                    print('Error haciendo append en sentencia with - lFrom')
            else:
                fin = True
    else:
        print('No es un comando WITH')        
    lINTO = list(lINTO) 
    lFROM = list(lFROM)

    return [lINTO, lFROM]

def parseAlter(lSql):
    lINTO = ''
    lFROM = []    
    lSqlSplit = lSql.split()
    if lSqlSplit[0] == 'ALTER':
        if lSqlSplit[1] == 'TABLE':
            return [lSqlSplit[2],'']
        else:
            print('ALTER no imprementado', lSqlSplit[1])
    else:
        print('No es un comando ALTER')

def parseMerge(lSql):
    lINTO = ''
    lFROM = []    

    lSqlSplit = lSql.split()
    try:
        if lSqlSplit[0] == 'MERGE':
            tmp = lSqlSplit.pop(0) # Descarto la palabra MERGE
            lINTO = lSqlSplit.pop(0) # Tomo la tabla a la cual hacemos MERGER
            while lSqlSplit[0][:5] != 'USING':
                tmp = lSqlSplit.pop(0) # Descartamos todo hasta encontrar USING 
                if len(lSqlSplit)==0:
                    break # Sino hay USING es una instruccion MERGER no reconocida
            
            if lSqlSplit[0][:5] == 'USING':
                tmp = lSqlSplit.pop(0) #descartamos

            if lSqlSplit[0][0:5] == 'USING(':
                lSqlSplit[0] = lSqlSplit[0][5:]


            if lSqlSplit[0][0] == '(' :
                    parentesis = 1
                    sentencia = ''
                    lSqlSplit[0] = lSqlSplit[0][1:]

                    while parentesis > 0:
                        abrir = lSqlSplit[0].count('(')
                        cerrar = lSqlSplit[0].count(')')
                        parentesis += abrir - cerrar
                        sentencia += ' ' + lSqlSplit.pop(0)

                    lFROM = parseSelect(sentencia)
                    lFROM = lFROM[1]
            else:
                lFROM = lSqlSplit.pop(0)
    except:
        print('MERGE -->', lSql)
    return [lINTO, lFROM]

def parseUpdate(lSql):
    lINTO = ''
    lFROM = []
    lSqlSplit = lSql.split()
    if lSqlSplit[0] == 'UPDATE':
        tmp = lSqlSplit.pop(0)
        lINTO = lSqlSplit.pop(0)
        while len(lSqlSplit)>0:
            if lSqlSplit[0] == 'FROM':
                fakeSelect = 'SELECT * ' + ' '.join(lSqlSplit)
                lFROM = parseSelect(fakeSelect)[1]
            tmp = lSqlSplit.pop(0)
    else:
        print('No es un comando UPDATE')
    return [lINTO, lFROM]


def parseDrop(lSql):
    lSqlSplit = lSql.split()
    if lSqlSplit[0] == 'DROP':
        if lSqlSplit[1] == 'TABLE':
            #print('**************Se crea la tabla \033[91m\033[1m', lSqlSplit[2],'\033[90m\033[0m')
            return [lSqlSplit[2],'']
        else:
            print('DROP no imprementado', lSqlSplit[1])
    else:
        print('No es un comando DROP')


def parseInsert(lSql):
    lINTO = ''
    lFROM = []

    lSqlSplit = lSql.split()
    if lSqlSplit[0] == 'INSERT':
        x = lSqlSplit.pop(0) # Quitamos el insert
        if lSqlSplit[0] == 'INTO':
            lSqlSplit.pop(0) # Quitamos el INTO
            lINTO = lSqlSplit.pop(0)
            if len(lSqlSplit) > 0:
                if lSqlSplit[0][0] == '(':
                    while lSqlSplit[0][-1] != ')':
                        tmp = lSqlSplit.pop(0) # Descartamos el detalle de campos
                    tmp = lSqlSplit.pop(0)
                if len(lSqlSplit) > 0:
                    if lSqlSplit[0][:6] != 'VALUES':
                        try:
                            sentencia = ' '.join(lSqlSplit)
                            resultado = parseSelect(sentencia)
                            lFROM = resultado[1]
                        except:
                            print('Error joining', lSqlSplit[0][:6] )
                    else:
                        lFROM = ['inserta valores']
            return [lINTO,lFROM]
        else:
            print('Comando insert no implementado', lSqlSplit[0])
    else:
        print('No es un comando INSERT')

def parseSelect(lSql):
    lSqlSplit = lSql.split()
    lINTO = ''
    lFROM = []
    if lSqlSplit[0] == 'SELECT':
        while len(lSqlSplit)>0:
            actual = lSqlSplit.pop(0)
            proxima = ''
            if actual == 'INTO' and len(lSqlSplit) > 0:
                lINTO = lSqlSplit.pop(0)
            elif actual == 'FROM' and len(lSqlSplit) > 0:
                proxima = lSqlSplit.pop(0)
                if proxima[0] != '(' and len(lSqlSplit) > 0:
                    lFROM.append(proxima)
            elif actual == 'JOIN' and len(lSqlSplit) > 0:
                proxima = lSqlSplit.pop(0)
                if proxima[0] != '(' and len(lSqlSplit) > 0:
                    lFROM.append( proxima )
        return [lINTO,list(set(lFROM))]
    else:
        print('SQL--->', lSql)
        print('No es un comando SELECT')

def parseTruncate(lSql):
    lSqlSplit = lSql.split()
    if lSqlSplit[0] == 'TRUNCATE':
        if lSqlSplit[1] == 'TABLE':
            #print('**************Se eliminan todos los registros de la tabla \033[91m\033[1m', lSqlSplit[2],'\033[90m\033[0m')
            return [lSqlSplit[2],'']
        else:
            print('TRUNCATE no imprementado', lSqlSplit[1])
    else:
        print('No es un comando TRUNCATE')

def parseDelete(lSql):
    lSqlSplit = lSql.split()
    if lSqlSplit[0] == 'DELETE':
        descartar = lSqlSplit.pop(0)
        if lSqlSplit[0] == 'FROM':
            descartar = lSqlSplit.pop(0)
        return [lSqlSplit[0],''] 

    else:
        print('No es un comando DELETE')

def parseCreate(lSql):
    lSqlSplit = lSql.split()
    if lSqlSplit[0] == 'CREATE':
        if lSqlSplit[1] == 'TABLE':
            #print('**************Se crea la tabla \033[91m\033[1m', lSqlSplit[2],'\033[90m\033[0m')
            return [lSqlSplit[2],'']
        elif lSqlSplit[1] == 'UNIQUE' and lSqlSplit[2] == 'CLUSTERED' and lSqlSplit[3] == 'INDEX':
            return ['Crea un idx','']

            return [lSqlSplit[2],'']
        elif lSqlSplit[1] == 'PROCEDURE':            
            pass
        else:
            print('CREATE no imprementado', lSqlSplit[1])
    else:
        print('No es un comando CREATE')

def parseIF(lSql):

    lSqlSplit = lSql.split()

    beginS = False
    condition = ""
    statement = ""

    while len(lSqlSplit) > 0:
        actual = lSqlSplit.pop(0)
        if actual in ['BEGIN', 'END', 'ELSE']:
            pass
        else:
            if actual in sc._stopWords and actual not in ['IF'] and condition[-1] != '(':
                    beginS = True

            if beginS == False:
                condition += ' ' + actual
            else:
                statement += ' ' + actual
    return ['', statement]

def sqlParse(sql):
    statements = []
    if sql == None:
        return statements
    try:
        sql = sql.upper()
        sql = limpiarComenatios(sql)
        tmp = ''
        for a in sql.split():
            if len(a)>1:
                a = a.replace(')',' ) ')
                a = a.replace('(',' ( ')
            tmp += a + ' '
        sql = tmp
    except:
        print('sql-->', sql)
    sql = sql.replace("WITH (NOLOCK)", "(NOLOCK)")
    sql = sql.replace("WITH(NOLOCK)", "(NOLOCK)")

    sqlSplit = sql.split()
    tables = []
    columns = []
    first = True
    insert = False
    sentencia = ''
    tipo = ''
    continuar = False
    updateset = False
    comentario = False
    alter = False
    

    while len(sqlSplit) > 0:
        tables = []
        if sqlSplit[0][:6] == 'INSERT':
            insert = True
        elif sqlSplit[0][:6] == 'VALUES':
            insert = False
        if sqlSplit[0][:6] == 'UPDATE':
            updateset = True
        if sqlSplit[0][:5] == 'ALTER':
            alter = True
        if alter and sqlSplit[0] in sc._stopWords and sqlSplit[0] not in ( ['WITH', 'ALTER']):
            alter = False
        
        nuevaInstruccion = False
        if  (sqlSplit[0] in sc._stopWords and continuar == False):
            if (updateset and sqlSplit[0] == 'SET') == False:
                if (alter and sqlSplit[0] == 'WITH') == False:
                    if comentario == False:
                        nuevaInstruccion = True

        if (sqlSplit[0] == '/*' or sqlSplit[0][0:2] == '/*'):
            comentario = True
        if first: # Primer paralabra
            first = False
            tipo = sqlSplit[0]
            if comentario:
                tipo = 'COMENTARIO'
            sentencia = sqlSplit.pop(0)
        else: # Nueva instruccion
            if nuevaInstruccion: 
                tables = ['**Sin Dato']
                columns = ['**Sin Dato']
                tables = parseSQLMain( lType = tipo, lSql = sentencia)
                statements.append([tipo, sentencia, tables, columns])
                tipo = sqlSplit[0]
                sentencia = sqlSplit.pop(0)
                if (sqlSplit[0] == '/*' or sqlSplit[0][0:2] == '/*'):
                    comentario == True
                    tipo = 'COMENTARIO'        
            else:
                actual = sqlSplit.pop(0)
                if (actual == '*/' or actual[-2:] == '*/'):
                    comentario = False
                if updateset and actual == 'SET':
                    updateset = False   
                #if actual in sc._continueWords or insert:
                if actual in sc._continueWords:
                    if actual != 'END':
                        continuar = True
                    else:
                        continuar = False
                        insert = False
                else:
                    continuar = False
                    insert = False
                sentencia += ' '+ actual
    if tables == [] and sentencia != '':
        tables = parseSQLMain( lType = tipo, lSql = sentencia)

    statements.append([tipo, sentencia, tables, columns])
    return statements

def limpiarComenatios(texto):
    lineas = texto.splitlines()
    lineaLimpia = []
    for l in lineas:
        l = l.strip()
        i = l.find('--')
        if i != -1:
            l=l[0:i]
        if not l.startswith(tuple(sc._skipLines)):
            lineaLimpia.append(l)

    return '\n'.join(lineaLimpia)