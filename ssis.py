import xml2py
import networkx as nx
import matplotlib.pyplot as plt
from sql_metadata import Parser
import numpy as np

import ssis_constants as sc
import ssis_sql as ss
import ssis_getCode as sgc

class MySSIS:
    pkgDict = ''
    pkgFile = ''

    _pkgTypesColors = {
        'STOCK:SEQUENCE' : {'type':'Contenedor', 'color':'tab:blue'},
        'SSIS.ExecutePackageTask.2' : {'type':'Excecute Pkg', 'color':'tab:green'},
        'Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask.ExecuteSQLTask' : {'type':'SQL Task', 'color':'tab:orange'},
        'SSIS.Pipeline.2' : {'type':'Data Flow Task', 'color':'tab:green'},
        'dataFlow' : {'type':'Data Flow', 'color':'tab:green'}
    }

    
    def __init__(self, file, tipo = 'file'):
        self.pkgDict = ''
        ok = True
        self.pkgFile = file
        print('---------------------------------------------------------------')
        print('-- Procesando')
        if tipo == 'file':
            try:
                self.pkgDict = xml2py.dict_load(file)
            except:
                ok = False
        elif tipo == 'variable':
            try:
                self.pkgFile = 'from variable'
                f =  open("temp/temp.xml", "wb")
                f.write(file.encode())
                f.close()
                self.pkgDict = xml2py.dict_load('temp/temp.xml')
            except:
                print('No se pudo procesar XML')
                ok = False
        if ok:
            try:
                if self.pkgDict['Executable']['_Encrypted'] == '1':
                    print('SSIS ENCRIPTADO')

                    
            except:

                    self._readProperties()
                    self._readVariables()        
                    self._readConnections()
                    self._readControlFlowV2()
                    self._pkgControlFlowNodesSorted = list(nx.topological_sort(self._pkgControlFlowNodes))
        print('---------------------------------------------------------------')

    def _readVariables(self):
        print('-- Leyendo Variables')
        self.pkgVariables = {}
        try:
            for o in self.pkgDict['Executable']['Variable']:

                for e in o['Property']:
                    if e['_Name'] == 'ObjectName':
                        lName = e['_text_']
                    elif e['_Name'] == 'DTSID':
                        lDTSID = e['_text_']
                    elif e['_Name'] == 'Namespace':
                        lNamespace = e['_text_']
                    elif e['_Name'] == 'Expression':
                        try:
                            lExpression = e['_text_']
                            lExpression = lExpression.replace('"', '')
                            lExpression = ss.limpiarComenatios(lExpression)
                            parsedSQL = ss.sqlParse(lExpression)
                        except:
                            lExpression = "Valor dinamico"

                lFullName = (lNamespace + '::' + lName).upper()
                self.pkgVariables[lFullName] = {
                    'Nombre' : lName,
                    'NameSpace' : lNamespace,
                    'DTSID' : lDTSID,
                    'SQL' : parsedSQL
                }
        except:
            print('Error obteniendo variables')

    def _readProperties(self):
        print('-- Leyendo propiedades')
        self.pkgProperties = {}
        if 'Executable' in self.pkgDict.keys():
            if 'Property' in self.pkgDict['Executable'].keys():
                for y in self.pkgDict['Executable']['Property']:
                    try:
                        lname = y['_Name']
                        try:
                            lvalue = y['_text_']
                        except:
                            lvalue = ''
                        self.pkgProperties[lname] = lvalue
                    except Exception as e:
                        print('No posee tag _Name', y)
                        print(e)

    def _readConnections(self):
        print('-- Leyendo conexiones')
        self.pkgConnections = {}
        try:
            for y in self.pkgDict['Executable']['ConnectionManager']:
                try:
                    lObjectName = ''
                    lCreationName = ''
                    lDTSID = ''
                    lConnectionString = ''
                    try:
                        for z in y['Property']:
                            if z['_Name'] == 'DTSID':
                                lDTSID = z['_text_']
                            elif z['_Name'] == 'ObjectName':
                                lObjectName = z['_text_']
                            elif z['_Name'] == 'CreationName':
                                lCreationName = z['_text_']
                            lConnectionString = y['ObjectData']['ConnectionManager']['Property'][1]['_text_']
                    except: # esto se lo hago porque cuando hay un solo objeto me da error
                        for z in self.pkgDict['Executable']['ConnectionManager']['Property']:
                            if z['_Name'] == 'DTSID':
                                lDTSID = z['_text_']
                            elif z['_Name'] == 'ObjectName':
                                lObjectName = z['_text_']
                            elif z['_Name'] == 'CreationName':
                                lCreationName = z['_text_']
                                lCreationName = ''

                    self.pkgConnections[lDTSID] = { 
                        'Nombre' : lObjectName + ' (' + lCreationName + ')',
                        'String' : lConnectionString
                    }
                except Exception as e:
                    print('Problema leyendo XML - ConnectionManager')
                    print(e)
        except:
            print('---- No hay conexiones detectadas')



    def _readControlFlowV2(self):
        print('-- Leyendo Control Flow V2')
        self.pkgControlFlow = {}
        self._pkgControlFlowNodes = nx.DiGraph()
        if 'Executable' in self.pkgDict.keys():
            if 'Executable' in self.pkgDict['Executable'].keys():
                self._readControlFlowV2Objetos(self.pkgDict['Executable']['Executable'])
        self._readControlFlowConstrainsV2()


    def _readControlFlowV2Objetos(self, subobj):
        if type(subobj) is list: # revisamos si tenemos una colección de objetos o solo un objeto
            for o in subobj:
                self._readControlFlowV2Propiedades(o)
                if 'Executable' in o.keys():
                    self._readControlFlowV2Objetos(o['Executable'])

        else:
            if 'Executable' in subobj.keys():
                    self._readControlFlowV2Objetos(subobj['Executable'])
            self._readControlFlowV2Propiedades(subobj)                    

    def _readControlFlowV2Propiedades(self, obj):
        ltype = obj['_ExecutableType']
        lObjectName = ''
        parsedSQL = []
        lTipoOrigen = ''
        lConnection = ''
        dataFlows = []
        paths = []
        for y in obj['Property']:
            if y['_Name'] == 'DTSID':
                lDTSID = y['_text_']
            elif y['_Name'] == 'ObjectName':
                lObjectName = y['_text_']
        if ltype in ('SSIS.Pipeline.2'):
            dataFlows, paths = self._readDataFlow(obj)
        else:
            try:                
                lSQL = obj['ObjectData']['SqlTaskData']['_SqlStatementSource']
                lSQL = ss.limpiarComenatios(lSQL)
                lTipoOrigen = obj['ObjectData']['SqlTaskData']['_SqlStmtSourceType']
                lConnection = obj['ObjectData']['SqlTaskData']['_Connection']
            except:
                lSQL = ''
                lTipoOrigen = ''
                lConnection = ''
            if ltype not in self._pkgTypesColors.keys():
                ltype = ltype.split(',')[0]   
            if lSQL != '':
                if lTipoOrigen == 'Variable':
                    lSQL = lSQL.upper()
                    lSQL = ss.limpiarComenatios(lSQL)
                    try:
                        parsedSQL = self.pkgVariables[lSQL]['SQL']
                    except:
                        parsedSQL = []
                        print('Variable no encontrada', lSQL)
                else:
                    parsedSQL = ss.sqlParse(lSQL)   
            else:
                        parsedSQL = []
        parsedSQL_tmp = []
        for s in parsedSQL:
            if s[0] == 'EXEC':
                sp = s[2][0]
                codeSP = sgc.getSPCode(sp)
                parsedSQLSP = ss.sqlParse(codeSP)
                for sSp in parsedSQLSP:
                    parsedSQL_tmp.append(sSp)
            else:
                parsedSQL_tmp.append(s)      
        parsedSQL = parsedSQL_tmp
        if lTipoOrigen == 'Variable':
            lVariable = lSQL
        else:
            lVariable = ''   

        temp = []
        for df in dataFlows:
            if dataFlows[df]['SQL'] != '':
                temp =  ss.sqlParse(dataFlows[df]['SQL'])
                lNombre = dataFlows[df]['Nombre']
                temp[0].append(lNombre)
                parsedSQL.append(temp[0])



        self.pkgControlFlow[lDTSID] = {
            'Nombre' : lObjectName, 
            'Tipo' : ltype, 
            'Origen' : lTipoOrigen,
            'Variable' : lVariable, 
            'SQL' : parsedSQL,
            'Connection' : lConnection} 

        self._pkgControlFlowNodes.add_node(lDTSID)


        

    def _readDataFlow(self, subobj): 
        dataFlows = {}
        path = []
        if 'components' in  subobj['ObjectData']['pipeline'].keys():
            for c in subobj['ObjectData']['pipeline']['components']['component']:
                try:
                    lID = c['_id']
                    lNombre = c['_name']
                except:
                    print('Error leyendo DataFlow')
                    lID = ''
                    lNombre = ''

                lSQL = ''
                lIdOutput = []
                lIdInput = []
                lAccessMode = ''

                try:
                    for p in c['properties']['property']:
                        if p['_name'] == 'AccessMode':
                            lAccessMode = p['_text_']
                except:
                    pass
                try:
                    for o in c['outputs']['output']:
                        lIdOutput.append(o['_id'])
                except Exception as e:
                    try:
                        lIdOutput.append(c['outputs']['output']['_id'])
                    except Exception as e:
                        print(e)

                        
                try:
                    lIdInput.append(c['inputs']['input']['_id'])
                except:
                    pass       
                try:
                    lSQL = ''
                    for p in c['properties']['property']:
                        if p['_name'] in ['OpenRowset'] and lAccessMode in ['0','3']:
                            if '_text_' in p.keys():
                                lSQL = 'select * from '+ p['_text_']
                        elif p['_name'] in ['SqlCommand'] and lAccessMode == '2':
                            if '_text_' in p.keys():
                                lSQL = p['_text_']
                        else:
                            pass
                            #print('--->Nombre:',lNombre, ' modo:',lAccessMode)
                            #print(p)

                except:
                        lSQL = ''
                dataFlows[lID] = {'Nombre' : lNombre, 'SQL': lSQL, 'idInput' : lIdInput, 'idOutput' : lIdOutput, 'AccessMode' : lAccessMode}
        else:
            print('Sin compoenents!!')

        if 'paths' in  subobj['ObjectData']['pipeline'].keys():
            if type(subobj['ObjectData']['pipeline']['paths']['path']) is dict:
                comienzaEn = subobj['ObjectData']['pipeline']['paths']['path']['_startId']
                terminaEn = subobj['ObjectData']['pipeline']['paths']['path']['_endId']
                path.append([comienzaEn, terminaEn])
            else:
                for p in subobj['ObjectData']['pipeline']['paths']['path']:
                        comienzaEn = p['_startId']
                        terminaEn = p['_endId']
                        path.append([comienzaEn, terminaEn])

        return dataFlows, path

    def listControlFlow(self):
        #Red = '\033[91m'
        #Green = '\033[92m'
        #Blue = '\033[94m'
        #Cyan = '\033[96m'
        #White = '\033[97m'
        #Yellow = '\033[93m'
        #Magenta = '\033[95m'
        #Grey = '\033[90m'
        #Black = '\033[90m'
        #Default = '\033[99m'
    
      noImprimir = ['Comentario', 'IF', 'SET', 'PRINT', 'DECLARE']

      for p in list(nx.topological_sort(self._pkgControlFlowNodes)):
        print('ID:', p)
        try:
          tipo = self._pkgTypesColors[self.pkgControlFlow[p]['Tipo']]['type']
        except:
          tipo = 'Desconocido [' + self.pkgControlFlow[p]['Tipo'] +']'
        print( '\033[95m [' + tipo + '] \033[94m' + self.pkgControlFlow[p]['Nombre'],'\033[90m')
        if self.pkgControlFlow[p]['Origen'] == 'Variable':
            print('\t\033[96mObtenido desde variable:',self.pkgControlFlow[p]['Variable'],'\033[90m')
        for t in self.pkgControlFlow[p]['SQL']:
            if t[0] not in noImprimir:
                texto = ''
                try:
                    texto = ' [' + t[4] + ']'
                except:
                    pass
                print('\t\033[90m', t[0], '\033[92m', t[2], '\033[90m',texto)
        for z in self._pkgControlFlowDependence:
          if z[1] == p:
            print('\033[93m    Dependencias: \033[92m', self.pkgControlFlow[z[0]]['Nombre'],'\033[90m')


    def _readControlFlowConstrainsV2(self):
        print('---- Leyendo contrains de control flow V2')
        self._pkgControlFlowDependence = []
        try:
            self._readControlFlowConstrainsDetailsV2(self.pkgDict['Executable']['PrecedenceConstraint'])
        except:
            print('---Error leyendo constrains')

    def _readControlFlowConstrainsDetailsV2(self, subObj):
        if type(subObj) is list: # revisamos si tenemos una colección de objetos o solo un objeto
            for o in subObj:
                self._readControlFlowConstrainsPropertiesV2(o['Executable'])
        else:
            self._readControlFlowConstrainsPropertiesV2(subObj['Executable'])

    def _readControlFlowConstrainsPropertiesV2(self, subObj):
        ldesde = ''
        lhasta = ''
        for x in subObj:
            if x['_IsFrom'] == '-1':
                ldesde = x['_IDREF']
            else:
                lhasta = x['_IDREF']
            if ldesde != '' and lhasta !='':
                self._pkgControlFlowNodes.add_edge(ldesde, lhasta)
                self._pkgControlFlowDependence.append([ldesde, lhasta])

    def _readControlFlowConstrains(self):
        print('---- Leyendo contrains de control flow')
        ldesde = ''
        lhasta = ''
        self._pkgControlFlowDependence = []
        for y in self.pkgDict['Executable']['PrecedenceConstraint']:
            for x in y['Executable']:
                if x['_IsFrom'] == '-1':
                    ldesde = x['_IDREF']
                else:
                    lhasta = x['_IDREF']
            self._pkgControlFlowNodes.add_edge(ldesde, lhasta)
            self._pkgControlFlowDependence.append([ldesde, lhasta])





    def GraphNodes(self):
        plt.rcParams['figure.figsize'] = [20, 20]
        # shapes: https://matplotlib.org/stable/api/markers_api.html
        lLabels = {}
        lColors = []
        for k, v in self.pkgControlFlow.items():
            lLabels[k] = v['Nombre']
            if v['Tipo'] in self._pkgTypesColors.keys():
                lColors.append(self._pkgTypesColors[v['Tipo']]['color'])
            else:
                lColors.append('tab:red')
        nx.draw_spring(
                self._pkgControlFlowNodes, 
                labels=lLabels,
                node_color=lColors,
                node_size=[len(v) * 500 for v in lLabels.values()],
                with_labels=True, 
                font_weight='bold',
                node_shape='8')
        plt.show()
        self._GraphNodesPrintReferences()


    def _GraphNodesPrintReferences(self):
        print('-------------------------------------------------')
        print('-- Referencias')
        print('-------------------------------------------------')
        for y in self._pkgTypesColors:
            print('-- Tipo de objeto', self._pkgTypesColors[y]['type'],'color', self._pkgTypesColors[y]['color'])
