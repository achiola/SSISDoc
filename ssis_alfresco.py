import http.client
import json
import mimetypes
from codecs import encode
import json
from datetime import datetime



class alfresco:
    token = ''
    token_time = 0
    alfresco_url = 'archivodigital.agdcorp.com.ar'
    alfresco_port = 8433
    alfresco_user = 'ssis'
    alfresco_password = 'ssis2023'
    alfresco_destination_folder = "/Sites/sistemas---bi/documentlibrary/SSIS"

    def __init__(self):
        self.token = self.getAlfrescoToken()

    def getAlfrescoToken(self):
        print('Generando token alfresco')
        conn = http.client.HTTPSConnection(self.alfresco_url, self.alfresco_port)

        payload = "{\r\n  \"userId\": \"" + self.alfresco_user + "\",\r\n  \"password\": \""+ self.alfresco_password+ "\"\r\n}‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍‍"
        headers = {
          'Content-Type': 'application/json'
        }
        payload = payload.encode('utf-8')

        conn.request("POST", "/alfresco/api/-default-/public/authentication/versions/1/tickets", payload, headers)
        res = conn.getresponse()
        data = res.read()
        temp = data.decode("utf-8")
        token = json.loads(temp)['entry']['id']
        self.token_time = datetime.now()
        return token

    def sendFile(self, carpeta, archivo, nombre):
        minutos = (self.token_time - datetime.now()).total_seconds() / 60
        if minutos > 60:
            self.getAlfrescoToken()

        print('Carpeta:', carpeta)
        print('archivo', archivo)
        print('nombre', nombre)
        conn = http.client.HTTPSConnection(self.alfresco_url, self.alfresco_port)
        dataList = []
        boundary = 'wL36Yn8afVp8Ag7AmP8qZ0SA4n1v9T'
        dataList.append(encode('--' + boundary))
        dataList.append(encode('Content-Disposition: form-data; name=filedata; filename={0}'.format(nombre)))

        fileType = mimetypes.guess_type(nombre)[0] or 'application/octet-stream'
        dataList.append(encode('Content-Type: {}'.format(fileType)))
        dataList.append(encode(''))

        with open(archivo, 'rb') as f:
          dataList.append(f.read())
        dataList.append(encode('--' + boundary))
        dataList.append(encode('Content-Disposition: form-data; name=relativePath;'))

        dataList.append(encode('Content-Type: {}'.format('text/plain')))
        dataList.append(encode(''))

        dataList.append(encode(self.alfresco_destination_folder+carpeta))
        dataList.append(encode('--'+boundary+'--'))
        dataList.append(encode(''))
        body = b'\r\n'.join(dataList)
        payload = body
        headers = {
           'Content-type': 'multipart/form-data; boundary={}'.format(boundary) 
        }
        conn.request(
            "POST", 
            "/alfresco/api/-default-/public/alfresco/versions/1/nodes/-root-/children?alf_ticket="+ self.token, 
            payload, 
            headers
        )
        res = conn.getresponse()
        data = res.read()
        return data.decode("utf-8")
    
    
