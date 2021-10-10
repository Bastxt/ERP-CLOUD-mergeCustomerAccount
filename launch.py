# -----------------------------------------------------------
# Carga masiva de contactos usando servicios de creacion y sincronizacion
# Los contactos se crean unicamente en el sitio aignado en la plantilla CSV
#
# (C) 2021 Oscar Roncancio, Bogotá, Colombia
# email oscar_roncancio@itis.com.co | oscar-roncancio@outlook.com
# -----------------------------------------------------------

import sys, time, threading,os
import requests
import xml.etree.ElementTree as ET
import csv
from tabulate import tabulate
from time import sleep
from rich.console import Console
from rich.table import Table
import time
from rich.live import Live

#instancia de clase console para uso de consola enriquezida
"""console: [asdasd]"""
console = Console()

#parametrizacion createPerson xml
#se carga la plantilla que contiene los campos necesarios para la creacion de contacto
filename = "CreateContact.xml"
xmlTree = ET.parse(filename)
rootElement = xmlTree.getroot() 

#parametrizacion mergeCustomerAccount xml
#se carga la plantilla que contiene los campos necesarios para la sincronizacion de contacto
filename_b = "mergeCustomerAccount.xml"
xmlTreeMerge = ET.parse(filename_b)
rootElementMerge = xmlTreeMerge.getroot() 

#conteo de etiquetas
c=0
cnt = 0
#conteo de registros
ln=0        #conteo de lineas
lnErro=0    #conteo de lineas con problemas[CSV]
wsCreOk = 0 #conteo de contactos creados
wsCreEr = 0 #conteo de contactos en error
relatOk = 0 #conteo de contactos vinculados
relatEr = 0 #conteo de contactos vinculados error
#Diccionario para creacion
payloadsCreatelog = {}
payloadsCreate = []
tabla = []


#tags para modificacion de payload createPerson
tags = [
"ns2:PersonFirstName",
"ns2:PersonLastName",
"ns3:ObjectId",
"ns3:StartDate",
"ns3:PrimaryFlag",
"ns4:PrimaryFlag",
"ns4:StartDate",
"ns4:EmailAddress",
]

#Prefijos para modificacion de payload createPerson
prefix_map = {
"ns2": "http://xmlns.oracle.com/apps/cdm/foundation/parties/personService/",
"ns3":"http://xmlns.oracle.com/apps/cdm/foundation/parties/relationshipService/",
"ns4":"http://xmlns.oracle.com/apps/cdm/foundation/parties/contactPointService/",
}

#Prefijos para respuesta createPerson
prefix_map_res = {
"ns5":"http://xmlns.oracle.com/apps/cdm/foundation/parties/relationshipService/"
}

#Prefijos para mergeCustomerAccount
prefix_map_merge = {
"ns1":"http://xmlns.oracle.com/apps/cdm/foundation/parties/customerAccountService/applicationModule/types/",
"ns2":"http://xmlns.oracle.com/apps/cdm/foundation/parties/customerAccountService/"
}

#tags para mergeCustomerAccount
tagsMerge = [
"ns2:CustomerAccountId",
"ns2:PartyId" ,
"ns2:CustomerAccountSiteId",
"ns2:PartySiteId",
"ns2:RelationshipId"
]


#Tabla que mostrara los elemenntaos encontrados en el archivo CSV
table = Table(show_header=True, header_style='bold #2070b2',
        title='[bold][#2070b2]CARGA DE CONTACTOS DESDE CSV')

table.add_column('Nombre', justify='right')
table.add_column('Apellido')
table.add_column('Organizacion', justify='center')
table.add_column('Fecha_c Contact', justify='center')
table.add_column('Pm Contact')
table.add_column('Pm Email')
table.add_column('Fecha_c Email')
table.add_column('Email')
table.add_column('Estado')

  
#Lectura de CSV
with open('contactos.csv', newline='') as csvfile:
  reader = csv.DictReader(csvfile,delimiter=';')
  for row in reader:
    #Validacion de campos completos
    if row['PersonFirstName'] != "" and row['PersonLastName'] != "" and row['ObjectId'] != "" and row['StartDate'] != "" and row['PrimaryFlag'] != "" and row['PrimaryFlag2'] != "" and row['StartDate3'] != "" and row['EmailAddress'] != "":
      creData=[row['PersonFirstName'],row['PersonLastName'],row['ObjectId'],row['StartDate'],row['PrimaryFlag'],row['PrimaryFlag2'],row['StartDate3'],row['EmailAddress']]
      mergeData = [row['CustomerAccountId'],row['PartyId'],row['CustomerAccountSiteId'],row['PartySiteId'],row['Cuenta']]
      ln+=1

      #Creacion de payloads createPerson
      for tag in tags:
        for element in rootElement.findall(".//"+tag,prefix_map):
          element.text = creData[c]
        c+=1  
      #Se añaden los elementos necesarios al diccionario payloadsCreate (posicion,payload,idCuenta,EmailContacto,array de datos para servico mergeCustomerAccount)
      payloadsCreate.append([ln,ET.tostring(rootElement).decode(),row['ObjectId'],row['EmailAddress'],mergeData])
      c=0
      table.add_row(row['PersonFirstName'],row['PersonLastName'],row['ObjectId'],row['StartDate'],row['PrimaryFlag'],row['PrimaryFlag2'],row['StartDate3'],row['EmailAddress'],'[bold green]OK')
    else:
        #Datos incorrectos
        table.add_row('[bold red]'+row['PersonFirstName']+'[/bold red]','[bold red]'+row['PersonLastName']+'[/bold red]','[bold red]'+row['ObjectId']+'[/bold red]','[bold red]'+row['StartDate']+'[/bold red]','[bold red]'+row['PrimaryFlag']+'[/bold red]','[bold red]'+row['PrimaryFlag2']+'[/bold red]','[bold red]'+row['StartDate3']+'[/bold red]','[bold red]'+row['EmailAddress']+'[/bold red]','[bold red]ERROR')
        lnErro+=1

  console.print(table)
  console.print("")
  console.print("Total Registros Leidos: [bold][green]"+str(ln))
  console.print("Total Registros Error: [bold][red]"+str(lnErro))

console.print("")
console.print("Lanzando servicio de creacion de contacto [createPerson:FoundationPartiesPersonService]...")
console.print("")

with console.status("[bold green]Creando contacto...") as status:
    while payloadsCreate:
        item = payloadsCreate.pop(0)
        #lanzar servicio de creacion:
        url="https://[url dominio]:443/foundationParties/PersonService"
        #headers
        headers = {'content-type': 'text/xml'}
        body = item[1]
        response = requests.post(url,data=body,headers=headers,auth=('CRM_ADMIN','1T1s2o2i$$'))
        sleep(1)
        if response.status_code == 200:
          xmlResponse = ET.ElementTree(ET.fromstring(response.content))
          rElement = xmlResponse.getroot() 
          #Busqueda de id de relacion para sincronizacion de contacto
          for element in rElement.findall(".//ns5:RelationshipId",prefix_map_res):
            #se agrega al elemento actual
            item[4].append( element.text)
          console.log("[bold green]createPerson[/bold green] | "+"Registro "+f"{item[0]}"+" [gray]| Cuenta: [bold cyan]"+item[4][4]+"[/bold cyan]"+" ID Relacion: "+item[4][5]+" Status: [bold green]["+ str(response.status_code)+"]")
          wsCreOk+=1
        else:
          console.log("[bold green]createPerson[/bold green] | "+"Registro "+f"{item[0]}"+" [gray]| Cuenta: [bold cyan]"+item[4][4]+"[/bold cyan] Status: [bold red]["+ str(response.status_code)+"]")        
          wsCreEr+=1

        #Crear payloads mergeCustomerAccount
        for tagM in tagsMerge:
          for elementMerg in rootElementMerge.findall(".//"+tagM,prefix_map_merge):
            if tagM == "ns2:RelationshipId":
              elementMerg.text = item[4][cnt+1]
            else:
              elementMerg.text = item[4][cnt]
          cnt+=1
        cnt=0


        #lanzar servicio mergeCustomerAccount:
        url_m="https://[url dominio]:443/crmService/CustomerAccountService"
        #headers
        headers_m = {'content-type': 'text/xml'}
        body_m = ET.tostring(rootElementMerge).decode()
        response_m = requests.post(url_m,data=body_m,headers=headers_m,auth=('user','password'))
        sleep(1)

        if response_m.status_code == 200:
          console.log(" [bold green]mergeCustomerAccount[/bold green] | "+"Registro "+f"{item[0]}"+" [gray]| Cuenta: [bold cyan]"+item[4][4]+"[/bold cyan]"+" ID Relacion: "+item[4][5]+" Status: [bold green]["+ str(response.status_code)+"]")
          relatOk+=1
        else:
          console.log(" [bold green]mergeCustomerAccount[/bold green] | "+"Registro "+f"{item[0]}"+" [gray]| Cuenta: [bold cyan]"+item[4][4]+"[/bold cyan] Status: [bold red]["+ str(response.status_code)+"]")        
          relatEr+=1

console.print("")
console.print("Total Registros Creados: [bold][green]"+str(wsCreOk))
console.print("Total Registros Vinculados: [bold][green]"+str(relatOk))
console.print("Total Registros en Error: [bold][red]"+str(wsCreEr)+"|"+str(relatEr))