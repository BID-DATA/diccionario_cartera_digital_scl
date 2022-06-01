# -*- coding: utf-8 -*-
"""
Created on Mon Aug 30 19:17:06 2021

@author: ALOP 

Modified on Tu Nov 2 13:11:09 2021
@modified: MARIAREY
"""
"con este script se llaman las operaciones del pipeline para el triage digital, se conecta con las respuestas del checklist y se filtra la preselección de operaciones digitales"

import sys
sys.stdout.encoding
'UTF-8'

import pandas as pd
import ibm_db
import openpyxl
from openpyxl import load_workbook
import numpy as np

##################### Extracción de datos operaciones #########################

# Aquí se extrae la información de las operaciones del pipeline

conn = ibm_db.connect("DATABASE=bludb;HOSTNAME=slpedw.iadb.org;PORT=50001;security=ssl;UID=;PWD=;", "", "") #Abriendo conexión con repositorio de datos DB2

sql_pipe = "SELECT DISTINCT C.OPER_NUM as OPERATION_NUMBER, C.PIPE_YR, C.OPER_ENGL_NM as OPERATION_NAME, C.OPERTYP_ENGL_NM AS OPERATION_TYPE_NAME, C.MODALITY_CD AS OPERATION_MODALITY, C.PREP_RESP_DEPT_CD AS DEPARTMENT, C.PREP_RESP_DIV_CD AS DIVISION,\
	C.PIPE_YR, C.TEAM_LEADER_NM, C.TEAM_LEADER_PCM, C.REGN AS REGION, C.CNTRY_BENFIT AS COUNTRY, C.STS_CD AS STATUS, C.STG_ENGL_NM AS STAGE, C.STS_ENGL_NM AS TAXONOMY, C.APPRVL_DT AS APPROVAL_DATE, C.APPRVL_DT_YR as APPROVAL_YEAR,\
    C.ORIG_APPRVD_USEQ_AMNT AS APPROVAL_AMOUNT, C.CURNT_DISB_EXPR_DT as CURRENT_EXPIRATION_DATE, C.RELTN_NUM AS RELATED_OPER, C.OPER_TYP_CD AS OPERATION_TYPE, A.OBJTV_ENGL as OBJECTIVE_EN,\
    A.OBJTV_SPANISH as OBJECTIVE_ES, C.FACILITY_TYP_ENGL_NM AS OUTPUT_DESCRIPTION, C.OPER_CAT_CD AS PIPE_T, C.QRR_DT AS QRR_DATE \
FROM ODS.SPD_ODS_HOPERMAS C \
JOIN ( select OPER_NUM, MAX(DW_CRTE_TS) AS MAX_DT from ODS.SPD_ODS_HOPERMAS GROUP BY OPER_NUM) t ON C.OPER_NUM= t.OPER_NUM and C.DW_CRTE_TS = t.MAX_DT \
 	JOIN ODS.OPER_ODS_OPER A ON C.OPER_NUM = A.OPER_NUM \
WHERE (DATE(C.APPRVL_DT) > DATE(NOW()) OR C.APPRVL_DT is null) AND C.PREP_RESP_DEPT_CD='SCL' AND (C.OPER_CAT_CD='A' OR C.OPER_CAT_CD is null) AND C.PIPE_YR>2020 AND (C.OPER_TYP_CD='LON' OR C.OPER_TYP_CD = 'TCP')" #SQL query de datos deseados pipeline A"
    
stmt_pipe = ibm_db.exec_immediate(conn, sql_pipe) #Querying data

#Creando base de datos con query pipe
cols_pipe = ['OPERATION_NUMBER', 'OPERATION_NAME', 'PIPE_YR', 'PIPE_T', 'OPERATION_TYPE_NAME', 'OPERATION_MODALITY', 'DEPARTMENT', 'DIVISION', 'TEAM_LEADER_NM',  'REGION', 'COUNTRY', 'STATUS', 'STAGE', 'TAXONOMY', 'APPROVAL_DATE', 'APPROVAL_YEAR', 'APPROVAL_AMOUNT', 'CURRENT_EXPIRATION_DATE', 
             'RELATED_OPER', 'OPERATION_TYPE', 'OBJECTIVE_EN', 'OBJECTIVE_ES', 'COMPONENT_NAME', 'OUTPUT_NAME', 'OUTPUT_DESCRIPTION', 'QRR_DATE']
Metadatos_pipe = pd.DataFrame(columns=cols_pipe)
result_pipe = ibm_db.fetch_both(stmt_pipe)
while(result_pipe):
    Metadatos_pipe = Metadatos_pipe.append(result_pipe, ignore_index=True)
    result_pipe = ibm_db.fetch_both(stmt_pipe)

Metadatos_pipe = Metadatos_pipe.iloc[:, 0:26]
Metadatos_pipe.shape #Visualizando resultado 

ibm_db.close #Cerrando la conexión

######### Juntar archivo de pipeline con revisión de texto para ver cuáles salieron como dig #######

Base_pipe=Metadatos_pipe[['OPERATION_NUMBER', 'OPERATION_NAME', 'PIPE_YR', 'PIPE_T', 'OPERATION_TYPE','OPERATION_TYPE_NAME',
           'OPERATION_MODALITY','TAXONOMY','STATUS','REGION','COUNTRY','DEPARTMENT','DIVISION','TEAM_LEADER_NM','APPROVAL_DATE','CURRENT_EXPIRATION_DATE', 
           'OBJECTIVE_EN', 'OBJECTIVE_ES', 'QRR_DATE']]

##### Leer información de checklist #####

path_cl = 'C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/documents/Inputs' 
checklist = pd.read_excel(path_cl+"/Triage_digital.xlsx", sheet_name = "Sheet1")
georef = pd.read_excel(path_cl+"/Georef.xlsx", sheet_name = "Sheet1")

checklist["OPERATION_NUMBER"]=checklist["Número de la operación:"]
checklist['INFO']=checklist['¿Se prevé comprar algún tipo de tecnología (tablets, servidores) o pagar por algún tipo de servicio digital (conexión a internet, licenciamientos, licencias de servicios como tableau?']
checklist['INFRA']=checklist['¿Se prevé desarrollar o adquirir sistemas de información, repositorios de información, hacer levantamiento de datos que van a ser almacenados de manera digital? ¿Se harán normas de interoperabilidad, terminologías?']
checklist['GOB']=checklist['¿Se prevé hacer cambios, creación de normas relacionadas con temas digitales (protección de información, historia clínica electrónica, firma digital, SIGED), adopción, creación de estándares, planes o estrategias?']
checklist['CULTURA']=checklist['¿Se prevé la creación o fortalecimiento de competencias, acciones de gestión de cambio, comunicación, uso de la información (capacitación para manejo y lectura de dashboards)?']
checklist['DIGITAL_TRANS']=checklist['Digital_trans']
checklist['DUMMY_DIGITAL_CL']=checklist['Digital_port']
checklist_tidy = checklist[['OPERATION_NUMBER', 'INFO','INFRA','GOB','CULTURA','DUMMY_DIGITAL_CL', 'DIGITAL_TRANS']]

checklist_tidy = checklist_tidy.dropna(subset=['OPERATION_NUMBER'])
checklist_tidy = checklist_tidy.dropna(subset=['INFO'])
#pegar todas las bases
Base_pipe = Base_pipe.merge(checklist_tidy, on = 'OPERATION_NUMBER', how = 'left')
Base_pipe['DUMMY_DIGITAL_CL'] = Base_pipe['DUMMY_DIGITAL_CL'].fillna("Pending Checklist") # las que no tienen checklist en pending
Base_pipe['DIGITAL_TRANS'] = Base_pipe['DIGITAL_TRANS'].fillna("Pending Checklist") # las que no tienen checklist en pending
Base_pipe['DUMMY_DIGITAL_CL'] = Base_pipe['DUMMY_DIGITAL_CL'].replace(1, "Digital")
Base_pipe['DIGITAL_TRANS'] = Base_pipe['DIGITAL_TRANS'].replace(1, "Digital Transformation")


##### Leer información de las que preclasificamos como digitales
preclasi = pd.read_excel(path_cl+"/operaciones - preclas.xlsx", sheet_name = "Sheet1")
preclasi['PRE CLASIFICACIÓN'] = preclasi['PRE CLASIFICACIÓN'].replace(0, "No se considera que se deba llenar CL")
Base_pipe = Base_pipe.merge(preclasi[['PRE CLASIFICACIÓN', 'OPERATION_NUMBER']], on = 'OPERATION_NUMBER', how = 'left')
Base_pipe['PIPE_YR'] = np.where(Base_pipe['OPERATION_TYPE'] == 'LON', Base_pipe['PIPE_YR'].astype(str) + '-' + Base_pipe['PIPE_T'], Base_pipe['PIPE_YR'].astype(str))


Base_pipe = Base_pipe.merge(georef[['COUNTRY', 'ABR_PBI']], on = 'COUNTRY', how = 'left')

Base_pipe =  Base_pipe.drop_duplicates()
Base_pipe = Base_pipe.sort_values(['DIGITAL_TRANS', 'APPROVAL_DATE'])

Base_seguimiento = Base_pipe[['OPERATION_NUMBER','OPERATION_NAME','OPERATION_TYPE', 'PIPE_YR', 'APPROVAL_DATE', 'QRR_DATE',
                              'OBJECTIVE_EN', 'OBJECTIVE_ES', 'INFO','INFRA','GOB','CULTURA', 'DIGITAL_TRANS', 'DIVISION','TEAM_LEADER_NM']]

Base_seguimiento = Base_seguimiento.sort_values(['DIGITAL_TRANS', 'APPROVAL_DATE'])

Base_seguimiento_TC = Base_seguimiento[Base_seguimiento['OPERATION_TYPE']== 'TCP']

Base_seguimiento_LON = Base_seguimiento[Base_seguimiento['OPERATION_TYPE']== 'LON']

##### Guardar #####

path_save = "C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/"

Base_pipe['DIGITAL']=Base_pipe['DUMMY_DIGITAL_CL']
Base_pipe=Base_pipe[['OPERATION_NUMBER','OPERATION_NAME','OPERATION_TYPE', 'PIPE_YR','OPERATION_TYPE_NAME','OPERATION_MODALITY','TAXONOMY','STATUS','REGION','COUNTRY', 'ABR_PBI',
                     'DEPARTMENT','DIVISION','TEAM_LEADER_NM','APPROVAL_DATE','PRE CLASIFICACIÓN', 'DIGITAL', 'INFO','INFRA','GOB','CULTURA', 'DIGITAL_TRANS']]

with pd.ExcelWriter(path_save+"cartera digital/Dashboard/output-pipe.xlsx") as writer:
    Base_pipe.to_excel(writer,sheet_name="Metadata",index=False)
    
with pd.ExcelWriter(path_save+"Seguimiento/seguimiento-operaciones.xlsx") as writer:
    Base_seguimiento_TC.to_excel(writer,sheet_name="Seguimiento TC",index=False)
    Base_seguimiento_LON.to_excel(writer,sheet_name="Seguimiento LON",index=False)
