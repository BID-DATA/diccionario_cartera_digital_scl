# -*- coding: utf-8 -*-
"""
Created on Mon Aug 30 19:17:06 2021

@author: ALOP 
Modified on Tu Nov 2 13:11:09 2021
"""
"con este script se llaman las operaciones del pipeline para el triage digital"

import sys
sys.stdout.encoding
'UTF-8'

import pandas as pd
import ibm_db

##################### Extracción de datos operaciones #########################

conn = ibm_db.connect("DATABASE=bludb;HOSTNAME=slpedw.iadb.org;PORT=50001;security=ssl;UID=usuario;PWD=contrasena;", "", "") #Abriendo conexión con repositorio de datos DB2

sql = "SELECT DISTINCT C.OPER_NUM AS OPERATION_NUMBER, C.OPER_TYP_CD, C.PIPE_YR, C.OPER_ENGL_NM, C.REGN, C.CNTRY_BENFIT, C.TEAM_LEADER_NM, C.TEAM_LEADER_PCM, \
    C.APPRVL_DT as APPROVAL_DATE, C.PREP_RESP_DIV_CD AS DIVISION \
FROM ODS.SPD_ODS_HOPERMAS C \
	JOIN (select OPER_NUM, MAX(DW_CRTE_TS) AS MAX_DT from ODS.SPD_ODS_HOPERMAS GROUP BY OPER_NUM) t ON C.OPER_NUM= t.OPER_NUM and C.DW_CRTE_TS = t.MAX_DT \
WHERE DATE(C.APPRVL_DT) > DATE(NOW()) AND C.PREP_RESP_DEPT_CD='SCL' AND C.OPER_CAT_CD='A'   " #SQL query de datos deseados

stmt = ibm_db.exec_immediate(conn, sql) #Querying data

#Creando base de datos con query
cols = ['OPERATION_NUMBER','APPROVAL_DATE', 'OPER_TYP_CD', 'PIPE_YR', 'DIVISION', 'OPER_ENGL_NM', 'REGN', 'CNTRY_BENFIT', 'TEAM_LEADER_NM', 'TEAM_LEADER_PCM']
Metadatos = pd.DataFrame(columns=cols)
result = ibm_db.fetch_both(stmt)
while(result):
   Metadatos = Metadatos.append(result, ignore_index=True)
   result = ibm_db.fetch_both(stmt)

Metadatos = Metadatos.iloc[:, 0:24]
Metadatos.shape #Visualizando resultado 
    
ibm_db.close #Cerrando la conexión


path = 'C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/Documents/Data Governance - SCL/Cartera digital'

Titulo = Metadatos['OPERATION_NUMBER']

with pd.ExcelWriter(path+"/Outputs/Triage_digital.xlsx") as writer:
    Metadatos.to_excel(writer,sheet_name="Metadata",index=False)
    Titulo.to_excel(writer, sheet_name="operaciones", index=False)