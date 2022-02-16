# -*- coding: utf-8 -*-
"""
Created on Tue Feb, 15 19:17:06 2021

@author: MARIAREY

"""
"Este es el primer archivo que se debe correr en pipeline. Con este script se llaman las operaciones del pipeline para el triage digital. Este código sirve únicamente \
para actualizar el excel con nombre y númerp de la operación y preseleccionar si la operación \
es digital o no"

import sys
sys.stdout.encoding
'UTF-8'

import pandas as pd
import ibm_db
import openpyxl
from openpyxl import load_workbook

##################### Extracción de datos operaciones #########################

# Aquí se extrae la información de las operaciones del pipeline

conn = ibm_db.connect("DATABASE=bludb;HOSTNAME=slpedw.iadb.org;PORT=50001;security=ssl;UID=mariarey;PWD=Andrea$15121995;", "", "") #Abriendo conexión con repositorio de datos DB2

sql_pipe = "SELECT DISTINCT C.OPER_NUM as OPERATION_NUMBER, C.PIPE_YR, C.OPER_ENGL_NM as OPERATION_NAME, C.OPERTYP_ENGL_NM AS OPERATION_TYPE_NAME, C.MODALITY_CD AS OPERATION_MODALITY, C.PREP_RESP_DEPT_CD AS DEPARTMENT, C.PREP_RESP_DIV_CD AS DIVISION,\
	C.PIPE_YR, C.TEAM_LEADER_NM, C.TEAM_LEADER_PCM, C.REGN AS REGION, C.CNTRY_BENFIT AS COUNTRY, C.STS_CD AS STATUS, C.STG_ENGL_NM AS STAGE, C.STS_ENGL_NM AS TAXONOMY, C.APPRVL_DT AS APPROVAL_DATE, C.APPRVL_DT_YR as APPROVAL_YEAR,\
    C.ORIG_APPRVD_USEQ_AMNT AS APPROVAL_AMOUNT, C.CURNT_DISB_EXPR_DT as CURRENT_EXPIRATION_DATE, C.RELTN_NUM AS RELATED_OPER, C.OPER_TYP_CD AS OPERATION_TYPE, A.OBJTV_ENGL as OBJECTIVE_EN,\
    A.OBJTV_SPANISH as OBJECTIVE_ES, C.FACILITY_TYP_ENGL_NM AS OUTPUT_DESCRIPTION \
FROM ODS.SPD_ODS_HOPERMAS C \
JOIN ( select OPER_NUM, MAX(DW_CRTE_TS) AS MAX_DT from ODS.SPD_ODS_HOPERMAS GROUP BY OPER_NUM) t ON C.OPER_NUM= t.OPER_NUM and C.DW_CRTE_TS = t.MAX_DT \
 	JOIN ODS.OPER_ODS_OPER A ON C.OPER_NUM = A.OPER_NUM \
WHERE (DATE(C.APPRVL_DT) > DATE(NOW()) OR C.APPRVL_DT is null) AND C.PREP_RESP_DEPT_CD='SCL' AND C.OPER_CAT_CD='A' AND C.PIPE_YR>2020 AND (C.OPER_TYP_CD='LON' OR C.OPER_TYP_CD = 'TCP')" #SQL query de datos deseados pipeline A"
    
stmt_pipe = ibm_db.exec_immediate(conn, sql_pipe) #Querying data

#Creando base de datos con query pipe
cols_pipe = ['OPERATION_NUMBER', 'OPERATION_NAME', 'PIPE_YR', 'OPERATION_TYPE_NAME', 'OPERATION_MODALITY', 'DEPARTMENT', 'DIVISION', 'TEAM_LEADER_NM',  'REGION', 'COUNTRY', 'STATUS', 'STAGE', 'TAXONOMY', 'APPROVAL_DATE', 'APPROVAL_YEAR', 'APPROVAL_AMOUNT', 'CURRENT_EXPIRATION_DATE', 'RELATED_OPER', 'OPERATION_TYPE', 'OBJECTIVE_EN', 'OBJECTIVE_ES', 'COMPONENT_NAME', 'OUTPUT_NAME', 'OUTPUT_DESCRIPTION']
Metadatos_pipe = pd.DataFrame(columns=cols_pipe)
result_pipe = ibm_db.fetch_both(stmt_pipe)
while(result_pipe):
    Metadatos_pipe = Metadatos_pipe.append(result_pipe, ignore_index=True)
    result_pipe = ibm_db.fetch_both(stmt_pipe)

Metadatos_pipe = Metadatos_pipe.iloc[:, 0:24]
Metadatos_pipe.shape #Visualizando resultado 

ibm_db.close #Cerrando la conexión
#base 

Base_pipe_oper=Metadatos_pipe[['OPERATION_NUMBER', 'OPERATION_NAME', 'PIPE_YR']]

##### Guardar #####

path_cl = 'C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/documents' 

with pd.ExcelWriter(path_cl+"/operaciones-pipe.xlsx") as writer:
    Base_pipe_oper.to_excel(writer,sheet_name="Oper",index=False)
    