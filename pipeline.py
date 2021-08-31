# -*- coding: utf-8 -*-
"""
Created on Mon Aug 30 19:17:06 2021

@author: ALOP
"""
"con este script se llaman las operaciones del pipeline para el triage digital"

import sys
sys.stdout.encoding
'UTF-8'

import numpy as np
import pandas as pd
from pandas import DataFrame
import unicodedata
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from itertools import chain
from datetime import datetime, date, time, timedelta
import re, string
from textblob import TextBlob

import os
import time

import ibm_db


##################### Extracción de datos operaciones #########################

con = ibm_db.connect("DATABASE=bludb;HOSTNAME=slpedw.iadb.org;PORT=50001;security=ssl;UID=alop;PWD=Colombia31.044064;", "", "") #Abriendo conexión con repositorio de datos DB2 

sql = "SELECT DISTINCT C.OPER_NUM AS OPERATION_NUMBER, C.OPER_TYP_CD, C.PIPE_YR, C.OPER_ENGL_NM, C.REGN, C.CNTRY_BENFIT, C.TEAM_LEADER_NM, C.TEAM_LEADER_PCM \
FROM ODS.SPD_ODS_HOPERMAS C \
	JOIN ( select OPER_NUM, MAX(DW_CRTE_TS) AS MAX_DT from ODS.SPD_ODS_HOPERMAS GROUP BY OPER_NUM) t ON C.OPER_NUM= t.OPER_NUM and C.DW_CRTE_TS = t.MAX_DT \
WHERE C.APPRVL_DT_YR > 2020 AND C.PREP_RESP_DEPT_CD='SCL' AND C.OPER_CAT_CD='A'   " #SQL query de datos deseados

stmt = ibm_db.exec_immediate(con, sql) #Querying data

#Creando base de datos con query
cols = ['OPERATION_NUMBER', 'OPER_TYP_CD', 'PIPE_YR', 'OPER_ENGL_NM', 'REGN', 'CNTRY_BENFIT', 'TEAM_LEADER_NM', 'TEAM_LEADER_PCM']
Metadatos = pd.DataFrame(columns=cols)
result = ibm_db.fetch_both(stmt)
while(result):
   Metadatos = Metadatos.append(result, ignore_index=True)
   result = ibm_db.fetch_both(stmt)

Metadatos = Metadatos.iloc[:, 0:24]
Metadatos.shape #Visualizando resultado 
    
ibm_db.close #Cerrando la conexión


path = 'C:/Users/ALOP/Inter-American Development Bank Group/Cartera Digital SCL - General'

with pd.ExcelWriter(path+"/documents/Triage_digital1.xlsx") as writer:
    Metadatos.to_excel(writer,sheet_name="Metadata",index=False)