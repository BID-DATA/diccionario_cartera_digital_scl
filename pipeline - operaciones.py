# -*- coding: utf-8 -*-
"""
Created on Tue Feb, 15 19:17:06 2021

@author: MARIAREY

"""
"Este es el primer archivo que se debe correr en pipeline. Con este script se llaman las operaciones del pipeline para el triage digital. Este código sirve únicamente \
para actualizar el excel con nombre y número de la operación y preseleccionar si la operación \
es digital o no"

import sys

sys.stdout.encoding
"UTF-8"

import sys
import pandas as pd
import pyodbc
from openpyxl import load_workbook
import numpy as np

##################### Extracción de datos operaciones #########################

# Aquí se extrae la información de las operaciones del pipeline

# Set up the connection to the database
conn = pyodbc.connect(
    "DRIVER=DenodoODBC Unicode(x64);"
    "SERVER=datamarketplace;"
    "DATABASE=ledw;"
    # your IADB credentials
    "UID=youruser;"
    "PWD=yourpassword;"
    "CHARSET=UTF-8;"
)

cursor = conn.cursor()

# SQL query of data desired from pipeline A
sql_pipe = "SELECT C.OPER_NUM as OPERATION_NUMBER, C.PIPE_YR, C.OPER_ENGL_NM as OPERATION_NAME, C.OPERTYP_ENGL_NM as OPERATION_TYPE_NAME, \
   C.MODALITY_CD as OPERATION_MODALITY, C.PREP_RESP_DEPT_CD as DEPARTMENT, C.PREP_RESP_DIV_CD as DIVISION, C.TEAM_LEADER_NM, C.TEAM_LEADER_PCM, \
   C.REGN as REGION, C.CNTRY_BENFIT as COUNTRY, C.STS_CD as STATUS, C.STG_ENGL_NM as STAGE, C.STS_ENGL_NM as TAXONOMY,  C.APPRVL_DT as APPROVAL_DATE, \
   C.APPRVL_DT_YR as APPROVAL_YEAR, C.OPER_STG_II as POD_DATE, C.ORIG_APPRVD_USEQ_AMNT as APPROVAL_AMOUNT, C.CURNT_DISB_EXPR_DT as CURRENT_EXPIRATION_DATE, \
   C.RELTN_NUM as RELATED_OPER, C.OPER_TYP_CD as OPERATION_TYPE, C.FACILITY_TYP_ENGL_NM as OUTPUT_DESCRIPTION, C.OPER_CAT_CD as PIPE_T, C.QRR_DT as QRR_DATE, \
   A.OBJTV_ENGL as OBJECTIVE_EN, A.OBJTV_SPANISH as OBJECTIVE_ES \
  FROM ledw.spd_ods_hopermas C \
  JOIN (select OPER_NUM, MAX(DW_CRTE_TS) AS MAX_DT from ledw.spd_ods_hopermas GROUP BY OPER_NUM) t ON C.OPER_NUM = t.OPER_NUM and C.DW_CRTE_TS = t.MAX_DT \
  JOIN ledw.oper_ods_oper A ON C.OPER_NUM = A.OPER_NUM \
  WHERE (C.prep_resp_dept_cd='SCL' AND C.PIPE_YR>=2022 AND (C.OPER_TYP_CD='TCP' OR C.OPER_TYP_CD='LON') AND \
         (C.OPER_CAT_CD='A' OR C.OPER_CAT_CD is null))"


# Querying data
cursor.execute(sql_pipe)

# Fetching the data as binary
rows = cursor.fetchall()

# Decoding the binary data to strings using UTF-8 encoding
decoded_rows = []
for row in rows:
    decoded_row = []
    for value in row:
        if isinstance(value, bytes):
            decoded_row.append(value.decode("utf-8", errors="replace"))
        else:
            decoded_row.append(value)
    decoded_rows.append(decoded_row)

# Defining the column names
column_names = [column[0] for column in cursor.description]

# Creating the DataFrame
Metadatos_pipe = pd.DataFrame(decoded_rows, columns=column_names)

# Closing the connection
conn.close()

### Analysis pipeline ######
############################


######### Juntar archivo de pipeline con revisión de texto para ver cuáles salieron como dig #######

# Database in mayuscules
Metadatos_pipe.columns = [column.upper() for column in Metadatos_pipe.columns]

Base_pipe_oper = Metadatos_pipe[
    ["OPERATION_NUMBER", "OPERATION_TYPE", "OPERATION_NAME", "PIPE_YR", "DIVISION"]
]


# filtrar pipe not 2024

Base_pipe_oper = Base_pipe_oper[Base_pipe_oper["PIPE_YR"] != 2024]

Base_pipe_oper = Base_pipe_oper[Base_pipe_oper["PIPE_YR"] >= 2017]

# keep only non-reviewed operations
path_cl = "C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/documents/Inputs/"

# Read the excel file
df_operaciones = pd.read_excel(path_cl + "operaciones - preclas.xlsx")

# Find the operations in df_metadatos that are not in df_operaciones
Base_pipe_oper["Is_Duplicated"] = Base_pipe_oper["OPERATION_NUMBER"].isin(
    df_operaciones["OPERATION_NUMBER"]
)
df_missing_operations = Base_pipe_oper.loc[Base_pipe_oper["Is_Duplicated"] == False]

# Add 'PRE CLASIFICACIÓN' column filled with NaNs to df_missing_operations
df_missing_operations["PRE CLASIFICACIÓN"] = np.nan

# Append df_missing_operations to df_operaciones
df_operaciones = pd.concat([df_operaciones, df_missing_operations], ignore_index=True)

# Drop the 'Is_Duplicated' column from the final DataFrame
df_operaciones.drop("Is_Duplicated", axis=1, inplace=True)

#### Space to do SPH pipeline operations

# read information of SPH

# Read the excel file
df_sph = pd.read_excel(
    path_cl + "DIV - revision operaciones.xlsx", sheet_name="pipeline-sph"
)

# Read the excel file
df_sph_ejecucion = pd.read_excel(
    path_cl + "DIV - revision operaciones.xlsx", sheet_name="ejecucion-sph"
)


Base_sph = Base_pipe_oper[Base_pipe_oper["DIVISION"] == "SPH"]

# Find the operations in df_metadatos that are not in df_operaciones
Base_sph["Is_Duplicated"] = Base_sph["OPERATION_NUMBER"].isin(
    df_sph["OPERATION_NUMBER"]
)

df_extra_sph = Base_sph.loc[Base_sph["Is_Duplicated"] == False]

df_extra_sph["TYPE_SPH"] = np.nan

# Append df_missing_operations to df_operaciones
df_sph = pd.concat([df_sph, df_extra_sph], ignore_index=True)

# Drop the 'Is_Duplicated' column from the final DataFrame
df_sph.drop("Is_Duplicated", axis=1, inplace=True)


##### Guardar #####

with pd.ExcelWriter(path_cl + "/operaciones - preclas.xlsx") as writer:
    df_operaciones.to_excel(writer, sheet_name="Oper", index=False)

    # go to file and add 0,1 for operation non digital - digital

with pd.ExcelWriter(path_cl + "/DIV - revision operaciones.xlsx") as writer:
    df_sph.to_excel(writer, sheet_name="pipeline-sph", index=False)
    df_sph_ejecucion.to_excel(writer, sheet_name="ejecucion-sph", index=False)

    # go to file and assign health or social protection
