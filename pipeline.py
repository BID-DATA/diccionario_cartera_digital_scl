# -*- coding: utf-8 -*-
"""
Created on Mon Aug 30 19:17:06 2021

@author: mariarey 

"""
"con este script se llaman las operaciones del pipeline para el triage digital, se conecta con las respuestas del checklist y se filtra la preselección de operaciones digitales"

import sys

sys.stdout.encoding
"UTF-8"

import sys
import pandas as pd
import pyodbc
from openpyxl import load_workbook
import numpy as np

##################### Extracción de datos operaciones #########################

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
  WHERE (C.prep_resp_dept_cd='SCL' AND C.PIPE_YR>2016 AND (C.OPER_TYP_CD='TCP' OR C.OPER_TYP_CD='LON') AND \
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

Base_pipe = Metadatos_pipe[
    [
        "OPERATION_NUMBER",
        "OPERATION_NAME",
        "PIPE_YR",
        "PIPE_T",
        "OPERATION_TYPE",
        "OPERATION_TYPE_NAME",
        "OPERATION_MODALITY",
        "TAXONOMY",
        "STATUS",
        "REGION",
        "COUNTRY",
        "DEPARTMENT",
        "DIVISION",
        "TEAM_LEADER_NM",
        "APPROVAL_DATE",
        "POD_DATE",
        "CURRENT_EXPIRATION_DATE",
        "OBJECTIVE_EN",
        "OBJECTIVE_ES",
        "QRR_DATE",
    ]
]

# filtrar pipe not 2024

Base_pipe = Base_pipe[Base_pipe["PIPE_YR"] != 2024]

Base_pipe = Base_pipe[Base_pipe["PIPE_YR"] >= 2017]

# filter only operations which haven't been approved

now = pd.Timestamp.now()

Base_pipe = Base_pipe[
    (Base_pipe["APPROVAL_DATE"] > now) | (Base_pipe["APPROVAL_DATE"].isnull())
]


##### Leer información de checklist #####

path_cl = "C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/documents/Inputs"
checklist = pd.read_excel(path_cl + "/Triage_digital.xlsx", sheet_name="Sheet1")
georef = pd.read_excel(path_cl + "/Georef.xlsx", sheet_name="Sheet1")

checklist["OPERATION_NUMBER"] = checklist["Número de la operación:"]
checklist["INFRA"] = checklist[
    "¿Se prevé comprar algún tipo de tecnología (tablets, servidores) o pagar por algún tipo de servicio digital (conexión a internet, licenciamientos, licencias de servicios como tableau?"
]
checklist["INFO"] = checklist[
    "¿Se prevé desarrollar o adquirir sistemas de información, repositorios de información, hacer levantamiento de datos que van a ser almacenados de manera digital? ¿Se harán normas de interoperabilidad, terminologías?"
]
checklist["GOB"] = checklist[
    "¿Se prevé hacer cambios, creación de normas relacionadas con temas digitales (protección de información, historia clínica electrónica, firma digital, SIGED), adopción, creación de estándares, planes o estrategias?"
]
checklist["CULTURA"] = checklist[
    "¿Se prevé la creación o fortalecimiento de competencias, acciones de gestión de cambio, comunicación, uso de la información (capacitación para manejo y lectura de dashboards)?"
]
checklist["DIGITAL_TRANS"] = checklist["Digital_trans"]
checklist["DUMMY_DIGITAL_CL"] = checklist["Digital_port"]
checklist["EMAIL"] = checklist["Email"]
checklist_tidy = checklist[
    [
        "OPERATION_NUMBER",
        "INFO",
        "INFRA",
        "GOB",
        "CULTURA",
        "DUMMY_DIGITAL_CL",
        "DIGITAL_TRANS",
        "EMAIL",
    ]
]

checklist_tidy = checklist_tidy.dropna(subset=["OPERATION_NUMBER"])
checklist_tidy = checklist_tidy.dropna(subset=["INFO"])
# pegar todas las bases
Base_pipe = Base_pipe.merge(checklist_tidy, on="OPERATION_NUMBER", how="left")
Base_pipe["DUMMY_DIGITAL_CL"] = Base_pipe["DUMMY_DIGITAL_CL"].fillna(
    "Pending Checklist"
)  # las que no tienen checklist en pending
Base_pipe["DIGITAL_TRANS"] = Base_pipe["DIGITAL_TRANS"].fillna(
    "Pending Checklist"
)  # las que no tienen checklist en pending
Base_pipe["DUMMY_DIGITAL_CL"] = Base_pipe["DUMMY_DIGITAL_CL"].replace(1, "Digital")
Base_pipe["DIGITAL_TRANS"] = Base_pipe["DIGITAL_TRANS"].replace(
    1, "Digital Transformation"
)


##### Leer información de las que preclasificamos como digitales
preclasi = pd.read_excel(path_cl + "/operaciones - preclas.xlsx", sheet_name="Oper")
preclasi["PRE CLASIFICACIÓN"] = preclasi["PRE CLASIFICACIÓN"].replace(
    0, "No se considera que se deba llenar CL"
)
Base_pipe = Base_pipe.merge(
    preclasi[["PRE CLASIFICACIÓN", "OPERATION_NUMBER"]],
    on="OPERATION_NUMBER",
    how="left",
)
Base_pipe["PIPE_YR"] = np.where(
    Base_pipe["OPERATION_TYPE"] == "LON",
    Base_pipe["PIPE_YR"].astype(str) + "-" + Base_pipe["PIPE_T"],
    Base_pipe["PIPE_YR"].astype(str),
)

# Si no las hemos preclasificado se marcan como 1
Base_pipe["PRE CLASIFICACIÓN"].replace("", 1, inplace=True)
Base_pipe["PRE CLASIFICACIÓN"].replace(" ", 1, inplace=True)
Base_pipe["PRE CLASIFICACIÓN"].fillna(1, inplace=True)

Base_pipe = Base_pipe.merge(georef[["COUNTRY", "ABR_PBI"]], on="COUNTRY", how="left")

Base_pipe = Base_pipe.drop_duplicates()
Base_pipe = Base_pipe.sort_values(["DIGITAL_TRANS", "APPROVAL_DATE"])

Base_pipe["ESPEC_CC"] = np.where(
    Base_pipe["DIVISION"] == "EDU",
    "Soledad Bos (soledadb@iadb.org)",
    np.where(
        Base_pipe["DIVISION"] == "LMK",
        "Dulce Baptista (dulced@iadb.org)",
        np.where(
            Base_pipe["DIVISION"] == "SPH",
            "Alex Bagolle (abagolle@iadb.org)",
            np.where(
                Base_pipe["DIVISION"] == "GDI",
                "Diana Bocarejo (dianabo@iadb.org)",
                np.where(
                    Base_pipe["DIVISION"] == "MIG", "Marta Paraiso (martap@iadb.org)", 0
                ),
            ),
        ),
    ),
)


Base_seguimiento = Base_pipe[
    [
        "OPERATION_NUMBER",
        "OPERATION_NAME",
        "OPERATION_TYPE",
        "PIPE_YR",
        "APPROVAL_DATE",
        "POD_DATE",
        "OBJECTIVE_EN",
        "OBJECTIVE_ES",
        "INFO",
        "INFRA",
        "GOB",
        "CULTURA",
        "DIGITAL_TRANS",
        "DIVISION",
        "TEAM_LEADER_NM",
    ]
]

Base_seguimiento = Base_seguimiento.sort_values(["DIGITAL_TRANS", "APPROVAL_DATE"])

Base_seguimiento_TC = Base_seguimiento[Base_seguimiento["OPERATION_TYPE"] == "TCP"]

Base_seguimiento_LON = Base_seguimiento[Base_seguimiento["OPERATION_TYPE"] == "LON"]

##### Guardar #####

path_save = (
    "C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/"
)

Base_pipe["DIGITAL"] = Base_pipe["DUMMY_DIGITAL_CL"]
Base_pipe = Base_pipe[
    [
        "OPERATION_NUMBER",
        "OPERATION_NAME",
        "OPERATION_TYPE",
        "PIPE_YR",
        "OPERATION_TYPE_NAME",
        "OPERATION_MODALITY",
        "TAXONOMY",
        "STATUS",
        "REGION",
        "COUNTRY",
        "ABR_PBI",
        "DEPARTMENT",
        "DIVISION",
        "TEAM_LEADER_NM",
        "EMAIL",
        "ESPEC_CC",
        "APPROVAL_DATE",
        "POD_DATE",
        "PRE CLASIFICACIÓN",
        "DIGITAL",
        "INFO",
        "INFRA",
        "GOB",
        "CULTURA",
        "DIGITAL_TRANS",
    ]
]


Base_pipe.loc[
    (Base_pipe["DIGITAL_TRANS"] == 0) & (Base_pipe["DIGITAL"] == "Digital"),
    "DIGITAL_TRANS",
] = "Digital"

with pd.ExcelWriter(path_save + "cartera digital/Dashboard/output-pipe.xlsx") as writer:
    Base_pipe.to_excel(writer, sheet_name="Metadata", index=False)
