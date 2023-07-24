# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 10:42:59 2020

@author: ALOP

Updated on Thu Jun 10 2021, deyanaram@iadb.org
Updated on Mon Nov 01, 2021, mariarey@iadb.org
Last updated on Mon July 24, 2023, mariarey@iadb.org
"""


# -*- coding: utf-8 -*-
"""
Created on Thu Jul  4 11:54:09 2019

@author: Noux
@proyecto: Análisis de ejes Transversales para BID.


El presente código ha sido creado con el fin de realizar un procesamiento de texto y búsqueda de palabras,
por medio de los cuales asignaremos una clasificación de acuerdo a un diccionario previamente establecido.

Este diccionario ha sido creado por los expertos de BID, las cuales han determinado palabras claves con lo
cual se puede clasificar el tipo de Producto al que se refiere el texto, es decir, clasificar en un texto 
tipo DIGITAL, NO DIGITAL y SIN DEFINIR.

La categoría SIN DEFINIR se refiere a que menciona palabras que por si sola aún no se puede clasificar el
texto y por lo cual depende del sentido de la frase para poder clasificarlas.

En este sentido y con la necesidad de clasificar de manera rápida y correcta a estos archivos, se crea un
diccionario adicional ya no de palabras, sino de un conjunto de palabras, las cuales explicar si el texto
es DIGITAL o no lo es.


"""

import sys

sys.stdout.encoding
"UTF-8"

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
import sys
import pyodbc


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

sql_cost = "WITH \
OUTPUTS AS ( \
SELECT DISTINCT *, ROW_NUMBER()OVER(PARTITION BY OPER_NUM, CMPNT_STMNT, OUTPUT_DEFNTN, IS_COST, YR ORDER BY YR DESC,DW_UPDT_TS DESC)NUMBER_ROW \
FROM ledw.oper_ods_output_ind \
) \
SELECT DISTINCT 	C.OPER_NUM as OPERATION_NUMBER,	C.OPER_ENGL_NM as OPERATION_NAME, C.OPERTYP_ENGL_NM AS OPERATION_TYPE_NAME, C.MODALITY_CD AS OPERATION_MODALITY, C.PREP_RESP_DEPT_CD AS DEPARTMENT, C.PREP_RESP_DIV_CD AS DIVISION, \
	C.REGN AS REGION, C.CNTRY_BENFIT AS COUNTRY, C.STS_CD AS STATUS, C.STG_ENGL_NM AS STAGE, C.STS_ENGL_NM AS TAXONOMY, C.OPER_EXEC_STS AS EXEC_STS, C.APPRVL_DT AS APPROVAL_DATE, C.APPRVL_DT_YR as APPROVAL_YEAR, \
    C.ORIG_APPRVD_USEQ_AMNT AS APPROVAL_AMOUNT, C.CURNT_DISB_EXPR_DT as CURRENT_EXPIRATION_DATE, C.RELTN_NUM AS RELATED_OPER,C.FACILITY_TYP_CD AS RELATION_TYPE, C.OPER_TYP_CD AS OPERATION_TYPE, A.OBJTV_ENGL as OBJECTIVE_EN,\
    A.OBJTV_SPANISH as OBJECTIVE_ES, B.CMPNT_STMNT as COMPONENT_NAME, B.OUTPUT_DEFNTN as OUTPUT_NAME, C.FACILITY_TYP_ENGL_NM AS OUTPUT_DESCRIPTION, B.ACTUAL_VAL AS OUTPUT_COST, B.IS_COST AS IS_COST, NUMBER_ROW, \
    OUTPUT_UOM \
FROM ledw.spd_ods_hopermas C \
  JOIN (SELECT OPER_NUM, MAX(DW_CRTE_TS) AS MAX_DT from ledw.spd_ods_hopermas GROUP BY OPER_NUM) t ON C.OPER_NUM= t.OPER_NUM and C.DW_CRTE_TS = t.MAX_DT  \
  LEFT JOIN ledw.oper_ods_oper A ON C.OPER_NUM = A.OPER_NUM \
  LEFT JOIN (SELECT * FROM OUTPUTS WHERE YR=-1 AND NUMBER_ROW=1) B ON C.OPER_NUM = B.OPER_NUM \
WHERE C.APPRVL_DT_YR >= 2015  AND C.PREP_RESP_DEPT_CD='SCL' AND ( C.OPER_TYP_CD= 'TCP' OR  C.OPER_TYP_CD = 'LON')"  # SQL query de datos deseados, MRT: se agrega filtro de fecha de aprobación menor al día de hoy y se quita =ACTIVE

# Querying data
cursor.execute(sql_cost)

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
Metadatos_tot = pd.DataFrame(decoded_rows, columns=column_names)

# Closing the connection
conn.close()

##### Analysis operations #####

# Database in mayuscules
Metadatos_tot.columns = [column.upper() for column in Metadatos_tot.columns]


# filter only operations which have been approved

now = pd.Timestamp.now()

Metadatos_tot = Metadatos_tot[
    (Metadatos_tot["APPROVAL_DATE"] < now) | (Metadatos_tot["APPROVAL_DATE"].isnull())
]


###### Datos para generar resultados #######

Metadatos = Metadatos_tot[
    (Metadatos_tot.IS_COST == 1) | (Metadatos_tot.IS_COST.isnull())
]
Resultados = Metadatos_tot[(Metadatos_tot.IS_COST == 0)]
Resultados["OUTPUT_FI"] = Resultados["OUTPUT_COST"]
Resultatos = Resultados[
    ["OPERATION_NUMBER", "COMPONENT_NAME", "OUTPUT_NAME", "OUTPUT_FI", "OUTPUT_UOM"]
]

# sql_results = "WITH  \
# OUTPUTS AS ( \
# SELECT DISTINCT OPER_NUM,  OUTPUT_UOM, OUTPUT_ID, OUTPUT_DEFNTN,CMPNT_STMNT, IS_COST,\
# SUM(ANNL_PLAN) AS ANNL_PLAN, \
# SUM(ACTUAL_VAL) AS ACTUAL_VAL,\
# SUM(ORIG_PLAN) AS ORIG_PLAN \
# FROM ODS.OPER_ODS_OUTPUT_IND \
# WHERE  YR>0 AND YR<=YEAR(DATE(NOW())) AND IS_COST=0 AND IS_CLOSED=0 \
# GROUP BY  OPER_NUM, OUTPUT_UOM, OUTPUT_ID, OUTPUT_DEFNTN,CMPNT_STMNT, IS_COST \
# ) \
# SELECT DISTINCT 	C.OPER_NUM as OPERATION_NUMBER, C.PREP_RESP_DEPT_CD AS DEPARTMENT, C.APPRVL_DT AS APPROVAL_DATE, C.OPER_TYP_CD AS OPERATION_TYPE, B.CMPNT_STMNT as COMPONENT_NAME, \
#    B.OUTPUT_DEFNTN as OUTPUT_NAME, B.OUTPUT_UOM, B.IS_COST,  B.ANNL_PLAN AS OUTPUT_FI, B.ACTUAL_VAL AS ACTUAL_VAL, B.ORIG_PLAN AS ORIG_PLAN \
# FROM ODS.SPD_ODS_HOPERMAS C \
#  JOIN (SELECT OPER_NUM, MAX(DW_CRTE_TS) AS MAX_DT from ODS.SPD_ODS_HOPERMAS GROUP BY OPER_NUM) t ON C.OPER_NUM= t.OPER_NUM and C.DW_CRTE_TS = t.MAX_DT \
# LEFT JOIN ODS.OPER_ODS_OPER A ON C.OPER_NUM = A.OPER_NUM \
# LEFT JOIN (SELECT * FROM OUTPUTS) B ON C.OPER_NUM = B.OPER_NUM  \
# WHERE C.APPRVL_DT_YR >= 2015  AND C.PREP_RESP_DEPT_CD='SCL' AND DATE(C.APPRVL_DT)<DATE(NOW()) AND ( C.OPER_TYP_CD= 'TCP' OR  C.OPER_TYP_CD = 'LON')"

# stmt_results = ibm_db.exec_immediate(conn, sql_results) #Querying data

# cols_result = ['OPERATION_NUMBER','COMPONENT_NAME', 'OUTPUT_NAME', 'OUTPUT_FI', 'OUTPUT_UOM']

# Resultados = pd.DataFrame(columns=cols_result)
# resulta_fi = ibm_db.fetch_both(stmt_results)
# while(resulta_fi):
#   Resultados = Resultados.append(resulta_fi, ignore_index=True)
#  resulta_fi = ibm_db.fetch_both(stmt_results)

# Resultados = Resultados.iloc[:, 0:6]
# Resultados.shape #Visualizando resultado
# Resultados.drop_duplicates(inplace=True)

ibm_db.close  # Cerrando la conexión

################# Lectura del archivo diccionario #############################

# Cambiar path al local dónde se tiene el folder de cartera digital

path = "C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/cartera digital/Dashboard/"

####Se forma un solo diccionario
Diccionario = pd.ExcelFile(path + "/Inputs/01_Diccionario_token_digital.xlsx")
Diccionario = Diccionario.parse("Sheet1")  ############Lectura de data
Diccionario.head()  ####ver los primeros registros de la data
Diccionario.columns.values  ###Los nombres de las columnas
Diccionario.shape  #####dimensiones de la data

Diccionario_En = Diccionario[["TIPO", "INGLES", "TOKENS"]]
Diccionario_En.dropna(inplace=True)
Diccionario_En.rename(columns={"INGLES": "PALABRAS"}, inplace=True)
Diccionario_En["IDIOMA"] = "en"

Diccionario_Es = Diccionario[["TIPO", "ESPANOL", "TOKENS"]]
Diccionario_Es.dropna(inplace=True)
Diccionario_Es.rename(columns={"ESPANOL": "PALABRAS"}, inplace=True)
Diccionario_Es["IDIOMA"] = "es"


Diccionario_Pt = Diccionario[["TIPO", "PORTUGUES", "TOKENS"]]
Diccionario_Pt.dropna(inplace=True)
Diccionario_Pt.rename(columns={"PORTUGUES": "PALABRAS"}, inplace=True)
Diccionario_Pt["IDIOMA"] = "pt"

Diccionario_Fr = Diccionario[["TIPO", "FRANCES", "TOKENS"]]
Diccionario_Fr.dropna(inplace=True)
Diccionario_Fr.rename(columns={"FRANCES": "PALABRAS"}, inplace=True)
Diccionario_Fr["IDIOMA"] = "fr"


Diccionario_Total = pd.concat(
    [Diccionario_Es, Diccionario_En, Diccionario_Pt, Diccionario_Fr]
)
Diccionario_Total2 = Diccionario_Total[["TIPO", "PALABRAS", "TOKENS"]].drop_duplicates()


#######Lectura de diccionario para los textos que se encuentran en sin definir #########
diccionario_bigrama = pd.read_excel(
    path + "/Inputs/02_Diccionario_bigrama_digital.xlsx", sheet_name="Hoja1"
)
diccionario_bigrama_En = diccionario_bigrama[["TIPO", "INGLES"]]
diccionario_bigrama_En.dropna(inplace=True)
diccionario_bigrama_En.rename(columns={"INGLES": "PALABRAS"}, inplace=True)
diccionario_bigrama_En["IDIOMA"] = "en"


diccionario_bigrama_Es = diccionario_bigrama[["TIPO", "ESPANOL"]]
diccionario_bigrama_Es.dropna(inplace=True)
diccionario_bigrama_Es.rename(columns={"ESPANOL": "PALABRAS"}, inplace=True)
diccionario_bigrama_Es["IDIOMA"] = "es"


diccionario_bigrama_Pt = diccionario_bigrama[["TIPO", "PORTUGUES"]]
diccionario_bigrama_Pt.dropna(inplace=True)
diccionario_bigrama_Pt.rename(columns={"PORTUGUES": "PALABRAS"}, inplace=True)
diccionario_bigrama_Pt["IDIOMA"] = "pt"

diccionario_bigrama_Fr = diccionario_bigrama[["TIPO", "FRANCES"]]
diccionario_bigrama_Fr.dropna(inplace=True)
diccionario_bigrama_Fr.rename(columns={"FRANCES": "PALABRAS"}, inplace=True)
diccionario_bigrama_Fr["IDIOMA"] = "fr"

diccionario_bigrama = pd.concat(
    [
        diccionario_bigrama_Es,
        diccionario_bigrama_En,
        diccionario_bigrama_Pt,
        diccionario_bigrama_Fr,
    ]
)


############Eliminacion de registros dobles cuyas fechas current expiration date tenga null #####################
Metadatos.columns = [w.upper() for w in Metadatos.columns]
p = datetime.strptime("1900-1-1", "%Y-%m-%d")
Metadatos[["CURRENT_EXPIRATION_DATE"]] = Metadatos[["CURRENT_EXPIRATION_DATE"]].fillna(
    p
)
w = Metadatos[["CURRENT_EXPIRATION_DATE"]].groupby(Metadatos["OPERATION_NUMBER"]).max()
w.reset_index(inplace=True)
w = w.rename(columns={"CURRENT_EXPIRATION_DATE": "CURRENT_EXPIRATION__DATE"})
Metadatos = Metadatos.merge(w)
Metadatos = Metadatos[
    Metadatos["CURRENT_EXPIRATION_DATE"] == Metadatos["CURRENT_EXPIRATION__DATE"]
]
Metadatos.drop(["CURRENT_EXPIRATION__DATE"], axis=1, inplace=True)


######################Subase donde se encuentra las columnas las cuales seran procesadas #############

Base = Metadatos[
    [
        "OPERATION_NUMBER",
        "OPERATION_NAME",
        "OBJECTIVE_ES",
        "OBJECTIVE_EN",
        "COMPONENT_NAME",
        "OUTPUT_NAME",
        "OUTPUT_DESCRIPTION",
    ]
]


#########################Generacion de stopwords ##############################################

listStopwordsEn = stopwords.words("english")
listStopwordsEs = stopwords.words("spanish")
listStopwordsFr = stopwords.words("french")
listStopwordsPt = stopwords.words("portuguese")
listStopwords = (
    listStopwordsEn + listStopwordsEs + listStopwordsFr + listStopwordsPt + ["It"]
)


######################           FUNCIONES                ###################################


def todate(text):
    """Lectura de fechas
    
    Descripcion
    ----------------------------------------------------------------------------------
    Transforma textos en fechas
    
    
    Parametros:
    ----------------------------------------------------------------------------------
        text (string)   ---  Texto de fechas en formato '%m/%d/%Y'
        
        
    Retorno
    ----------------------------------------------------------------------------------
        date
    ----------------------------------------------------------------------------------
        
    
    
    """
    fecha = datetime.strptime(text, "%m/%d/%Y")
    return fecha


def limpieza_texto1(text):

    """Lectura de texto

    Descripcion
    ----------------------------------------------------------------------------------
    Devuelve un texto plano, eliminando simbolos o caracteres innecesario.
    
    
    Parametros:
    ----------------------------------------------------------------------------------
        text (string)   ---  . Texto el cual va a ser procesado
        
        
    Retorno
    ----------------------------------------------------------------------------------
        string
    """

    text = unicodedata.normalize("NFD", text)
    text = text.encode("ascii", "ignore")
    text = text.decode("utf-8")
    return str(text)


def limpieza_texto2(text, diccionario):

    """Lectura de texto

    Descripcion
    ----------------------------------------------------------------------------------
    Busca palabras compuestas del diccionario para poderlas identificar de mejor manera.
    Las palabras que va a buscar en esta parte serán las siguientes:
    
        banda ancha
        e government
        big data
        data mining
        on line
        en linea
    
    Si en el texto SE encuentra alguna palabra llamada ' en linea con',no será tomado en cuenta para identificar las palabras,
    debido a que esta palabra se refiere a estar alineado con algo y la palbra que deseamos buscar debe estar relacionado con 'on line'. 
    
    Parametros:
    ----------------------------------------------------------------------------------
        text (String)   ---   Texto el cual va a ser procesado
        
        
    Retorno
    ----------------------------------------------------------------------------------
        Numero 1 o 0    ---  Si retorna 1, es porque encontro
    """

    text = unicodedata.normalize("NFD", text)
    text = text.encode("ascii", "ignore")
    text = text.decode("utf-8")
    text = " " + text.lower()
    text = text.replace("en linea con", " ")
    text = re.sub("[%s]" % re.escape(string.punctuation), " ", text)
    diccionario = diccionario[diccionario["TOKENS"] == 2]
    #    b =(text.find(' banda ancha ')>=0) | (text.find(' e government ')>=0) | (text.find(' big data ')>=0) | (text.find(' data mining ')>=0) | (text.find(' on line ')>=0)  | (text.find(' data warehouse ')>=0) | (text.find(' en linea ')>=0)
    a = []
    text1 = " " + text + " "
    for w in diccionario["PALABRAS"]:
        if text1.find(" " + w + " ") >= 0:
            a.append(w)
    return a


def search_tec_inno(text):

    """Lectura de texto

    Descripcion
    ----------------------------------------------------------------------------------
    Busca si en el texto existen palabras que se encuentran relacionadas con la tecnologia e innovacion
    
    
    Parametros:
    ----------------------------------------------------------------------------------
        text (string)   --- Texto el cual va a ser procesado
        
        
    Retorno
    ----------------------------------------------------------------------------------
        Numerico 1 o 0   --- 1 indica que en el texto si existen al menos una palabra que se refiere a tecnología e innovación
                             0 caso contrario
    """
    text = unicodedata.normalize("NFD", text)
    text = text.encode("ascii", "ignore")
    text = text.decode("utf-8")
    text = re.sub("[%s]" % re.escape(string.punctuation), " ", text)
    text = text.lower()
    b = (
        (text.find("tecnolog") >= 0)
        | (text.find("technolog") >= 0)
        | (text.find("innova") >= 0)
    )
    if b == True:
        a = 1
    else:
        a = 0
    return a


def searchword(listWords, tokens):

    """Función de búsqueda de palabras
    
    Descripcion
    -------------------------------------------------------------------------------------------------------
        Devuelve el listado de palabras que coinciden entre el diccionario de palabras y el texto tokenizado
        
    
    Parameters
    -------------------------------------------------------------------------------------------------------
    
        tokens (list)     -- Diccionario de palabras a buscar a el texto
        listWords (list)  -- Una lista en donce incluye el texto tokenizado
     
    Returns
    -----------------------------------------------------------------------------------------------------
        list    
    

    
    """
    frozensetListWords = frozenset(listWords)
    return [w for w in tokens if w in frozensetListWords]


def corpusword(text, diccionario, listStopwords):

    """ Procesamiento del texto
    
    Descripcion
    ------------------------------------------------------------------------------------------------------------
    
    La función recibe el texto(text), lo convierte en un texto plano, tokeniza el texto,
    elimina stopwords, para finalmente realizar la búsqueda de las palabras del diccionario en el texto procesado.
    
    Parametros
    -------------------------------------------------------------------------------------------------------------
        text  (string)            ---   El texto a procesar
        diccionario  (Dataframe)  ---   Diccionario de palabras para la búsqueda
        listStopwords (list)      ---   Lista de stopwords
        
    Retorno
    -------------------------------------------------------------------------------------------------------------
        list

    Nota
    -------------------------------------------------------------------------------------------------------------        
    
    Esta función necesita de dos funciones adicionales:
        
        limpieza_texto1(....)
        searchword(.....)  
    
    """
    tokenizer = RegexpTokenizer(r"[a-zA-Z]+")
    text = limpieza_texto1(text)
    tokens = tokenizer.tokenize(text)
    tokens = [w for w in tokens if not w in listStopwords]
    tokens = [w.lower() for w in tokens]
    tokens = searchword(diccionario, tokens)
    return list(set(tokens))


def singular(text):
    """ Procesamiento del texto
    
    Descripcion
    ------------------------------------------------------------------------------------------------------------
    
        Convierte las palabras de plural a singular.
    
    Parametros
    -------------------------------------------------------------------------------------------------------------
        text  (string)            ---   El texto a procesar

        
    Retorno
    -------------------------------------------------------------------------------------------------------------
        string

    """

    y = TextBlob(text)
    a = []
    for j in list(range(len(y.words))):
        a.append(y.words[j].singularize())
        b = " ".join(a)
    return b


def searchsysteminformation(text):

    """ Busqueda se sistemas de información.
    
    Descripcion
    ------------------------------------------------------------------------------------------------------------
    
        La función recibe el texto(text), y busca las palabras "sistemas" e "información" y que no estén separadas
        por ningún signo de puntuación. El mismo proceso para "tecnologia" e "información".
    
    Parametros
    -------------------------------------------------------------------------------------------------------------
        text  (string)            ---   El texto a procesar

        
    Retorno
    -------------------------------------------------------------------------------------------------------------
        Retorna:"sistema de información" o tic si encuentra las condiciones dadas.
            


    """
    text = limpieza_texto1(text)
    text = text.lower()
    text = text.replace(",", " , ")
    text = text.replace(".", " , ")
    text = text.replace(":", " , ")
    text = text.replace(";", " , ")
    o = re.sub("[%s]" % re.escape(string.punctuation), " , ", text)
    o = o.split()
    u = DataFrame(o, columns=["AB"])
    p = u[
        (u["AB"] == "sistema")
        | (u["AB"] == "sistemas")
        | (u["AB"] == "informacion")
        | (u["AB"] == "system")
        | (u["AB"] == "systems")
        | (u["AB"] == ",")
        | (u["AB"] == "information")
        | (u["AB"] == "technologies")
        | (u["AB"] == "technology")
        | (u["AB"] == "tecnologias")
        | (u["AB"] == "tecnologia")
        | (u["AB"] == "informacao")
        | (u["AB"] == "sistemes")
        | (u["AB"] == "sisteme")
    ]["AB"]
    p = list(p)
    p = "".join(p)
    a = []
    if (
        (p.find("systeminformation") >= 0)
        | (p.find("sistemainformacion") >= 0)
        | (p.find("sistemasinformacion") >= 0)
        | (p.find("informationsystem") >= 0)
        | (p.find("informationsystems") >= 0)
        | (p.find("sistemasinformacao") >= 0)
        | (p.find("sistemainformacao") >= 0)
        | (p.find("sistemesinformation") >= 0)
        | (p.find("sistemeinformation") >= 0)
    ):
        a.append("sistema de informacion")
    if (
        (p.find("informationtechnology") >= 0)
        | (p.find("informationtechnologies") >= 0)
        | (p.find("tecnologiasinformaciones") >= 0)
        | (p.find("tecnologiasinformacion") >= 0)
        | (p.find("tecnologiainformacion") >= 0)
        | (p.find("tecnologiainformacao") >= 0)
        | (p.find("tecnologiasinformacao") >= 0)
    ):
        a.append("tic")
    return a


def repeticiones(lista1, Base, Valor):

    """ Función de repetición de registros
    
    Descripcion
    ----------------------------------------------------------------------------------------------
    
    Esta función devuelve una lista de registros repetidos acordes al número de palabras encontradas.
    
    
    Parametros
    --------------------------------------------------------------------------------------------
    
    
        lista1 (list)       ---  Es una lista de listas, en la cual cada elemento se encuentra palabras
        Base (Dataframe)    ---  Base de datos que se esta utilizando
        Valor (String)      ---  Nombre de la columna sobre la cual se esta realizando la búsqueda de las palabras
    
    
    Retorno
    ---------------------------------------------------------------------------------------------
    
        list
        
    Nota:
    -----------------------------------------------------------------------------------------------
    Todos los registros procesados están asociados a un número de proyecto y en el caso del OUTPUT_NAME
    adicional al número de proyecto  están asociados con COMPONENT_NAME. Para poder llevar a cabo este registro
    se crea esta función con el fin de indicar a que número de proyecto , o COMPONENT_NAME está asociado cada
    uno de los textos procesados.
    
    """
    conteo = DataFrame([len(x) for x in lista1], columns=["n_veces"])
    conteo.index = lista1.index
    repeticiones = list(
        [np.repeat(Base[Valor][x], conteo["n_veces"][x]) for x in conteo.index]
    )
    lista_repeticiones = [y for x in repeticiones for y in x]
    return lista_repeticiones


def globalfuncion(Base, Diccionario, Variable_Analizar, listStopWords):

    """ Función de generación de resultados
    
    Descripcion
    ----------------------------------------------------------------------------------------------------------
    Esta función genera una data frame mostrando el nombre del proyecto, la columna  que se esta analizando y 
    tipo de producto a lo cual se clasifico el texto.
    
    Parametros:   
    ----------------------------------------------------------------------------------------------------------
        
        Base  (DataFrame)            ---   Base de datos donde se encuentran la información que se va a procesar.
        Diccionario  (DataFrame)     ---   Diccionario donde se encuentran las palabras y el tipo de producto por cada palabra.
        Variable_Analizar (String)   ---   Nombre de la columna sobre la cual se va a realizar el procesamiento del texto.
        listStopWords (list)         ---   Lista de stopwords
        
    Retorno:     
    -----------------------------------------------------------------------------------------------------------
        DataFrame
        
        
    Nota:
    ------------------------------------------------------------------------------------------------------------
    
    Esta función depende de las siguientes funciones:
        repeticiones(....)
        corpusword(.....)
        search_tec_inno(......)
        limpieza_texto1(text)
        
    
    """

    Idioma = "PALABRAS"

    if Variable_Analizar == "OUTPUT_NAME":
        Base_Aux = Base[
            [
                "OPERATION_NUMBER",
                "COMPONENT_NAME",
                Variable_Analizar,
                "OUTPUT_DESCRIPTION",
            ]
        ]
        Base_Aux = Base_Aux[
            (pd.isnull(Base_Aux["OUTPUT_DESCRIPTION"]) == False)
            | (pd.isnull(Base_Aux["COMPONENT_NAME"]) == False)
        ]
        Base_Aux["OUTPUT_NAME"] = Base_Aux["OUTPUT_NAME"].fillna("")
        Base_Aux["OUTPUT_DESCRIPTION"] = Base_Aux["OUTPUT_DESCRIPTION"].fillna("")
        a = list([str(i) for i in (Base_Aux["OUTPUT_NAME"])])
        b = list([str(j) for j in (Base_Aux["OUTPUT_DESCRIPTION"])])
        c = []
        for i in range(len(a)):
            if (a[i] != "") & (b[i] != ""):
                c.append(a[i] + str(" ") + b[i])
            elif b[i] == "":
                c.append(a[i])
            else:
                c.append(b[i])

        Base_Aux["OUTPUT_NAME"] = c
        Base_Aux.drop(["OUTPUT_DESCRIPTION"], axis=1, inplace=True)

    else:
        Base_Aux = DataFrame()
        Base_Aux = Base[["OPERATION_NUMBER", Variable_Analizar]]
        Base_Aux.drop_duplicates(inplace=True)
        Base_Aux.dropna(inplace=True)
        Base_Aux[Variable_Analizar] = Base_Aux[Variable_Analizar].apply(str)

    list_of_words = Base_Aux[Variable_Analizar].apply(
        corpusword,
        args=(
            Diccionario_Total[Diccionario_Total.TOKENS == 1]["PALABRAS"],
            listStopwords,
        ),
    )
    list_of_words2 = Base_Aux[Variable_Analizar].apply(
        limpieza_texto2, args=(Diccionario_Total,)
    )
    list_of_words3 = Base_Aux[Variable_Analizar].apply(searchsysteminformation)
    list_of_words = list_of_words + list_of_words2 + list_of_words3
    rep_name = repeticiones(list_of_words, Base_Aux, "OPERATION_NUMBER")
    rep_variable = repeticiones(list_of_words, Base_Aux, Variable_Analizar)

    dframe = DataFrame()

    if Variable_Analizar == "OUTPUT_NAME":
        Base_Aux["COMPONENT_NAME"] = Base_Aux["COMPONENT_NAME"].astype(str)
        rep_component = repeticiones(list_of_words, Base_Aux, "COMPONENT_NAME")
        dframe["COMPONENT_NAME"] = rep_component

    list_of_words = list(chain(*list_of_words))

    #

    dframe["OPERATION_NUMBER"] = rep_name
    dframe[Variable_Analizar] = rep_variable
    dframe["WORDS"] = list_of_words
    #

    #    Base_Aux[Variable_Analizar]=Base_Aux[Variable_Analizar].str.replace(' xxxxxx ',' Red ')
    dframe = dframe.merge(
        Diccionario[["TIPO", Idioma]], left_on="WORDS", right_on=Idioma, how="left"
    )

    if Variable_Analizar == "OUTPUT_NAME":
        dframe2 = dframe[
            ["OPERATION_NUMBER", "COMPONENT_NAME", Variable_Analizar, "WORDS"]
        ]
        dframe2.drop_duplicates(inplace=True)
        dframe = dframe[
            ["OPERATION_NUMBER", "COMPONENT_NAME", Variable_Analizar, "TIPO"]
        ].drop_duplicates()
        dframe = pd.crosstab(
            [
                dframe["OPERATION_NUMBER"],
                dframe["COMPONENT_NAME"],
                dframe[Variable_Analizar],
            ],
            columns=dframe["TIPO"],
        )

    else:
        dframe2 = dframe[["OPERATION_NUMBER", Variable_Analizar, "WORDS", "TIPO"]]
        #        dframe2.drop_duplicates(inplace=True)
        dframe = dframe[
            ["OPERATION_NUMBER", Variable_Analizar, "TIPO"]
        ].drop_duplicates()
        dframe = pd.crosstab(
            [dframe["OPERATION_NUMBER"], dframe[Variable_Analizar]],
            columns=dframe["TIPO"],
        )

    #

    dframe.reset_index(inplace=True)
    X = set(dframe.columns)  #####conjunto de las columnas
    Y = set(
        {"NEGATIVO", "NEUTRO", "NEUTRO POSITIVO", "POSITIVO"}
    )  ###columnas necesarias para aplicar la condicion
    b = list(Y - X)

    if len(b) > 0:
        aux = DataFrame(
            np.repeat(0, len(b) * dframe.shape[0]).reshape((dframe.shape[0], len(b))),
            columns=b,
        )
        dframe = pd.concat([dframe, aux], axis=1)
    Base_Aux.index = range(len(Base_Aux))
    dframe = Base_Aux.merge(dframe, how="left")
    dframe.fillna(np.nan, inplace=True)

    if Variable_Analizar == "OUTPUT_NAME":
        dframe = dframe[
            [
                "OPERATION_NUMBER",
                "COMPONENT_NAME",
                Variable_Analizar,
                "NEGATIVO",
                "NEUTRO",
                "NEUTRO POSITIVO",
                "POSITIVO",
            ]
        ]
        dframe = dframe.groupby(
            [
                dframe["OPERATION_NUMBER"],
                dframe["COMPONENT_NAME"],
                dframe[Variable_Analizar],
            ]
        ).sum()
    else:
        dframe = dframe[
            [
                "OPERATION_NUMBER",
                Variable_Analizar,
                "NEGATIVO",
                "NEUTRO",
                "NEUTRO POSITIVO",
                "POSITIVO",
            ]
        ]
        dframe = dframe.groupby(
            [dframe["OPERATION_NUMBER"], dframe[Variable_Analizar]]
        ).sum()

    # Aplicar condiciones
    dframe["RESULT" + "_" + Variable_Analizar] = np.where(
        (dframe["NEGATIVO"] == 0)
        & (dframe["NEUTRO"] == 0)
        & (dframe["NEUTRO POSITIVO"] == 0)
        & (dframe["POSITIVO"] == 0),
        "NO DIGITAL",
        np.where(
            (dframe["NEGATIVO"] >= 1)
            & (dframe["NEUTRO"] == 0)
            & (dframe["NEUTRO POSITIVO"] == 0)
            & (dframe["POSITIVO"] == 0),
            "NO DIGITAL",
            np.where(
                (dframe["NEGATIVO"] >= 1)
                & (dframe["NEUTRO"] >= 1)
                & (dframe["NEUTRO POSITIVO"] == 0)
                & (dframe["POSITIVO"] == 0),
                "NO DIGITAL",
                np.where(
                    (dframe["NEGATIVO"] == 0)
                    & (dframe["NEUTRO"] == 0)
                    & (dframe["NEUTRO POSITIVO"] >= 1)
                    & (dframe["POSITIVO"] == 0),
                    "NO DIGITAL",
                    np.where(
                        (dframe["NEGATIVO"] >= 1)
                        & (dframe["NEUTRO"] == 0)
                        & (dframe["NEUTRO POSITIVO"] >= 1)
                        & (dframe["POSITIVO"] == 0),
                        "NO DIGITAL",
                        np.where(
                            (dframe["NEGATIVO"] == 0)
                            & (dframe["NEUTRO"] >= 1)
                            & (dframe["NEUTRO POSITIVO"] == 0)
                            & (dframe["POSITIVO"] == 0),
                            "SIN DEFINIR",
                            "DIGITAL",
                        ),
                    ),
                ),
            ),
        ),
    )
    dframe.drop(
        ["NEGATIVO", "NEUTRO", "NEUTRO POSITIVO", "POSITIVO"], axis=1, inplace=True
    )
    dframe.reset_index(inplace=True)
    dframe["RESULT" + "_" + Variable_Analizar] = np.where(
        (dframe[Variable_Analizar] == " ")
        | (dframe[Variable_Analizar] == "x")
        | (dframe[Variable_Analizar] == "xx")
        | (dframe[Variable_Analizar] == ".")
        | (dframe[Variable_Analizar] == ",")
        | [x in str(range(20)) for x in dframe[Variable_Analizar]]
        | (dframe[Variable_Analizar].apply(type) == int)
        | (dframe[Variable_Analizar] == "*")
        | (dframe[Variable_Analizar] == "#")
        | (dframe[Variable_Analizar] == "-")
        | (dframe[Variable_Analizar] == "_")
        | (dframe[Variable_Analizar] == "- ")
        | (dframe[Variable_Analizar] == " -")
        | (dframe[Variable_Analizar] == ". -"),
        np.nan,
        dframe["RESULT" + "_" + Variable_Analizar],
    )

    dframe["RESULT_" + Variable_Analizar + "_TECN-INNOV"] = dframe[
        Variable_Analizar
    ].apply(search_tec_inno)

    return [dframe, dframe2]


####funcion para el procesamiento de lo sin definir#####

# def SinDefinir(Base,diccionario2,Variable_Analizar):
#
#    ''' Clasificación de los productos SIN DEFINIR
#
#    Descripción
#    ----------------------------------------------------------------------------------------------------------
#
#        Esta función genera una nueva columna sobre un data frame, en la cual clasifica los textos que resultaron SIN DEFINIR
#        de la primera corrida.
#
#    Parametros
#    ----------------------------------------------------------------------------------------------------------
#
#
#           Base (Dataframe)             --- Base procesada con el tipo de producto
#           diccionario2 (Dataframe)     --- Diccionario de palabras compuestas con las cuales se identifica si un producto siendo SIN DEFINIR es digital
#           Variable_Analizar(String)    --- Nombre de la columna sobre la cual se va a procesar el texto.
#
#    Retorno
#    ----------------------------------------------------------------------------------------------------------
#
#        Dataframe
#
#    '''
#
#
#    Base_Aux=Base[Base['RESULT_'+Variable_Analizar]=='SIN DEFINIR'][['OPERATION_NUMBER',Variable_Analizar]]
#    Base_Aux.drop_duplicates(inplace=True)
#    a=[]
#    for i in Base_Aux[Variable_Analizar]:
#        k=0;
#        try:
#            i=limpieza_texto1(i)
#            for j in diccionario2['PALABRAS']:
#                if (i.lower().find(j)>=0):
#                    k=k+1;
#                else:
#                    k
#
#            if k>0:
#                a.append('DIGITAL')
#            else:
#                a.append('NO DIGITAL')
#        except:
#            a.append('')
#    Base_Aux['RESULT_'+Variable_Analizar+'_2']=a
##    Base_Aux['WORDS']=b
#    Base=Base.merge(Base_Aux,how='left')
#    Base['RESULT_'+Variable_Analizar]=np.where(pd.isnull(Base['RESULT_'+Variable_Analizar+'_2']),Base['RESULT_'+Variable_Analizar],Base['RESULT_'+Variable_Analizar+'_2'])
#    Base.drop(['RESULT_'+Variable_Analizar+'_2'], axis=1,inplace=True)
#    return Base


def SinDefinir2(Base, diccionario2, Variable_Analizar):

    """ Clasificación de los productos SIN DEFINIR
    
    Descripción
    ----------------------------------------------------------------------------------------------------------
    
        Esta función genera una nueva columna sobre un data frame, en la cual clasifica los textos que resultaron SIN DEFINIR
        de la primera corrida.
        
    Parametros
    ----------------------------------------------------------------------------------------------------------
        
            
           Base (Dataframe)             --- Base procesada con el tipo de producto
           diccionario2 (Dataframe)     --- Diccionario de palabras compuestas con las cuales se identifica si un producto siendo SIN DEFINIR es digital
           Variable_Analizar(String)    --- Nombre de la columna sobre la cual se va a procesar el texto.
           
    Retorno
    ----------------------------------------------------------------------------------------------------------
    
        Dataframe
        
    """

    Base_Aux = Base[Base["RESULT_" + Variable_Analizar] == "SIN DEFINIR"][
        ["OPERATION_NUMBER", Variable_Analizar]
    ]
    Base_Aux.drop_duplicates(inplace=True)
    a = []
    b = []
    for i in Base_Aux[Variable_Analizar]:
        k = 0
        try:
            i = limpieza_texto1(i)
            c = []
            for j in diccionario2["PALABRAS"]:
                if i.lower().find(j) >= 0:
                    c.append(j)
                    k = k + 1
                else:
                    k

            if k > 0:
                a.append("DIGITAL")
                b.append(c)
            else:
                a.append("NO DIGITAL")
                b.append(c)
        except:
            a.append("")
    Base_Aux["RESULT_" + Variable_Analizar + "_2"] = a
    Base_Aux["WORDS"] = b
    #    Base_Aux['WORDS']=b
    Base = Base.merge(Base_Aux, how="left")
    Base["RESULT_" + Variable_Analizar] = np.where(
        pd.isnull(Base["RESULT_" + Variable_Analizar + "_2"]),
        Base["RESULT_" + Variable_Analizar],
        Base["RESULT_" + Variable_Analizar + "_2"],
    )
    Base.drop(["RESULT_" + Variable_Analizar + "_2", "WORDS"], axis=1, inplace=True)
    list_of_words = list(chain(*Base_Aux["WORDS"]))
    rep_component = repeticiones(Base_Aux["WORDS"], Base_Aux, "OPERATION_NUMBER")
    rep_variable = repeticiones(Base_Aux["WORDS"], Base_Aux, Variable_Analizar)
    Base_Aux = pd.DataFrame(
        [rep_component, rep_variable, list_of_words],
        index=["OPERATION_NUMBER", Variable_Analizar, "WORDS"],
    ).T

    return [Base, Base_Aux]


###############################Aplicar funciones y consolidacion de resultados#####################

A = globalfuncion(Base, Diccionario_Total2, "OPERATION_NAME", listStopwords)
B = globalfuncion(Base, Diccionario_Total2, "OBJECTIVE_ES", listStopwords)
C = globalfuncion(Base, Diccionario_Total2, "OBJECTIVE_EN", listStopwords)
D = globalfuncion(Base, Diccionario_Total2, "COMPONENT_NAME", listStopwords)
E = globalfuncion(Base, Diccionario_Total2, "OUTPUT_NAME", listStopwords)


##############################################################
A[0] = SinDefinir2(A[0], diccionario_bigrama, "OPERATION_NAME")[0]
B[0] = SinDefinir2(B[0], diccionario_bigrama, "OBJECTIVE_ES")[0]
C[0] = SinDefinir2(C[0], diccionario_bigrama, "OBJECTIVE_EN")[0]
D[0] = SinDefinir2(D[0], diccionario_bigrama, "COMPONENT_NAME")[0]
E[0] = SinDefinir2(E[0], diccionario_bigrama, "OUTPUT_NAME")[0]


A1 = SinDefinir2(A[0], diccionario_bigrama, "OPERATION_NAME")[1]
B1 = SinDefinir2(B[0], diccionario_bigrama, "OBJECTIVE_ES")[1]
C1 = SinDefinir2(C[0], diccionario_bigrama, "OBJECTIVE_EN")[1]
D1 = SinDefinir2(D[0], diccionario_bigrama, "COMPONENT_NAME")[1]
E1 = SinDefinir2(E[0], diccionario_bigrama, "OUTPUT_NAME")[1]

A1 = pd.concat(
    [A[1][["OPERATION_NUMBER", "WORDS"]], A1[["OPERATION_NUMBER", "WORDS"]]],
    ignore_index=True,
)
B1 = pd.concat(
    [B[1][["OPERATION_NUMBER", "WORDS"]], B1[["OPERATION_NUMBER", "WORDS"]]],
    ignore_index=True,
)
C1 = pd.concat(
    [C[1][["OPERATION_NUMBER", "WORDS"]], C1[["OPERATION_NUMBER", "WORDS"]]],
    ignore_index=True,
)
D1 = pd.concat(
    [D[1][["OPERATION_NUMBER", "WORDS"]], D1[["OPERATION_NUMBER", "WORDS"]]],
    ignore_index=True,
)
E1 = pd.concat(
    [E[1][["OPERATION_NUMBER", "WORDS"]], E1[["OPERATION_NUMBER", "WORDS"]]],
    ignore_index=True,
)


BC = B[0].merge(C[0], how="outer")

BC["RESULT_OBJETIVO"] = np.where(
    (BC["RESULT_OBJECTIVE_ES"] == "DIGITAL") | (BC["RESULT_OBJECTIVE_EN"] == "DIGITAL"),
    "DIGITAL",
    "NO DIGITAL",
)

a = BC[["RESULT_OBJECTIVE_ES_TECN-INNOV", "RESULT_OBJECTIVE_EN_TECN-INNOV"]].apply(
    np.nanmax, axis=1
)
BC["RESULT_OBJECTIVE_TECN-INNOV"] = a

###################### Base Unificada#############################
# Consolidado.drop(columns=['RESULT_OBJECTIVE_EN','RESULT_OBJECTIVE_EN_2','RESULT_OBJECTIVE_ES_TECN-INNOV','RESULT_OBJECTIVE_EN_TECN-INNOV'],inplace=True)
# Consolidado.rename(columns={'RESULT_OBJECTIVE_ES_2':'NOUX_RESULT_OBJECTIVE','RESULT_OPERATION_NAME_2':'NOUX_RESULT_OPERATION_NAME','RESULT_COMPONENT_NAME_2':'NOUX_COMPONENT_NAME','RESULT_OUTPUT_NAME_2':'NOUX_RESULT_OUTPUT_NAME'},inplace=True)
# Consolidado=Consolidado[['OPERATION_NUMBER','OPERATION_NAME','NOUX_RESULT_OPERATION_NAME','RESULT_'+'OPERATION_NAME_'+'TECN-INNOV',
#                         'OBJECTIVE_ES','OBJECTIVE_EN','NOUX_RESULT_OBJECTIVE','RESULT_OBJECTIVE_TECN-INNOV',
#                         'COMPONENT_NAME','NOUX_COMPONENT_NAME','RESULT_'+'COMPONENT_NAME_'+'TECN-INNOV',
#                         'OUTPUT_NAME','NOUX_RESULT_OUTPUT_NAME','RESULT_'+'OUTPUT_NAME_'+'TECN-INNOV']]
#
# Consolidado.drop_duplicates(inplace=True)
#
#

################ DEFINIR DIGITAL/NO DIGITAL POR OPERACION ##################################
y = os.listdir(path)
if "Resultados" not in y:
    os.mkdir(path + "/Resultados")


Titulo = A[0][
    [
        "OPERATION_NUMBER",
        "OPERATION_NAME",
        "RESULT_OPERATION_NAME",
        "RESULT_" + "OPERATION_NAME_" + "TECN-INNOV",
    ]
]
Objetivo = BC[
    [
        "OPERATION_NUMBER",
        "OBJECTIVE_ES",
        "OBJECTIVE_EN",
        "RESULT_OBJETIVO",
        "RESULT_OBJECTIVE_TECN-INNOV",
    ]
]

Componentes1 = D[0][["OPERATION_NUMBER", "COMPONENT_NAME", "RESULT_COMPONENT_NAME"]]
Producto1 = E[0][
    ["OPERATION_NUMBER", "COMPONENT_NAME", "OUTPUT_NAME", "RESULT_OUTPUT_NAME"]
]

Componentes = D[0][["OPERATION_NUMBER", "COMPONENT_NAME", "RESULT_COMPONENT_NAME"]]
Componentes = (
    Componentes.groupby(["OPERATION_NUMBER", "RESULT_COMPONENT_NAME"])["COMPONENT_NAME"]
    .count()
    .unstack()
)
Componentes.fillna(0, inplace=True)
Componentes["DIGITAL_COMP"] = Componentes["DIGITAL"] / (
    Componentes["DIGITAL"] + Componentes["NO DIGITAL"]
)
Componentes.drop(columns=["DIGITAL", "NO DIGITAL"], inplace=True)
Componentes.reset_index(inplace=True)

Producto = E[0][["OPERATION_NUMBER", "OUTPUT_NAME", "RESULT_OUTPUT_NAME"]]
Producto = (
    Producto.groupby(["OPERATION_NUMBER", "RESULT_OUTPUT_NAME"])["OUTPUT_NAME"]
    .count()
    .unstack()
)
Producto.fillna(0, inplace=True)
Producto["DIGITAL_OUT"] = Producto["DIGITAL"] / (
    Producto["DIGITAL"] + Producto["NO DIGITAL"]
)
Producto.drop(columns=["DIGITAL", "NO DIGITAL"], inplace=True)
Producto.reset_index(inplace=True)
Producto = Producto.drop_duplicates()

Producto1 = Producto1.merge(Producto, on="OPERATION_NUMBER", how="left")
Producto1 = Producto1.merge(
    Metadatos[
        [
            "OUTPUT_COST",
            "IS_COST",
            "OPERATION_NUMBER",
            "OUTPUT_NAME",
            "OUTPUT_UOM",
            "DIVISION",
            "COMPONENT_NAME",
        ]
    ],
    on=["OPERATION_NUMBER", "OUTPUT_NAME", "COMPONENT_NAME"],
    how="left",
)
Producto1.OUTPUT_COST.fillna(0, inplace=True)
Producto1.OUTPUT_COST = Producto1.OUTPUT_COST.astype(float)
Producto1 = Producto1.drop_duplicates()
Producto1 = Producto1.merge(
    Resultados[["OPERATION_NUMBER", "COMPONENT_NAME", "OUTPUT_NAME", "OUTPUT_FI"]],
    on=["OPERATION_NUMBER", "COMPONENT_NAME", "OUTPUT_NAME"],
    how="left",
)
Producto1 = Producto1.drop_duplicates()

# Group digital cost by operation ONLY DIGITALS
Costos_op = Producto1.groupby(
    ["OPERATION_NUMBER", "COMPONENT_NAME", "RESULT_OUTPUT_NAME", "IS_COST"],
    as_index=False,
)[
    "OUTPUT_COST"
].sum()  # totales costos por output
Costos_tot = Costos_op.groupby(["OPERATION_NUMBER", "IS_COST"], as_index=False)[
    "OUTPUT_COST"
].sum()  # totales por operación
Costos_tot[["TOTAL_COST"]] = Costos_tot[["OUTPUT_COST"]]

Costos_op_digital = Costos_op[Costos_op.RESULT_OUTPUT_NAME == "DIGITAL"]

Final = Titulo[
    [
        "OPERATION_NUMBER",
        "RESULT_OPERATION_NAME",
        "RESULT_" + "OPERATION_NAME_" + "TECN-INNOV",
    ]
].merge(
    Objetivo[
        ["OPERATION_NUMBER", "RESULT_OBJETIVO", "RESULT_OBJECTIVE_TECN-INNOV"]
    ].merge(Componentes.merge(Producto, how="outer"), how="outer"),
    how="outer",
)
Final.fillna(0, inplace=True)
Final = Final.merge(Costos_op_digital, on="OPERATION_NUMBER", how="left")


Final["DUMMY_DIGITAL"] = np.where(
    (Final["RESULT_OPERATION_NAME"] == "DIGITAL")
    | (Final["RESULT_OBJETIVO"] == "DIGITAL")
    | (Final["DIGITAL_COMP"] > 0)
    | (Final["DIGITAL_OUT"] > 0.1),
    1,
    np.where(
        (Final["DIGITAL_OUT"] > 0.045) & (Final["OUTPUT_COST"] > 0),
        1,
        np.where(
            (Final["OPERATION_NUMBER"] == "BR-L1526")
            | (Final["OPERATION_NUMBER"] == "EC-L1155")
            | (Final["OPERATION_NUMBER"] == "UR-L1176"),
            1,
            0,
        ),
    ),
)
Final["DUMMY_OUTPUT_DIG"] = np.where((Final["DIGITAL_OUT"] > 0), 1, 0)
Final["DUMMY_INNOVACION"] = Final[
    ["RESULT_" + "OPERATION_NAME_" + "TECN-INNOV", "RESULT_OBJECTIVE_TECN-INNOV"]
].apply(np.nanmax, axis=1)
Final["DUMMY_TITULO"] = np.where((Final["RESULT_OPERATION_NAME"] == "DIGITAL"), 1, 0)
Final["DUMMY_COMPONENTE"] = np.where((Final["DIGITAL_COMP"] > 0), 1, 0)
Final["DUMMY_OBJETIVO_DIG"] = np.where((Final["RESULT_OBJETIVO"] == "DIGITAL"), 1, 0)
Final["DUMMY_INN_DIGITAL"] = np.where(
    (Final["DUMMY_DIGITAL"] == 1) & (Final["DUMMY_INNOVACION"] == 1), 1, 0
)
Final = Final[
    [
        "OPERATION_NUMBER",
        "DUMMY_DIGITAL",
        "DUMMY_OBJETIVO_DIG",
        "DUMMY_OUTPUT_DIG",
        "DUMMY_TITULO",
        "DUMMY_COMPONENTE",
        "OUTPUT_COST",
    ]
]

Bas = Metadatos[
    [
        "OPERATION_NUMBER",
        "RELATED_OPER",
        "RELATION_TYPE",
        "EXEC_STS",
        "OPERATION_TYPE",
        "OPERATION_TYPE_NAME",
        "OPERATION_MODALITY",
        "TAXONOMY",
        "STATUS",
        "REGION",
        "COUNTRY",
        "DEPARTMENT",
        "DIVISION",
        "APPROVAL_DATE",
        "APPROVAL_AMOUNT",
        "CURRENT_EXPIRATION_DATE",
    ]
]

Bas.drop_duplicates(inplace=True)

P = Bas[["OPERATION_NUMBER", "APPROVAL_DATE"]].groupby(["OPERATION_NUMBER"]).max()
P.reset_index(inplace=True)
Bas = Bas.merge(P, how="outer", on="OPERATION_NUMBER")
Bas.drop(columns=["APPROVAL_DATE_x"], inplace=True)
Bas.drop_duplicates(inplace=True)
Bas.rename(columns={"APPROVAL_DATE_y": "APPROVAL_DATE"}, inplace=True)
Bas = Bas.merge(Final, how="outer")
# Bas['APPROVAL_DATE']=Bas['APPROVAL_DATE'].apply(todate)


###### Incluir transformación digital de checklist ######

path_cl = "C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/documents/Inputs"
checklist = pd.read_excel(
    path_cl + "/Triage_digital.xlsx", sheet_name="Sheet1"
)  # leer checklist completa
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
checklist_tidy = checklist[
    [
        "OPERATION_NUMBER",
        "INFO",
        "INFRA",
        "GOB",
        "CULTURA",
        "DUMMY_DIGITAL_CL",
        "DIGITAL_TRANS",
    ]
]
checklist_tidy = checklist_tidy.drop_duplicates()  # quitar duplicados
checklist_port = checklist_tidy.merge(
    Bas, on="OPERATION_NUMBER", how="left"
)  # Juntar con total para ver operaciones en cartera
today = time.strftime("%Y-%m-%d")
checklist_port2 = checklist_port.loc[(checklist_port["APPROVAL_DATE"] < today)]
checklist_port2 = checklist_port2[
    [
        "OPERATION_NUMBER",
        "RELATED_OPER",
        "RELATION_TYPE",
        "EXEC_STS",
        "OPERATION_TYPE",
        "OPERATION_TYPE_NAME",
        "OPERATION_MODALITY",
        "TAXONOMY",
        "STATUS",
        "REGION",
        "COUNTRY",
        "DEPARTMENT",
        "DIVISION",
        "APPROVAL_DATE",
        "APPROVAL_AMOUNT",
        "CURRENT_EXPIRATION_DATE",
        "INFO",
        "INFRA",
        "GOB",
        "CULTURA",
        "DUMMY_DIGITAL_CL",
        "DUMMY_OBJETIVO_DIG",
        "DUMMY_OUTPUT_DIG",
        "DIGITAL_TRANS",
    ]
]
checklist_port2["DUMMY_DIGITAL"] = checklist_port2["DUMMY_DIGITAL_CL"]


# pegar data frames
Bas[["INFO", "INFRA", "GOB", "CULTURA", "DIGITAL_TRANS", "DUMMY_DIGITAL_CL"]] = None

Bas = pd.concat([Bas, checklist_port2])
Bas = Bas.sort_values(by=["OPERATION_NUMBER", "INFO"])
Bas = Bas.drop_duplicates(subset="OPERATION_NUMBER", keep="first")

# Agregando columna con descripción de outputs clasificados como DIGITAL

# costos_digital = Costos_op_digital.merge(Final,how='outer')
# costos_digital = costos_digital[costos_digital.DUMMY_DIGITAL==1]

tempdf = Producto1[Producto1.RESULT_OUTPUT_NAME == "DIGITAL"][
    ["OPERATION_NUMBER", "OUTPUT_NAME"]
]
dig_out_desc = pd.DataFrame()
dig_out_desc["OPERATION_NUMBER"] = tempdf.OPERATION_NUMBER.drop_duplicates()
dig_out_desc["DIG_OUTPUT_DESCRIPTION"] = tempdf.groupby("OPERATION_NUMBER")[
    "OUTPUT_NAME"
].transform(lambda x: "; ".join(x))

Bas = Bas.merge(dig_out_desc, on="OPERATION_NUMBER", how="left")
Bas = Bas.merge(Producto, on="OPERATION_NUMBER", how="left")
# Bas = Bas.merge(costos_digital[['OPERATION_NUMBER', 'DUMMY_DIGITAL', 'OUTPUT_COST']], on =['OPERATION_NUMBER', 'DUMMY_DIGITAL'], how = 'left')
Bas = Bas.merge(
    Costos_tot[["OPERATION_NUMBER", "TOTAL_COST"]], on=["OPERATION_NUMBER"], how="left"
)
Bas["OUTPUT_DIG_COST"] = Bas["OUTPUT_COST"] / Bas["TOTAL_COST"]


###### Incluir operaciones revisadas manualmente por las divisiones #######

# leer el archivo que contiene la revisión manual de las divisiones

# path manual donde se tiene el repo de cartera
path_revision = "C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/Documents/GitHub/diccionario_cartera_digital_scl/Inputs/"
resultado_manual = pd.read_excel(
    path_revision + "revision_dig_divisiones.xlsx", sheet_name="Sheet1"
)  # leer revisión manual
Bas = Bas.merge(resultado_manual, on=["OPERATION_NUMBER", "DIVISION"], how="left")
Bas["DUMMY_DIGITAL"] = np.where(
    Bas["RESULTADO_DIV"] == 0,
    0,
    np.where(Bas["RESULTADO_DIV"] == 1, 1, Bas["DUMMY_DIGITAL"]),
)
Bas.drop(columns=["RESULTADO_DIV"], inplace=True)

#######################################################################################################
# NUBE DE PALABRAS

Palabras = pd.concat([A1, B1, C1, D1, E1], axis=0, ignore_index=True)
Dicc = pd.concat(
    [
        Diccionario_Total[["PALABRAS", "IDIOMA", "TIPO"]],
        diccionario_bigrama[["PALABRAS", "IDIOMA", "TIPO"]],
    ],
    ignore_index=True,
)
Palabras = Palabras.merge(Dicc, right_on="PALABRAS", left_on="WORDS", how="left")

# Palabras=Palabras[(Palabras['IDIOMA']=='en')&(Palabras['TIPO']=='POSITIVO')][['OPERATION_NUMBER','WORDS']] #Esta versión arroja nube de palabras incompleta
Palabras = Palabras[
    (Palabras["TIPO"] == "POSITIVO") | (Palabras["TIPO"] == "NEUTRO POSITIVO")
][["OPERATION_NUMBER", "WORDS", "TIPO"]]

Palabras["WORDS2"] = Palabras["WORDS"].apply(singular)
Palabras = Palabras[["OPERATION_NUMBER", "WORDS2", "TIPO"]]
Palabras.rename(columns={"WORDS2": "WORDS"}, inplace=True)

# Palabras=DataFrame(Palabras["PALABRAS","WORDS"].groupby([Palabras['OPERATION_NUMBER']],Palabras['WORDS','PALABRAS']).count()) #Esta línea no corre, lo puse como está en la versión de EDU_IADB_cartera_digital que si corre
# Palabras=DataFrame(Palabras["WORDS"].groupby([Palabras['OPERATION_NUMBER'],Palabras['WORDS']]).count())
Palabras = DataFrame(
    Palabras["WORDS"]
    .groupby([Palabras["OPERATION_NUMBER"], Palabras["WORDS"], Palabras["TIPO"]])
    .count()
)
Palabras.rename(columns={"WORDS": "COUNT_WORDS"}, inplace=True)
Palabras.rename(columns={"PALABRAS": "COUNT_WORDS"}, inplace=True)
Palabras.reset_index(inplace=True)

########EXPORTAR ARCHIVOS#############


with pd.ExcelWriter(path + "output.xlsx") as writer:
    Titulo.to_excel(writer, sheet_name="Operation_Name", index=False)
    Objetivo.to_excel(writer, sheet_name="Objetivo", index=False)
    Componentes1.to_excel(writer, sheet_name="Component", index=False)
    Producto1.to_excel(writer, sheet_name="Output_Name", index=False)
    Bas.to_excel(writer, sheet_name="Metadata", index=False)
    Palabras.to_excel(writer, sheet_name="palabras", index=False)
