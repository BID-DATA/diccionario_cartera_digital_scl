# -*- coding: utf-8 -*-
"""
Created on Mon Sep 21 10:10:58 2020
Revision code cartera digital 1
@author: ALOP
"""
import sys
sys.stdout.encoding
'UTF-8'
import nltk
import numpy as np
import pandas as pd
import hashlib
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



############## Lectura del archivo portafolio #############################


path = "C:/Users/alop/OneDrive - Inter-American Development Bank Group/Desktop/GitRepositories/calculo_cartera_digital_scl"
#
#Metadatos=pd.ExcelFile('06.18.19 - Portfolio Ejercicio 1 - 010109 061319.xlsx')
Metadatos=pd.ExcelFile(path+'/input/00_base_convergencia.xlsx')
#Metadatos =Metadatos.parse('Raw data')
Metadatos =Metadatos.parse('Sheet1')
Metadatos.head()####ver los primeros registros de la data
Metadatos.columns.values ###Los nombres de las columnas
Metadatos.shape #####dimensiones de la data




############## Lectura del archivo diccionario #############################

####Se forma un solo diccionario
Diccionario=pd.ExcelFile(path+'/input/01_Diccionario_token_digital.xlsx')
Diccionario =Diccionario.parse('Sheet1')############Lectura de data
Diccionario.head()####ver los primeros registros de la data
Diccionario.columns.values ###Los nombres de las columnas
Diccionario.shape #####dimensiones de la data

Diccionario_En=Diccionario[['TIPO','INGLES','TOKENS']]
Diccionario_En.dropna(inplace=True)
Diccionario_En.rename(columns = {'INGLES':'PALABRAS'},inplace=True)
Diccionario_En['IDIOMA']='en'

Diccionario_Es=Diccionario[['TIPO','ESPANOL','TOKENS']]
Diccionario_Es.dropna(inplace=True)
Diccionario_Es.rename(columns = {'ESPANOL':'PALABRAS'},inplace=True)
Diccionario_Es['IDIOMA']='es'


Diccionario_Pt=Diccionario[['TIPO','PORTUGUES','TOKENS']]
Diccionario_Pt.dropna(inplace=True)
Diccionario_Pt.rename(columns = {'PORTUGUES':'PALABRAS'},inplace=True)
Diccionario_Pt['IDIOMA']='pt'

Diccionario_Fr=Diccionario[['TIPO','FRANCES','TOKENS']]
Diccionario_Fr.dropna(inplace=True)
Diccionario_Fr.rename(columns = {'FRANCES':'PALABRAS'},inplace=True)
Diccionario_Fr['IDIOMA']='fr'


Diccionario_Total = pd.concat([Diccionario_Es,Diccionario_En,Diccionario_Pt,Diccionario_Fr])
Diccionario_Total2 = Diccionario_Total[['TIPO','PALABRAS','TOKENS']].drop_duplicates()

#######Lectura de diccionario para los textos que se encuentran en sin definir #########

diccionario_bigrama = pd.read_excel(path+'/input/02_Diccionario_bigrama_digital.xlsx',sheet_name='Hoja1')
diccionario_bigrama_En=diccionario_bigrama[['TIPO','INGLES']]
diccionario_bigrama_En.dropna(inplace=True)
diccionario_bigrama_En.rename(columns = {'INGLES':'PALABRAS'},inplace=True)
diccionario_bigrama_En['IDIOMA']='en'


diccionario_bigrama_Es=diccionario_bigrama[['TIPO','ESPANOL']]
diccionario_bigrama_Es.dropna(inplace=True)
diccionario_bigrama_Es.rename(columns = {'ESPANOL':'PALABRAS'},inplace=True)
diccionario_bigrama_Es['IDIOMA']='es'


diccionario_bigrama_Pt=diccionario_bigrama[['TIPO','PORTUGUES']]
diccionario_bigrama_Pt.dropna(inplace=True)
diccionario_bigrama_Pt.rename(columns = {'PORTUGUES':'PALABRAS'},inplace=True)
diccionario_bigrama_Pt['IDIOMA']='pt'

diccionario_bigrama_Fr=diccionario_bigrama[['TIPO','FRANCES']]
diccionario_bigrama_Fr.dropna(inplace=True)
diccionario_bigrama_Fr.rename(columns = {'FRANCES':'PALABRAS'},inplace=True)
diccionario_bigrama_Fr['IDIOMA']='fr'

diccionario_bigrama = pd.concat([diccionario_bigrama_Es,diccionario_bigrama_En,diccionario_bigrama_Pt,diccionario_bigrama_Fr])
############Eliminacion de registros dobles cuyas fechas current expiration date tenga null #####################
Metadatos.columns=[w.upper() for w in Metadatos.columns]


#p=datetime.strptime('1900-1-1','%Y-%m-%d')
#Metadatos[['CURRENT_EXPIRATION_DATE']]=Metadatos[['CURRENT_EXPIRATION_DATE']].fillna(p)
#w=Metadatos[['CURRENT_EXPIRATION_DATE']].groupby(Metadatos['OPERATION_NUMBER']).max()
#w.reset_index(inplace=True)
#w=w.rename(columns = {'CURRENT_EXPIRATION_DATE':'CURRENT_EXPIRATION__DATE'})
#Metadatos = Metadatos.merge(w)
#Metadatos = Metadatos[Metadatos['CURRENT_EXPIRATION_DATE']==Metadatos['CURRENT_EXPIRATION__DATE']]
#Metadatos.drop(['CURRENT_EXPIRATION__DATE'],axis=1,inplace=True)


######################Subase donde se encuentra las columnas las cuales seran procesadas #############

Base = Metadatos[['OPERATION_NUMBER','OPERATION_NAME',
                  'OBJECTIVE_ES','OBJECTIVE_EN','COMPONENT_NAME','OUTPUT_NAME','OUTPUT_DESCRIPTION']]



#########################Generacion de stopwords ##############################################
nltk.download('stopwords')
listStopwordsEn=stopwords.words('english')
listStopwordsEs=stopwords.words('spanish')
listStopwordsFr=stopwords.words('french')
listStopwordsPt=stopwords.words('portuguese')
listStopwords= listStopwordsEn + listStopwordsEs + listStopwordsFr + listStopwordsPt +['It']

listStopwords_df = pd.DataFrame(listStopwords)
listStopwords_df
######################           FUNCIONES                ###################################

def todate(text):
    '''Lectura de fechas
    
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
        
    
    
    '''
    fecha=datetime.strptime(text,'%m/%d/%Y')
    return fecha
    

def limpieza_texto1(text):
    
    '''Lectura de texto

    Descripcion
    ----------------------------------------------------------------------------------
    Devuelve un texto plano, eliminando simbolos o caracteres innecesario.
    
    
    Parametros:
    ----------------------------------------------------------------------------------
        text (string)   ---  . Texto el cual va a ser procesado
        
        
    Retorno
    ----------------------------------------------------------------------------------
        string
    '''

    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    return str(text)




def limpieza_texto2(text,diccionario):
    
    '''Lectura de texto

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
    '''

    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    text=' ' + text.lower()
    text = text.replace('en linea con',' ')
    text=re.sub('[%s]' % re.escape(string.punctuation), ' ', text)
    diccionario=diccionario[diccionario['TOKENS']==2]
#    b =(text.find(' banda ancha ')>=0) | (text.find(' e government ')>=0) | (text.find(' big data ')>=0) | (text.find(' data mining ')>=0) | (text.find(' on line ')>=0)  | (text.find(' data warehouse ')>=0) | (text.find(' en linea ')>=0) 
    a=[]
    text1=' '+text+' '
    for w in diccionario['PALABRAS']:
        if text1.find(' '+w+' ')>=0:
            a.append(w)           
    return a

def search_tec_inno(text):
    
    '''Lectura de texto

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
    '''
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    text=re.sub('[%s]' % re.escape(string.punctuation), ' ', text)
    text=text.lower()
    b =(text.find('tecnolog')>=0) | (text.find('technolog')>=0) | (text.find('innova')>=0)
    if b==True:
        a=1
    else:
        a=0
    return a


def searchword(listWords,tokens):
    
    '''Función de búsqueda de palabras
    
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
    

    
    '''
    frozensetListWords=frozenset(listWords)
    return [w for w in tokens if w in frozensetListWords] 


def corpusword(text,diccionario,listStopwords):
    
    ''' Procesamiento del texto
    
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
    
    '''
    tokenizer = RegexpTokenizer(r'[a-zA-Z]+')
    text=limpieza_texto1(text) 
    tokens=tokenizer.tokenize(text)
    tokens=[w for w in tokens if not w in listStopwords]
    tokens=[w.lower() for w in tokens] 
    tokens = searchword(diccionario,tokens)
    return list(set(tokens))


def singular(text):
    ''' Procesamiento del texto
    
    Descripcion
    ------------------------------------------------------------------------------------------------------------
    
        Convierte las palabras de plural a singular.
    
    Parametros
    -------------------------------------------------------------------------------------------------------------
        text  (string)            ---   El texto a procesar

        
    Retorno
    -------------------------------------------------------------------------------------------------------------
        string

    '''
    
    y=TextBlob(text)
    a=[]
    for j in list(range(len(y.words))):
        a.append(y.words[j].singularize())
        b=' '.join(a)
    return b


def searchsysteminformation(text):
    
        
    ''' Busqueda se sistemas de información.
    
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
            


    '''
    text=limpieza_texto1(text)
    text=text.lower()
    text=text.replace(',',' , ')
    text=text.replace('.',' , ')
    text=text.replace(':',' , ')
    text=text.replace(';',' , ')
    o=re.sub('[%s]' % re.escape(string.punctuation), ' , ', text)
    o=o.split()
    u=DataFrame(o,columns=['AB'])
    p=u[(u['AB']=='sistema') | (u['AB']=='sistemas') | (u['AB']=='informacion') | (u['AB']=='system') | (u['AB']=='systems') 
    | (u['AB']==',') | (u['AB']=='information')  | (u['AB']=='technologies') | (u['AB']=='technology') | (u['AB']=='tecnologias')
    | (u['AB']=='tecnologia') |  (u['AB']=='informacao') | (u['AB']=='sistemes') | (u['AB']=='sisteme')]['AB'] 
    p=list(p)
    p=''.join(p)
    a=[]
    if (p.find('systeminformation')>=0) |(p.find('sistemainformacion')>=0) | (p.find('sistemasinformacion')>=0) | (p.find('informationsystem')>=0)\
        |(p.find('informationsystems')>=0) |(p.find('sistemasinformacao')>=0) |(p.find('sistemainformacao')>=0) | (p.find('sistemesinformation')>=0)\
        |(p.find('sistemeinformation')>=0):
        a.append('sistema de informacion')
    if (p.find('informationtechnology')>=0) | (p.find('informationtechnologies')>=0)  | (p.find('tecnologiasinformaciones')>=0)\
    | (p.find('tecnologiasinformacion')>=0) | (p.find('tecnologiainformacion')>=0) | (p.find('tecnologiainformacao')>=0) | (p.find('tecnologiasinformacao')>=0):
        a.append('tic')
    return a


def repeticiones(lista1,Base,Valor):
    
    ''' Función de repetición de registros
    
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
    
    '''
    conteo = DataFrame([len(x) for x in lista1],columns=["n_veces"])
    conteo.index=lista1.index
    repeticiones = list([np.repeat(Base[Valor][x],conteo['n_veces'][x]) for x in conteo.index])
    lista_repeticiones = [y for x in repeticiones for y in x]
    return lista_repeticiones
  
    
def globalfuncion(Base,Diccionario,Variable_Analizar,listStopWords):
    
    ''' Función de generación de resultados
    
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
        
    
    '''

    Idioma='PALABRAS'
      
    if(Variable_Analizar=='OUTPUT_NAME'):
       Base_Aux = Base[['OPERATION_NUMBER','COMPONENT_NAME',Variable_Analizar,'OUTPUT_DESCRIPTION']]
       Base_Aux = Base_Aux[(pd.isnull(Base_Aux['OUTPUT_DESCRIPTION'])==False) | (pd.isnull(Base_Aux['COMPONENT_NAME'])==False)]
       Base_Aux['OUTPUT_NAME'] = Base_Aux['OUTPUT_NAME'].fillna('')
       Base_Aux['OUTPUT_DESCRIPTION'] = Base_Aux['OUTPUT_DESCRIPTION'].fillna('')
       a=list([str(i) for i in (Base_Aux['OUTPUT_NAME'])])
       b=list([str(j) for j in (Base_Aux['OUTPUT_DESCRIPTION'])])
       c=[]
       for i in range(len(a)):
            if (a[i]!='') & (b[i]!=''):
                c.append(a[i]+str(' ')+b[i])
            elif b[i]=='':
                c.append(a[i])
            else:
                c.append(b[i])

       Base_Aux['OUTPUT_NAME']=c
       Base_Aux.drop(['OUTPUT_DESCRIPTION'], axis=1,inplace=True)
        
    else:    
       Base_Aux= DataFrame()
       Base_Aux=Base[['OPERATION_NUMBER',Variable_Analizar]]
       Base_Aux.drop_duplicates(inplace=True)
       Base_Aux.dropna(inplace=True)
       Base_Aux[Variable_Analizar]=Base_Aux[Variable_Analizar].apply(str)
    
    
    list_of_words=Base_Aux[Variable_Analizar].apply(corpusword,args=(Diccionario_Total[Diccionario_Total.TOKENS==1]['PALABRAS'],listStopwords,))
    list_of_words2=Base_Aux[Variable_Analizar].apply(limpieza_texto2,args=(Diccionario_Total,))
    list_of_words3=Base_Aux[Variable_Analizar].apply(searchsysteminformation)
    list_of_words=list_of_words+list_of_words2+list_of_words3
    rep_name=repeticiones(list_of_words,Base_Aux,'OPERATION_NUMBER')
    rep_variable = repeticiones(list_of_words,Base_Aux,Variable_Analizar)
    
    
    dframe =DataFrame()

   
    
    if(Variable_Analizar=='OUTPUT_NAME'):
     Base_Aux['COMPONENT_NAME']=Base_Aux['COMPONENT_NAME'].astype(str)
     rep_component=repeticiones(list_of_words,Base_Aux,'COMPONENT_NAME')
     dframe['COMPONENT_NAME']=rep_component
    
     list_of_words=list(chain(*list_of_words))
    
#    

    dframe['OPERATION_NUMBER']=rep_name
    dframe[Variable_Analizar]=rep_variable
    
    dframe['WORDS']=list_of_words
    dframe.explode('WORDS')

#    Base_Aux[Variable_Analizar]=Base_Aux[Variable_Analizar].str.replace(' xxxxxx ',' Red ')
    dframe = dframe.merge(Diccionario[['TIPO',Idioma]],left_on='WORDS',right_on=Idioma,how='left')
    
    if(Variable_Analizar=='OUTPUT_NAME'):
        dframe2 = dframe[['OPERATION_NUMBER','COMPONENT_NAME',Variable_Analizar,'WORDS']]
        dframe2.drop_duplicates(inplace=True)
        dframe = dframe[['OPERATION_NUMBER','COMPONENT_NAME',Variable_Analizar,'TIPO']].drop_duplicates()
        dframe = pd.crosstab([dframe['OPERATION_NUMBER'],dframe['COMPONENT_NAME'],dframe[Variable_Analizar]],columns=dframe['TIPO'])
        
    else:
        dframe2 = dframe[['OPERATION_NUMBER',Variable_Analizar,'WORDS','TIPO']]
#        dframe2.drop_duplicates(inplace=True)
        dframe = dframe[['OPERATION_NUMBER',Variable_Analizar,'TIPO']].drop_duplicates()
        dframe = pd.crosstab([dframe['OPERATION_NUMBER'],dframe[Variable_Analizar]],columns=dframe['TIPO'])
         
#
        
    dframe.reset_index(inplace=True)
    X=set(dframe.columns) #####conjunto de las columnas
    Y=set(['NEGATIVO','NEUTRO','NEUTRO POSITIVO','POSITIVO']) ###columnas necesarias para aplicar la condicion
    b=list(Y-X)
    
    if len(b)>0:
        aux=DataFrame(np.repeat(0,len(b)*dframe.shape[0]).reshape((dframe.shape[0],len(b))),columns=b)
        dframe=pd.concat([dframe,aux],axis=1)
    Base_Aux.index=range(len(Base_Aux))
    dframe = Base_Aux.merge(dframe,how='left')
    dframe.fillna(np.nan,inplace=True)
    
    
    
    if (Variable_Analizar=='OUTPUT_NAME'):
        dframe = dframe [['OPERATION_NUMBER','COMPONENT_NAME',Variable_Analizar,'NEGATIVO','NEUTRO','NEUTRO POSITIVO','POSITIVO']]
        dframe=dframe.groupby([dframe['OPERATION_NUMBER'],dframe['COMPONENT_NAME'],dframe[Variable_Analizar]]).sum()
    else:
        dframe = dframe [['OPERATION_NUMBER',Variable_Analizar,'NEGATIVO','NEUTRO','NEUTRO POSITIVO','POSITIVO']]
        dframe=dframe.groupby([dframe['OPERATION_NUMBER'],dframe[Variable_Analizar]]).sum()
    
#Aplicar condiciones
    dframe['RESULT'+'_'+Variable_Analizar] = np.where((dframe['NEGATIVO']==0) & (dframe['NEUTRO']==0) & (dframe['NEUTRO POSITIVO']==0) & (dframe['POSITIVO']==0),'NO DIGITAL',
    np.where((dframe['NEGATIVO']>=1) & (dframe['NEUTRO']==0) & (dframe['NEUTRO POSITIVO']==0) & (dframe['POSITIVO']==0),'NO DIGITAL',
    np.where((dframe['NEGATIVO']>=1) & (dframe['NEUTRO']>=1) & (dframe['NEUTRO POSITIVO']==0) & (dframe['POSITIVO']==0),'NO DIGITAL',
    np.where((dframe['NEGATIVO']==0) & (dframe['NEUTRO']==0) & (dframe['NEUTRO POSITIVO']>=1) & (dframe['POSITIVO']==0),'NO DIGITAL',
    np.where((dframe['NEGATIVO']>=1) & (dframe['NEUTRO']==0) & (dframe['NEUTRO POSITIVO']>=1) & (dframe['POSITIVO']==0),'NO DIGITAL',
    np.where((dframe['NEGATIVO']==0) & (dframe['NEUTRO']>=1) & (dframe['NEUTRO POSITIVO']==0) & (dframe['POSITIVO']==0),'SIN DEFINIR','DIGITAL'))))))
    dframe.drop(['NEGATIVO','NEUTRO','NEUTRO POSITIVO','POSITIVO'], axis=1,inplace=True)
    dframe.reset_index(inplace=True)
    dframe['RESULT'+'_'+Variable_Analizar]=np.where((dframe[Variable_Analizar]==' ') | (dframe[Variable_Analizar]=='x') | (dframe[Variable_Analizar]=='xx') | (dframe[Variable_Analizar]=='.') 
                        | (dframe[Variable_Analizar]==',') | [x in str(range(20)) for x in dframe[Variable_Analizar]]     |  (dframe[Variable_Analizar].apply(type)==int)|   (dframe[Variable_Analizar]=='*') 
                        | (dframe[Variable_Analizar]=='#') | (dframe[Variable_Analizar]=='-') | (dframe[Variable_Analizar]=='_') | (dframe[Variable_Analizar]=='- ') 
                        | (dframe[Variable_Analizar]==' -')|  (dframe[Variable_Analizar]=='. -')  ,np.nan,dframe['RESULT'+'_'+Variable_Analizar])        
    
    dframe['RESULT_'+Variable_Analizar+'_TECN-INNOV']=dframe[Variable_Analizar].apply(search_tec_inno)
    
    return [dframe,dframe2]






####funcion para el procesamiento de lo sin definir#####
    
#def SinDefinir(Base,diccionario2,Variable_Analizar):
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

def SinDefinir2(Base,diccionario2,Variable_Analizar):
    
    ''' Clasificación de los productos SIN DEFINIR
    
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
        
    '''
    
        
    Base_Aux=Base[Base['RESULT_'+Variable_Analizar]=='SIN DEFINIR'][['OPERATION_NUMBER',Variable_Analizar]]
    Base_Aux.drop_duplicates(inplace=True)
    a=[]
    b=[]
    for i in Base_Aux[Variable_Analizar]:
        k=0;
        try:
            i=limpieza_texto1(i)
            c=[]
            for j in diccionario2['PALABRAS']:
                if (i.lower().find(j)>=0):
                    c.append(j)
                    k=k+1;
                else:
                    k

            if k>0:
                a.append('DIGITAL')
                b.append(c)
            else:
                a.append('NO DIGITAL')
                b.append(c)
        except:
            a.append('')
    Base_Aux['RESULT_'+Variable_Analizar+'_2']=a
    Base_Aux['WORDS']=b
    Base_Aux.explode('WORDS')
#    Base_Aux['WORDS']=b
    Base=Base.merge(Base_Aux,how='left')
    Base['RESULT_'+Variable_Analizar]=np.where(pd.isnull(Base['RESULT_'+Variable_Analizar+'_2']),Base['RESULT_'+Variable_Analizar],Base['RESULT_'+Variable_Analizar+'_2'])
    Base.drop(['RESULT_'+Variable_Analizar+'_2','WORDS'], axis=1,inplace=True)
   
    list_of_words=list(chain(*Base_Aux['WORDS']))
    rep_component=repeticiones(Base_Aux['WORDS'],Base_Aux,'OPERATION_NUMBER')
    rep_variable=repeticiones(Base_Aux['WORDS'],Base_Aux,Variable_Analizar)
    Base_Aux=pd.DataFrame([rep_component,rep_variable,list_of_words],index=['OPERATION_NUMBER',Variable_Analizar,'WORDS']).T
    
    return [Base,Base_Aux]



###############################Aplicar funciones y consolidacion de resultados#####################
    
A=globalfuncion(Base,Diccionario_Total2,'OPERATION_NAME',listStopwords)
B=globalfuncion(Base,Diccionario_Total2,'OBJECTIVE_ES',listStopwords)
C=globalfuncion(Base,Diccionario_Total2,'OBJECTIVE_EN',listStopwords)
D=globalfuncion(Base,Diccionario_Total2,'COMPONENT_NAME',listStopwords)
E=globalfuncion(Base,Diccionario_Total2,'OUTPUT_NAME',listStopwords)


##############################################################
A[0]=SinDefinir2(A[0],diccionario_bigrama,'OPERATION_NAME')[0]
B[0]=SinDefinir2(B[0],diccionario_bigrama,'OBJECTIVE_ES')[0]
C[0]=SinDefinir2(C[0],diccionario_bigrama,'OBJECTIVE_EN')[0]
D[0]=SinDefinir2(D[0],diccionario_bigrama,'COMPONENT_NAME')[0]
E[0]=SinDefinir2(E[0],diccionario_bigrama,'OUTPUT_NAME')[0]


A1=SinDefinir2(A[0],diccionario_bigrama,'OPERATION_NAME')[1]
B1=SinDefinir2(B[0],diccionario_bigrama,'OBJECTIVE_ES')[1]
C1=SinDefinir2(C[0],diccionario_bigrama,'OBJECTIVE_EN')[1]
D1=SinDefinir2(D[0],diccionario_bigrama,'COMPONENT_NAME')[1]
E1=SinDefinir2(E[0],diccionario_bigrama,'OUTPUT_NAME')[1]

A1=pd.concat([A[1][['OPERATION_NUMBER','WORDS']],A1[['OPERATION_NUMBER','WORDS']]],ignore_index=True)
B1=pd.concat([B[1][['OPERATION_NUMBER','WORDS']],B1[['OPERATION_NUMBER','WORDS']]],ignore_index=True)
C1=pd.concat([C[1][['OPERATION_NUMBER','WORDS']],C1[['OPERATION_NUMBER','WORDS']]],ignore_index=True)
D1=pd.concat([D[1][['OPERATION_NUMBER','WORDS']],D1[['OPERATION_NUMBER','WORDS']]],ignore_index=True)
E1=pd.concat([E[1][['OPERATION_NUMBER','WORDS']],E1[['OPERATION_NUMBER','WORDS']]],ignore_index=True)



BC=B[0].merge(C[0],how='outer')

BC['RESULT_OBJETIVO']=np.where((BC['RESULT_OBJECTIVE_ES']=='DIGITAL') |(BC['RESULT_OBJECTIVE_EN']=='DIGITAL') ,'DIGITAL','NO DIGITAL')

a=BC[['RESULT_OBJECTIVE_ES_TECN-INNOV','RESULT_OBJECTIVE_EN_TECN-INNOV']].apply(np.nanmax,axis=1)
BC['RESULT_OBJECTIVE_TECN-INNOV']=a

###################### Base Unificada#############################
#Consolidado.drop(columns=['RESULT_OBJECTIVE_EN','RESULT_OBJECTIVE_EN_2','RESULT_OBJECTIVE_ES_TECN-INNOV','RESULT_OBJECTIVE_EN_TECN-INNOV'],inplace=True)
#Consolidado.rename(columns={'RESULT_OBJECTIVE_ES_2':'NOUX_RESULT_OBJECTIVE','RESULT_OPERATION_NAME_2':'NOUX_RESULT_OPERATION_NAME','RESULT_COMPONENT_NAME_2':'NOUX_COMPONENT_NAME','RESULT_OUTPUT_NAME_2':'NOUX_RESULT_OUTPUT_NAME'},inplace=True)
#Consolidado=Consolidado[['OPERATION_NUMBER','OPERATION_NAME','NOUX_RESULT_OPERATION_NAME','RESULT_'+'OPERATION_NAME_'+'TECN-INNOV',
#                         'OBJECTIVE_ES','OBJECTIVE_EN','NOUX_RESULT_OBJECTIVE','RESULT_OBJECTIVE_TECN-INNOV',
#                         'COMPONENT_NAME','NOUX_COMPONENT_NAME','RESULT_'+'COMPONENT_NAME_'+'TECN-INNOV',
#                         'OUTPUT_NAME','NOUX_RESULT_OUTPUT_NAME','RESULT_'+'OUTPUT_NAME_'+'TECN-INNOV']]
#
#Consolidado.drop_duplicates(inplace=True)
#
#

################ DEFINIR DIGITAL/NO DIGITAL POR OPERACION ##################################
y=os.listdir(path+"AREAS TRANSVERSALES/Digital")
if "Resultados" not in y:
    os.mkdir(path+"AREAS TRANSVERSALES/Digital/Resultados")


Titulo=A[0][['OPERATION_NUMBER','OPERATION_NAME','RESULT_OPERATION_NAME','RESULT_'+'OPERATION_NAME_'+'TECN-INNOV']]
Objetivo=BC[['OPERATION_NUMBER','OBJECTIVE_ES','OBJECTIVE_EN','RESULT_OBJETIVO','RESULT_OBJECTIVE_TECN-INNOV']]

Componentes1=D[0][['OPERATION_NUMBER','COMPONENT_NAME','RESULT_COMPONENT_NAME']]
Producto1=E[0][['OPERATION_NUMBER','COMPONENT_NAME','OUTPUT_NAME','RESULT_OUTPUT_NAME']]



Componentes=D[0][['OPERATION_NUMBER','COMPONENT_NAME','RESULT_COMPONENT_NAME']]
Componentes=Componentes.groupby(['OPERATION_NUMBER','RESULT_COMPONENT_NAME'])['COMPONENT_NAME'].count().unstack()
Componentes.fillna(0,inplace=True)
Componentes['DIGITAL_COMP']=Componentes['DIGITAL']/(Componentes['DIGITAL']+Componentes['NO DIGITAL'])
Componentes.drop(columns=['DIGITAL','NO DIGITAL'],inplace=True)
Componentes.reset_index(inplace=True)

Producto=E[0][['OPERATION_NUMBER','OUTPUT_NAME','RESULT_OUTPUT_NAME']]
Producto=Producto.groupby(['OPERATION_NUMBER','RESULT_OUTPUT_NAME'])['OUTPUT_NAME'].count().unstack()
Producto.fillna(0,inplace=True)
Producto['DIGITAL_OUT']=Producto['DIGITAL']/(Producto['DIGITAL']+Producto['NO DIGITAL'])
Producto.drop(columns=['DIGITAL','NO DIGITAL'],inplace=True)
Producto.reset_index(inplace=True)

Final=Titulo[["OPERATION_NUMBER","RESULT_OPERATION_NAME",'RESULT_'+'OPERATION_NAME_'+'TECN-INNOV']].merge(Objetivo[['OPERATION_NUMBER','RESULT_OBJETIVO','RESULT_OBJECTIVE_TECN-INNOV']].merge(Componentes.merge(Producto,how='outer'),how='outer'),how='outer')
Final.fillna(0,inplace=True)




Final['DUMMY_DIGITAL']=np.where((Final['RESULT_OPERATION_NAME']=='DIGITAL') | (Final['RESULT_OBJETIVO']=='DIGITAL'),1,np.where(((Final['RESULT_OPERATION_NAME']=='DIGITAL') | (Final['RESULT_OBJETIVO']=='DIGITAL')) &((Final['DIGITAL_COMP']>0.32) | (Final['DIGITAL_OUT']>0.32)),1,0))
Final['DUMMY_INNOVACION']=Final[['RESULT_'+'OPERATION_NAME_'+'TECN-INNOV','RESULT_OBJECTIVE_TECN-INNOV']].apply(np.nanmax,axis=1)
Final['DUMMY_INN_DIGITAL']=np.where((Final['DUMMY_DIGITAL']==1) & (Final['DUMMY_INNOVACION']==1),1,0)
Final=Final[['OPERATION_NUMBER','DUMMY_DIGITAL','DUMMY_INNOVACION','DUMMY_INN_DIGITAL']]

Bas=Metadatos[['OPERATION_NUMBER','RELATED_OPER','RELATION_TYPE','EXEC_STS','OPERATION_TYPE','OPERATION_TYPE_NAME',
           'OPERATION_MODALITY','TAXONOMY','STATUS','REGION','COUNTRY','DEPARTMENT','DIVISION','APPROVAL_DATE',
           'APPROVAL_AMOUNT','CURRENT_EXPIRATION_DATE']]

Bas.drop_duplicates(inplace=True)

P=Bas[['OPERATION_NUMBER','APPROVAL_DATE']].groupby(['OPERATION_NUMBER']).max()
P.reset_index(inplace=True)
Bas=Bas.merge(P,how='outer',on='OPERATION_NUMBER')
Bas.drop(columns=['APPROVAL_DATE_x'],inplace=True)
Bas.drop_duplicates(inplace=True)
Bas.rename(columns={'APPROVAL_DATE_y':'APPROVAL_DATE'},inplace=True)
Bas=Bas.merge(Final,how='outer')
Bas['APPROVAL_DATE']=Bas['APPROVAL_DATE'].apply(todate)



#######################################################################################################
#NUBE DE PALABRAS

Palabras=pd.concat([A1,B1,C1,D1,E1],axis=0,ignore_index=True)
Dicc=pd.concat([Diccionario_Total[['PALABRAS','IDIOMA','TIPO']],diccionario_bigrama[['PALABRAS','IDIOMA','TIPO']]],ignore_index=True)
Palabras=Palabras.merge(Dicc,right_on='PALABRAS',left_on='WORDS',how='left')

Palabras=Palabras[(Palabras['IDIOMA']=='en')&(Palabras['TIPO']=='POSITIVO')][['OPERATION_NUMBER','WORDS']]

Palabras["WORDS2"]=Palabras["WORDS"].apply(singular)
Palabras=Palabras[["OPERATION_NUMBER","WORDS2"]]
Palabras.rename(columns={'WORDS2':'WORDS'},inplace=True)


Palabras=DataFrame(Palabras["WORDS"].groupby([Palabras['OPERATION_NUMBER'],Palabras['WORDS']]).count())
Palabras.rename(columns={'WORDS':'COUNT_WORDS'},inplace=True)
Palabras.reset_index(inplace=True)

########EXPORTAR ARCHIVOS#############
with pd.ExcelWriter(path+"/output/output.xlsx") as writer:
    Titulo.to_excel(writer,sheet_name="Operation_Name",index=False)
    Objetivo.to_excel(writer,sheet_name="Objetivo",index=False)
    Componentes1.to_excel(writer,sheet_name="Component",index=False)
    Producto1.to_excel(writer,sheet_name="Output_Name",index=False)
    Bas.to_excel(writer,sheet_name="Metadata",index=False)
    Palabras.to_excel(writer,sheet_name="Hoja1",index=False)


