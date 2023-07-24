###### Código apra crear tabla de KPIs 
# Autor: María Reyes Retana
# Fecha: 23 de marzo 2022

##### librerias #####

library(tidyverse)
library(readxl)
library(openxlsx)
library(stringi)
library(rlist)
library(here)
options(scipen = 999)

###### Fix para SPH por tipo #####

# leer tipo de operaciones para sph - cartera

# vamos a poner el working directory en la carpeta de general 
# cambia el wd donde tienes la carpeta de General de social digital en tu computadora
setwd("C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/")

tipo_sph <- readxl::read_xlsx("documents/Inputs/DIV - revision operaciones.xlsx", 
                              sheet = "ejecucion-sph") %>% 
  select(OPERATION_NUMBER, DIVISION, TYPE_SPH)

# lo mismo para pipeline
tipo_sph_pipe <- readxl::read_xlsx("documents/Inputs/DIV - revision operaciones.xlsx",
                                   sheet = "pipeline-sph") %>% 
  select(OPERATION_NUMBER, DIVISION, TYPE_SPH)

#total
tipo_sph_tot <- tipo_sph %>% 
  rbind(tipo_sph_pipe) %>% 
  distinct()

##### Leer outputs #####

# lee outputs directamente de los archivos que genera del código en python

# output cartera
output_portfolio <- readxl::read_xlsx("cartera digital/Dashboard/output.xlsx", 
                                           sheet = "Metadata")%>% 
  left_join(tipo_sph_tot) %>% 
  mutate(DIVISION = case_when(DIVISION == "SPH" & !is.na(TYPE_SPH) ~ TYPE_SPH, 
                              TRUE ~ DIVISION)) # para unir type sph
# seleccionar operaciones digitales
oper_dig_port <- output_portfolio %>% 
  select(OPERATION_NUMBER, DUMMY_DIGITAL)

output_aux <- output_portfolio %>% 
  select(OPERATION_NUMBER, APPROVAL_DATE)

# output pipeline
output_pipe <- readxl::read_xlsx("cartera digital/Dashboard/output-pipe.xlsx", 
                                      sheet = "Metadata") %>% 
  mutate(YEAR = format(APPROVAL_DATE, format = "%Y"), 
         YEAR = if_else(is.na(APPROVAL_DATE), as.numeric(str_sub(PIPE_YR,1L,4L)), as.numeric(YEAR)))

# output pipeline digital
oper_dig <- output_pipe %>% 
  mutate(DUMMY_DIGITAL = DIGITAL) %>% 
  select(OPERATION_NUMBER, DUMMY_DIGITAL) %>% 
  rbind(oper_dig_port)

# total outputs
output_name <- readxl::read_xlsx("cartera digital/Dashboard/output.xlsx", 
                                      sheet = "Output_Name") %>% 
  left_join(output_aux) %>% 
  mutate(YEAR = as.numeric(format(APPROVAL_DATE, format = "%Y"))) %>% 
  mutate(ISO = str_sub(OPERATION_NUMBER, 1L, 2L)) %>% 
  left_join(oper_dig)

# vamos a cambiar el WD a la carpeta de github
# leer funciones para hacer math de palabras

setwd("C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/Documents/GitHub/diccionario_cartera_digital_scl/")
source("Funcion palabras.R")

##### Crear tabla para KPIs ####

# Generar tabla de output cartera
output_portfolio_tidy <- output_portfolio %>% # Aquí es donde debemos cambiar las divisiones para SPH
  # Cambiar 0,1 en lugar de no, sí
  mutate(INFO = if_else(INFO == "Si", 1, if_else(INFO == "No", 0, NA_real_)), 
         INFRA = if_else(INFRA == "Si", 1, if_else(INFRA == "No", 0, NA_real_)),
         GOB = if_else(GOB == "Si", 1, if_else(GOB == "No", 0, NA_real_)),
         CULTURA = if_else(CULTURA == "Si", 1, if_else(CULTURA == "No", 0, NA_real_)),
         TYPE = "Portfolio", 
         YEAR = as.numeric(format(APPROVAL_DATE, format = "%Y"))) %>% 
  # crear resumen por división
  group_by(OPERATION_TYPE, DUMMY_DIGITAL, COUNTRY, DIVISION, TYPE, YEAR) %>% 
  summarise(DIGITAL = n(), 
            DIGITAL_COST = sum(OUTPUT_COST, na.rm = TRUE), 
            TOTAL_COST = sum(as.numeric(TOTAL_COST), na.rm = TRUE),
            TD = sum(DIGITAL_TRANS, na.rm = TRUE), 
            INFO = sum(INFO, na.rm = TRUE),
            INFRA = sum(INFRA, na.rm = TRUE), 
            GOB = sum(GOB, na.rm = TRUE), 
            CULTURA = sum(CULTURA, na.rm = TRUE)) %>% 
  # filtrar digitales
  filter(DUMMY_DIGITAL == 1)

# Generar base de datos de pipeline
output_pipe_tidy <- output_pipe %>% 
  mutate(TYPE = "Pipeline") %>% 
  #transformar variables
  mutate(DUMMY_DIGITAL = case_when(DIGITAL == "Digital" ~ 1, 
                                   DIGITAL == "Pending Checklist" ~ NA_real_, 
                                   TRUE ~ 0)) %>% 
  # crear resumen por división
  group_by(OPERATION_TYPE, DUMMY_DIGITAL, COUNTRY, DIVISION, TYPE, YEAR) %>% 
  mutate(DIGITAL_TRANS = case_when(DIGITAL_TRANS == "Digital" ~ 0, 
                                   DIGITAL_TRANS == "Digital Transformation" ~ 1, 
                                   DIGITAL_TRANS == "Pending Checklist" ~ NA_real_, 
                                   TRUE ~ 0)) %>% 
  mutate(INFO = if_else(INFO == "Si", 1, if_else(INFO == "No", 0, NA_real_)), 
         INFRA = if_else(INFRA == "Si", 1, if_else(INFRA == "No", 0, NA_real_)),
         GOB = if_else(GOB == "Si", 1, if_else(GOB == "No", 0, NA_real_)),
         CULTURA = if_else(CULTURA == "Si", 1, if_else(CULTURA == "No", 0, NA_real_))) %>% 
  # Resumen variables generadas
  summarise(DIGITAL = n(), 
            TD = sum(DIGITAL_TRANS, na.rm = TRUE), 
            INFO = sum(INFO), 
            INFRA = sum(INFRA), 
            GOB = sum(GOB), 
            CULTURA = sum(CULTURA), 
            OPERACIONES = n()) %>% 
  filter(DUMMY_DIGITAL == 1) %>% 
  mutate(DIGITAL_COST = NA, 
         TOTAL_COST = NA)

# Generar output total tomando portfolio y pipelina
output_total_kpi <- output_portfolio_tidy %>% 
# pegar pipeline y seleccionar variables necesarias
  rbind(output_pipe_tidy) %>% 
  select(TYPE, YEAR, OPERATION_TYPE, DIVISION, DIGITAL, TD, DIGITAL_COST, TOTAL_COST) %>% 
  ungroup() 

# Resumen de todo (digital+no digital) pipe por división
output_pipe_non <- output_pipe %>% 
  mutate(TYPE = "Pipeline") %>% 
  group_by(DIVISION, YEAR, TYPE) %>% 
  summarise(OPERACIONES = n()) 

# Resumen de todo (digital+no digital) portfolio por división
output_total_non <- output_portfolio %>% 
  mutate(YEAR = as.numeric(format(APPROVAL_DATE, format = "%Y")),
         TYPE = "Portfolio") %>% 
  # Para edu quitamos TC, porque se quiere saber número de lons no TC 
  filter(!(DIVISION == "EDU" & OPERATION_TYPE == "TCP")) %>% 
  group_by(DIVISION, YEAR, TYPE) %>% 
  summarise(OPERACIONES = n()) %>% 
  rbind(output_pipe_non)

# Resumen de todo (digital+no digital) pipe en todo SCL 
output_pipe_non_sin <- output_pipe %>% 
  mutate(TYPE = "Pipeline") %>% 
  # no agrupación por división
  group_by(YEAR, TYPE) %>% 
  summarise(OPERACIONES = n())

# Resumen de todo (digital+no digital) portfolio en el sector
output_total_non_sin <- output_portfolio %>% 
  mutate(YEAR = as.numeric(format(APPROVAL_DATE, format = "%Y")),
         TYPE = "Portfolio") %>% 
  group_by(YEAR, TYPE) %>% 
  summarise(OPERACIONES = n()) %>% 
  rbind(output_pipe_non_sin)

# Generar cálculo de indicadores para todo SCL
output_scl <- output_total_kpi %>% 
  ungroup() %>% 
  group_by(YEAR, TYPE) %>% # aquí se agrega operation_type si se quiere ver por tipo de operacion (lon y tc) 
  summarise(DIGITAL = sum(DIGITAL), 
            TD = sum(TD), 
            COUNTRIES = n_distinct(COUNTRY),
            DIGITAL_COST = sum(DIGITAL_COST), 
            TOTAL_COST = sum(TOTAL_COST)) %>%
  full_join(output_total_non_sin) %>% 
  mutate(TD = replace_na(TD,0),
         COUNTRIES = replace_na(COUNTRIES, 0),
         DIGITAL = replace_na(DIGITAL, 0)) %>% 
  mutate(DIGITAL_PER = (DIGITAL/OPERACIONES)*100, 
         DIGITAL_COST_PER = (DIGITAL_COST/TOTAL_COST)*100) %>% 
  filter(YEAR>2020) %>% 
  pivot_longer(cols = DIGITAL:DIGITAL_COST_PER, names_to = "KEY", values_to = "Current") %>% 
  mutate(DIVISION = "SCL")

# Cálculo de indicadores por división
output_sumado_kpi <- output_total_kpi %>% 
  ungroup() %>% 
  group_by(DIVISION, YEAR, TYPE) %>% # aquí se agrega operation_type si se quiere ver por tipo de operacion (lon y tc) 
# Por division, año y tipo se suman indicadores digital, Transformación digital, número de países, costo digital y total  
  summarise(DIGITAL = sum(DIGITAL), 
         TD = sum(TD), 
         COUNTRIES = n_distinct(COUNTRY),
         DIGITAL_COST = sum(DIGITAL_COST), 
         TOTAL_COST = sum(TOTAL_COST)) %>% 
  ungroup() %>% 
  complete(DIVISION, YEAR, TYPE) %>% 
  filter(!(TYPE == "Pipeline" & YEAR<2022)) %>% 
  # unir todos (calculo porcentaje)
  full_join(output_total_non) %>% 
  mutate(TD = replace_na(TD,0),
         COUNTRIES = replace_na(COUNTRIES, 0),
         DIGITAL = replace_na(DIGITAL, 0)) %>% 
  mutate(DIGITAL_PER = (DIGITAL/OPERACIONES)*100, 
         DIGITAL_COST_PER = (DIGITAL_COST/TOTAL_COST)*100) %>% 
  filter(YEAR>2020) %>% 
  arrange(DIVISION, YEAR) %>% 
  filter(DIVISION != "SCL") %>% 
  pivot_longer(cols = DIGITAL:DIGITAL_COST_PER, names_to = "KEY", values_to = "Current") %>% 
  group_by(YEAR, TYPE, KEY) %>% 
  mutate(TYPE = case_when(DIVISION == "SPH - Health" & TYPE == "Portfolio" ~ "Portfolio - Health",
                          DIVISION == "SPH - SP" & TYPE == "Portfolio" ~ "Portfolio - Social Protection",
                          TRUE ~ TYPE)) %>% 
  # pegar output de SCL
  rbind(output_scl) 

# quitar NA y reemplazar por ceros
output_sumado_kpi[is.na(output_sumado_kpi)] <- 0

# Este archivo se lee desde la carpeta de inputs del repositorio, 
# por lo que debemos asegurarnos que esté actualizado 
# (con el de la carpeta de segumiento de divisiones)
# Adicionalmente se debe hacer "corte"al final del año entonces se agregara KPI_año 
# y el rbind correspondiente abajo del año anterior para tener el KPI total

 KPI_2022 <- read_xlsx("Inputs/KPI_division.xlsx", 
                      sheet = "KPI_2022") 

         
  KPI <- read_xlsx("Inputs/KPI_division.xlsx", 
                   sheet = "KPI_2023") %>% 
    rbind(KPI_2022) %>% 
    left_join(output_sumado_kpi) %>% 
    mutate(YEAR = case_when( (is.na(YEAR) & Year == 2023) ~ 2023, 
                             (is.na(YEAR) & Year == 2022) ~ 2022,
                            TRUE ~ as.numeric(YEAR))) %>% 
    group_by(Year) %>% 
    mutate(
         # Usar la columna del último update 
         Current = case_when(!is.na(UpdateQ4) ~ as.numeric(UpdateQ4),
                             !is.na(UpdateQ3) ~ as.numeric(UpdateQ3), 
                             !is.na(UpdateQ2) ~ as.numeric(UpdateQ2),
                             !is.na(UpdateQ1) ~ as.numeric(UpdateQ1), 
                             TRUE ~ Current),
         # En caso de que no exista un update y no se lean automáticamente poner 0
         Current = ifelse(is.na(Current), 0, Current), 
         Target = as.numeric(Target), 
         `Baseline 2021` = as.numeric(`Baseline 2021`), 
         Target = as.numeric(Target), 
         Target = ifelse(is.na(Target), 0, Target), 
         `Baseline 2021` = ifelse(is.na(`Baseline 2021`), 0, `Baseline 2021`)) %>% 
  # compute remaining targets for 2022 and 2023
  mutate(remaining_2022 = case_when(Current>0 & Target>Current ~ Target-Current,
                                    Current ==0 ~ Target, 
                                    Current>=Target ~ 0,
                                    TRUE ~NA_real_)) %>% 
  # create current aux for dashboard
  mutate(current_aux = case_when(Current>0 & Target>Current ~ Current,
                                 Current ==0 ~ Target, 
                                 Current>=Target ~ 0,
                                 TRUE ~NA_real_)) %>% 
  mutate(DIVISION = str_sub(DIVISION, 1L,3L)) %>% 
  # remove pipeline for year<this year. No tiene sentido mostrar pipeline de años anteriores por ahora
  # porque se muestra el pipeline actual 
  filter(!(TYPE == "Pipeline" & YEAR<2023),
         !(Year == 2022 & YEAR ==2023),
         !(Year == 2023 & YEAR == 2022),
         !(Year == 2023 & YEAR == 2021)) %>% 
    mutate(Year = as.numeric(Year)) %>% 
    ungroup() %>% 
    select(-Year) %>% 
    distinct()

##### Crear indicadores con lectura de texto ####

output_n <- output_name %>% 
  # seleccionar variables utilizadas
  select(OPERATION_NUMBER, COMPONENT_NAME, OUTPUT_NAME, RESULT_OUTPUT_NAME, DIGITAL_OUT, OUTPUT_UOM, 
         DIVISION, OUTPUT_FI, APPROVAL_DATE, YEAR, ISO, DUMMY_DIGITAL) %>%
  distinct() %>% 
  rowwise() %>% 
  # generar base de datos con matchs usando función de léctura de texto
  # "nearest_match
  mutate(OUTPUT = list(nearest_match(OUTPUT_NAME, division = DIVISION, column = INDICATOR)))

indicator_countries <- output_total_kpi %>% 
  ungroup() %>% 
  group_by(DIVISION, YEAR, TYPE) %>% # aquí se agrega operation_type si se quiere ver por tipo de operacion (lon y tc) 
  summarise(INDICATOR = n_distinct(COUNTRY)) %>%
  mutate(OUTPUT1 = "Countries that are supported with digital transformation operation") %>% 
  filter(TYPE=="Portfolio") %>% 
  select(-c(TYPE))

indicator_countries_scl <-  output_total_kpi %>% 
  ungroup() %>% 
  group_by(YEAR, TYPE) %>% # aquí se agrega operation_type si se quiere ver por tipo de operacion (lon y tc) 
  summarise(INDICATOR = n_distinct(COUNTRY)) %>%
  mutate(OUTPUT1 = "Countries that are supported with digital transformation operation") %>% 
  filter(TYPE=="Portfolio") %>% 
  select(-c(TYPE)) %>% 
  mutate(DIVISION = "SCL")
  
revision_out <- output_n %>%  # tomando base de datos de lectura de texto
  mutate(OUTPUT1 = OUTPUT[1], OUTPUT2 = OUTPUT[2]) %>% # seleccionar primeros dos resultados
  filter(!is.na(OUTPUT1)) %>% # me quedo con los que no tienen NA en 1
  select(-c(OUTPUT)) %>% 
  mutate(DUMMY_DOB = ifelse(DUMMY_DIGITAL ==1 | RESULT_OUTPUT_NAME == "DIGITAL", 1, 0)) %>% 
  filter(DUMMY_DOB == 1) %>% 
  arrange(DIVISION, YEAR) %>% 
  mutate(OUTPUT_FI = replace(OUTPUT_FI, is.na(OUTPUT_FI), 0)) %>% 
  filter(!str_detect(OUTPUT_UOM, paste(c("Hogares", "Personas", "beneficiaries", "students", "personas"),collapse = '|'))) %>% 
  mutate(OUTPUT_FI = as.numeric(OUTPUT_FI), 
         DUMMY_DIGITAL = as.numeric(DUMMY_DIGITAL), 
         YEAR = as.numeric(YEAR))
  #Divisiones

divisiones <- c('SPH', 'LMK', 'GDI', 'MIG', 'EDU')

divisiones_indicators <- list()

for (i in divisiones) {
 
  # tomo base de datos de revision outputs con lectura de texto
divisiones_indicators[[i]]  <- revision_out %>% 
  filter(DIVISION == i) %>% # filtro división del for loop
  group_by(OUTPUT1, DIVISION, YEAR, DUMMY_DIGITAL) %>% # agrupo por división/año
  summarise(NUMBER_SIMPLE = n(), # cuento número de outputs
            COUNTRIES = n_distinct(ISO), # cuento número de países 
            NUMBER = sum(as.numeric(OUTPUT_FI, na.rm = TRUE))) %>% # sumo número
  # reportado como output físico
  mutate(INDICATOR = ifelse(str_detect(OUTPUT1, "Countries"), COUNTRIES, NUMBER)) %>% 
  select(OUTPUT1, DIVISION, YEAR, INDICATOR) %>% 
  rbind(indicator_countries %>% filter(DIVISION == i)) # pego base de indicadors
# filtrando por división
  
}

# tomo base de datos de revisión de outputs con lectura de texto
divisiones_indicators[['SCL']] <- revision_out %>% 
  # filtro outputs generales elegidos
  filter(OUTPUT1 == "Knowledge products related to digital transformation" |
        OUTPUT1== "New virtual platforms available") %>% 
  group_by(OUTPUT1, YEAR, DUMMY_DIGITAL) %>% # agrupo sin tomar en cuenta división 
  summarise(NUMBER_SIMPLE = n(), # cuento número de outputs
            COUNTRIES = n_distinct(ISO), # cuento número de países
            NUMBER = sum(as.numeric(OUTPUT_FI, na.rm = TRUE))) %>% # sumo número
  # reportado como output físico
  mutate(INDICATOR = ifelse(str_detect(OUTPUT1, "Countries"), COUNTRIES, NUMBER)) %>% 
  select(OUTPUT1, YEAR, INDICATOR) %>% 
  mutate(DIVISION = "SCL") %>% 
  rbind(indicator_countries_scl) # pego base de países de SCL

# Genero base de datos completa usando cada división + SCL
Productos <- do.call("rbind", list(divisiones_indicators$SPH, divisiones_indicators$LMK, 
                                     divisiones_indicators$GDI, divisiones_indicators$MIG, 
                                     divisiones_indicators$EDU, divisiones_indicators$SCL)) %>% 
  mutate(INDICATOR = as.numeric(INDICATOR), 
         YEAR = as.numeric(YEAR)) 

###### Salvar excel #####

# vuelvo a cambiar el directorio a la carpeta de general porque ahí es donde se guardan los documentos 
setwd("C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/")

# leo workbook de excel utilizado
 wb = loadWorkbook("cartera digital/Dashboard/KPI.xlsx")
# escribo pestaña KPI
 writeData(wb, "KPI", KPI)
# escribo pestaña Productos
 writeData(wb, "Productos", Productos)
# escribo pestaña de Productos  con detalle 
 writeData(wb, "Productos_detail", revision_out)
 # guardo el nuevo workbook con pestañas actualizadas 
 saveWorkbook(wb, "cartera digital/Dashboard/KPI.xlsx",
              overwrite = TRUE)
   
 