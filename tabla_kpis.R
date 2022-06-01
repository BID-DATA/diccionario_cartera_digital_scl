###### Código apra crear tabla de KPIs 
# Autor: María Reyes Retana
# Fecha: 23 de marzo 2022

##### librerias #####

library(tidyverse)
library(readxl)
library(openxlsx)
library(stringi)
library(rlist)
options(scipen = 999)

##### Leer outputs #####

# lee outputs directamente del output del código en python

output_portfolio <- readxl::read_xlsx("C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/cartera digital/Dashboard/output.xlsx", sheet = "Metadata")

oper_dig_port <- output_portfolio %>% 
  select(OPERATION_NUMBER, DUMMY_DIGITAL)

output_aux <- output_portfolio %>% 
  select(OPERATION_NUMBER, APPROVAL_DATE)

output_pipe <- readxl::read_xlsx("C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/cartera digital/Dashboard/output-pipe.xlsx", sheet = "Metadata") %>% 
  mutate(YEAR = format(APPROVAL_DATE, format = "%Y"), 
YEAR = if_else(is.na(APPROVAL_DATE), str_sub(PIPE_YR,1L,4L), YEAR))

oper_dig <- output_pipe %>% 
  mutate(DUMMY_DIGITAL = DIGITAL) %>% 
  select(OPERATION_NUMBER, DUMMY_DIGITAL) %>% 
  rbind(oper_dig_port)

output_name <- readxl::read_xlsx("C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/cartera digital/Dashboard/output.xlsx", sheet = "Output_Name") %>% 
  left_join(output_aux) %>% 
  mutate(YEAR = format(APPROVAL_DATE, format = "%Y")) %>% 
  mutate(ISO = str_sub(OPERATION_NUMBER, 1L, 2L)) %>% 
  left_join(oper_dig)

source("Funcion palabras.R")

# kpi_division <- readxl::read_xlsx("Inputs/KPI_division.xlsx") %>% 
#   mutate(YEAR_KPI = as.character(YEAR)) %>% 
#   select(-YEAR)
# 
# kpi_2022 <- kpi_division %>% 
#   filter(YEAR_KPI == "2022")

##### Crear tabla para KPIs ####

output_portfolio_tidy <- output_portfolio %>%
  mutate(INFO = if_else(INFO == "Si", 1, if_else(INFO == "No", 0, NA_real_)), 
         INFRA = if_else(INFRA == "Si", 1, if_else(INFRA == "No", 0, NA_real_)),
         GOB = if_else(GOB == "Si", 1, if_else(GOB == "No", 0, NA_real_)),
         CULTURA = if_else(CULTURA == "Si", 1, if_else(CULTURA == "No", 0, NA_real_)),
         TYPE = "Portfolio",
         YEAR = format(APPROVAL_DATE, format = "%Y")) %>% 
  group_by(OPERATION_TYPE, DUMMY_DIGITAL, COUNTRY, DIVISION, TYPE, YEAR) %>% 
  summarise(DIGITAL = n(), 
            DIGITAL_COST = sum(OUTPUT_COST, na.rm = TRUE), 
            TOTAL_COST = sum(as.numeric(TOTAL_COST), na.rm = TRUE),
            TD = sum(DIGITAL_TRANS, na.rm = TRUE), 
            INFO = sum(INFO, na.rm = TRUE),
            INFRA = sum(INFRA, na.rm = TRUE), 
            GOB = sum(GOB, na.rm = TRUE), 
            CULTURA = sum(CULTURA, na.rm = TRUE)) %>% 
  filter(DUMMY_DIGITAL == 1) %>% 
  mutate(type = "Portfolio")

output_pipe_tidy <- output_pipe %>% 
  mutate(TYPE = "Pipeline") %>% 
  mutate(DUMMY_DIGITAL = case_when(DIGITAL == "Digital" ~ 1, 
                                   DIGITAL == "Pending Checklist" ~ NA_real_, 
                                   TRUE ~ 0)) %>% 
  group_by(OPERATION_TYPE, DUMMY_DIGITAL, COUNTRY, DIVISION, TYPE, YEAR) %>% 
  mutate(DIGITAL_TRANS = case_when(DIGITAL_TRANS == "Digital" ~ 0, 
                                   DIGITAL_TRANS == "Digital Transformation" ~ 1, 
                                   DIGITAL_TRANS == "Pending Checklist" ~ NA_real_, 
                                   TRUE ~ 0)) %>% 
  mutate(INFO = if_else(INFO == "Si", 1, if_else(INFO == "No", 0, NA_real_)), 
         INFRA = if_else(INFRA == "Si", 1, if_else(INFRA == "No", 0, NA_real_)),
         GOB = if_else(GOB == "Si", 1, if_else(GOB == "No", 0, NA_real_)),
         CULTURA = if_else(CULTURA == "Si", 1, if_else(CULTURA == "No", 0, NA_real_))) %>% 
  summarise(DIGITAL = n(), 
            TD = sum(DIGITAL_TRANS, na.rm = TRUE), 
            INFO = sum(INFO), 
            INFRA = sum(INFRA), 
            GOB = sum(GOB), 
            CULTURA = sum(CULTURA), 
            OPERACIONES = n()) %>% 
  filter(DUMMY_DIGITAL == 1) %>% 
  mutate(DIGITAL_COST = NA, 
         TOTAL_COST = NA) %>% 
  mutate(type = "Pipeline")

output_total_kpi <- output_portfolio_tidy %>% 
  rbind(output_pipe_tidy) %>% 
  select(TYPE, YEAR, OPERATION_TYPE, DIVISION, DIGITAL, TD, DIGITAL_COST, TOTAL_COST) %>% 
  ungroup() 

output_pipe_non <- output_pipe %>% 
  mutate(TYPE = "Pipeline") %>% 
  group_by(DIVISION, YEAR, TYPE) %>% 
  summarise(OPERACIONES = n())

output_total_non <- output_portfolio %>% 
  mutate(YEAR = format(APPROVAL_DATE, format = "%Y"),
         TYPE = "Portfolio") %>% 
  group_by(DIVISION, YEAR, TYPE) %>% 
  summarise(OPERACIONES = n()) %>% 
  rbind(output_pipe_non)

output_pipe_non_sin <- output_pipe %>% 
  mutate(TYPE = "Pipeline") %>% 
  group_by(YEAR, TYPE) %>% 
  summarise(OPERACIONES = n())

output_total_non_sin <- output_portfolio %>% 
  mutate(YEAR = format(APPROVAL_DATE, format = "%Y"),
         TYPE = "Portfolio") %>% 
  group_by(YEAR, TYPE) %>% 
  summarise(OPERACIONES = n()) %>% 
  rbind(output_pipe_non_sin)

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
  mutate(DIGITAL_PER = DIGITAL/OPERACIONES, 
         DIGITAL_COST_PER = DIGITAL_COST/TOTAL_COST) %>% 
  # select(YEAR, TYPE, DIVISION,DIGITAL, DIGITAL_TARGET, OPERACIONES, DIGITAL_PER,
  #        DIGITAL_TARGET_PER, TD, TD_TARGET, COUNTRIES, COUNTRIES_TARGET, DIGITAL_COST,TOTAL_COST, DIGITAL_COST_PER, DIGITAL_COST_TARGET, 
  #        DIGITAL_TOOLS_TARGET, YEAR_KPI) %>% 
  filter(YEAR>2020) %>% 
  pivot_longer(cols = DIGITAL:DIGITAL_COST_PER, names_to = "KEY", values_to = "Current") %>% 
  mutate(DIVISION = "SCL")

output_sumado_kpi <- output_total_kpi %>% 
  ungroup() %>% 
  group_by(DIVISION, YEAR, TYPE) %>% # aquí se agrega operation_type si se quiere ver por tipo de operacion (lon y tc) 
  summarise(DIGITAL = sum(DIGITAL), 
         TD = sum(TD), 
         COUNTRIES = n_distinct(COUNTRY),
         DIGITAL_COST = sum(DIGITAL_COST), 
         TOTAL_COST = sum(TOTAL_COST)) %>% 
  ungroup() %>% 
  add_row(DIVISION = "GDI", YEAR = "2022", TYPE = "Portfolio", DIGITAL = 0, TD = 0, COUNTRIES = 0, DIGITAL_COST=0, TOTAL_COST = NA) %>% 
  add_row(DIVISION = "MIG", YEAR = "2022", TYPE = "Portfolio", DIGITAL = 0, TD = 0, COUNTRIES = 0, DIGITAL_COST=0, TOTAL_COST = NA) %>% 
 # left_join(kpi_2022, by = c("DIVISION", "TYPE")) %>% 
 # filter(TYPE == "Portfolio") %>% 
  full_join(output_total_non) %>% 
  mutate(TD = replace_na(TD,0),
         COUNTRIES = replace_na(COUNTRIES, 0),
         DIGITAL = replace_na(DIGITAL, 0)) %>% 
  mutate(DIGITAL_PER = DIGITAL/OPERACIONES, 
         DIGITAL_COST_PER = DIGITAL_COST/TOTAL_COST) %>% 
  # select(YEAR, TYPE, DIVISION,DIGITAL, DIGITAL_TARGET, OPERACIONES, DIGITAL_PER,
  #        DIGITAL_TARGET_PER, TD, TD_TARGET, COUNTRIES, COUNTRIES_TARGET, DIGITAL_COST,TOTAL_COST, DIGITAL_COST_PER, DIGITAL_COST_TARGET, 
  #        DIGITAL_TOOLS_TARGET, YEAR_KPI) %>% 
  filter(YEAR>2020) %>% 
  arrange(DIVISION, YEAR) %>% 
  filter(DIVISION != "SCL") %>% 
  pivot_longer(cols = DIGITAL:DIGITAL_COST_PER, names_to = "KEY", values_to = "Current") %>% 
  group_by(YEAR, TYPE, KEY) %>% 
  mutate(TYPE = ifelse(DIVISION== "SPH" & TYPE == "Pipeline", "Pipeline - Health", 
                       ifelse(DIVISION== "SPH" & TYPE == "Portfolio", "Portfolio - Health", TYPE))) %>% 
  rbind(output_scl) 

KPI <- read_xlsx("C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/Documents/GitHub/diccionario_cartera_digital_scl/Inputs/KPI_division.xlsx", sheet = "KPI") %>% 
  left_join(output_sumado_kpi) %>% 
  mutate(YEAR = ifelse(is.na(YEAR), 2022, as.numeric(YEAR)),
         Current = ifelse(is.na(Current), 0, Current), 
         Target_2022 = as.numeric(Target_2022), 
         `Baseline 2021` = as.numeric(`Baseline 2021`), 
         Target_2023 = as.numeric(Target_2023), 
         Target_2022 = ifelse(is.na(Target_2022), 0, Target_2022), 
         Target_2023 = ifelse(is.na(Target_2023), 0, Target_2023),
         `Baseline 2021` = ifelse(is.na(`Baseline 2021`), 0, `Baseline 2021`)) %>% 
  mutate(remaining_2022 = ifelse(Current>0 & Target_2022>Current, Target_2022-Current,
                                 ifelse(Current ==0, Target_2022, 
                                        ifelse(Current>=Target_2022, 0, NA)))) %>% 
  mutate(current_aux = ifelse(Current>0 & Target_2022>Current, Current,
                              ifelse(Current ==0, Target_2022, 
                                     ifelse(Current>=Target_2022, 0, NA))))
           # mutate(remaining_2022= case_when(Current>0 & Target_2022>Current ~ Target_2022 - Current, 
           #                          Current == 0 ~ Target_2022, 
           #                          Current>=Target_2022, 0, 
           #                          TRUE ~NA_real_))

##### Crear indicadores con lectura de texto ####

# output <- output_name %>% 
#   select(OPERATION_NUMBER, COMPONENT_NAME, OUTPUT_NAME, RESULT_OUTPUT_NAME, DIGITAL_OUT, OUTPUT_UOM, 
#          DIVISION, OUTPUT_FI, APPROVAL_DATE, YEAR, ISO, DUMMY_DIGITAL) %>% 
#   distinct() %>% 
#   rowwise() %>% 
#   mutate(OUTPUT = nearest_result(OUTPUT_NAME, division = DIVISION))

output_n <- output_name %>% 
  select(OPERATION_NUMBER, COMPONENT_NAME, OUTPUT_NAME, RESULT_OUTPUT_NAME, DIGITAL_OUT, OUTPUT_UOM, 
         DIVISION, OUTPUT_FI, APPROVAL_DATE, YEAR, ISO, DUMMY_DIGITAL) %>%
  distinct() %>% 
  rowwise() %>% 
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
  
revision_out <- output_n %>% 
  mutate(OUTPUT1 = OUTPUT[1], OUTPUT2 = OUTPUT[2]) %>% 
  filter(!is.na(OUTPUT1)) %>% 
  select(-c(OUTPUT)) %>% 
  mutate(DUMMY_DOB = ifelse(DUMMY_DIGITAL ==1 | RESULT_OUTPUT_NAME == "DIGITAL", 1, 0)) %>% 
  filter(DUMMY_DOB == 1) %>% 
  arrange(DIVISION, YEAR) %>% 
  mutate(OUTPUT_FI = replace(OUTPUT_FI, is.na(OUTPUT_FI), 0)) %>% 
  filter(!str_detect(OUTPUT_UOM, paste(c("Hogares", "Personas", "beneficiaries", "students", "personas"),collapse = '|'))) %>% 
  mutate(OUTPUT_FI = as.numeric(OUTPUT_FI))
  #Divisiones

divisiones <- c('SPH', 'LMK', 'GDI', 'MIG', 'EDU')

divisiones_indicators <- list()

for (i in divisiones) {
 
divisiones_indicators[[i]]  <- revision_out %>% 
  filter(DIVISION == i) %>% 
  group_by(OUTPUT1, DIVISION, YEAR, DUMMY_DIGITAL) %>% 
  summarise(NUMBER_SIMPLE = n(),
            COUNTRIES = n_distinct(ISO), 
            NUMBER = sum(as.numeric(OUTPUT_FI, na.rm = TRUE))) %>% 
  mutate(INDICATOR = ifelse(str_detect(OUTPUT1, "Countries"), COUNTRIES, NUMBER)) %>% 
  select(OUTPUT1, DIVISION, YEAR, INDICATOR) %>% 
  rbind(indicator_countries %>% filter(DIVISION == i))
  
}

divisiones_indicators[['SCL']] <- revision_out %>% 
  filter(OUTPUT1 == "Knowledge products related to digital transformation" |
        OUTPUT1== "New virtual platforms available") %>% 
  group_by(OUTPUT1, YEAR, DUMMY_DIGITAL) %>% 
  summarise(NUMBER_SIMPLE = n(),
            COUNTRIES = n_distinct(ISO), 
            NUMBER = sum(as.numeric(OUTPUT_FI, na.rm = TRUE))) %>% 
  mutate(INDICATOR = ifelse(str_detect(OUTPUT1, "Countries"), COUNTRIES, NUMBER)) %>% 
  select(OUTPUT1, YEAR, INDICATOR) %>% 
  mutate(DIVISION = "SCL") %>% 
  rbind(indicator_countries_scl)

Productos <- do.call("rbind", list(divisiones_indicators$SPH, divisiones_indicators$LMK, 
                                     divisiones_indicators$GDI, divisiones_indicators$MIG, 
                                     divisiones_indicators$EDU, divisiones_indicators$SCL)) %>% 
  mutate(INDICATOR = as.numeric(INDICATOR))

###### Salvar excel #####

# outputs <- list(output_total_kpi = output_total_kpi, output_sumado_kpi = output_sumado_kpi)
# 
# write.xlsx(outputs, "C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/cartera digital/Dashboard/KPI.xlsx",overwrite = FALSE)

 wb = loadWorkbook("C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/cartera digital/Dashboard/KPI.xlsx")
 
 writeData(wb, "KPI", KPI)

 writeData(wb, "Productos", Productos)
 
 writeData(wb, "Productos_detail", revision_out)
 
 saveWorkbook(wb, "C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/cartera digital/Dashboard/KPI.xlsx", overwrite = TRUE)
 
 