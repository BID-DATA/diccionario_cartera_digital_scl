###### Código apra crear tabla de KPIs 
# Autor: María Reyes Retana
# Fecha: 23 de marzo 2022

##### librerias #####

library(tidyverse)
library(readxl)
library(openxlsx)

##### Leer outputs #####

output_portfolio <- readxl::read_xlsx("Inputs/output.xlsx", sheet = "Metadata")

output_pipe <- readxl::read_xlsx("Inputs/output-pipe.xlsx", sheet = "Metadata")

kpi_division <- readxl::read_xlsx("Inputs/KPI_division.xlsx") %>% 
  mutate(YEAR_KPI = as.character(YEAR)) %>% 
  select(-YEAR)

kpi_2022 <- kpi_division %>% 
  filter(YEAR_KPI == "2022")

##### Crear tabla para KPIs ####

output_portfolio_tidy <- output_portfolio %>%
  mutate(INFO = if_else(INFO == "Si", 1, if_else(INFO == "No", 0, NA_real_)), 
         INFRA = if_else(INFRA == "Si", 1, if_else(INFRA == "No", 0, NA_real_)),
         GOB = if_else(GOB == "Si", 1, if_else(GOB == "No", 0, NA_real_)),
         CULTURA = if_else(CULTURA == "Si", 1, if_else(CULTURA == "No", 0, NA_real_)),
         TYPE = "portfolio",
         YEAR = format(APPROVAL_DATE, format = "%Y")) %>% 
  group_by(OPERATION_TYPE, DUMMY_DIGITAL, COUNTRY, DIVISION, TYPE, YEAR) %>% 
  summarise(DIGITAL = n(), 
            DIGITAL_COST = sum(OUTPUT_COST, na.rm = TRUE), 
            TD = sum(DIGITAL_TRANS, na.rm = TRUE), 
            INFO = sum(INFO, na.rm = TRUE),
            INFRA = sum(INFRA, na.rm = TRUE), 
            GOB = sum(GOB, na.rm = TRUE), 
            CULTURA = sum(CULTURA, na.rm = TRUE)) %>% 
  filter(DUMMY_DIGITAL == 1) 

output_pipe_tidy <- output_pipe %>% 
  mutate(TYPE = "pipeline") %>% 
  mutate(DUMMY_DIGITAL = case_when(DIGITAL == "Digital" ~ 1, 
                                   DIGITAL == "Pending Checklist" ~ NA_real_, 
                                   TRUE ~ 0),
         YEAR = format(APPROVAL_DATE, format = "%Y"), 
         YEAR = if_else(OPERATION_TYPE == "TCP", PIPE_YR, YEAR)) %>% 
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
            CULTURA = sum(CULTURA)) %>% 
  filter(DUMMY_DIGITAL == 1) %>% 
  mutate(DIGITAL_COST = NA)

output_total_kpi <- output_portfolio_tidy %>% 
  rbind(output_pipe_tidy) %>% 
  select(TYPE, YEAR, OPERATION_TYPE, DIVISION, DIGITAL, TD, DIGITAL_COST) %>% 
  ungroup() 

output_total_non <- output_portfolio %>% 
  mutate(YEAR = format(APPROVAL_DATE, format = "%Y"),
         TYPE = "portfolio") %>% 
  group_by(DIVISION, YEAR, TYPE) %>% 
  summarise(OPERACIONES = n())

output_sumado_kpi <- output_total_kpi %>% 
  ungroup() %>% 
  group_by(DIVISION, YEAR, TYPE) %>% 
  summarise(DIGITAL = sum(DIGITAL), 
         TD = sum(TD), 
         COUNTRIES = n_distinct(COUNTRY),
         DIGITAL_COST = sum(DIGITAL_COST)) %>% 
  left_join(kpi_2022) %>% 
  filter(TYPE == "portfolio") %>% 
  full_join(output_total_non) %>% 
  mutate(TD = replace_na(TD,0),
         COUNTRIES = replace_na(COUNTRIES, 0),
         DIGITAL = replace_na(DIGITAL, 0)) %>% 
  mutate(DIGITAL_PER = DIGITAL/OPERACIONES) %>% 
  select(YEAR, TYPE, DIVISION,DIGITAL, DIGITAL_TARGET, OPERACIONES, DIGITAL_PER,
         DIGITAL_TARGET_PER, TD, TD_TARGET, COUNTRIES, COUNTRIES_TARGET, DIGITAL_COST, DIGITAL_COST_TARGET, 
         DIGITAL_TOOLS_TARGET, YEAR_KPI) %>% 
  filter(YEAR>2020)

###### Salvar excel #####

outputs <- list(output_total_kpi = output_total_kpi, output_sumado_kpi = output_sumado_kpi)

write.xlsx(outputs, "C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/cartera digital/Dashboard/KPI.xlsx", overwrite = TRUE)

wb = loadWorkbook("C:/Users/MARIAREY/OneDrive - Inter-American Development Bank Group/General/cartera digital/Dashboard/output.xlsx.xlsx")

writeData(wb, "kpi_sumado", output_sumado_kpi)

