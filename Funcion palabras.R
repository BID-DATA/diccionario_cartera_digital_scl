# función keywords

# Functions
nearest_result <-  function(text, division){
  # ToDo() - Basic NLP clean encoding - remove special characters, etc
  text <- tolower(text)
  aux <- read_xlsx('Inputs/keywords_help.xlsx') %>% 
    filter(division == division) %>% 
    # Todo() create vector of similarity
    filter(str_detect(text, str_to_lower(str_replace(KEYWORDS, ',', '|')))) %>% 
    select(INDICATOR) %>% pull()
  
  # Case without match
  if( length(aux)!=1 ) {aux<-NA}
  return(aux)
}

clean_text <- function(text){
  text = str_trim(str_to_lower(text))
  text = stri_trans_general(text, id = "Latin-ASCII")
}

clean_detect <- function(text){
  text <- str_replace_all(text, regex("([0-9]+)|(\\sy\\s)"), "")
  text <- str_replace_all(text, regex("(,\\s)|(\\s,)"), ",")
  text <- str_replace_all(text, ',', '|')
  text <- clean_text(text)    
  text <- str_replace_all(text, regex("\\s+"), " ")
  return(text)
}

nearest_match <-  function(division, text, column){
  
  # ToDo() - Basic NLP clean encoding - remove special characters, etc
  aux <- read_xlsx('Inputs/keywords_help.xlsx') %>% 
    filter({{division}} == division) %>% 
    # Todo() create vector of similarity
    filter(str_detect(clean_text(text),
                      clean_detect(KEYWORDS))) %>% 
    select(!!enquo(column)) %>% pull()
  
  # Case without match
  if( length(aux)<1 ) {aux<-NA}
  return(aux)
}