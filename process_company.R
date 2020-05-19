library(readr)
library(dplyr)
library(purrr)
library(tidyr)
library(stringr)
library(readxl)
library(xlsx)

process_company <- function(codes,country) {
  
  #filter industry
  ind_com <- com_ex2016[grepl(codes, com_ex2016$hscode),]
  ind_custom <- custom[grepl(codes, custom$code),]
  
  #取前6碼
  ind_com$hscode <- substr(ind_com$hscode,1,6)
  ind_com <- group_by(ind_com, company_ban, country, hscode)%>% summarise(value_median=sum(single_product_median))
  ind_com_country <- ind_com[grepl(country,ind_com$country),]
  
  #number of companies
  n_company_df <- group_by(ind_com_country, hscode, country)%>% summarise(n_company=n())
  ind_custom <- filter(ind_custom, year==2016)
  
  
  n_company_df$top3.ratio <- NA
  n_company_df$top3.company <- NA
  ind_com_country$ratio <- NA
  
  if(nrow(ind_com_country) >0){
  for (i in 1:length(ind_com_country$hscode)){
      ind_com_country[ind_com_country$hscode==ind_com_country$hscode[i],'ratio'] <- ind_com_country[ind_com_country$hscode==ind_com_country$hscode[i],]$value_median/sum(ind_com_country[ind_com_country$hscode==ind_com_country$hscode[i],]$value_median)*100
      n_company_df[n_company_df$hscode==ind_com_country$hscode[i],'top3.ratio']  <- ifelse(ind_com_country %>% ungroup() %>% filter(hscode==ind_com_country$hscode[i])%>%nrow()==0 |is.null(ind_custom[ind_custom$code==ind_com_country$hscode[i],'code']%>%nrow())
                                                                                   ,'NA'
                                                                                   ,(sum(ind_com_country %>% ungroup() %>% filter(hscode==ind_com_country$hscode[i]) %>% 
                                                                                     arrange(desc(value_median)) %>% head(3) %>% select(value_median))/sum(ind_custom %>% filter(ind_custom[grepl(paste0("^",ind_com_country$hscode[i]),ind_custom$code),])%>% select(value)*1000)) *100 %>% round(2))
      
      n_company_df[n_company_df$hscode==ind_com_country$hscode[i],'top3.company']  <- ifelse(ind_com_country %>% ungroup() %>% filter(hscode==ind_com_country$hscode[i])%>%nrow()==0 
                                                                                           ,'NA'
                                                                                           , paste0(ind_com_country$company_ban[1:3],'(',ind_com_country$ratio[1:3],'%)', collapse = '; '))
                                                                                           
                                                                                           
      
      }
  
  n_company_df$top3.ratio <- as.numeric(n_company_df$top3.ratio)                       
  n_company_df$top3.ratio <- round(n_company_df$top3.ratio,2)
  n_company_df$top3.ratio <- ifelse(n_company_df$top3.ratio > 100, 100, n_company_df$top3.ratio)
  
  }else{n_company_df <- data.frame(hscode='-',country='-',n_company=0,top3.ratio=0,top.company=0)}
  
  names(n_company_df) <- c('國家','出口商數','前三大出口商占比','前三大廠商')
  return(n_company_df)
  
}
