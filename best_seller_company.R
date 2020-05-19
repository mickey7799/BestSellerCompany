current_path <- dirname(rstudioapi::getActiveDocumentContext()$path)
setwd(current_path)

library(dplyr)
library(stringr)
library(readr)
library(xlsx)
library(data.table)
library(purrr)
library(tidyr)
library(stringr)

source('./config.R', encoding = 'UTF-8')
source('./process_company.R', encoding = 'UTF-8')

#========preparation=======
#====rawdata====
# 國家ISO對照表
country_name <- read_csv(file = '//172.20.23.190/ds/Raw Data/2016大數爬蟲案/data/ITC HS6/itc_df_complete.csv',
                         col_types = c('c_c')) %>% unique()

# UN Comtrade原始資料
rawData <- fread('//172.26.1.102/dstore/Projects/data/uncomtrade/annual/transformed/un-import-hs6-2011-2016.csv', 
                 colClasses = c(rep('numeric', 2),'character',rep('numeric',12)), select = c(1:3,13,15))
rawData$reporter_code <- str_pad(rawData$reporter_code, 3, pad = '0')
rawData$partner_code <- str_pad(rawData$partner_code, 3, pad = '0')
rawData$hscode6 <- str_pad(rawData$hscode6, 6, pad = '0')

# 將ISO轉換為中文國家名稱
importData <- merge(x = rawData, y = country_name, by.x = 'reporter_code', by.y = 'ds_code')
colnames(importData)[6] <- 'reporter'
importData <- merge(x = importData, y = country_name, by.x = 'partner_code', by.y = 'ds_code')
colnames(importData)[7] <- 'partner'
importData <- filter(importData, partner != '所有國家')[c(6:7, 3:5)]

# 以15年資料補16年資料
country_list <- unique(importData$reporter)
countries <- c()

for(i in 1:length(country_list)){
  tmp <- importData %>% filter(reporter == country_list[i])
  
  if(sum(is.na(tmp$v2016)) == nrow(tmp)){
   
    countries <- c(countries, country_list[i]) # 將15年有缺值的國家列出來
  }
  rm(tmp)
}

importData[is.na(importData)] <- 0 
importData <- filter(importData, v2016 != 0) # 僅計算16年有出口之國家


# 出口資料
#===讀取資料用===#funtion
readMonthlyData <- function(year, month, currency = c('usd', 'twd')){
  
  monthly <- fread(paste0('//172.26.1.102/dstore/Projects/data/mof-export-', currency, '/',year,'-',month,'.tsv'), 
                   colClasses =  c(rep('character',6),'numeric','character','numeric'), 
                   select = c(1,2,4,7,8,9), encoding = 'UTF-8')
  names(monthly) <- c('code','product','country','weight','unit','value')
  monthly[monthly$unit=='TNE ','weight'] <- monthly[monthly$unit=='TNE ','weight'] *1000
  monthly$code <- str_pad(monthly$code, 11, pad = "0")
  monthly$code <- as.character(monthly$code)
  monthly$year <- year
  monthly$month <- month
  monthly
}

selectData <- function(start_yr, end_yr, start_mon, end_mon, currency = c('us', 'nt')){
  
  file_list <- c()
  
  for(year in start_yr:end_yr){
    for(month in start_mon:end_mon){
      month <- sprintf('%02d',month)
      
      file_name <- (paste0(year,'-',month))
      file_list <- c(file_list, file_name)
    }
  }
  years <- substr(file_list, 1, 4)      #篩出月份與年份
  months <- substr(file_list, 6, 7)
  
  ls <- map2(years, months, function(x, y) {readMonthlyData(year = x, month = y, currency = currency)})
  df <- purrr::reduce(ls, rbind)
  df
}

#手動輸入年份與月份;將會計算設定年份與前兩年之資料(共三年)
end_yr <- 2017 ; start_yr <- 2015
start_mon <- 1 ; end_mon <- 12
currency <-'usd'

#讀取海關資料
custom <- selectData(start_yr,end_yr,start_mon,end_mon,currency)
custom$code <- substr(custom$code,1,6)
custom <- group_by(custom, code, country, year) %>% summarise(value=sum(value))

# 廠商原始資料
comData <- fread('//172.26.1.102/dstore/Projects/analyze-ebs-performance/ebs_performance_company_product.csv',
                    encoding="UTF-8",colClasses=c('text','numeric','text','text','text','text','text','numeric','text'))

comData$hscode <- str_pad(comData$hscode, 10, pad = '0')
comData$country <- gsub('/.*',"", comData$country)

com_ex2016 <- filter(comData, trade_flow==2, year==2016)

dic <- c('1' = 100000000,
         '2' = 75000000,
         '3' = 25000000,
         '4' = 7500000,
         '5' = 3750000,
         '6' = 2000000,
         '7' = 1250000,
         '8' = 900000,
         '9' = 700000,
         '10'= 500000,
         '11'= 350000,
         '12'= 250000,
         '13'= 150000,
         '14'= 75000,
         '15'= 50000,
         '20'=0)


#map export range to median
com_ex2016$single_product_median <- dic[com_ex2016$single_product_range]


#source2
rawData<- fread('//172.20.23.190/ds/Raw Data/kmg/export_2016.csv')

rawData$HSCODE <- str_pad(rawData$HSCODE%>%as.character(), 11, pad = '0')

rawData$BAN <- str_pad(rawData$BAN%>%as.character(), 8, pad = '0')

com_df <- rawData[,c(2,4,5,8)]

#mapping country code to Chinese name
country_map <- read_csv('C:/Users/2187/Desktop/dataset/地區分類/ds_export_markets.csv',col_types = c('ccc______'))

com_df <- left_join(com_df, country_map, by=c('COUNTRY'='mof_enCode'))[,c(1:2, 6, 4)]

com_df[is.na(com_df$export_countryName),'export_countryName'] <- '全球'

com_df$AMOUNT_ONE <- com_df$AMOUNT_ONE*10000

names(com_df) <- c("ban_real", "code", "country","value")





#====product_list====
# 需計算之產業產品定義
products <- read.xlsx(paste0('//172.20.23.190/ds/產業/2 HScode/確定版總表V2.xlsx'), 1,
                      encoding='UTF-8',stringsAsFactors = F)
# 6碼產品中文名稱
hscode_name <- read_delim(file = '//172.26.1.102/dstore/Projects/mof-crawler/full_hscode11.tsv', delim = '\t', 
                          col_types = c('_c_ccc____'))[,c(3,1,2,4)] %>% unique()
names(hscode_name)[1] <- 'hscode6'

  
  #針對各國單一產品做出相對的金額比重成長率及排名
  importData_class <- importData %>% group_by(reporter, hscode6) %>% 
    mutate(share = round((v2016/sum(v2016))*100,2),
           growth = round((v2016/v2015 -1) *100, 2),
           rank = rank(desc(v2016), ties.method = 'first'))
  
  
  # 各國各產品總進口額 & 各國各產品進口來源國數
  total <- importData_class %>% group_by(reporter, hscode6) %>% 
    summarise(t_v2015=sum(v2015), t_v2016=sum(v2016), n_partner=length(partner))
  total[,'growth'] <- round((total$t_v2016/total$t_v2015 -1) *100, 2)
  total <- total[,c(1:2,4,6,5)]
  
  
  # 各國自臺進口
  tw <- filter(importData_class, partner == '臺灣')[,c(1,3,5:8)]
  names(tw)[3:6] <- paste0('tw_',names(tw)[3:6])
  
  tmp <- left_join(total, hscode_name, by = c('hscode6'))[,c(1:2,6:8,3:5)] %>%
    left_join(tw, by = c('reporter','hscode6')) 
  
  
  bs_data <- tmp %>% filter(tw_rank %in% 1:3) %>% as.data.frame()
  
  # 產品顯示名稱
  bs_data$text <- ifelse(bs_data$hs6cn == '其他',
                         paste0( '其他', bs_data$hs4cn), #若六碼產品名只有"其他"會加入4碼產品名
                         paste0( bs_data$hs6cn))
  
  bs_data <- bs_data %>% 
    select('reporter','hscode6','tw_v2016','tw_share','tw_growth','text', 'tw_rank')
  names(bs_data) <- c('name', 'code', 'val', 'share', 'growth', 'text', 'ranking')
  
  
  #計算廠商
  #取前6碼
  com_df$code <- substr(com_df$code,1,6)
  ind_com <- group_by(com_df, ban_real, country, code)%>% summarise(value=sum(value))%>%select(c(3,1,2,4))
  
  #number of companies
  n_company <- group_by(ind_com, code, country)%>% summarise(n_company=n())
  ind_custom <- filter(custom, year==2016)
  
  n_company_df$top3.ratio <- NA
  n_company_df$top3.ratio <- as.numeric(n_company_df$top3.ratio)
  n_company_df$top3.company <- NA
  
  #篩市場
  n_company_df <- filter(n_company, country=='印度')
  india_com <- filter(ind_com, country=='印度')
  bs <- filter(bs_data, name=='印度')
  india_custom <- filter(ind_custom, country=='印度')
  
  india_com$ratio <- NA
  
  india_com <- india_com[india_com$code%in%bs$code,]
  n_company_df <- n_company_df[n_company_df$code%in%bs$code,]
  
  #算各產品廠商佔台灣出口比重
  for (i in 1:length(india_com$code)){
  india_com[india_com$code==india_com$code[i],'ratio'] <- india_com[india_com$code==india_com$code[i],]$value/sum(india_custom[grepl(paste0("^",india_com$code[i]),india_custom$code),]$value*1000) * 100 %>% round(2)
  print(paste0(i,'/',length(india_com$code)))
  }
  
  india_com$ratio <- ifelse(india_com$ratio>100,100,india_com$ratio)
  
  for (i in 1:length(n_company_df$code)){
    #india_com[india_com$code==bs$code[i],'ratio'] <- india_com[india_com$code==bs$code[i],]$value_median/sum(india_custom[grepl(paste0("^",bs$code[i]),india_custom$code),])%>% select(value)*1000)*100
    n_company_df[n_company_df$code==n_company_df$code[i],'top3.ratio']  <- ifelse(india_com %>% ungroup() %>% filter(code==n_company_df$code[i])%>%nrow()==0 |india_custom[india_custom$code==n_company_df$code[i],]%>%nrow()==0
                                                                                         ,'NA'
                                                                                         ,sum(india_com %>% ungroup() %>% filter(code==n_company_df$code[i]) %>% arrange(desc(value)) %>% head(3) %>% select(value))/sum(india_custom[grepl(paste0("^",n_company_df$code[i]),india_custom$code),]%>% ungroup()%>%select(value)*1000) * 100 %>% round(2)
                                                                                      )
    
    temp <- india_com %>% filter(code==n_company_df$code[i]) %>% arrange(desc(value))
    n_company_df[n_company_df$code==n_company_df$code[i],'top3.company']  <- ifelse(india_com %>% ungroup() %>% filter(code==n_company_df$code[i])%>%nrow()==0 |is.null(india_custom[india_custom$code==n_company_df$code[i],]%>%nrow())
                                                                                           ,'NA'
                                                                                           , paste0(temp$ban_real[1:3],'('
                                                                                                    ,round(temp$ratio,2)[1:3],'%)', collapse = '; ')
                                                                                    )
    print(paste0(i,'/',length(n_company_df$code)))  
  }
  n_company_df$top3.ratio <- ifelse(n_company_df$top3.ratio>100,100,n_company_df$top3.ratio)
  n_company_df$top3.ratio <- n_company_df$top3.ratio%>%round(2)  
#join data  
  bs_company <- left_join(bs, n_company_df, by=c('code')) 
  
#map products to industry
  bs_company$industry <- NA
        
lookup_frame <- products[,c(1:3)]
lookup_frame <- lookup_frame[!lookup_frame$industry==lookup_frame$class,] 
lookup_frame <- lookup_frame[complete.cases(lookup_frame),]

get_ind <- function(code, lookup_frame) {
  # 檢查每個row的code裡面是否包含符合的code，回傳T/F
  in_codes <- map_lgl(lookup_frame$HS.Code, function(x) grepl( paste0("^",unlist(str_split(x, ',')),collapse = '|'),code))
  # 用T/F把符合的row的類別取回來
  code_class <- lookup_frame$class[in_codes]
  code_industry <- lookup_frame$industry[in_codes]
  # 如果上一步沒找到東西，就說沒有，有的話就回傳
  if (length(code_class)>0) return(paste0( code_industry,'-',code_class)%>% paste0(collapse='; '))
  else print('No matching class found.')
}


for (i in 1:length(bs_company$code)){
  bs_company[bs_company$code==bs_company$code[i],'industry'] <- get_ind(bs_company$code[i], lookup_frame)
  
  
  }
 
  
  fwrite(bs_company, paste0('output0316/', '印度', '.csv'), row.names = F)
  
 

write.csv(data.frame(country = countries), 'output0316/countries.csv', 
          row.names = F, fileEncoding = 'UTF-8')


