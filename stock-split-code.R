# ---------------------------------------------
# Research Seminar Corporate Finance : Group 6
#----------------------------------------------


# Working Directory --------------------------------------------------------------------------------------

getwd()
# setwd("C:/Users/juzu/Desktop/MBF/Research Seminar Corporate Finance/data")


# Packages & Libraries -----------------------------------------------------------------------------------

#install.packages("ff")
#install.packages("ffbase")
#install.packages("RPostgres")

library("ff")
library("ffbase")
library("stargazer")
library("readxl")
library("lubridate")
library("tidyverse")
library("RPostgres")
library("data.table")

# Data gathering for stock split equity titles  ----------------------------------------------------------

#CRSP Database: Daily stock files, 01.01.2010 - 31.12.2020 
#entire database
#@https://wrds-web.wharton.upenn.edu/wrds//ds/crsp/stock_a/

raw_data.ff <- read.table.ffdf(file="2010_2020.csv", sep=",")

colnames_wrds <- c("permno","date", "share_code","exchange_code","ticker","company_name","share_class",
                   "primary_exchange","permco","header_exchange_code","declaration_date","payment_date",
                   "distribution_code","factor_to_adjust_price_FACPR","factor_to_adjust_shares_FACSHR", 
                   "share_price","trading_volume","shares_oustanding_in_ks", "number_of_trades")

colnames(raw_data.ff) <- colnames_wrds

df_splits_2010_2020=raw_data.ff[raw_data.ff$distribution_code==5523,]

#table.ff(df_splits_2010_2020$distribution_code)
#5523= stock split without dividend:
#CRSP guide:
#p 15: reverse splits 
#p. 130: split

df_2010_2020<- as.data.frame(df_splits_2010_2020)

#length(unique(df_2010_2020$company_name))
#1512 unique companies that conducted a stock split from 2010-2020

#CRSP Database: Daily stock files 01.01.2000 - 31.12.2010 
#entire database

raw_data.ff2 <- read.table.ffdf(file="2000_2010.csv", sep=",")

colnames(raw_data.ff2) <- colnames_wrds

df_splits_2000_2010 <- raw_data.ff2[raw_data.ff2$distribution_code==5523,]

#5523= stock split without dividend:
#CRSP guide:
#p 15: reverse splits 
#p. 130: split

df_2000_2010<- as.data.frame(df_splits_2000_2010)

#length(unique(df_2000_2010$company_name))
#2389 unique companies that conducted a stock split from 2010-2020 

split_df <- rbind(df_2000_2010,df_2010_2020)
# write.csv2(split_df,"stocksplit_df_raw.csv",row.names=FALSE)

rm(raw_data.ff)
rm(raw_data.ff2)

split_df_2 <- split_df[(!is.na(split_df$declaration_date)) & (split_df$declaration_date != ""),]
#only splits where the delcaration date was recorded

split_df_3 <- split_df_2[split_df_2$share_code==11,]
#only common shares - as in empirical literature

# Expanding event window around declaration date to clear for all confounding events in 10 day period around split announcement:

split_df_3$declaration_date <- as.Date(format(split_df_3$declaration_date, format = '%Y%m%d'), format = '%Y%m%d')

split_df_3$declaration_date_1_post <- split_df_3$declaration_date+1
split_df_3$declaration_date_2_post <- split_df_3$declaration_date+2
split_df_3$declaration_date_3_post <- split_df_3$declaration_date+3
split_df_3$declaration_date_4_post <- split_df_3$declaration_date+4
split_df_3$declaration_date_5_post <- split_df_3$declaration_date+5

split_df_3$declaration_date_1_pre <- split_df_3$declaration_date-1
split_df_3$declaration_date_2_pre <- split_df_3$declaration_date-2
split_df_3$declaration_date_3_pre <- split_df_3$declaration_date-3
split_df_3$declaration_date_4_pre <- split_df_3$declaration_date-4
split_df_3$declaration_date_5_pre <- split_df_3$declaration_date-5

split_df_3$declaration_date<-as.numeric(format(split_df_3$declaration_date, format = '%Y%m%d'))

split_df_3$declaration_date_1_post<-as.numeric(format(split_df_3$declaration_date_1_post, format = '%Y%m%d'))
split_df_3$declaration_date_2_post<-as.numeric(format(split_df_3$declaration_date_2_post, format = '%Y%m%d'))
split_df_3$declaration_date_3_post<-as.numeric(format(split_df_3$declaration_date_3_post, format = '%Y%m%d'))
split_df_3$declaration_date_4_post<-as.numeric(format(split_df_3$declaration_date_4_post, format = '%Y%m%d'))
split_df_3$declaration_date_5_post<-as.numeric(format(split_df_3$declaration_date_5_post, format = '%Y%m%d'))

split_df_3$declaration_date_1_pre <-as.numeric(format(split_df_3$declaration_date_1_pre, format = '%Y%m%d'))
split_df_3$declaration_date_2_pre <-as.numeric(format(split_df_3$declaration_date_2_pre, format = '%Y%m%d'))
split_df_3$declaration_date_3_pre <-as.numeric(format(split_df_3$declaration_date_3_pre, format = '%Y%m%d'))
split_df_3$declaration_date_4_pre <-as.numeric(format(split_df_3$declaration_date_4_pre, format = '%Y%m%d'))
split_df_3$declaration_date_5_pre <-as.numeric(format(split_df_3$declaration_date_5_pre, format = '%Y%m%d'))


# Confounding Events -------------------------------------------------------

# -> elimminating effects that could effect stock returns on announcement date
#PERMNO's which conducted stocksplits (2000 - 2020) and where the declaration date is available (see unique_identifier)

#Linked with CRSP/Compustat Merged Database (under CRSP) - Linking Table (to  GVKEY)
#@https://wrds-web.wharton.upenn.edu/wrds//ds/crsp/ccm_a/linktable/index.cfm#

linking_table <- read.csv("linking_table.csv")

# unique_identifier <- as.numeric(as.character(unique(split_df_3$permno)))
# unique_identifier_2 <- matrix(unique_identifier,ncol = 1)
# write.table(unique_identifier_2, file="unique_identifier_2.txt", append = FALSE,row.names = FALSE, col.names = FALSE)

colnames(linking_table)[3]<-"permno"
split_df_4<-merge(x = split_df_3, y = linking_table[,c("permno","GVKEY","cusip")], by = "permno", all.x = TRUE)

# unique_cusips <- unique(split_df_4$cusip)
# unique_cusips_2 <- matrix(unique_cusips,ncol = 1)
# write.table(unique_cusips_2, file="unique_cusips_2.txt", append = FALSE,row.names = FALSE, col.names = FALSE, quote = FALSE)

#WRDS: Capital IQ - Key Developments, 01.01.2000 - 21.11.2020 
# - used linked- cusips from linking tool to get all the company announcements related to the LPERMNO's which have seen a stock split
#@https://wrds-web.wharton.upenn.edu/wrds//ds/comp/ciq/keydev/index.cfm

confounding_df <- read.csv("confounding.csv")

colnames(confounding_df)[4]<-"declaration_date"
colnames(confounding_df)[6]<-"GVKEY"

key_development_lookup <- read.csv2("key_development_lookup.csv")
colnames(key_development_lookup)[1] <- "keydeveventtypeid"
#MANUAL CHECK: WHICH EVENTS SHOULD BE EXCLUDED

confounding_df=confounding_df[!confounding_df$keydeveventtypeid %in% c(53,55,78,140,144),]
#removing only superficial confounding events from the black list (announcement of certain dates, internal meeting dates etc.) that will bear
#no information to the pulic

merged_df_1<-left_join(x = split_df_4, y = confounding_df[,c(3,4,6)], by = c("declaration_date","GVKEY"))

#Days after the announcement date 
#+1
colnames(confounding_df)[4]<-"declaration_date_1_post"
colnames(confounding_df)[3]<-"id_1_post"
merged_df_2<-left_join(x = merged_df_1, y = confounding_df[,c(3,4,6)], by = c("declaration_date_1_post","GVKEY"))
#+2
colnames(confounding_df)[4]<-"declaration_date_2_post"
colnames(confounding_df)[3]<-"id_2_post"
merged_df_3<-left_join(x = merged_df_2, y = confounding_df[,c(3,4,6)], by = c("declaration_date_2_post","GVKEY"))
#+3
colnames(confounding_df)[4]<-"declaration_date_3_post"
colnames(confounding_df)[3]<-"id_3_post"
merged_df_4<-left_join(x = merged_df_3, y = confounding_df[,c(3,4,6)], by = c("declaration_date_3_post","GVKEY"))
#+4
colnames(confounding_df)[4]<-"declaration_date_4_post"
colnames(confounding_df)[3]<-"id_4_post"
merged_df_5<-left_join(x = merged_df_4, y = confounding_df[,c(3,4,6)], by = c("declaration_date_4_post","GVKEY"))
#+5
colnames(confounding_df)[4]<-"declaration_date_5_post"
colnames(confounding_df)[3]<-"id_5_post"
merged_df_6<-left_join(x = merged_df_5, y = confounding_df[,c(3,4,6)], by = c("declaration_date_5_post","GVKEY"))

#Days before the announcement date
#-1
colnames(confounding_df)[4]<-"declaration_date_1_pre"
colnames(confounding_df)[3]<-"id_1_pre"
merged_df_7<-left_join(x = merged_df_6, y = confounding_df[,c(3,4,6)], by = c("declaration_date_1_pre","GVKEY"))
#-2
colnames(confounding_df)[4]<-"declaration_date_2_pre"
colnames(confounding_df)[3]<-"id_2_pre"
merged_df_8<-left_join(x = merged_df_7, y = confounding_df[,c(3,4,6)], by = c("declaration_date_2_pre","GVKEY"))
#-3
colnames(confounding_df)[4]<-"declaration_date_3_pre"
colnames(confounding_df)[3]<-"id_3_pre"
merged_df_9<-left_join(x = merged_df_8, y = confounding_df[,c(3,4,6)], by = c("declaration_date_3_pre","GVKEY"))
#-4
colnames(confounding_df)[4]<-"declaration_date_4_pre"
colnames(confounding_df)[3]<-"id_4_pre"
merged_df_10<-left_join(x = merged_df_9, y = confounding_df[,c(3,4,6)], by = c("declaration_date_4_pre","GVKEY"))
#-5
colnames(confounding_df)[4]<-"declaration_date_5_pre"
colnames(confounding_df)[3]<-"id_5_pre"
merged_df_11<-left_join(x = merged_df_10, y = confounding_df[,c(3,4,6)], by = c("declaration_date_5_pre","GVKEY"))
merged_df_11<-unique(merged_df_11)

#after looking up all different events provided by Capital IQ, all confounding events in a 10-day window around the stock split
#announcement were listed. Stock splits with such an event were removed from the data set

indx <- apply(merged_df_11[,32:42], 1, function(x) all(is.na(x)))
confounding_10days <- merged_df_11[indx,]
confounding_10days_small <- confounding_10days[,c(1:19)]

#write.csv2(confounding_10days,"confounding_10days.csv",row.names=FALSE)


# Further filter Criteria ---------------------------------------------------------

confounding_10days_small$market_cap <- as.numeric(as.character(confounding_10days_small$shares_oustanding_in_ks))*1000*as.numeric(as.character(confounding_10days_small$share_price))

non_conf_large <- confounding_10days_small[confounding_10days_small$market_cap> 0,]

non_conf_df <- non_conf_large[as.numeric(as.character(non_conf_large$factor_to_adjust_price_FACPR))>0.25,]

non_conf_df$year <- as.numeric(substr(non_conf_df$declaration_date, 1, 4))

non_conf_df_10_19 <- non_conf_df[(non_conf_df$year>=2010) & (non_conf_df$year<=2019),]

final_df<-unique(non_conf_df_10_19)
final_df_2<-na.omit(final_df)

final_df_2$permco <- as.numeric(as.character(final_df_2$permco))
final_df_2$trading_volume <- as.numeric(as.character(final_df_2$trading_volume))

final_df_3 <- final_df_2[order(final_df_2$trading_volume, decreasing = TRUE),]
final_df_4 <- final_df_3[!duplicated(final_df_3$permco) | !duplicated(final_df_3$declaration_date),]
#delete duplicated values with lowest trading volume


write.csv2(final_df_4,"final_df_10_19_ff.csv",row.names=FALSE)

# Return data for split titles --------------------------------------------

#Market return during 2000-2020 (NYSE,AMEX,NASDAQ - value weighted)
#@https://wrds-web.wharton.upenn.edu/wrds//ds/crsp/indexes_a/mktindex/cap_d.cfm

market_return_df <- read.csv("market_returns.csv")

# Daily stock returns: direct SQL querry from WRDS 
# -> requires an account and storage of account name / PW on local file, see below:
# @https://wrds-www.wharton.upenn.edu/pages/support/programming-wrds/programming-r/r-from-your-computer/
# for the querry structure, see below
# @https://wrds-www.wharton.upenn.edu/pages/support/programming-wrds/programming-r/querying-wrds-data-r/#introduction


wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='dajulian')

# Finding the trading days for the entire cross-section of 
# stocks listed on the New York Stock Exchange (NYSE), t
# he American Stock Exchange (AMEX), and the Nasdaq National Market System (NMS)
#with the  NYSE Trade and Quote (TAQ) database
#@https://onnokleen.de/post/taq_via_wrds/ :

res <- dbSendQuery(wrds, "select distinct table_name
                   from information_schema.columns
                   where table_schema='taqmsec'
                   order by table_name")
df_dates <- dbFetch(res, n = -1)
dbClearResult(res)

dates_trades <-
  df_dates %>%
  filter(grepl("ctm",table_name), !grepl("ix_ctm",table_name)) %>%
  mutate(table_name = substr(table_name, 5, 12)) %>%
  unlist()%>%
  as.Date(format(final_df_4$declaration_date, format = '%Y%m%d'), format = '%Y%m%d')
  
final_df_4$declaration_date <- as.Date(format(final_df_4$declaration_date, format = '%Y%m%d'), format = '%Y%m%d')

#Check if all declaration dates are trading dates on the exchanges
all(final_df_4$declaration_date %in% dates_trades)

final_df_4$start_date <- final_df_4$declaration_date-10
final_df_4$announce_date_and_1 <-final_df_4$declaration_date+1
final_df_4$end_date <-final_df_4$declaration_date+10

final_df_4$permno <- as.numeric(as.character(final_df_4$permno))


get_stock_return_until_announcement <- function(data.frame) {
  data_1<- tryCatch(
    {
      
      a<- as.character(data.frame$permno[1])
      
      b<- as.character(data.frame$start_date[1])
      
      c<- as.character(data.frame$announce_date_and_1[1])
      
      d<-sprintf("select ret
                 from crsp.dsf
                 where permno = '%s'
                 and date between '%s'
                 and '%s'", a,b,c)
      
      res <- dbSendQuery(wrds, d)
      
      data <- dbFetch(res, n=-1)
      
      dbClearResult(res)
      
      return(data)
      
    },
    error = function(e){
      message('Error in the process')
      print(e)
      dbClearResult(res)
    }
    
    
      )
}


get_stock_return_after_announcement <- function(data.frame) {
  data_1<- tryCatch(
    {
      
      a<- as.character(data.frame$permno[1])
      
      b<- as.character(data.frame$declaration_date[1])
      
      c<- as.character(data.frame$end_date[1])
      
      d<-sprintf("select ret
                 from crsp.dsf
                 where permno = '%s'
                 and date between '%s'
                 and '%s'", a,b,c)
      
      res <- dbSendQuery(wrds, d)
      
      data <- dbFetch(res, n=-1)
      
      dbClearResult(res)
      
      return(data)
      
    },
    error = function(e){
      message('Error in the process')
      print(e)
      dbClearResult(res)
    }
    
    
      )
}

return_df_before_announcement <- matrix(data=NA, nrow= 20, ncol= nrow(final_df_4))

for (i in 1:nrow(final_df_4)){
  sub_df <- final_df_4[i,]
  results <- get_stock_return_until_announcement(sub_df)
  results_formated <- c(results$ret,rep(NA,20-length(results$ret)))
  return_df_before_announcement[,i] <- results_formated
}

return_df_before_announcement <- as.data.frame(return_df_before_announcement)

return_dt_before_announcement <- data.table(return_df_before_announcement)

return_df_until_announcement<- as.data.frame(return_dt_before_announcement[, lapply(.SD, function(x) tail(x[!is.na(x)],6))])


for (i in 1:ncol(return_df_before_announcement)){
  return_df_until_decl[,i]<- return_df_before_announcement[first_value[i]:last_non_NA[i],i]
}

return_df_until_decl <- return_df_before_announcement[last_non_NA-6:last_non_NA,]

return_df_after_announcement <- matrix(data=NA, nrow= 20, ncol= nrow(final_df_4))

for (i in 1:nrow(final_df_4)){
  sub_df <- final_df_4[i,]
  results <- get_stock_return_after_announcement(sub_df)
  results_formated <- c(results$ret,rep(NA,20-length(results$ret)))
  return_df_after_announcement[,i] <- results_formated
}

return_df_after_announcement <- as.data.frame(return_df_after_announcement)

return_df_after_announcement <- return_df_after_announcement[1:5,]

eventwindow_df <- rbind(return_df_until_announcement,return_df_after_announcement)

a<- as.character(final_df_4$permno[2])

b<- as.character(final_df_4$start_date[2])

c<- as.character(final_df_4$end_date[2])

d<-sprintf("select date,ret
           from crsp.dsf
           where permno = '%s'
           and date between '%s'
           and '%s'", a,b,c)

res <- dbSendQuery(wrds, d)


data <- dbFetch(res, n=-1)
dbClearResult(res)
data


