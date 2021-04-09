# ---------------------------------------------
# Research Seminar Corporate Finance : Group 6
#----------------------------------------------


# Working Directory -------------------------------------------------------

getwd()
# setwd("C:/Users/juzu/Desktop/MBF/Research Seminar Corporate Finance/data")


# Packages & Libraries ----------------------------------------------------

#install.packages("ff")
#install.packages("ffbase")
#install.packages("RPostgres")

library("ff")
library("ffbase")
library("stargazer")
library("readxl")
library("lubridate")
library("writexl")
library("tidyverse")
library("RPostgres")

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

table.ff(df_splits_2010_2020$distribution_code)
#5523= stock split without dividend:
#CRSP guide:
#p 15: reverse splits 
#p. 130: split


df_2010_2020<- as.data.frame(df_splits_2010_2020)

# length(unique(df_2010_2020$company_name))
#1512 companies that conducted a stock split from 2010-2020

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

# length(unique(df_2000_2010$company_name))
#2389 companies that conducted a stock split from 2010-2020 

split_df <- rbind(df_2000_2010,df_2010_2020)
#write.csv2(split_df,"stocksplit_df.csv",row.names=FALSE)

# rm(raw_data.ff)
# rm(raw_data.ff2)

split_df_2 <- split_df[(!is.na(split_df$declaration_date)) & (split_df$declaration_date != ""),]
#only splits where the delcaration date was recorded

split_df_3 <- split_df_2[split_df_2$share_code==11,]
#only common shares - as in empirical literature

unique_identifier <- as.numeric(as.character(unique(split_df_3$permno)))

unique_identifier_2 <- matrix(unique_identifier,ncol = 1)

# write.table(unique_identifier_2, file="unique_identifier_2.txt", append = FALSE,row.names = FALSE, col.names = FALSE)

# Confunding Events -------------------------------------------------------

# -> elimminating effects that could effect stock returns on announcement date
#PERMNO's which conducted stocksplits (2000 - 2020) and where the declaration date is available (see unique_identifier)

#Linked with CRSP/Compustat Merged Database (under CRSP) - Linking Table (to  GVKEY)
#@https://wrds-web.wharton.upenn.edu/wrds//ds/crsp/ccm_a/linktable/index.cfm#

linking_table <- read.csv("linking_table.csv")

colnames(linking_table)[3]<-"permno"

split_df_4<-merge(x = split_df_3, y = linking_table[,c("permno","GVKEY","cusip")], by = "permno", all.x = TRUE)

unique_cusips <- unique(split_df_4$cusip)

unique_cusips_2 <- matrix(unique_cusips,ncol = 1)

# write.table(unique_cusips_2, file="unique_cusips_2.txt", append = FALSE,row.names = FALSE, col.names = FALSE, quote = FALSE)

#WRDS: Capital IQ - Key Developments, 01.01.2000 - 21.11.2020 
# - used linked- cusips from linking tool to get all the company announcements related to the LPERMNO's which have seen a stock split
#@https://wrds-web.wharton.upenn.edu/wrds//ds/comp/ciq/keydev/index.cfm

confounding_df <- read.csv("confounding.csv")

colnames(confounding_df)[4]<-"declaration_date"

colnames(confounding_df)[6]<-"GVKEY"

split_df_4$declaration_date<-as.numeric(as.character(split_df_4$declaration_date))

merged_df<-merge(x = split_df_4, y = confounding_df, by = c("declaration_date","GVKEY"), all.x = TRUE)

merged_df_2<-unique(merged_df)

key_development_lookup <- read.csv2("key_development_lookup.csv")

colnames(key_development_lookup)[1] <- "keydeveventtypeid"

merged_df_3<-merge(x = merged_df_2, y = key_development_lookup, by = "keydeveventtypeid", all.x = TRUE)

# write.csv2(merged_df_3, file = "C:\\Users\\juzu\\Desktop\\MBF\\Research Seminar Corporate Finance\\data\\split_events_merged.csv",row.names = FALSE)

non_confounding_df<- merged_df_3[is.na(merged_df_3$keydevid),]
#removing all cofounding events / selecting only splits with no confounding events

# SHORTCUT ----------------------------------------------------------------
#--------------------------------------------------------------------------
merged_df_3<-read.csv2("split_events_merged.csv")

results_df<-read.csv2("results_df.csv")
#--------------------------------------------------------------------------
#--------------------------------------------------------------------------


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

non_confounding_df$declaration_date <- as.Date(format(non_confounding_df$declaration_date, format = '%Y%m%d'), format = '%Y%m%d')

non_confounding_df$start_date <- non_confounding_df$declaration_date-250

non_confounding_df$end_date <-non_confounding_df$declaration_date+10

non_confounding_df$permno <- as.numeric(as.character(non_confounding_df$permno))

a<- as.character(non_confounding_df$permno[1])

b<- as.character(non_confounding_df$start_date[1])

c<- as.character(non_confounding_df$end_date[1])


d<-sprintf("select ret
           from crsp.dsf
           where permno = '%s'
           and date between '%s'
           and '%s'", a,b,c)

res <- dbSendQuery(wrds, d)

data <- dbFetch(res, n=-1)
dbClearResult(res)

get_stock_return <- function(data.frame) {
  data_1<- tryCatch(
    {
      
      a<- as.character(data.frame$permno[1])
      
      b<- as.character(data.frame$start_date[1])
      
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

return_df <- matrix(data=NA, nrow= 185, ncol= nrow(non_confounding_df))

for (i in 1:nrow(non_confounding_df)){
  sub_df <- non_confounding_df[i,]
  results <- get_stock_return(sub_df)
  results_formated <- c(results$ret,rep(NA,185-length(results$ret)))
  return_df[,i] <- results_formated
}

head(return_df,10)
# 
# res <- dbSendQuery(wrds, "select date,cusip,permno,ret,retx
#                    from crsp.dsf
#                    where permno = '14593'
#                    and date between '2010-01-01'
#                    and '2011-01-01'")
results_df <- as.data.frame(return_df)

head(results_df)

# write.csv2(results_df, file = "results_df.csv",row.names = FALSE)