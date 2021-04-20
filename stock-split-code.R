# ---------------------------------------------
# Research Seminar Corporate Finance : Group 6
#----------------------------------------------


# Working Directory --------------------------------------------------------------------------------------

# If no R-project is set up:
# getwd()
# setwd("C:/Users/XXXX/XXXX)


# Packages & Libraries -----------------------------------------------------------------------------------

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

#Due to size of data sets load in files for 2010 - 2020 and for 2000 - 2010  seperately with ff packages

raw_data.ff <- read.table.ffdf(file="2010_2020_cusip.csv", sep=",")

colnames_wrds <- c("permno","date", "share_code","exchange_code","ticker","company_name","share_class",
                   "primary_exchange","permco","header_exchange_code","declaration_date","payment_date",
                   "distribution_code","factor_to_adjust_price_FACPR","factor_to_adjust_shares_FACSHR",
                   "share_price","trading_volume","shares_oustanding_in_ks", "number_of_trades")
df_splits_2010_2020=raw_data.ff[raw_data.ff$distribution_code==5523,]

#table.ff(df_splits_2010_2020$distribution_code)
#5523 = (reverse) stock split without dividend:
#CRSP guide:
#p 15: reverse splits 
#p. 130: split

df_2010_2020<- as.data.frame(df_splits_2010_2020)
raw_data.ff2 <- read.table.ffdf(file="2000_2010.csv", sep=",")
colnames(raw_data.ff2) <- colnames_wrds
df_splits_2000_2010 <- raw_data.ff2[raw_data.ff2$distribution_code==5523,]
df_2000_2010<- as.data.frame(df_splits_2000_2010)

#Merged dataset
split_df <- rbind(df_2000_2010,df_2010_2020)
#write.csv(split_df,"stocksplit_df_raw.csv",row.names=FALSE)

rm(raw_data.ff)
rm(raw_data.ff2)

# Shortcut to prepared data ------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------
split_df<-read.csv2("stocksplit_df_raw.csv")
# --------------------------------------------------------------------------------------------------------

split_df_2 <- split_df[(!is.na(split_df$declaration_date)) & (split_df$declaration_date != ""),]
#only splits where the delcaration date / "announcement date" was recorded
split_df_2$year <- as.numeric(substr(split_df_2$declaration_date, 1, 4))
split_df_3 <- split_df_2[(split_df_2$year>=2010) & (split_df_2$year<=2019),]
#focus on 2010 - 2019 to avoid distortions due to the GFC and COVID 19
split_df_4 <- split_df_3[split_df_3$share_code==11,]
#only common shares - as in empirical literature

unique_permnos<-unique(split_df_2$permno)
#write.table(unique_permnos,"permnos.txt",row.names = FALSE, col.names = FALSE, quote = FALSE)

#Later the extracted permnos will be used to download the lookup table / linking table
#THe linking table will then later match the stock splits to the confounding events

#Expanding event window around declaration date to clear for all confounding events in 10 day period around split announcement:

split_df_4$declaration_date <- as.Date(format(split_df_4$declaration_date, format = '%Y%m%d'), format = '%Y%m%d')
#After announcement date
split_df_4$declaration_date_1_post <- split_df_4$declaration_date+1
split_df_4$declaration_date_2_post <- split_df_4$declaration_date+2
split_df_4$declaration_date_3_post <- split_df_4$declaration_date+3
split_df_4$declaration_date_4_post <- split_df_4$declaration_date+4
split_df_4$declaration_date_5_post <- split_df_4$declaration_date+5
#Before announcement date
split_df_4$declaration_date_1_pre <- split_df_4$declaration_date-1
split_df_4$declaration_date_2_pre <- split_df_4$declaration_date-2
split_df_4$declaration_date_3_pre <- split_df_4$declaration_date-3
split_df_4$declaration_date_4_pre <- split_df_4$declaration_date-4
split_df_4$declaration_date_5_pre <- split_df_4$declaration_date-5

split_df_4$declaration_date<-as.numeric(format(split_df_4$declaration_date, format = '%Y%m%d'))
#After announcement date
split_df_4$declaration_date_1_post<-as.numeric(format(split_df_4$declaration_date_1_post, format = '%Y%m%d'))
split_df_4$declaration_date_2_post<-as.numeric(format(split_df_4$declaration_date_2_post, format = '%Y%m%d'))
split_df_4$declaration_date_3_post<-as.numeric(format(split_df_4$declaration_date_3_post, format = '%Y%m%d'))
split_df_4$declaration_date_4_post<-as.numeric(format(split_df_4$declaration_date_4_post, format = '%Y%m%d'))
split_df_4$declaration_date_5_post<-as.numeric(format(split_df_4$declaration_date_5_post, format = '%Y%m%d'))
#Before announcement date
split_df_4$declaration_date_1_pre <-as.numeric(format(split_df_4$declaration_date_1_pre, format = '%Y%m%d'))
split_df_4$declaration_date_2_pre <-as.numeric(format(split_df_4$declaration_date_2_pre, format = '%Y%m%d'))
split_df_4$declaration_date_3_pre <-as.numeric(format(split_df_4$declaration_date_3_pre, format = '%Y%m%d'))
split_df_4$declaration_date_4_pre <-as.numeric(format(split_df_4$declaration_date_4_pre, format = '%Y%m%d'))
split_df_4$declaration_date_5_pre <-as.numeric(format(split_df_4$declaration_date_5_pre, format = '%Y%m%d'))


# Confounding Events -------------------------------------------------------------------------------------

#Elimminating effects that could affect stock returns on announcement date / in event window:

#PERMNO's which conducted stocksplits (2010 - 2019) and where the declaration date is available (see unique_permnos) 
#were loaded into the linking table to receive the respective GVKEY

#Linked with CRSP/Compustat Merged Database (under CRSP) - Linking Table (to  GVKEY)
#@https://wrds-web.wharton.upenn.edu/wrds//ds/crsp/ccm_a/linktable/index.cfm#

linking_table <- read.csv("linking_table_new.csv")
colnames(linking_table)[3]<-"permno"
colnames(linking_table)[8]<-"ticker"

linking_table_small<-linking_table[,c("GVKEY","ticker","permno")]
linking_table_small_u<-unique(linking_table_small<-linking_table[,c("GVKEY","ticker","permno")])
#Matches  split data frame from PERMNO to the GVKEY identifiers

split_df_5<-merge(x = split_df_4, y = linking_table_small_u, by = c("permno","ticker"), all.x = TRUE)
split_df_6 <- na.omit(split_df_5)

#unique_GVKEY <-unique(linking_table_small_u$GVKEY)
#write.table(unique_GVKEY,"gvkey_confounding.txt",row.names = FALSE, col.names = FALSE, quote = FALSE)

#WRDS: Capital IQ - Key Developments, 01.01.2000 - 21.11.2020 
# - used linked- GVKEYS from linking tool to get all the company announcements / news related to the LPERMNO's which have seen a stock split
# - Capital IQ provides all the confounding events for the GVKEY in a data set

#*Note: The "confounding_df" includes all confounding events from 01.01.2000 - 21.11.2020 for all companies to which
#a link could be found with the methodology described above. So it contains more GVKEYS from splitting companies than the matched "splitd_df_6" 
#This data frame focuses on 2010-2019. The reason being,initially further analysis were conducted with the extended data set (2000-2020).
#However, due to the market shocks thos analysis were than neglected 
#To avoid downloading (timely) a redudant and smaller data set, the larger data frame (confounding_GVKEY.csv) was used.
#It obivously includes all the confounding events in 2010-2020 from all companies conducting stock splits.

#@https://wrds-web.wharton.upenn.edu/wrds//ds/comp/ciq/keydev/index.cfm

confounding_df <- read.csv("confounding_GVKEY.csv")
colnames(confounding_df)[4]<-"declaration_date"
colnames(confounding_df)[6]<-"GVKEY"

#key_development_lookup <- read.csv2("key_development_lookup.csv")
#colnames(key_development_lookup)[1] <- "keydeveventtypeid"
#MANUAL CHECK: WHICH EVENTS SHOULD BE EXCLUDED

confounding_df=confounding_df[!confounding_df$keydeveventtypeid %in% c(53,55,78,140,144),]
#Removing only superficial confounding events from the black list that will bear
#no information to the public (expected announcement date estimations, certain company meeting dates etc.)

#Merging all the dates (10 days around any announcement date of a stock split) with confounding events via  date and identifier (GVKEY)

merged_df_1<-left_join(x = split_df_6, y = confounding_df[,c(3,4,6)], by = c("declaration_date","GVKEY"))

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

rm(merged_df_1,merged_df_2,merged_df_3,merged_df_4,merged_df_5,merged_df_6,merged_df_7,merged_df_8,merged_df_9,merged_df_10)

#After looking up all confounding events (provided by Capital IQ) for each equity and mapping them around the 10-day event window for each stock split,
#stock splits with a match in their event window were removed from the data set.

indx <- apply(merged_df_11[,32:42], 1, function(x) all(is.na(x)))
confounding_10days <- merged_df_11[indx,]
confounding_10days_small <- confounding_10days[,c(1:19)]
#write.csv(confounding_10days,"confounding_10days.csv",row.names=FALSE)


# Further filter criteria --------------------------------------------------------------------------------

confounding_10days_small$market_cap <- 
  as.numeric(as.character(confounding_10days_small$shares_oustanding_in_ks))*1000*as.numeric(as.character(confounding_10days_small$share_price))
#Calculation of market cap [

non_conf_df <- confounding_10days_small[as.numeric(as.character(confounding_10days_small$factor_to_adjust_price_FACPR))>0.25,]
non_conf_large <- non_conf_df[non_conf_df$market_cap>0,]
#Only onsidering split ratios >1.25
#Only considering positive market cap (sanity check / safety)
#MCAP = TOGGLE FOR SMALLER DATA SAMPLE WITH LARGE MCAP / SAMPLE2]
final_df<-non_conf_large

#Due to insufficient return data (only 52 returns in estimation window - see below)"FARMERS & MERCHANTS BANCORP INC" is removed
final_df <- final_df[!final_df$ticker=="FMAO",]

#Conversion from factors to numeric variables
final_df$permco <- as.numeric(as.character(final_df$permco))
final_df$trading_volume <- as.numeric(as.character(final_df$trading_volume))
final_df$permno <- as.numeric(as.character(final_df$permno))


# Return / market data for the estimation of abnormal returns of the split titles ------------------------

#Market return during 2000-2020 (NYSE,AMEX,NASDAQ - value weighted)
#@https://wrds-web.wharton.upenn.edu/wrds//ds/crsp/indexes_a/mktindex/cap_d.cfm
#To accurately reflect the broad market which was initially considered, a value weighted market index of 
#all three main exchanges was considered as market index

market_return_df <- read.csv("market_returns.csv")
colnames(market_return_df)[1]<-"date"
market_return_df$date <- as.Date(format(market_return_df$date , format = '%Y%m%d'), format = '%Y%m%d')

#Daily stock returns / stock prices and traiding days: direct SQL querry from CRSP through WRDS 
#-> requires an account and storage of account name / PW on local file, see below:
#@https://wrds-www.wharton.upenn.edu/pages/support/programming-wrds/programming-r/r-from-your-computer/
#for the querry structure, see below
#@https://wrds-www.wharton.upenn.edu/pages/support/programming-wrds/programming-r/querying-wrds-data-r/#introduction


wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='dajulian')

#Sanity check to ensure that all stock split announcements happened on trading days

#Finding the trading days for the entire cross-section of 
#stocks listed on the New York Stock Exchange (NYSE), the
#American Stock Exchange (AMEX), and the Nasdaq National Market System (NMS)
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
  as.Date(format(final_df$declaration_date, format = '%Y%m%d'), format = '%Y%m%d')
  
final_df$declaration_date <- as.Date(format(final_df$declaration_date, format = '%Y%m%d'), format = '%Y%m%d')

# -------------------------------------------------------------------------
# Eventstudy --------------------------------------------------------------
# -------------------------------------------------------------------------

#In the following section the eventstudy is conducted. First the effective returns of the splitting stocks in the event window are gathered.
#Then the betas and alphas are calculated for the splitting stocks in the estimation window. With those parameters, and the market returns
#abnormal returns can be calculated (both in the estimation window - for variance estimation purposes and the event window).
#Finaly the the cumulative aggregated abnormal returns are used for regresions.

# Date preparation ---------------------------------------------------------------------------------------

#Check if all declaration dates are trading dates on the exchanges
all(final_df$declaration_date %in% dates_trades)

#Event window definition (query only works with calendar days)
final_df$start_date <- final_df$declaration_date-10
final_df$end_date <-final_df$declaration_date+10

#Start of estimation period (as far ahead as possible to ensure that enough trading days can be found to estimate beta)
final_df$est_start_date <-final_df$declaration_date-220


# Return gathering for event window ----------------------------------------------------------------------

get_stock_return_until_announcement <- function(data.frame) {
  data_1<- tryCatch(
    {
      
      a<- as.character(data.frame$permno[1])
      b<- as.character(data.frame$start_date[1])
      c<- as.character(data.frame$declaration_date[1])
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

return_df_before_announcement <- matrix(data=NA, nrow= 20, ncol= nrow(final_df))

for (i in 1:nrow(final_df)){
  sub_df <- final_df[i,]
  results <- get_stock_return_until_announcement(sub_df)
  results_formated <- c(results$ret,rep(NA,20-length(results$ret)))
  return_df_before_announcement[,i] <- results_formated
}

return_df_before_announcement <- as.data.frame(return_df_before_announcement)
return_dt_before_announcement <- data.table(return_df_before_announcement)
return_df_until_announcement<- as.data.frame(return_dt_before_announcement[, lapply(.SD, function(x) tail(x[!is.na(x)],6))])

return_df_after_announcement <- matrix(data=NA, nrow= 20, ncol= nrow(final_df))

for (i in 1:nrow(final_df)){
  sub_df <- final_df[i,]
  results <- get_stock_return_after_announcement(sub_df)
  results_formated <- c(results$ret,rep(NA,20-length(results$ret)))
  return_df_after_announcement[,i] <- results_formated
}

return_df_after_announcement <- as.data.frame(return_df_after_announcement)
return_df_after_announcement <- return_df_after_announcement[2:6,]

eventwindow_df <- rbind(return_df_until_announcement,return_df_after_announcement)
colnames(eventwindow_df)<-final_df$ticker
rownames(eventwindow_df)<- c("t=-5","t=-4","t=-3","t=-2","t=-1","t=0","t=+1","t=+2","t=+3","t=+4","t=+5")


# Beta estimation and abnormal return caluclation in estimation and event window--------------------------

get_stock_beta_from_estimation <- function(data.frame) {
  data_1<- tryCatch(
    {
      
      a<- as.character(data.frame$permno[1])
      b<- as.character(data.frame$est_start_date[1])
      c<- as.character(data.frame$declaration_date[1])
      d<-sprintf("select date,ret
                 from crsp.dsf
                 where permno = '%s'
                 and date between '%s'
                 and '%s'", a,b,c)
      
      res <- dbSendQuery(wrds, d)
      data <- dbFetch(res, n=-1)
      dbClearResult(res)
      data_with_gap <- head(data,-16)
      data_with_gap_120 <- tail(data_with_gap,120)
      rets<-merge(data_with_gap_120,market_return_df,by="date",all.x="true")
      regression<-lm(rets$ret ~ rets$vwretd)
      AR <- rets$ret - (regression$coefficients[1]+regression$coefficients[2]*rets$vwretd)
      AR2 <- c(AR,rep(NA,120-length(AR)))
      outList <- list("AR" = AR2,"COEF" =regression$coefficients)
      return(outList)
    },
    error = function(e){
      message('Error in the process')
      print(e)
      dbClearResult(res)
    }
    
    
      )
}


alphas_betas_df <- matrix(data=NA, nrow= 2, ncol= nrow(final_df))
abnormal_ret_estimw_df <- matrix(data=NA, nrow=120 , ncol= nrow(final_df))

for (i in 1:nrow(final_df)){
  sub_df <- final_df[i,]
  results <- get_stock_beta_from_estimation(sub_df)
  alphas_betas_df[,i] <- results$COEF
  abnormal_ret_estimw_df[,i] <- results$AR
}

alphas_betas_df <- as.data.frame(alphas_betas_df)

rownames(alphas_betas_df)<-c("alpha","beta")
colnames(alphas_betas_df)<-final_df$ticker
abnormal_ret_estimw_df <- as.data.frame(abnormal_ret_estimw_df)
colnames(abnormal_ret_estimw_df)<-final_df$ticker


# Variance estimation from estimation window -------------------------------------------------------------

ar_variances_EW<-apply(abnormal_ret_estimw_df,2,var)

ar_variances_EW_11<-11*ar_variances_EW
ar_variances_EW_7<- 7*ar_variances_EW
ar_variances_EW_3<- 3*ar_variances_EW

var_acar_11 <-sum(ar_variances_EW_11)/nrow(final_df)^2
var_acar_7 <-sum(ar_variances_EW_7)/nrow(final_df)^2
var_acar_3 <-sum(ar_variances_EW_3)/nrow(final_df)^2

sd_acar_11 <-var_acar_11^0.5
sd_acar_7 <-var_acar_7^0.5
sd_acar_3 <-var_acar_3^0.5
SD_s <- c(sd_acar_11,sd_acar_7,sd_acar_3)

market_model_df <- matrix(data=NA, nrow= 11, ncol= nrow(final_df))

for (i in 1:nrow(final_df)){
  sub_df <- final_df[i,]
  indx <- match(sub_df$declaration_date,market_return_df$date)
  m_returns <- market_return_df$vwretd[(indx-5):(indx+5)]
  est_returns<-m_returns*alphas_betas_df[2,i]+alphas_betas_df[1,i]
  market_model_df[,i]<-est_returns
}


# Tests / regressions and analysis --------------------------------------------------------------------------------

#Definition of different event windows [abnormal return data frames (eventwindow X stocks) = car_df]
car_df_orig <- eventwindow_df - market_model_df
car_df_3 <- car_df_orig[3:9,]
car_df_1 <- car_df_orig[5:7,]
list_1<-list(car_df_orig,car_df_3,car_df_1)

#Definition of different event windows
days1<- -5:5
days2<- -3:3
days3<- -1:1
list_2<-list(days1,days2,days3)

#Definition of different cumulative abnormal returns
CAR_11 <- apply(car_df_orig,2,sum)
CAR_7 <-apply(car_df_3,2,sum)
CAR_3 <-apply(car_df_1,2,sum)
list_3<-list(CAR_11,CAR_7,CAR_3)

EW_length<-c(11,7,3)

#T-tests to test for different eventwindows (CAR / CAAR)
for(a in 1:3){
  
  car_df<-as.data.frame(list_1[a])
  sd_ew <- SD_s[a]
  days_EW <- list_2[a]
  
  #Variance estimation based on cumulative abnormal returns
  AAR <- apply(car_df, 1, mean)
  AR_AAR_df<- (car_df - AAR)^2
  daily_variance<- apply(AR_AAR_df,1,sum)*(1/(nrow(final_df)-1))
  daily_sd<-daily_variance^0.5
  
  
  daily_t_Values<-AAR/(daily_sd/sqrt(nrow(final_df)))
  p_values_daily <- rep(NA,nrow(car_df))
  for(i in 1:nrow(car_df)){
    p_values_daily[i] <-2*pt(-abs(daily_t_Values[i]),df=nrow(final_df)-1)
  }
  #T.test on average cumulative abnormal return
  CAR_table_out<-as.data.frame(matrix(NA,ncol = 5, nrow = nrow(car_df)))
  colnames(CAR_table_out) <- c("Days","Average Abnormal Returns","Percentage of which positive","T-statistic","p-value")
  
  CAR_table_out[,1] <- days_EW
  CAR_table_out[,2] <- AAR
  CAR_table_out[,3] <- apply(car_df, 1, function(i) (sum(i>0)/ncol(car_df)))
  CAR_table_out[,4] <- daily_t_Values
  CAR_table_out[,5] <- p_values_daily
  
  CAR <- apply(car_df,2,sum)
  ACAR <- mean(CAR) #CAAR
  var_CAR<-(1/nrow(final_df)^2)*sum((CAR-ACAR)^2)
  sd_CAR<-var_CAR^0.5
  cum_t_val<-ACAR/sd_CAR
  cum_p_value<-2*pt(-abs(cum_t_val),df=nrow(final_df)-1)
  cum_t_val_with_acar_var<-ACAR/sd_ew
  cum_p_val_with_acar_var<-2*pt(-abs(cum_t_val_with_acar_var),df=nrow(final_df)-1)
  
  cum_stats <-rbind(ACAR,sd_CAR,cum_t_val,cum_p_value,sd_ew,cum_t_val_with_acar_var,cum_p_val_with_acar_var)
  rownames(cum_stats) <- c("Average cumulative abnormal return","SD","T-Statistic","p-value","SD Estimation window (AR's)","T-Statistic","p-value")
  
  b<-EW_length[a]
  
  name1<-"Abnorma_return_table_with_EW"
  name2<-"Cumulative_stats_with_EW"
  name11<-paste(name1, b, 'csv', sep = '.')
  name22<-paste(name2, b, 'csv', sep = '.')
  
  # write.csv(CAR_table_out,name11, row.names=FALSE)
  # write.csv(cum_stats,name22)
}

#Regression variables:
regression_df <- final_df
#1.1 Split factor
regression_df$factor_to_adjust_shares_FACSHR<-as.numeric(regression_df$factor_to_adjust_shares_FACSHR)+1
regression_df$two_for_one <- ifelse(regression_df$factor_to_adjust_shares_FACSHR==2,1,0)
regression_df$over_two_for_one <- ifelse(regression_df$factor_to_adjust_shares_FACSHR>2,1,0)
#1.2 Split target price
pre_split_stock_price<- rep(NA, nrow(regression_df))
get_stock_price_pre_split <- function(data.frame) {
  data_1<- tryCatch(
    {
      
      a<- as.character(data.frame$permno[1])
      b<- as.character(data.frame$est_start_date[1])
      c<- as.character(data.frame$declaration_date[1])
      d<-sprintf("select prc
                 from crsp.dsf
                 where permno = '%s'
                 and date between '%s'
                 and '%s'", a,b,c)
      
      res <- dbSendQuery(wrds, d)
      data <- dbFetch(res, n=-1)
      dbClearResult(res)
      data_with_gap <- head(data,-5)
      data_with_gap_1 <- tail(data_with_gap,1)
      return(data_with_gap_1)
    },
    error = function(e){
      message('Error in the process')
      print(e)
      dbClearResult(res)
    }
    
    
      )
}

for (i in 1:nrow(regression_df)){
  sub_df <- regression_df[i,]
  pre_split_stock_price[i] <- get_stock_price_pre_split(sub_df)
}
regression_df$pre_split_price <- as.numeric(pre_split_stock_price)
regression_df$post_split_target_price  <- regression_df$pre_split_price / regression_df$factor_to_adjust_shares_FACSHR
#1.4 Firm size
regression_df$equity_value <- regression_df$pre_split_price*as.numeric(regression_df$shares_oustanding_in_ks)*1000/regression_df$factor_to_adjust_shares_FACSHR

#1.4 Exchange
regression_df$regression_NSYE <- ifelse(regression_df$exchange_code==1,1,0)
regression_df$regression_AMEX <- ifelse(regression_df$exchange_code==2,1,0)

#Regressoin on different event windows (CAAR)
for(b in 1:3){
  c<-EW_length[b]
  regressand<-unlist(list_3[b])


  # -----------------------------------------------------------------------
  # 1.1 Split factor
  # -----------------------------------------------------------------------
  
  model_split_ratio <- lm(regressand ~ regression_df$two_for_one + regression_df$over_two_for_one)
                            
  summary(model_split_ratio)
  name_r<-paste("C:/Users/juzu/Desktop/MBF/Research Seminar Corporate Finance/", "model_split_ratio_",c,".html",sep = '')
  stargazer(model_split_ratio, type = "html", dep.var.labels=c("Cumulative Abnormal Return"),column.sep.width = "1pt",
            out = name_r)
  
  # -----------------------------------------------------------------------
  # 1.2 Split target price
  # -----------------------------------------------------------------------
  
  model_post_split_range <- lm(regressand ~ regression_df$post_split_target_price)
  name_r<-paste("C:/Users/juzu/Desktop/MBF/Research Seminar Corporate Finance/", "model_split_range_",c,".html",sep = '')
  stargazer(model_post_split_range, type = "html", dep.var.labels=c("Cumulative Abnormal Return"),column.sep.width = "1pt",
            out = name_r)
  
  # -----------------------------------------------------------------------
  # 1.3 Split factor -> pre split share price
  # -----------------------------------------------------------------------
  
  model_split_factor_EV <- lm(regression_df$factor_to_adjust_shares_FACSHR ~ regression_df$pre_split_price)
  stargazer(model_split_factor_EV, type = "html", dep.var.labels=c("Split factor"),column.sep.width = "1pt",
            out = "C:/Users/juzu/Desktop/MBF/Research Seminar Corporate Finance/model_split_factor_EV_nan.html")
  
  # -----------------------------------------------------------------------
  # 1.4 Firm Size
  # -----------------------------------------------------------------------
  
  model_size <- lm(regressand ~ log(regression_df$equity_value))
  name_r<-paste("C:/Users/juzu/Desktop/MBF/Research Seminar Corporate Finance/", "model_size_",c,".html",sep = '')
  stargazer(model_size, type = "html", dep.var.labels=c("Cumulative Abnormal Return"),column.sep.width = "1pt",
            out = name_r)
  
  # -----------------------------------------------------------------------
  # 1.5 Exchange
  # -----------------------------------------------------------------------
  
  model_exchange <- lm(regressand ~ regression_df$regression_NSYE)
  name_r<-paste("C:/Users/juzu/Desktop/MBF/Research Seminar Corporate Finance/", "model_exchange_",c,".html",sep = '')
  stargazer(model_exchange, type = "html", dep.var.labels=c("Cumulative Abnormal Return"),column.sep.width = "1pt",
            out = name_r)
  
  # -----------------------------------------------------------------------
  # 1.6 Multivariate
  # -----------------------------------------------------------------------
  
  model_multivariate <- lm(regressand ~ regression_df$post_split_target_price+regression_df$two_for_one + regression_df$over_two_for_one
                             log(regression_df$equity_value) + factor(regression_df$exchange_code))
  name_r<-paste("C:/Users/juzu/Desktop/MBF/Research Seminar Corporate Finance/", "model_multivariate_",c,".html",sep = '')
  stargazer(model_multivariate, type = "html", dep.var.labels=c("Cumulative Abnormal Return"),column.sep.width = "1pt",
            out = name_r)
}

