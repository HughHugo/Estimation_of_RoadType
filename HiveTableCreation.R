rm(list = ls())
args <- commandArgs(trailing = TRUE)   
if (length(args) != 2) {  
  print("Please input the start date;")
  print("Please input the last date.")
  q()
}
start.date <- args[[1]]
last.date <- args[[2]]
days <- as.character(format(seq(from=as.Date(start.date), to=as.Date(last.date), by='day'),'%Y%m%d'))
library(SparkR)
connectBackend.orig <- getFromNamespace('connectBackend', pos='package:SparkR')
connectBackend.patched <- function(hostname, port, timeout = 3600*48) {
  connectBackend.orig(hostname, port, timeout)
}
assignInNamespace("connectBackend", value=connectBackend.patched, pos='package:SparkR')
library(magrittr)
sc <- sparkR.init(appName="TripRoadClass")
sqlContext <- sparkRSQL.init(sc)
hiveContext <- sparkRHive.init(sc)
  
road.class.index <- c(41000,42000,43000,44000,45000,47000,51000,52000,53000,54000)
day.roadlevel.name <- c('deviceid', 'tripnumber', 'duration', 'mileage', 'speed_mean', 'speed_sd', 
                        paste('speed',seq(0,100,5),'pct',sep='_'), paste('speed',seq(0,150,10),c(seq(10,150,10),Inf),sep='_'), 
                        paste('m',0:23,1:24,sep='_'), paste('road', road.class.index, 'duration', sep = '_'), 
                        paste('road', road.class.index, 'mileage', sep = '_'), 
                        unlist(lapply(paste('road', road.class.index, sep='_'), 
                                      function(x){
                                        paste(x, c('speed_mean', paste('speed',seq(0,100,25),sep='_'),'speed_sd','acc_sd'), sep = '_')
                                      }))
)


sql(hiveContext, "DROP TABLE IF EXISTS trip_road_feature")
sql(hiveContext, paste("CREATE EXTERNAL TABLE IF NOT EXISTS trip_road_feature(", 
                       paste(paste(day.roadlevel.name[1], " BIGINT", sep = ''), 
                             paste(day.roadlevel.name[2:3], " INT", collapse = ',\n', sep = ''), 
                             paste(day.roadlevel.name[4:27], " DOUBLE", collapse = ',\n', sep = ''), 
                             paste(day.roadlevel.name[28:43], " INT", collapse = ',\n', sep = ''), 
                             paste(day.roadlevel.name[44:67], " DOUBLE", collapse = ',\n', sep = ''), 
                             paste(day.roadlevel.name[68:77], " INT", collapse = ',\n', sep = ''), 
                             paste(day.roadlevel.name[78:167], " DOUBLE", collapse = ',\n', sep = ''), 
                             sep = ',\n'), 
                       ")\nPARTITIONED BY(\nstat_date string\n)", 
                       "ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\'", 
                       "LOCATION \'/user/kettle/obdbi/original/ciitc/roads/features\'", 
                       sep = '\n'))
for (i in 1:length(days)){
  sql(hiveContext, paste("ALTER TABLE trip_road_feature ADD PARTITION (stat_date=\'", 
                         days[i], "\')", sep = ''))
}

SparkR:::sparkR.stop()

