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
sql(hiveContext, "DROP TABLE IF EXISTS trip_road")
sql(hiveContext, paste("CREATE EXTERNAL TABLE IF NOT EXISTS trip_road(", 
                       "deviceid BIGINT,", 
                       "tripnumber INT,", 
                       "time_stamp INT,", 
                       "lat DOUBLE,", 
                       "lon DOUBLE,", 
                       "heading SMALLINT,", 
                       "speed SMALLINT,", 
                       "acc_normal DOUBLE,", 
                       "roadname String,", 
                       "crosspoint_lon DOUBLE,", 
                       "crosspoint_lat DOUBLE,", 
                       "roadlevel INT,", 
                       "maxspeed SMALLINT,", 
                       "intersection String,", 
                       "intersectiondistance SMALLINT,", 
                       "roadlevel_fix INT", 
                       ")\nPARTITIONED BY(\nstat_date string\n)", 
                       "ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\'", 
                       "LOCATION \'/user/kettle/obdbi/original/ciitc/roads\'", 
                       sep = '\n'))

for (i in 1:length(days)){
  sql(hiveContext, paste("ALTER TABLE trip_road ADD PARTITION (stat_date=\'", 
                         days[i], "\')", sep = ''))
}
SparkR:::sparkR.stop()

for (i in 1:length(days)){
  if (i == 1){
    sparkR.stop()
  }
  sc <- sparkR.init(appName="TripRoadClass")
  sqlContext <- sparkRSQL.init(sc)
  hiveContext <- sparkRHive.init(sc)
  SparkR:::includePackage(sqlContext, 'lubridate')
  
  trip.road.name <- c('deviceid', 'tripnumber', 'time_stamp', 'lat', 'lon', 'heading', 'speed', 'acc_normal', 'roadname', 'crosspoint_lon', 
                      'crosspoint_lat', 'roadlevel', 'maxspeed', 'intersection', 'intersectiondistance', 'roadlevel_fix')
  day.road.data <- sql(hiveContext, paste("SELECT ", paste(trip.road.name, collapse = ','), " FROM trip_road WHERE stat_date=\'", 
                                          days[i], "\'", sep = ''))
  day.road.data.rdd <- SparkR:::toRDD(day.road.data)
  
  trip.road.features <- SparkR:::lapplyPartition(day.road.data.rdd, 
                                                 function(x){
                                                   require(lubridate)
                                                   GPSDist <- function(Lat,Lon,Lat0,Lon0){
                                                     Lat <- Lat*pi/180;Lon<-Lon*pi/180;Lat0<-Lat0*pi/180;Lon0<-Lon0*pi/180
                                                     m <- 2*6378.178*asin(sqrt(
                                                       cos(Lat0)*cos(Lat)*(sin((Lon-Lon0)/2))^2+
                                                         (sin((Lat-Lat0)/2))^2
                                                     ))
                                                     return(m)
                                                   }
                                                   
                                                   IntervalTime <- function(series, name, min, max , gap, inf.plus=T, inf.minus=F){
                                                     if (inf.plus & !inf.minus){
                                                       interval.matrix <- cbind(seq(min,max,gap),c(seq(min+gap,max,gap),Inf))
                                                     } else if (inf.plus & inf.minus){
                                                       interval.matrix <- cbind(c(-Inf,seq(min,max,gap)),c(seq(min,max,gap),Inf))
                                                     } else if (!inf.plus & inf.minus){
                                                       interval.matrix <- cbind(c(-Inf,seq(min,max-gap,gap)),seq(min,max,gap))
                                                     } else {
                                                       interval.matrix <- cbind(seq(min,max-gap,gap),seq(min+gap,max,gap))
                                                     }
                                                     interval.time <- apply(interval.matrix, 1, function(x) length(which(series>x[1] & series<=x[2])))
                                                     names(interval.time) <- paste(name, interval.matrix[,1], interval.matrix[,2], sep='_')
                                                     return(interval.time)
                                                   }
                                                   
                                                   RoadFeature <- function(road.data){
                                                     if (nrow(road.data) < 30){
                                                       road.feature <- rep(NA, 8)
                                                       return(road.feature)
                                                     }
                                                     names(road.data) <- c('speed', 'acc')
                                                     road.feature <- c(mean(road.data$speed, na.rm = T), 
                                                                       quantile(road.data$speed, (0:4)/4, na.rm = T), 
                                                                       sd(road.data$speed, na.rm = T), 
                                                                       sd(road.data$acc, na.rm = T))
                                                     return(road.feature)
                                                   }
                                                   
                                                   
                                                   part <- as.data.frame(matrix(unlist(x), ncol = 16,byrow = T), stringsAsFactors = F)
                                                   names(part) <- c('deviceid', 'tripnumber', 'time_stamp', 'lat', 'lon','heading', 'speed', 'acc_normal', 'roadname', 'crosspoint_lon', 
                                                                    'crosspoint_lat', 'roadlevel', 'maxspeed', 'intersection', 'intersectiondistance', 'roadlevel_fix')
                                                   part <- part[apply(part[,1:8], 1, function(x) all(!is.na(x))),]
                                                   part <- as.data.frame(part, stringsAsFactors = F)
                                                   numeric.names <- c('deviceid', 'tripnumber', 'time_stamp', 'lat', 'lon','heading', 'speed', 'acc_normal', 'crosspoint_lon', 
                                                                      'crosspoint_lat', 'roadlevel', 'maxspeed', 'intersectiondistance', 'roadlevel_fix')
                                                   part[,numeric.names] <- apply(part[,numeric.names], 2, as.numeric)
                                                   part <- part[which(part$lat != 0),]
                                                   user.tripnumber <- unique(part[, c('deviceid', 'tripnumber')])
                                                   trip.feature.partition <- data.frame()
                                                   for (j in 1:nrow(user.tripnumber)){
                                                     trip.data <- part[part$deviceid==user.tripnumber[j,1]&part$tripnumber==user.tripnumber[j,2],]
                                                     n <- nrow(trip.data)
                                                     if (n < 60){
                                                       next
                                                     }
                                                     trip.data <- trip.data[order(trip.data$time_stamp, decreasing = F),]
                                                     trip.data <- unique(trip.data)
                                                     trip.data$am <- c(GPSDist(trip.data[2:nrow(trip.data), 'lat'], 
                                                                               trip.data[2:nrow(trip.data), 'lon'], 
                                                                               trip.data[1:(nrow(trip.data)-1), 'lat'], 
                                                                               trip.data[1:(nrow(trip.data)-1), 'lon']), 
                                                                       0)
                                                     trip.data$period <- hour(as.POSIXlt(trip.data$time_stamp, origin='1970-01-01 00:00:00'))
                                                     trip.data[trip.data$roadlevel_fix == 41000, 'roadlevel_fix'] <- 1
                                                     trip.data[trip.data$roadlevel_fix == 43000, 'roadlevel_fix'] <- 2
                                                     trip.data[trip.data$roadlevel_fix %in% c(42000,44000,51000,52000,53000), 'roadlevel_fix'] <- 3
                                                     trip.data[trip.data$roadlevel_fix %in% c(45000,47000,54000), 'roadlevel_fix'] <- 4
                                                     road.class.index <- 1:4
                                                     
                                                     speed.feature <- c(trip.data[1,'deviceid'], 
                                                                        trip.data[1,'tripnumber'], 
                                                                        nrow(trip.data), sum(trip.data$am), 
                                                                        mean(trip.data$speed, na.rm = T), 
                                                                        sd(trip.data$speed, na.rm = T), 
                                                                        sd(trip.data$acc_normal, na.rm = T),
                                                                        quantile(trip.data$speed, (0:20)/20, na.rm = T), 
                                                                        length(which(trip.data$speed==0)), 
                                                                        t(IntervalTime(trip.data$speed, name = 'speed', min = 0, max = 150, 
                                                                                       gap = 10, inf.plus = T, inf.minus = F)), 
                                                                        unlist(lapply(0:23,function(x) sum(trip.data[which(trip.data$period==x),'am'], na.rm = T))), 
                                                                        unlist(lapply(road.class.index, function(x) length(which(trip.data$roadlevel_fix==x)))), 
                                                                        unlist(lapply(road.class.index, 
                                                                                      function(x) sum(trip.data[trip.data$roadlevel_fix==x, 'am'], na.rm = T))), 
                                                                        unlist(lapply(road.class.index, 
                                                                                      function(x) RoadFeature(trip.data[trip.data$roadlevel_fix==x, c('speed', 'acc_normal')])))
                                                     )
                                                     
                                                     trip.feature.partition <- rbind(trip.feature.partition, speed.feature)
                                                   }
                                                   
                                                   trip.feature.partition <- split(trip.feature.partition, 1:nrow(trip.feature.partition))
                                                   return(trip.feature.partition)
                                                 }
  )
  
  SparkR:::saveAsTextFile(trip.road.features, paste("/user/kettle/obdbi/original/ciitc/roads/features/stat_date=", days[i], sep = ''))
  
  
  road.class.index <- 1:4
  day.roadlevel.name <- c('deviceid', 'tripnumber', 'duration', 'mileage', 'speed_mean', 'speed_sd', 'acc_sd', 
                          paste('speed',seq(0,100,5),'pct',sep='_'), 
                          'duration_still', 
                          paste('speed',seq(0,150,10),c(seq(10,150,10),Inf),sep='_'), 
                          paste('m',0:23,1:24,sep='_'), paste('road', road.class.index, 'duration', sep = '_'), 
                          paste('road', road.class.index, 'mileage', sep = '_'), 
                          unlist(lapply(paste('road', road.class.index, sep='_'), 
                                        function(x){
                                          paste(x, c('speed_mean', paste('speed',seq(0,100,25),sep='_'),'speed_sd','acc_sd'), sep = '_')
                                        }))
                          )
  sql(hiveContext, "DROP TABLE IF EXISTS trip_roadlevel_temp")
  sql(hiveContext, paste("CREATE EXTERNAL TABLE IF NOT EXISTS trip_roadlevel_temp(", 
                         paste(day.roadlevel.name, " String", collapse = ",\n", sep = ''), 
                         ")", 
                         "ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\'", 
                         paste("LOCATION \'/user/kettle/obdbi/original/ciitc/roads/features/stat_date=", days[i], "\'", sep = ''), 
                         sep = "\n"))
  sql(hiveContext, paste("INSERT OVERWRITE TABLE trip_roadlevel_temp SELECT ", 
                         paste(paste('trim(',day.roadlevel.name,') as ', day.roadlevel.name, sep = ''), collapse = ', '), 
                         " FROM trip_roadlevel_temp", sep = ''))
  sql(hiveContext, "DROP TABLE IF EXISTS trip_roadlevel_temp")
  
  SparkR:::sparkR.stop()
}
