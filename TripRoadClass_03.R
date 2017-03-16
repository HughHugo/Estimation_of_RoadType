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
for (i in 1:length(days)){
  if (i == 1){
    sparkR.stop()
  }
  sc <- sparkR.init(appName="TripRoadClass")
  sqlContext <- sparkRSQL.init(sc)
  hiveContext <- sparkRHive.init(sc)
  SparkR:::includePackage(sqlContext, 'lubridate')
  SparkR:::includePackage(sqlContext, 'XML')
  day.data <- sql(hiveContext, paste("SELECT deviceid,tripnumber,time_stamp,lat,lon,heading,speed,acc_normal FROM trip_data_ciitc WHERE stat_date=\'", 
                                     days[i], "\'", sep = ''))
  
  
  day.data.rdd <- SparkR:::toRDD(day.data)
  
  trip.row <- SparkR:::lapplyPartition(day.data.rdd, 
                                       function(x){
                                         
                                         GPSDist <- function(Lat,Lon,Lat0,Lon0){
                                           Lat <- Lat*pi/180;Lon<-Lon*pi/180;Lat0<-Lat0*pi/180;Lon0<-Lon0*pi/180
                                           m <- 2*6378.178*asin(sqrt(
                                             cos(Lat0)*cos(Lat)*(sin((Lon-Lon0)/2))^2+
                                               (sin((Lat-Lat0)/2))^2
                                           ))
                                           return(m)
                                         }
                                         
                                         RoadClass <- function(trip.data, gap = 20){
                                           #*********************************************
                                           #**Query the road class of a trip trajectory**
                                           #*********************************************
                                           #trip.data includes lon,lat,time_stamp,direction,speed
                                           names(trip.data) <- c('lon', 'lat', 'time_stamp', 'heading', 'speed')
                                           trip.data <- trip.data[order(trip.data$time_stamp, decreasing = F),]
                                           url.head <- 'http://restapi.amap.com/v3/autograsp?carid=65e81234567&locations='
                                           url.tail <- '&output=xml&key=196506b424d4b7983d6c6a0a358165e8'
                                           n <- nrow(trip.data)
                                           lb <- seq(from = 1, to = n, by = gap)
                                           if (n%%gap == 0){
                                             ub <- seq(from = gap, to = n, by = gap)
                                           } else{
                                             ub <- c(seq(from = gap, to = n, by = gap), n)
                                           }
                                           cal.mat <- data.frame(lb = lb, ub = ub)
                                           
                                           #XML method
                                           require(XML)
                                           getRoad <- function(node) {
                                             require(XML)
                                             value <- unlist(lapply(xmlChildren(node),xmlValue))
                                             mat <- matrix(value, nrow = 1)
                                             return(mat)
                                           }
                                           xml.data <- data.frame()
                                           for (i in 1:nrow(cal.mat)){
                                             require(XML)
                                             url.data <- paste(paste(apply(trip.data[cal.mat[i,1]:cal.mat[i,2], c('lon', 'lat')], 1,
                                                                           function(x) paste(x[1],x[2],sep=',')),collapse = '|'),
                                                               '&time=', paste(trip.data[cal.mat[i,1]:cal.mat[i,2], 'time_stamp'], collapse = ','),
                                                               '&direction=', paste(trip.data[cal.mat[i,1]:cal.mat[i,2], 'heading'], collapse = ','),
                                                               '&speed=', paste(trip.data[cal.mat[i,1]:cal.mat[i,2], 'speed'], collapse = ','),
                                                               sep = '')
                                             url.road <- paste(url.head, url.data, url.tail, sep = '')
                                             parsed_url <- xmlParse(url.road)
                                             xml.values <- as.data.frame(t(xpathSApply(parsed_url, '//response/roads/road', fun = getRoad)), stringsAsFactors = F)
                                             if (ncol(xml.values) == 0){
                                               xml.values <- as.data.frame(matrix(NA, ncol = 6, nrow = cal.mat[i,2]-cal.mat[i,1]+1))
                                             }
                                             xml.data <- rbind(xml.data, xml.values)
                                           }
                                           
                                           names(xml.data) <- c('roadname', 'crosspoint', 'roadlevel', 'maxspeed', 'intersection', 'intersectiondistance')
                                           xml.data$roadlevel <- as.numeric(xml.data$roadlevel)
                                           return(xml.data)
                                         }
                                         
                                         part <- as.data.frame(matrix(unlist(x), ncol = 8,byrow = T), stringsAsFactors = F)
                                         names(part) <- c('deviceid','tripnumber','time_stamp','lat','lon','heading','speed','acc_sd')
                                         part <- apply(part, 2, as.numeric)
                                         part <- part[apply(part, 1, function(x) all(!is.na(x))),]
                                         part <- as.data.frame(part)
                                         part <- part[which(part$lat != 0),]
                                         user.tripnumber <- unique(part[, c('deviceid', 'tripnumber')])
                                         partition.data <- data.frame()
                                         for (j in 1:nrow(user.tripnumber)){
                                           trip.data <- part[part$deviceid==user.tripnumber[j,1]&part$tripnumber==user.tripnumber[j,2],]
                                           n <- nrow(trip.data)
                                           if (n < 60){
                                             next
                                           }
                                           trip.data <- trip.data[order(trip.data$time_stamp, decreasing = F),]
                                           trip.data <- unique(trip.data)
                                           trip.road <- RoadClass(trip.data[,c('lon', 'lat', 'time_stamp', 'heading', 'speed')])
                                           trip.road <- as.data.frame(cbind(trip.road$roadname, 
                                                                            do.call('rbind', lapply(trip.road$crosspoint, 
                                                                                                    function(x){
                                                                                                      if (is.na(x)){
                                                                                                        cp <- c(0,0)
                                                                                                        } else {
                                                                                                          cp <- as.numeric(unlist(strsplit(x, ',')))
                                                                                                          }
                                                                                                      return(cp)})), 
                                                                            trip.road[,3:6]), stringAsFactors = F)
                                           names(trip.road) <- c('roadname', 'crosspoint_lon', 'crosspoint_lat', 'roadlevel', 'maxspeed', 'intersection', 'intersectiondistance')
                                           trip.road$roadname <- as.character(trip.road$roadname)
                                           trip.data <- as.data.frame(cbind(trip.data, trip.road), stringsAsFactors = F)
                                           
                                           
                                           trip.data[is.na(trip.data$roadlevel) | trip.data$roadlevel == -1 | trip.data$roadlevel == 49, 'roadlevel'] <- 0
                                           roadclass.rle <- rle(trip.data$roadlevel)
                                           if (length(roadclass.rle$values) == 1){
                                             trip.data$roadlevel.fix <- NA
                                             next
                                           }
                                           roadclass.miss <- which(roadclass.rle$values == 0)
                                           for (jj in roadclass.miss){
                                             if (jj == 1){
                                               roadclass.rle$lengths[jj+1] <- roadclass.rle$lengths[jj+1] + roadclass.rle$lengths[jj]
                                               roadclass.rle$lengths[jj] <- 0
                                             } else if (jj == length(roadclass.rle$lengths)){
                                               roadclass.rle$lengths[jj-1] <- roadclass.rle$lengths[jj-1] + roadclass.rle$lengths[jj]
                                               roadclass.rle$lengths[jj] <- 0
                                             } else {
                                               roadclass.rle$lengths[jj-1] <- roadclass.rle$lengths[jj-1] + ceiling(roadclass.rle$lengths[jj]/2)
                                               roadclass.rle$lengths[jj+1] <- roadclass.rle$lengths[jj+1] + floor(roadclass.rle$lengths[jj]/2)
                                               roadclass.rle$lengths[jj] <- 0
                                             }
                                           }
                                           trip.data$roadlevel.fix <- as.character(inverse.rle(roadclass.rle))
                                           
                                           partition.data <- as.data.frame(rbind(partition.data, trip.data), stringsAsFactors = F)
                                         }
                                         partition.data <- split(partition.data, 1:nrow(partition.data))
                                         return(partition.data)
                                       }
  )
  
  SparkR:::saveAsTextFile(trip.row, paste("/user/kettle/obdbi/original/ciitc/roads/stat_date=", days[i], sep = ''))
  
  trip.road.name <- c('deviceid', 'tripnumber', 'time_stamp', 'lat', 'lon','heading', 'speed', 'acc_normal', 'roadname', 'crosspoint_lon', 'crosspoint_lat', 
                      'roadlevel', 'maxspeed', 'intersection', 'intersectiondistance', 'roadlevel_fix')
  sql(hiveContext, "DROP TABLE IF EXISTS trip_road_temp")
  sql(hiveContext, paste("CREATE EXTERNAL TABLE IF NOT EXISTS trip_road_temp(", 
                         paste(trip.road.name, " String", collapse = ",\n", sep = ''), 
                         ")", 
                         "ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\'", 
                         paste("LOCATION \'/user/kettle/obdbi/original/ciitc/roads/stat_date=", days[i], "\'", sep = ''), 
                         sep = "\n"))
  sql(hiveContext, paste("INSERT OVERWRITE TABLE trip_road_temp SELECT ", 
                         paste(paste('trim(',trip.road.name,') as ', trip.road.name, sep = ''), collapse = ', '), 
                         " FROM trip_road_temp", sep = ''))
  sql(hiveContext, "DROP TABLE IF EXISTS trip_road_temp")
  sql(hiveContext, paste("CREATE EXTERNAL TABLE IF NOT EXISTS trip_road_temp(", 
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
                         "roadlevel_fix INT)", 
                         "ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\'", 
                         paste("LOCATION \'/user/kettle/obdbi/original/ciitc/roads/stat_date=", days[i], "\'", sep = ''),
                         sep = "\n"))
  
  day.road.data <- sql(hiveContext, paste("SELECT ", paste(trip.road.name, collapse = ','), " FROM trip_road_temp", sep = ''))
  
  
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
                                                     interval.time <- apply(interval.matrix, 1, function(x) length(which(series>=x[1] & series<x[2])))
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
                                                     road.class.index <- c(41000,42000,43000,44000,45000,47000,51000,52000,53000,54000)
                                                     
                                                     speed.feature <- c(trip.data[1,'deviceid'], 
                                                                        trip.data[1,'tripnumber'], 
                                                                        nrow(trip.data), sum(trip.data$am), 
                                                                        mean(trip.data$speed, na.rm = T), 
                                                                        sd(trip.data$speed, na.rm = T), 
                                                                        quantile(trip.data$speed, (0:20)/20, na.rm = T), 
                                                                        t(IntervalTime(trip.data$speed, name = 'speed', min = 0, max = 150, 
                                                                                       gap = 10, inf.plus = T, inf.minus = F)), 
                                                                        unlist(lapply(1:24,function(x) sum(trip.data[which(trip.data$period==x),'am'], na.rm = T))), 
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
