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
library(magrittr)
for (i in 1:length(days)){
  if (i == 1){
    sparkR.stop()
  }
  sc <- sparkR.init(appName="TripRoadClass")
  sqlContext <- sparkRSQL.init(sc)
  hiveContext <- sparkRHive.init(sc)
  
  trip.road.name <- c('deviceid', 'tripnumber', 'time_stamp', 'lat', 'lon','heading', 'speed', 'acc_normal', 'roadname', 'crosspoint_lon', 'crosspoint_lat', 
                      'roadlevel', 'maxspeed', 'intersection', 'intersectiondistance', 'roadlevel_fix')
  sql(hiveContext, "DROP TABLE IF EXISTS trip_road_temp")
  sql(hiveContext, paste("CREATE EXTERNAL TABLE IF NOT EXISTS trip_road_temp(", 
                         paste(trip.road.name, " String", collapse = ",\n", sep = ''), 
                         ")", 
                         "PARTITIONED BY (stat_date string)", 
                         "ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\'", 
                         "LOCATION \'/user/kettle/obdbi/original/ciitc/roads\'",
                         sep = "\n"))
  sql(hiveContext, paste("ALTER TABLE trip_road_temp add PArtition (stat_date=\'", days[i], "\')", sep = ''))
  sql(hiveContext, paste("INSERT OVERWRITE TABLE trip_road_temp SELECT ", 
                         paste(paste('trim(',trip.road.name,') as ', trip.road.name, sep = ''), collapse = ', '), 
                         " FROM trip_road_temp", sep = ''))
  #sql(hiveContext, "DROP TABLE IF EXISTS trip_road_temp")
  SparkR:::sparkR.stop()
}




