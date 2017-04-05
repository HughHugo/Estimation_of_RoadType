load('F:/data/¸ßËÙÔ¤²â/trip_road_features_20170322.Rdata')
trip.features <- trip.features[which(trip.features$mileage/(trip.features$speed_mean*trip.features$duration/3600)<1.5),]




