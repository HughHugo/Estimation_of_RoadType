rm(list = ls())
gc()
road.class.index <- c(41000,42000,43000,44000,45000,47000,51000,52000,53000,54000)
road.class <- c('高速公路', '国道', '主要大街、城市快速路', '主要道路', '次要道路', '普通道路', 
                '省道', '乡公路', '县乡村内部道路1', '县乡村内部道路2')
road.class.table <- data.frame(road.class.index = road.class.index, road.class = road.class)
road.class.index <- c(41000,42000,43000,44000,45000,51000)
road.class <- c('高速公路', '国道', '主要大街、城市快速路', '主要道路', '次要道路', '省道')
road.class.table <- data.frame(road.class.index = road.class.index, road.class = road.class)

load('F:/data/高速预测/trip_road_features_20170316.Rdata')
trip.features.1 <- trip.features
load('F:/data/高速预测/trip_road_features_20170320.Rdata')
trip.features <- rbind(trip.features.1, trip.features)
trip.features <- trip.features[which(trip.features$mileage/(trip.features$speed_mean*trip.features$duration/3600)<1.5),]



##各类型行驶时长分布
duration <- lapply(road.class.index, 
                   function(x){
                     cnames <- paste("road_", x, "_duration", sep = '')
                     cbind(trip.features[which(trip.features[, cnames] > 0), cnames]/60, x)
                     }
                   )
duration <- as.data.frame(do.call('rbind', duration))
names(duration) <- c('duration', 'road.class.index')
duration <- merge(duration, road.class.table, by = 'road.class.index', all.x = T, all.y = F)

duration$road.type <- factor(duration$road.class)
library(ggplot2)
ggplot(duration, aes(x = duration, fill = road.class)) + geom_density(adjust = 1, alpha = 0.2) + xlim(0, 60) + 
  theme(legend.position = c(0.9,0.65), legend.background = element_blank(), legend.key = element_blank(), 
        legend.text = element_text(size = 8)) + guides(fill = guide_legend(title = NULL))
ggsave('F:/data/高速预测/pictures/duration.png', width = 36, height = 20, unit = 'cm', dpi = 1000)

library(plotly)
p <- plot_ly(data = duration, x = ~duration, color = ~road.class) %>% add_histogram()
plot_ly(duration, x = ~road.class, y = ~duration) %>% add_boxplot() %>% layout(yaxis = list(range = c(0, 120)))


##各类型时长比例分布
duration.ratio <- lapply(road.class.index, 
                         function(x){
                           cnames <- paste("road_", x, "_duration", sep = '')
                           cbind(100*trip.features[which(trip.features[, cnames] > 0), cnames]/
                                   trip.features[which(trip.features[, cnames] > 0), 'duration'], x)
                           }
                         )
duration.ratio <- as.data.frame(do.call('rbind', duration.ratio))
names(duration.ratio) <- c('duration.ratio', 'road.class.index')
duration.ratio <- merge(duration.ratio, road.class.table, by = 'road.class.index', all.x = T, all.y = F)

duration.ratio$road.type <- factor(duration.ratio$road.class)
library(ggplot2)
ggplot(duration.ratio, aes(x = duration.ratio, fill = road.class)) + geom_density(adjust = 0.5, alpha = 0.2) + xlim(0, 100) + 
  theme(legend.position = c(0.9,0.65), legend.background = element_blank(), legend.key = element_blank(), 
        legend.text = element_text(size = 8)) + guides(fill = guide_legend(title = NULL))
ggsave('F:/data/高速预测/pictures/duration_ratio.png', width = 24, height = 12, unit = 'cm', dpi = 1000)

library(plotly)
p <- plot_ly(data = duration.ratio, x = ~duration.ratio, color = ~road.class) %>% add_histogram()
plot_ly(duration.ratio, x = ~road.class, y = ~duration.ratio) %>% add_boxplot()


##各类型行驶距离分布
mileage <- lapply(road.class.index, 
                  function(x){
                    cnames <- paste("road_", x, "_mileage", sep = '')
                    cbind(trip.features[which(trip.features[, cnames] > 0), cnames], x)
                    }
                  )
mileage <- as.data.frame(do.call('rbind', mileage))
names(mileage) <- c('mileage', 'road.class.index')
mileage <- merge(mileage, road.class.table, by = 'road.class.index', all.x = T, all.y = F)

mileage$road.type <- factor(mileage$road.class)
library(ggplot2)
ggplot(mileage, aes(x = mileage, fill = road.class)) + geom_density(adjust = 0.5, alpha = 0.2) + xlim(0, 10) + 
  theme(legend.position = c(0.9,0.65), legend.background = element_blank(), legend.key = element_blank(), 
        legend.text = element_text(size = 8)) + guides(fill = guide_legend(title = NULL))
ggsave('F:/data/高速预测/pictures/duration_ratio.png', width = 24, height = 12, unit = 'cm', dpi = 1000)

library(plotly)
p <- plot_ly(data = mileage, x = ~mileage, color = ~road.class) %>% add_histogram()
p
plot_ly(mileage, x = ~road.class, y = ~mileage) %>% add_boxplot() %>% layout(yaxis = list(range = c(0, 100)))


##各类型距离比例分布


##各类型平均速度分布
speed.mean <- lapply(road.class.index, 
                     function(x){
                       cnames <- paste("road_", x, "_speed_mean", sep = '')
                       cbind(trip.features[which(!is.na(trip.features[, cnames])), cnames], x)
                     }
)
speed.mean <- as.data.frame(do.call('rbind', speed.mean))
names(speed.mean) <- c('speed.mean', 'road.class.index')
speed.mean <- merge(speed.mean, road.class.table, by = 'road.class.index', all.x = T, all.y = F)

speed.mean$road.type <- factor(speed.mean$road.class)
library(ggplot2)
ggplot(speed.mean, aes(x = speed.mean, fill = road.class)) + geom_density(adjust = 0.5, alpha = 0.2) + xlim(0, 150) + 
  theme(legend.position = c(0.9,0.65), legend.background = element_blank(), legend.key = element_blank(), 
        legend.text = element_text(size = 8)) + guides(fill = guide_legend(title = NULL))
ggsave('F:/data/高速预测/pictures/speed_mean.png', width = 36, height = 20, unit = 'cm', dpi = 1000)

library(plotly)
p <- plot_ly(data = speed.mean, x = ~speed.mean, color = ~road.class) %>% add_histogram()
plot_ly(speed.mean, x = ~road.class, y = ~speed.mean) %>% add_boxplot() %>% layout(yaxis = list(range = c(0, 150)))

##各类型加速度标准差分布
acc_sd <- lapply(road.class.index, 
                     function(x){
                       cnames <- paste("road_", x, "_acc_sd", sep = '')
                       cbind(trip.features[which(!is.na(trip.features[, cnames])), cnames], x)
                     }
)
acc_sd <- as.data.frame(do.call('rbind', acc_sd))
names(acc_sd) <- c('acc_sd', 'road.class.index')
acc_sd <- acc_sd[acc_sd$acc_sd <= 0.2,]
acc_sd <- merge(acc_sd, road.class.table, by = 'road.class.index', all.x = T, all.y = F)

acc_sd$road.type <- factor(acc_sd$road.class)
library(ggplot2)
ggplot(acc_sd, aes(x = acc_sd, fill = road.class)) + geom_density(adjust = 0.5, alpha = 0.2) + xlim(0, 0.2) + 
  theme(legend.position = c(0.9,0.65), legend.background = element_blank(), legend.key = element_blank(), 
        legend.text = element_text(size = 8)) + guides(fill = guide_legend(title = NULL))
ggsave('F:/data/高速预测/pictures/acc_sd.png', width = 36, height = 20, unit = 'cm', dpi = 1000)

library(plotyly)
p <- plot_ly(data = acc_sd, x = ~acc_sd, color = ~road.class) %>% add_histogram()
plot_ly(acc_sd, x = ~road.class, y = ~acc_sd) %>% add_boxplot() %>% layout(yaxis = list(range = c(0, 0.1)))



##各类型总体平均速度、总体速度标准差以及总体加速度标准差
sd.merge <- function(dura.sd){
  ###
  #函数用于计算多段行程速度标准差之间总的标准差
  #输入字段为包含dura,speed.mean,sd三个字段的DF
  ###
  names(dura.sd) <- c('dura','mean','sd')
  n <- nrow(dura.sd)
  if (n == 1){
    sd.cal <- dura.sd
  } else if (n == 2){
    sd.cal <- c(sum(dura.sd$dura, na.rm = T), 
                sum(dura.sd$dura*dura.sd$mean, na.rm = T)/sum(dura.sd$dura, na.rm = T), 
                sqrt((sum((dura.sd$dura-1)*dura.sd$sd^2, na.rm = T) + 
                        dura.sd[1,'dura']*dura.sd[2,'dura']*(dura.sd[1,'mean']-dura.sd[2,'mean'])^2/(dura.sd[1,'dura']+dura.sd[2,'dura']))/
                       (sum(dura.sd$dura, na.rm = T)-1)))
  } else if (n >= 3){
    n.1 <- floor(n/2)
    sd.1 <- sd.merge(dura.sd[1:n.1,])
    sd.2 <- sd.merge(dura.sd[(n.1+1):n,])
    sd.cal <- sd.merge(as.data.frame(rbind(sd.1,sd.2)))
  }
  return(sd.cal)
}
roadlevel.stat <- lapply(road.class.index, 
                         function(x){
                           cnames <- paste("road_", x, "_", c('duration', 'speed_mean', 'speed_sd', 'acc_sd','mileage'), sep = '')
                           road.data <- trip.features[which(trip.features[, cnames[2]] > 0), cnames]
                           road.stat <- c(x, sum(road.data[,5]), 
                                          sd.merge(road.data[,1:3]), 
                                          sd.merge(as.data.frame(cbind(road.data[,1], 0, road.data[,4])))[3])
                           return(road.stat)
                         })
roadlevel.stat <- as.data.frame(do.call('rbind', roadlevel.stat))
names(roadlevel.stat) <- c('road.class.index', 'mileage', 'duration', 'speed.mean', 'speed.sd', 'acc.sd')


##各类道路类型速度、加速度分布差异估计
JSD <- function(y1, y2, gap){
  breaks <- seq(from = floor(min(y1, y2)/gap)*gap, to = ceiling(max(y1, y2)/gap)*gap, by = gap)
  y1.prob <- table(cut(y1,breaks = breaks))/length(y1)
  y2.prob <- table(cut(y2,breaks = breaks))/length(y2)
  y1.prob[y1.prob == 0] <- 1e-20
  y2.prob[y2.prob == 0] <- 1e-20
  y.prob.mean <- (y1.prob + y2.prob)/2
  jsd <- 0.5 * (sum(y1.prob * log2(y1.prob/y.prob.mean)) + sum(y2.prob * log2(y2.prob/y.prob.mean)))
  return(jsd)
}
road.class.index <- c(41000,42000,43000,44000,45000,47000,51000,52000,53000,54000)
road.class <- c('高速公路', '国道', '主要大街、城市快速路', '主要道路', '次要道路', '普通道路', '省道', 
                '乡公路', '县乡村内部道路1', '县乡村内部道路2')
speed.mean.jsd <- as.data.frame(matrix(NA, ncol = length(road.class.index), nrow = length(road.class.index), 
                                       dimnames = list(road.class, road.class)))
for (i in 1:length(road.class.index)){
  for (j in 1:length(road.class.index)){
    speed.mean.i <- trip.features[, paste("road_", road.class.index[i], "_speed_mean", sep = '')]
    speed.mean.j <- trip.features[, paste("road_", road.class.index[j], "_speed_mean", sep = '')]
    speed.mean.i <- speed.mean.i[speed.mean.i >0 & !is.na(speed.mean.i)]
    speed.mean.j <- speed.mean.j[speed.mean.j >0 & !is.na(speed.mean.j)]
    speed.mean.jsd[i,j] <- JSD(speed.mean.i, speed.mean.j, gap = 1)
  }
}
library(xlsx)
write.xlsx(speed.mean.jsd, file = 'F:/data/高速预测/Outcom20170316.xlsx', sheetName = 'speed.mean.jsd.may', append = T)
hc.average <- hclust(as.dist(speed.mean.jsd), method = 'average')
plot(hc.average, main = '道路类型谱系图', xlab = '道路类型', sub = '', cex = 2)
roadlevel.cluster <- cutree(hc.average, 4)







rm(list = ls())
gc()
road.class.index <- 1:4
road.class <- c('第一类道路', '第二类道路', '第三类道路', '第四类道路')
road.class.table <- data.frame(road.class.index = road.class.index, road.class = road.class)

load('F:/data/高速预测/trip_road_features_20170322.Rdata')
trip.features <- trip.features[which(trip.features$mileage/(trip.features$speed_mean*trip.features$duration/3600)<1.5),]



##各类型行驶时长分布
duration <- lapply(road.class.index, 
                   function(x){
                     cnames <- paste("road_", x, "_duration", sep = '')
                     cbind(trip.features[which(trip.features[, cnames] > 0), cnames]/60, x)
                   }
)
duration <- as.data.frame(do.call('rbind', duration))
names(duration) <- c('duration', 'road.class.index')
duration <- merge(duration, road.class.table, by = 'road.class.index', all.x = T, all.y = F)

duration$road.type <- factor(duration$road.class)
library(ggplot2)
ggplot(duration, aes(x = duration, fill = road.class)) + geom_density(adjust = 1, alpha = 0.2) + xlim(0, 60) + 
  theme(legend.position = c(0.9,0.65), legend.background = element_blank(), legend.key = element_blank(), 
        legend.text = element_text(size = 8)) + guides(fill = guide_legend(title = NULL))
ggsave('F:/data/高速预测/pictures/duration_merged.png', width = 36, height = 20, unit = 'cm', dpi = 1000)

library(plotly)
p <- plot_ly(data = duration, x = ~duration, color = ~road.class) %>% add_histogram()
plot_ly(duration, x = ~road.class, y = ~duration) %>% add_boxplot() %>% layout(yaxis = list(range = c(0, 80)))


##各类型时长比例分布
duration.ratio <- lapply(road.class.index, 
                         function(x){
                           cnames <- paste("road_", x, "_duration", sep = '')
                           cbind(100*trip.features[which(trip.features[, cnames] > 0), cnames]/
                                   trip.features[which(trip.features[, cnames] > 0), 'duration'], x)
                         }
)
duration.ratio <- as.data.frame(do.call('rbind', duration.ratio))
names(duration.ratio) <- c('duration.ratio', 'road.class.index')
duration.ratio <- merge(duration.ratio, road.class.table, by = 'road.class.index', all.x = T, all.y = F)

duration.ratio$road.type <- factor(duration.ratio$road.class)
library(ggplot2)
ggplot(duration.ratio, aes(x = duration.ratio, fill = road.class)) + geom_density(adjust = 0.5, alpha = 0.2) + xlim(0, 100) + 
  theme(legend.position = c(0.9,0.65), legend.background = element_blank(), legend.key = element_blank(), 
        legend.text = element_text(size = 8)) + guides(fill = guide_legend(title = NULL))
ggsave('F:/data/高速预测/pictures/duration_ratio_merged.png', width = 24, height = 12, unit = 'cm', dpi = 1000)

library(plotly)
p <- plot_ly(data = duration.ratio, x = ~duration.ratio, color = ~road.class) %>% add_histogram()
plot_ly(duration.ratio, x = ~road.class, y = ~duration.ratio) %>% add_boxplot()


##各类型行驶距离分布
mileage <- lapply(road.class.index, 
                  function(x){
                    cnames <- paste("road_", x, "_mileage", sep = '')
                    cbind(trip.features[which(trip.features[, cnames] > 0), cnames], x)
                  }
)
mileage <- as.data.frame(do.call('rbind', mileage))
names(mileage) <- c('mileage', 'road.class.index')
mileage <- merge(mileage, road.class.table, by = 'road.class.index', all.x = T, all.y = F)

mileage$road.type <- factor(mileage$road.class)
library(ggplot2)
ggplot(mileage, aes(x = mileage, fill = road.class)) + geom_density(adjust = 1, alpha = 0.2) + xlim(0, 40) + 
  theme(legend.position = c(0.9,0.65), legend.background = element_blank(), legend.key = element_blank(), 
        legend.text = element_text(size = 8)) + guides(fill = guide_legend(title = NULL))
ggsave('F:/data/高速预测/pictures/mileage_merged.png', width = 24, height = 12, unit = 'cm', dpi = 1000)

library(plotly)
p <- plot_ly(data = mileage, x = ~mileage, color = ~road.class) %>% add_histogram()
p
plot_ly(mileage, x = ~road.class, y = ~mileage) %>% add_boxplot() %>% layout(yaxis = list(range = c(0, 100)))


##各类型距离比例分布


##各类型平均速度分布
speed.mean <- lapply(road.class.index, 
                     function(x){
                       cnames <- paste("road_", x, "_speed_mean", sep = '')
                       cbind(trip.features[which(!is.na(trip.features[, cnames])), cnames], x)
                     }
)
speed.mean <- as.data.frame(do.call('rbind', speed.mean))
names(speed.mean) <- c('speed.mean', 'road.class.index')
speed.mean <- merge(speed.mean, road.class.table, by = 'road.class.index', all.x = T, all.y = F)

speed.mean$road.type <- factor(speed.mean$road.class)
library(ggplot2)
ggplot(speed.mean, aes(x = speed.mean, fill = road.class)) + geom_density(adjust = 0.5, alpha = 0.2) + xlim(0, 150) + 
  theme(legend.position = c(0.9,0.65), legend.background = element_blank(), legend.key = element_blank(), 
        legend.text = element_text(size = 8)) + guides(fill = guide_legend(title = NULL))
ggsave('F:/data/高速预测/pictures/speed_mean_merged.png', width = 36, height = 20, unit = 'cm', dpi = 1000)

library(plotly)
p <- plot_ly(data = speed.mean, x = ~speed.mean, color = ~road.class) %>% add_histogram()
plot_ly(speed.mean, x = ~road.class, y = ~speed.mean) %>% add_boxplot() %>% layout(yaxis = list(range = c(0, 150)))

##各类型加速度标准差分布
acc_sd <- lapply(road.class.index, 
                 function(x){
                   cnames <- paste("road_", x, "_acc_sd", sep = '')
                   cbind(trip.features[which(!is.na(trip.features[, cnames])), cnames], x)
                 }
)
acc_sd <- as.data.frame(do.call('rbind', acc_sd))
names(acc_sd) <- c('acc_sd', 'road.class.index')
acc_sd <- acc_sd[acc_sd$acc_sd <= 0.2,]
acc_sd <- merge(acc_sd, road.class.table, by = 'road.class.index', all.x = T, all.y = F)

acc_sd$road.type <- factor(acc_sd$road.class)
library(ggplot2)
ggplot(acc_sd, aes(x = acc_sd, fill = road.class)) + geom_density(adjust = 0.5, alpha = 0.2) + xlim(0, 0.2) + 
  theme(legend.position = c(0.9,0.65), legend.background = element_blank(), legend.key = element_blank(), 
        legend.text = element_text(size = 8)) + guides(fill = guide_legend(title = NULL))
ggsave('F:/data/高速预测/pictures/acc_sd.png', width = 36, height = 20, unit = 'cm', dpi = 1000)

library(plotyly)
p <- plot_ly(data = acc_sd, x = ~acc_sd, color = ~road.class) %>% add_histogram()
plot_ly(acc_sd, x = ~road.class, y = ~acc_sd) %>% add_boxplot() %>% layout(yaxis = list(range = c(0, 0.1)))



##各类型总体平均速度、总体速度标准差以及总体加速度标准差
sd.merge <- function(dura.sd){
  ###
  #函数用于计算多段行程速度标准差之间总的标准差
  #输入字段为包含dura,speed.mean,sd三个字段的DF
  ###
  names(dura.sd) <- c('dura','mean','sd')
  n <- nrow(dura.sd)
  if (n == 1){
    sd.cal <- dura.sd
  } else if (n == 2){
    sd.cal <- c(sum(dura.sd$dura, na.rm = T), 
                sum(dura.sd$dura*dura.sd$mean, na.rm = T)/sum(dura.sd$dura, na.rm = T), 
                sqrt((sum((dura.sd$dura-1)*dura.sd$sd^2, na.rm = T) + 
                        dura.sd[1,'dura']*dura.sd[2,'dura']*(dura.sd[1,'mean']-dura.sd[2,'mean'])^2/(dura.sd[1,'dura']+dura.sd[2,'dura']))/
                       (sum(dura.sd$dura, na.rm = T)-1)))
  } else if (n >= 3){
    n.1 <- floor(n/2)
    sd.1 <- sd.merge(dura.sd[1:n.1,])
    sd.2 <- sd.merge(dura.sd[(n.1+1):n,])
    sd.cal <- sd.merge(as.data.frame(rbind(sd.1,sd.2)))
  }
  return(sd.cal)
}
roadlevel.stat <- lapply(road.class.index, 
                         function(x){
                           cnames <- paste("road_", x, "_", c('duration', 'speed_mean', 'speed_sd', 'acc_sd','mileage'), sep = '')
                           road.data <- trip.features[which(trip.features[, cnames[2]] > 0), cnames]
                           road.stat <- c(x, sum(road.data[,5]), 
                                          sd.merge(road.data[,1:3]), 
                                          sd.merge(as.data.frame(cbind(road.data[,1], 0, road.data[,4])))[3])
                           return(road.stat)
                         })
roadlevel.stat <- as.data.frame(do.call('rbind', roadlevel.stat))
names(roadlevel.stat) <- c('road.class.index', 'mileage', 'duration', 'speed.mean', 'speed.sd', 'acc.sd')