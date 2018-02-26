rm(list = ls())
gc()
load('F:/data/高速预测/trip_road_features_20170413.Rdata')
trip.features <- trip.features[3600*trip.features$mileage/(trip.features$duration*trip.features$speed_mean)<=2,]

library(lubridate)
trip.features$wd <- wday(as.POSIXlt(trip.features$tripnumber+trip.features$duration/2, 
                                    origin='1970-01-01 00:00:00',format='%Y-%m-%d %H:%M:%S'))-1
trip.features[trip.features$wd == 0, 'wd'] <- 7
trip.features$acc_sd <- trip.features$acc_sd*1000

trip.features[, 29:45] <- 100*trip.features[, 29:45]/trip.features$duration
trip.features[, 46:69] <- 100*trip.features[, 46:69]/trip.features$mileage
trip.features[, 70:73] <- 100*trip.features[, 70:73]/trip.features$duration
trip.features[, 74:77] <- 100*trip.features[, 74:77]/trip.features$mileage


set.seed(0)
obs.num <- nrow(trip.features)
train.ind <- sample(obs.num, floor(obs.num*0.7))
# train.data.duration.1 <- trip.features[train.ind, c(3:69, 70)]
# train.data.duration.2 <- trip.features[train.ind, c(3:69, 71)]
# train.data.duration.3 <- trip.features[train.ind, c(3:69, 72)]
# train.data.duration.4 <- trip.features[train.ind, c(3:69, 73)]
# train.data.mileage.1 <- trip.features[train.ind, c(3:69, 74)]
# train.data.mileage.2 <- trip.features[train.ind, c(3:69, 75)]
# train.data.mileage.3 <- trip.features[train.ind, c(3:69, 76)]
# train.data.mileage.4 <- trip.features[train.ind, c(3:69, 77)]
test.data <- trip.features[-train.ind,]



#运用LASSO估计道路类型时长及里程
mse.lasso <- data.frame(matrix(NA, ncol = 2, nrow = 8, 
                               dimnames = list(c(paste('dura', 1:4, sep = '.'), paste('mileage', 1:4, sep = '')), 
                                               c('mse', 'variables'))))

library(glmnet)
library(doParallel)
registerDoParallel(4)
duration.1.cv <- cv.glmnet(x = data.matrix(trip.features[train.ind, c(3:69)]), 
                           y = trip.features[train.ind, 70], alpha=1, parallel = T)
duration.1.pred <- data.frame(dura.1 = test.data$road_1_duration, 
                              prediction = predict(duration.1.cv, s = duration.1.cv$lambda.1se, newx = data.matrix(test.data[,3:69])))
mse.lasso[1,1] <- mean((duration.1.pred[,1]-duration.1.pred[,2])^2)

duration.2.cv <- cv.glmnet(x = data.matrix(trip.features[train.ind, c(3:69)]), 
                           y = trip.features[train.ind, 71], alpha=1, parallel = T)
duration.2.pred <- data.frame(dura.2 = test.data$road_2_duration, 
                              prediction = predict(duration.2.cv, s = duration.2.cv$lambda.1se, newx = data.matrix(test.data[,3:69])))
mse.lasso[2,1] <- mean((duration.2.pred[,1]-duration.2.pred[,2])^2)

duration.3.cv <- cv.glmnet(x = data.matrix(trip.features[train.ind, c(3:69)]), 
                           y = trip.features[train.ind, 73], alpha=1, parallel = T)
duration.3.pred <- data.frame(dura.3 = test.data$road_3_duration, 
                              prediction = predict(duration.3.cv, s = duration.3.cv$lambda.1se, newx = data.matrix(test.data[,3:69])))
mse.lasso[3,1] <- mean((duration.3.pred[,1]-duration.3.pred[,2])^2)

duration.4.cv <- cv.glmnet(x = data.matrix(trip.features[train.ind, c(3:69)]), 
                           y = trip.features[train.ind, 73], alpha=1, parallel = T)
duration.4.pred <- data.frame(dura.4 = test.data$road_4_duration, 
                              prediction = predict(duration.4.cv, s = duration.4.cv$lambda.1se, newx = data.matrix(test.data[,3:69])))
mse.lasso[4,1] <- mean((duration.4.pred[,1]-duration.4.pred[,2])^2)


mileage.1.cv <- cv.glmnet(x = data.matrix(trip.features[train.ind, c(3:69)]), 
                           y = trip.features[train.ind, 74], alpha=1, parallel = T)
mileage.1.pred <- data.frame(dura.1 = test.data$road_1_mileage, 
                              prediction = predict(mileage.1.cv, s = mileage.1.cv$lambda.1se, newx = data.matrix(test.data[,3:69])))
mse.lasso[5,1] <- mean((mileage.1.pred[,1]-mileage.1.pred[,2])^2)

mileage.2.cv <- cv.glmnet(x = data.matrix(trip.features[train.ind, c(3:69)]), 
                           y = trip.features[train.ind, 75], alpha=1, parallel = T)
mileage.2.pred <- data.frame(dura.2 = test.data$road_2_mileage, 
                              prediction = predict(mileage.2.cv, s = mileage.2.cv$lambda.1se, newx = data.matrix(test.data[,3:69])))
mse.lasso[6,1] <- mean((mileage.2.pred[,1]-mileage.2.pred[,2])^2)

mileage.3.cv <- cv.glmnet(x = data.matrix(trip.features[train.ind, c(3:69)]), 
                           y = trip.features[train.ind, 76], alpha=1, parallel = T)
mileage.3.pred <- data.frame(dura.3 = test.data$road_3_mileage, 
                              prediction = predict(mileage.3.cv, s = mileage.3.cv$lambda.1se, newx = data.matrix(test.data[,3:69])))
mse.lasso[7,1] <- mean((mileage.3.pred[,1]-mileage.3.pred[,2])^2)

mileage.4.cv <- cv.glmnet(x = data.matrix(trip.features[train.ind, c(3:69)]), 
                           y = trip.features[train.ind, 77], alpha=1, parallel = T)
mileage.4.pred <- data.frame(dura.4 = test.data$road_4_mileage, 
                              prediction = predict(mileage.4.cv, s = mileage.4.cv$lambda.1se, newx = data.matrix(test.data[,3:69])))
mse.lasso[8,1] <- mean((mileage.4.pred[,1]-mileage.4.pred[,2])^2)


var.name <- names(trip.features[,3:69])
mse.lasso$variables <- list(var.name[which(predict(duration.1.cv, type='coefficients', s=duration.1.cv$lambda.1se)>0)-1], 
                            var.name[which(predict(duration.2.cv, type='coefficients', s=duration.2.cv$lambda.1se)>0)-1], 
                            var.name[which(predict(duration.3.cv, type='coefficients', s=duration.3.cv$lambda.1se)>0)-1], 
                            var.name[which(predict(duration.4.cv, type='coefficients', s=duration.4.cv$lambda.1se)>0)-1],
                            var.name[which(predict(mileage.1.cv, type='coefficients', s=mileage.1.cv$lambda.1se)>0)-1], 
                            var.name[which(predict(mileage.2.cv, type='coefficients', s=mileage.2.cv$lambda.1se)>0)-1], 
                            var.name[which(predict(mileage.3.cv, type='coefficients', s=mileage.3.cv$lambda.1se)>0)-1], 
                            var.name[which(predict(mileage.4.cv, type='coefficients', s=mileage.4.cv$lambda.1se)>0)-1])

##感觉单纯用lasso来拟合道路类型的数据结果并不理想，试试先分类，再拟合？


#分类：寻找出具有各类形成类别的行程
road.target <- apply(trip.features[,70:77]>0, 2, function(x) factor(as.numeric(x)))
road.classification <- data.frame(matrix(NA, ncol = 2, nrow = 8, 
                                         dimnames = list(c(paste('dura', 1:4, sep = '.'), paste('mileage', 1:4, sep = '')), 
                                                         c('mse', 'variables'))))

library(glmnet)
library(doParallel)
registerDoParallel(4)
duration.1.class.cv <- cv.glmnet(x = data.matrix(trip.features[train.ind, c(3:69)]), family = 'binomial', 
                           type.measure = 'auc', y = road.target[train.ind, 1], alpha=1, parallel = T)
duration.1.class.pred <- data.frame(dura.1 = as.numeric(test.data$road_1_duration>0), 
                              prediction = predict(duration.1.class.cv, s = duration.1.class.cv$lambda.1se, 
                                                   newx = data.matrix(test.data[,3:69])))
mse.lasso[1,1] <- mean((duration.1.pred[,1]-duration.1.pred[,2])^2)













library(caret)

zerovar <- nearZeroVar(trip.features[, 3:69])

cor.mat.pearson <- cor(trip.features[,3:77], method = 'pearson')
cor.mat.spearman <- cor(trip.features[,3:77], method = 'spearman')
cor.mat.kendall <- cor(trip.features[,3:69], method = 'kendall')

high.cor <- findCorrelation(cor.mat, cutoff=0.5)






duration.sum <- rowSums(trip.features[,70:73])
mileage.sum <- rowSums(trip.features[,74:77])


