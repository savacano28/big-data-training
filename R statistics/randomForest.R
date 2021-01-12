model <- bagging(y~.,data=app[sub,],mfinal=2)
pred <- predict.bagging(model,newdata=app[-sub,])
sum(pred$class==app[-sub,]$y)/nrow(app[-sub,])
pred$confusion

install.packages("randomForest")
library(randomForest)
modelr=randomForest(y~.,data=app,ntree=200,mtry=3)
predr<-predict(modelr,newdata=app[-sub,],type="class")
sum(predr==app[-sub,]$y)/nrow(app[-sub,])

