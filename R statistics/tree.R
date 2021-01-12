library('rpart')
install.packages('rpart.plot')
install.packages('rattle')
library('rpart.plot')
library(rattle)
library(RColorBrewer)

app=data.frame(cbind(s,sl))
names(app)[785]="y"
app$y=as.factor(app$y)
test=data.frame(cbind(t,tl))
names(test)[785]="y"
test$y=as.factor(tl)
arbre=rpart(x.1~.,data=app[0:784])
arbre=rpart(y~.,data=app)
res=predict(arbre,newdata=test,type='class')
table(res,test$y)
res  0  1  2  3  4  5  6  7  8  9
0 80  1  6 19  0  6  0  0  0  2
1  0 83  6  8  2  2  1  4 12  5
2  3 19 43  6  0  1  9  0  3  3
3  2  1  3 47  0  1  0  0  6  0
4  0  0  1  0 52  3  9  0  0  3
5 14  0  2 14 16 53  7  9  2  3
6  2  1 10  6  5  5 73  0  4  0
7  9  1  5  4  8  1  7 77  3 25
8  2  0 13  2  1  1  1  0 49  0
9  1  2  4  9  4  7  0 11 10 65

png(file="/home/scasanova/Desktop/R statistics/arbre1.png",width=1500, height=1500)
fancyRpartPlot(arbre,caption=NULL)
dev.off()

res.pca<- PCA(s)
barplot(res.pca$eig[,2],main="Pourcentage de variance expliquée")


#https://www.gormanalysis.com/blog/decision-trees-in-r-using-rpart/
#https://www.datamentor.io/r-programming/saving-plot/
