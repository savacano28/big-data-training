install.packages("readtext")
visaData<-read.delim("/home/scasanova/Desktop/R statistics/VisaPremier.txt", header = TRUE, sep = "\t")
summary(visaData)
colnames(visaData)
rownames(visaData)
visaData[0,]
res.pca<-PCA(visaData,quali.sup=c(2,4,6,8,9,22,40,45),graph=FALSE)
plot(res.pca,choix = "var")
plot(res.pca,choix = "ind")
barplot(res.pca$eig[,2],main="Pourcentage de variance expliquée")
barplot(res.pca$eig[,3],main="Pourcentage de variance expliquée")
res.pca
res.pca$eig
res.pca$var

inertie=c()
for(k in 2:10){
res=kmeans(visaData[,-c(2,4,6,8,9,22,40,45)],centers=k,nstart = 10)
inertie[k-1]=res$tot.withinss}
plot(2:10,inertie,type='l')

res=res=kmeans(visaData[,-c(2,4,6,8,9,22,40,45)],centers=4,nstart = 10)
str(visaData)

##http://www.sthda.com/english/wiki/reading-data-from-txt-csv-files-r-base-functions
