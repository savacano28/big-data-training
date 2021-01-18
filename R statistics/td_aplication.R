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

##http://www.sthda.com/english/wiki/reading-data-from-txt-csv-files-r-base-functions
