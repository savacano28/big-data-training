autos = read.xls("/home/scasanova/Downloads/autos.xls");
summary(autos);
str(autos);
rownames(autos);
rownames(autos)=autos$Modele;
rownames(autos);
autos$Modele=NULL;
res.pca <- PCA(autos[,1:9], quali.sup = c(7), quanti.sup=c(8:9), graph = FALSE);
summary(autos);
plot (res.pca, choix = 'var');
dev.new ();
plot (res.pca, choix = 'ind');
dev.new ();
barplot(res.pca$eig[,2],main="Porcentage de variance expliquée");
res.pca$var$coord;
res.pca$var$contrib;
res.pca$var$cos2;

inertie=c()
for (k in 2:6){
res=kmeans(auto[,-c(7,8,9)],centers=k,nstart = 10)
inertie[k-1]=res$tot.withinss
}
plot(2:6,inertie,type='l')
