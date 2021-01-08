#clustering
install.packages("dplyr")
library(dplyr)
sample1<-sample_n(data.frame(train), 1000)
sample_sans_label<-sample1[,2:785]

mnist_clustering=kmeans(sample_sans_label,centers=9,nstart = 20)
mnist_centers <- mnist_clustering$centers

# Plot typical cluster digits
par(mfrow=c(3,4))
for (i in 1:10) show_digit(mnist_centers[i,])

#inertia
inertie = c()
for (k in 1:9){
res=kmeans(sample_sans_label,centers=k,nstart = 10)
inertie[k]=res$tot.withinss}
plot(1:9,inertie,type='l')

#pca
library(FactoMineR)
sample.pca <- PCA(sample1, graph=FALSE)
plot(sample.pca, choix= "ind",col.ind = pca$cluster,graph.type = "classic")



