sample1<-sample_n(data.frame(train), 1000)
sample_sans_label<-sample1[1,1:785]

inertie = c()
for (k in 1:15){
res=kmeans(sample_sans_label,centers=k,nstart = 10)
inertie[k]=res$tot.withinss}
plot(1:15,inertie,type='l')

mnist_clustering=kmeans(sample_sans_label,centers=10,nstart = 10)
mnist_centers <- mnist_clustering$centers

# Plot typical cluster digits
par(mfrow = c(2, 5), mar=c(0.5, 0.5, 0.5, 0.5))
layout(matrix(seq_len(nrow(mnist_centers)), 2, 5, byrow = FALSE))
for(i in seq_len(nrow(mnist_centers))) {
  image(matrix(mnist_centers[i, ], 28, 28)[, 28:1], 
        col = gray.colors(12, rev = TRUE), xaxt="n", yaxt="n")
}



