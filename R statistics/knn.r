data=data.frame(train)
s=data[1:5000,2:785]
t=data[5001:6000,2:785]
sl=data[1:5000,786]
tl=data[5001:6000,786]
clas=knn(s,t,sl,10)
mean(clas==tl)
[1] 0.926

tbc=NULL
for(k in 1:20){
clas=knn(s,t,sl,k)
tbc[k]=mean(clas==tl)}

plot(1:20,tbc,ylim=c(0,1),xlab='k',ylab='taux de bons classements')

table(clas,tl)
