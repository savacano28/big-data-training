install.packages("readtext")
visaData<-read.delim("/home/scasanova/Desktop/R statistics/VisaPremier.txt", header = TRUE, sep = "\t")
summary(visaData)
colnames(visaData)


##http://www.sthda.com/english/wiki/reading-data-from-txt-csv-files-r-base-functions