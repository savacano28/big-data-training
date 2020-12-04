
//imports 
//mongoimport --host localhost:27017 --db DB_TEST --collection musees --drop --file .../musees.csv --headerline --type csv
//mongoimport --host localhost:27017 --db DB_TEST --collection livres --drop --file .../livres.json --legacy

//livres 
db.livres.find()
db.livres.find({ "titre": "Le prince" })
db.livres.find({ "annee": 1532 })
db.livres.ensureIndex({ titre: "text" })
db.livres.ensureIndex({ $text: { $search: "potter" } })
db.livres.find({ $text: { $search: "potter" } })
db.livres.getIndexes()
db.livres.find({ anne: 1973 }).explain("executionStats")
db.livres.ensureIndex({ anne: 1 })
db.livres.find({ anne: 1973 }).explain("executionStats")

//musees
db.musees.find()
db.musees.count()
db.musees.find({ "VILLE": "LYON" }).count()
db.musees.find({ "NOMREG": { $in: ["AUVERGNE", "RHÃ”NE-ALPES"] } }).count()
db.musees.find({ "NOMDUMUSEE": /.*Municipal.*/ }).count()
db.musees.find({ "SITEWEB": "" }).count()
