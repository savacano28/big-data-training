//Requête simple 
//1. Liste des joueurs, n'affiche que le nom et le pays:
db.joueurs.aggregate([{$lookup: {from: "pays", localField:"pays_id",foreignField:"_id",as: "pays"}},{$addFields: {"nom_pays": "$pays.nom_pays"}},{$project: {_id:0,nom:1,nom_pays:1}}]);

//2.Nom des joueuses
db.joueurs.find({sexe:"Fémenin"},{_id:0,nom:1});

//3.Liste des tournois se déroulant en France ou dont la dotation est supérieure à 800000.
db.tournois.aggregate([{$lookup: {from: "pays", localField:"pays_id",foreignField:"_id",as: "pays"}},{$match:{$or:[{"pays.nom_pays":"France"},{"dotation":{$gt:800000}}]}},{$addFields:{"pays":"$pays.nom_pays"}},{$project:{_id:0,pays_id:0}}]);

//4.Nom des joueurs français qui jouent dans une équipe de double.
db.equipes.aggregate([{ $project: {joueur:"$joueur0_id", _id: 0 } },{ $unionWith: { coll: "equipes", pipeline: [ { $project: { joueur:"$joueur1_id", _id: 0 } } ]} },{$group:{_id:"$joueur"}},{$lookup: {from: "joueurs", localField:"_id",foreignField:"_id",as: "joueur"}},{$lookup:{from:"pays", localField:"joueur.pays_id",foreignField:"_id",as:"pays"}},{$match:{"pays.nom_pays":"France"}},{$addFields:{joueur_nom:"$joueur.nom"}},{$project:{"joueur_nom":1,_id:0}} ]);

//5.Pour chaque tournoi, son nom, le nom du pays où il se déroule, sa dotation et la monnaie dans laquelle elle s’exprime.
db.tournois.aggregate([{$lookup: {from: "pays", localField:"pays_id",foreignField:"_id",as: "pays"}},{$addFields:{"pays":"$pays.nom_pays","monnaie":"$pays.monnaie"}},{$project:{_id:0,nom:1,"pays":1,dotation:1,"monnaie":1}}]);

//6.Le nom du tournoi qui a la dotation minimum
db.tournois.find({},{nom:1,dotation:1,_id:0}).sort({dotation:1}).limit(1)

//7.Le nom du tournoi qui a la dotation maximum
db.tournois.find({},{nom:1,dotation:1,_id:0}).sort({dotation:-1}).limit(1)

//Requête complexe 
//1.Affichez la liste des joueurs triée par ordre alphabétique de nom.
db.joueurs.find({},{_id:0}).sort({nom:1})

//2.Affichez la moyenne des scores de chaque joueur de simple (indiquer le nom des joueurs)
//score : points* coefTournois
db.simples.aggregate([{$lookup:{from:"tournois",localField:"tournoi_id",foreignField:"_id",as:"tournoi"}},{$group:{_id:"$joueur_id",avg_score:{$avg:{$multiply:["$points",{$arrayElemAt: ["$tournoi.coef",0]}]}},no_simples:{$sum:1}}},{$lookup:{from:"joueurs",localField:"_id",foreignField:"_id",as:"joueur"}},{$addFields: {"nom_joueur": "$joueur.nom"}},{$project:{nom_joueur:1,avg_score:1,_id:0}},{$sort:{"avg_score":1}}])
//score: just les points
db.simples.aggregate([{$group:{_id:"$joueur_id",avg_score:{$avg:"$points"},no_simples:{$sum:1}}},{$lookup:{from:"joueurs",localField:"_id",foreignField:"_id",as:"joueur"}},{$addFields: {"nom_joueur": "$joueur.nom"}},{$project:{nom_joueur:1,avg_score:1,_id:0}},{$sort:{"avg_score":1}}])

//3.Affichez le score final de chaque équipe de double et classer les équipesde la meilleure à la moins bonne
db.equipes.aggregate([{$lookup:{from:"tournois",localField:"tournoi_id",foreignField:"_id",as:"tournoi"}},{$group:{_id:"$no",final_score:{$sum:{$multiply:["$points",{$arrayElemAt: ["$tournoi.coef",0]}]}},no_doubles:{$sum:1}}},{$project:{final_score:1, equipe:"$_id",_id:0}},{$sort:{"final_score":-1}}])

//4.Affichez, pour chaque joueur son numéro de joueur, son nom, son pays et son score total en simple.
db.simples.aggregate([{$lookup:{from:"tournois",localField:"tournoi_id",foreignField:"_id",as:"tournoi"}},{$group:{_id:"$joueur_id",total_simple_score:{$sum:{$multiply:["$points",{$arrayElemAt: ["$tournoi.coef",0]}]}},no_simples:{$sum:1}}},{$lookup:{from:"joueurs",localField:"_id",foreignField:"_id",as:"joueur"}},{$lookup:{from:"pays",localField:"joueur.pays_id",foreignField:"_id",as:"pays"}},{$addFields:{"pays":"$pays.nom_pays","joueur_no":"$joueur.no","joueur_nom":"$joueur.nom"}},{$project:{"joueur_nom":1,total_simple_score:1,"joueur_no":1,"pays":1,_id:0}},{$sort:{"total_simple_score":-1}}])

//5.Affichez le nom du joueur qui a joué tous les tournois en simple
db.simples.aggregate([{$group:{_id:"$joueur_id",simples:{$addToSet:"$tournoi_id"}}},{$lookup:{from:"joueurs",localField:"_id",foreignField:"_id",as:"joueur"}},{$addFields:{"joueur_nom":"$joueur.nom",no_simples:{"$size":"$simples"}}},{$match:{"no_simples":4}},{$project:{_id:0,joueur_nom:1}},{$sort:{"joueur.nom":1}}])


