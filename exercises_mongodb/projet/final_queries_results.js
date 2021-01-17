/***********************Requête simple***************/ 
//1. Liste des joueurs, n'affiche que le nom et le pays:
db.joueurs.aggregate([{$lookup: {from: "pays", localField:"pays_id",foreignField:"_id",as: "pays"}},
                      {$addFields: {"nom_pays": "$pays.nom_pays"}},
                      {$project: {_id:0,nom:1,nom_pays:1}}]);
{ "nom" : "Roddick", "nom_pays" : [ "Etat-Unis" ] }
{ "nom" : "Ginepri", "nom_pays" : [ "Etat-Unis" ] }
{ "nom" : "Gasquet", "nom_pays" : [ "France" ] }
{ "nom" : "Monfils", "nom_pays" : [ "France" ] }
{ "nom" : "Mauresmo", "nom_pays" : [ "France" ] }
{ "nom" : "Daveport", "nom_pays" : [ "Etat-Unis" ] }

//2.Nom des joueuses
db.joueurs.find({sexe:"Fémenin"},{_id:0,nom:1});
{ "nom" : "Mauresmo" }
{ "nom" : "Daveport" }

//3.Liste des tournois se déroulant en France ou dont la dotation est supérieure à 800000.
db.tournois.aggregate([{$lookup: {from: "pays", localField:"pays_id",foreignField:"_id",as: "pays"}},
                       {$match:{$or:[{"pays.nom_pays":"France"},{"dotation":{$gt:800000}}]}},
                       {$addFields:{"pays":"$pays.nom_pays"}},
                       {$project:{_id:0,pays_id:0}}])					   
{ "nom" : "Roland Garros", "date" : "10-06-2000", "coef" : 10, "dotation" : 700000, "pays" : [ "France" ] }
{ "nom" : "Flusshing Meadows", "date" : "10-11-2000", "coef" : 6, "dotation" : 1000000, "pays" : [ "Etat-Unis" ] }
{ "nom" : "Open de Paris-Bercy", "date" : "10-12-2000", "coef" : 4, "dotation" : 300000, "pays" : [ "France" ] }

//4.Nom des joueurs français qui jouent dans une équipe de double.
db.equipes.aggregate([{ $project: {joueur:"$joueur0_id", _id: 0 } },
                      { $unionWith: { coll: "equipes", pipeline: [ { $project: { joueur:"$joueur1_id", _id: 0 } } ]} },
                      {$group:{_id:"$joueur"}},
                      {$lookup: {from: "joueurs", localField:"_id",foreignField:"_id",as: "joueur"}},
                      {$lookup:{from:"pays", localField:"joueur.pays_id",foreignField:"_id",as:"pays"}},
                      {$match:{"pays.nom_pays":"France"}},
                      {$addFields:{joueur_nom:"$joueur.nom"}},
                      {$project:{"joueur_nom":1,_id:0}} ]);
{ "joueur_nom" : [ "Monfils" ] }
{ "joueur_nom" : [ "Gasquet" ] }

//5.Pour chaque tournoi, son nom, le nom du pays où il se déroule, sa dotation et la monnaie dans laquelle elle s’exprime.
db.tournois.aggregate([{$lookup: {from: "pays", localField:"pays_id",foreignField:"_id",as: "pays"}},
                       {$addFields:{"pays":"$pays.nom_pays","monnaie":"$pays.monnaie"}},
                       {$project:{_id:0,nom:1,"pays":1,dotation:1,"monnaie":1}}]);
{ "nom" : "Roland Garros", "dotation" : 700000, "pays" : [ "France" ], "monnaie" : [ "EU" ] }
{ "nom" : "Open d'Autralie", "dotation" : 700000, "pays" : [ "Australie" ], "monnaie" : [ "AUD" ] }
{ "nom" : "Flusshing Meadows", "dotation" : 1000000, "pays" : [ "Etat-Unis" ], "monnaie" : [ "USD" ] }
{ "nom" : "Open de Paris-Bercy", "dotation" : 300000, "pays" : [ "France" ], "monnaie" : [ "EU" ] }

//6.Le nom du tournoi qui a la dotation minimum
db.tournois.find({},{nom:1,dotation:1,_id:0}).sort({dotation:1}).limit(1)
{ "nom" : "Open de Paris-Bercy", "dotation" : 300000 }

//7.Le nom du tournoi qui a la dotation maximum
db.tournois.find({},{nom:1,dotation:1,_id:0}).sort({dotation:-1}).limit(1)
{ "nom" : "Flusshing Meadows", "dotation" : 1000000 }

/***********************Requête complexe***************/ 
//1.Affichez la liste des joueurs triée par ordre alphabétique de nom.
db.joueurs.find({},{_id:0}).sort({nom:1})
{ "no" : 200, "nom" : "Daveport", "sexe" : "Fémenin", "pays_id" : 3 }
{ "no" : 30, "nom" : "Gasquet", "sexe" : "Masculin", "pays_id" : 2 }
{ "no" : 20, "nom" : "Ginepri", "sexe" : "Masculin", "pays_id" : 3 }
{ "no" : 100, "nom" : "Mauresmo", "sexe" : "Fémenin", "pays_id" : 2 }
{ "no" : 40, "nom" : "Monfils", "sexe" : "Masculin", "pays_id" : 2 }
{ "no" : 10, "nom" : "Roddick", "sexe" : "Masculin", "pays_id" : 3 }

//2.Affichez la moyenne des scores de chaque joueur de simple (indiquer le nom des joueurs)
//score : points* coefTournois
db.simples.aggregate([{$lookup:{from:"tournois",localField:"tournoi_id",foreignField:"_id",as:"tournoi"}},
                      {$group:{_id:"$joueur_id",avg_score:{$avg:{$multiply:["$points",{$arrayElemAt: ["$tournoi.coef",0]}]}},no_simples:{$sum:1}}},
                      {$lookup:{from:"joueurs",localField:"_id",foreignField:"_id",as:"joueur"}},
                      {$addFields: {"nom_joueur": "$joueur.nom"}},
                      {$project:{nom_joueur:1,avg_score:1,_id:0}},{$sort:{"avg_score":1}}])					  
{ "avg_score" : 5, "nom_joueur" : [ "Gasquet" ] }
{ "avg_score" : 15, "nom_joueur" : [ "Monfils" ] }
{ "avg_score" : 39.5, "nom_joueur" : [ "Roddick" ] }
{ "avg_score" : 40, "nom_joueur" : [ "Daveport" ] }
{ "avg_score" : 80, "nom_joueur" : [ "Ginepri" ] }

//score: just les points
db.simples.aggregate([{$group:{_id:"$joueur_id",avg_score:{$avg:"$points"},no_simples:{$sum:1}}},
                      {$lookup:{from:"joueurs",localField:"_id",foreignField:"_id",as:"joueur"}},
                      {$addFields: {"nom_joueur": "$joueur.nom"}},
                      {$project:{nom_joueur:1,avg_score:1,_id:0}},
                      {$sort:{"avg_score":1}}])
{ "avg_score" : 1, "nom_joueur" : [ "Gasquet" ] }
{ "avg_score" : 3, "nom_joueur" : [ "Monfils" ] }
{ "avg_score" : 4, "nom_joueur" : [ "Daveport" ] }
{ "avg_score" : 5.75, "nom_joueur" : [ "Roddick" ] }
{ "avg_score" : 8, "nom_joueur" : [ "Ginepri" ] }				  

//3.Affichez le score final de chaque équipe de double et classer les équipesde la meilleure à la moins bonne
db.equipes.aggregate([{$lookup:{from:"tournois",localField:"tournoi_id",foreignField:"_id",as:"tournoi"}},
                      {$group:{_id:"$no",final_score:{$sum:{$multiply:["$points",{$arrayElemAt: ["$tournoi.coef",0]}]}},no_doubles:{$sum:1}}},
                      {$project:{final_score:1, equipe:"$_id",_id:0}},
                      {$sort:{"final_score":-1}}])
{ "final_score" : 132, "equipe" : 1 }
{ "final_score" : 118, "equipe" : 2 }					  

//4.Affichez, pour chaque joueur son numéro de joueur, son nom, son pays et son score total en simple.
db.simples.aggregate([{$lookup:{from:"tournois",localField:"tournoi_id",foreignField:"_id",as:"tournoi"}},
                      {$group:{_id:"$joueur_id",total_simple_score:{$sum:{$multiply:["$points",{$arrayElemAt: ["$tournoi.coef",0]}]}},no_simples:{$sum:1}}},
                      {$lookup:{from:"joueurs",localField:"_id",foreignField:"_id",as:"joueur"}},
                      {$lookup:{from:"pays",localField:"joueur.pays_id",foreignField:"_id",as:"pays"}},
                      {$addFields:{"pays":"$pays.nom_pays","joueur_no":"$joueur.no","joueur_nom":"$joueur.nom"}},
                      {$project:{"joueur_nom":1,total_simple_score:1,"joueur_no":1,"pays":1,_id:0}},
                      {$sort:{"total_simple_score":-1}}])
{ "total_simple_score" : 158, "pays" : [ "Etat-Unis" ], "joueur_no" : [ 10 ], "joueur_nom" : [ "Roddick" ] }
{ "total_simple_score" : 80, "pays" : [ "Etat-Unis" ], "joueur_no" : [ 20 ], "joueur_nom" : [ "Ginepri" ] }
{ "total_simple_score" : 40, "pays" : [ "Etat-Unis" ], "joueur_no" : [ 200 ], "joueur_nom" : [ "Daveport" ] }
{ "total_simple_score" : 15, "pays" : [ "France" ], "joueur_no" : [ 40 ], "joueur_nom" : [ "Monfils" ] }
{ "total_simple_score" : 5, "pays" : [ "France" ], "joueur_no" : [ 30 ], "joueur_nom" : [ "Gasquet" ] }

//5.Affichez le nom du joueur qui a joué tous les tournois en simple
db.simples.aggregate([{$group:{_id:"$joueur_id",simples:{$addToSet:"$tournoi_id"}}},
                      {$lookup:{from:"joueurs",localField:"_id",foreignField:"_id",as:"joueur"}},
                      {$addFields:{"joueur_nom":"$joueur.nom",no_simples:{"$size":"$simples"}}},
                      {$match:{"no_simples":4}},
                      {$project:{_id:0,joueur_nom:1}},
                      {$sort:{"joueur.nom":1}}])
{ "joueur_nom" : [ "Roddick" ] }
