//First bloc queries
//1
db.joueurs.aggregate([{$lookup: {from: "pays", localField:"pays_id",foreignField:"_id",as: "pays"}},{$project: {_id:0,nom:1, pays:1}}]);
db.joueurs.aggregate([{$lookup: {from: "pays", localField:"pays_id",foreignField:"_id",as: "pays"}},{$addFields: {"nom_pays": "$pays.nom_pays"}},{$project: {_id:0,nom:1,nom_pays:1}}]);

//2
db.joueurs.find({sexe:"Fémenin"},{_id:0,nom:1});

//3
db.tournois.aggregate([{$lookup: {from: "pays", localField:"pays_id",foreignField:"_id",as: "pays"}},{$match:{"pays.nom_pays":"France","dotation":{$gt:400000}}},{$addFields:{"pays":"$pays.nom_pays"}},{$project:{_id:0,pays_id:0}}]);

//4
db.equipes.aggregate([{ $project: {joueur:"$joueur0_id", _id: 0 } },{ $unionWith: { coll: "equipes", pipeline: [ { $project: { joueur:"$joueur1_id", _id: 0 } } ]} },{$group:{_id:"$joueur"}},{$lookup: {from: "joueurs", localField:"_id",foreignField:"_id",as: "joueur"}},{$match:{"joueur.pays_id":2}},{$addFields:{joueur_nom:"$joueur.nom"}},{$project:{"joueur_nom":1,_id:0}} ]);
db.equipes.aggregate([{ $project: {joueur:"$joueur0_id", _id: 0 } },{ $unionWith: { coll: "equipes", pipeline: [ { $project: { joueur:"$joueur1_id", _id: 0 } } ]} },{$group:{_id:"$joueur"}},{$lookup: {from: "joueurs", localField:"_id",foreignField:"_id",as: "joueur"}},{$lookup:{from:"pays", localField:"joueur.pays_id",foreignField:"_id",as:"pays"}},{$match:{"pays.nom_pays":"France"}},{$addFields:{joueur_nom:"$joueur.nom"}},{$project:{"joueur_nom":1,_id:0}} ]);

//5
db.tournois.aggregate([{$lookup: {from: "pays", localField:"pays_id",foreignField:"_id",as: "pays"}},{$addFields:{"pays":"$pays.nom_pays","monnaie":"$pays.monnaie"}},{$project:{_id:0,nom:1,"pays":1,dotation:1,"monnaie":1}}]);

//6
db.tournois.find({},{nom:1,dotation:1,_id:0}).sort({dotation:1}).limit(1)

//7
db.tournois.find({},{nom:1,dotation:1,_id:0}).sort({dotation:-1}).limit(1)

//Second bloc 
//1
db.joueurs.find().sort({nom:1})

//2
db.simples.aggregate([{$group:{_id:"$joueur_id",avg_score:{$avg:"$score"},no_simples:{$sum:1}}},{$lookup:{from:"joueurs",localField:"_id",foreignField:"_id",as:"joueur"}},{$project:{"joueur.nom":1,avg_score:1,no_simples:1,"joueur.no":1,_id:0}},{$sort:{"avg_score":1}}])

//3
db.equipes.aggregate([{$group:{_id:"$no",final_score:{$sum:"$score"},no_simples:{$sum:1}}},{$project:{final_score:1,no_simples:1, equipe:"$_id",_id:0}},{$sort:{"final_score":-1}}])

//4
db.simples.aggregate([{$group:{_id:"$joueur_id",total_simple_score:{$sum:"$score"},no_simples:{$sum:1}}},{$lookup:{from:"joueurs",localField:"_id",foreignField:"_id",as:"joueur"}},{$lookup:{from:"pays",localField:"joueur.pays_id",foreignField:"_id",as:"pays"}},{$addFields:{"pays":"$pays.nom_pays","joueur_no":"$joueur.no","joueur_nom":"$joueur.nom"}},{$project:{"joueur_nom":1,total_simple_score:1,no_simples:1,"joueur_no":1,"pays":1,_id:0}},{$sort:{"total_simple_score":-1}}])

//5
db.simples.aggregate([{$group:{_id:"$joueur_id",simples:{$addToSet:"$tournoi_id"}}},{$lookup:{from:"joueurs",localField:"_id",foreignField:"_id",as:"joueur"}},{$addFields:{"joueur_nom":"$joueur.nom",no_simples:{"$size":"$simples"}}},{$match:{"no_simples":4}},{$project:{_id:0,joueur_nom:1}},{$sort:{"joueur.nom":1}}])
