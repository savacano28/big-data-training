use TD2;

db.joueurs.insertMany([
  {_id: 1, no: 10, nom: "Roddick",sexe: "Masculin",pays_id:3},
  {_id: 2, no: 20, nom: "Ginepri",sexe: "Masculin",pays_id:3},
  {_id: 3, no: 30, nom: "Gasquet",sexe: "Masculin",pays_id:2},
  {_id: 4, no: 40, nom: "Monfils",sexe: "Masculin",pays_id:2},
  {_id: 5, no: 100, nom: "Mauresmo",sexe: "Fémenin",pays_id:2},
  {_id: 6, no: 200, nom: "Daveport",sexe: "Fémenin",pays_id:3}
])

db.pays.insertMany([
  {_id: 1, code_pays: "AUS", nom_pays: "Australie",monnaie: "AUD"},
  {_id: 2, code_pays: "FRA", nom_pays: "France",monnaie: "EU"},
  {_id: 3, code_pays: "USA", nom_pays: "Etat-Unis",monnaie: "USD"}
])

db.equipes.insertMany([
  {no: 1,joueur0_id:1,joueur1_id:4,tournoi_id:1,points:9},
  {no: 1,joueur0_id:1,joueur1_id:4,tournoi_id:3,points:7},
  {no: 2,joueur0_id:2,joueur1_id:3,tournoi_id:1,points:7},
  {no: 2,joueur0_id:2,joueur1_id:3,tournoi_id:3,points:8}
])

db.simples.insertMany([
  {joueur_id:1,tournoi_id:1,points:7},
  {joueur_id:2,tournoi_id:1,points:8},
  {joueur_id:6,tournoi_id:1,points:4},
  {joueur_id:1,tournoi_id:2,points:8},
  {joueur_id:3,tournoi_id:2,points:1},
  {joueur_id:4,tournoi_id:2,points:3},
  {joueur_id:1,tournoi_id:3,points:8},
  {joueur_id:1,tournoi_id:4,points:0},
])

db.tournois.insertMany([
  {_id:1,nom: "Roland Garros", date: "10-06-2000",coef:10, dotation:700000, pays_id:2},
  {_id:2,nom: "Open d'Autralie", date: "15-10-2000",coef:5, dotation:700000, pays_id:1},
  {_id:3,nom: "Flusshing Meadows", date: "10-11-2000",coef:6, dotation:1000000, pays_id:3},
  {_id:4,nom: "Open de Paris-Bercy", date: "10-12-2000",coef:4, dotation:300000, pays_id:2}
  ])

show collections
