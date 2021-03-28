#-- code python pour : Module creation entrepot donnees
# -*- coding: utf-8 -*-

import sys, re, nltk
import psycopg2
from Datalake_Parametrage import myPathRoot_DATASOURCE
from Datalake_Parametrage import myPathRoot_LANDINGZONE
from Datalake_Parametrage import myPathRoot_CURRATEDZONE
from Datalake_Parametrage import db_user
from Datalake_Parametrage import db_psw
from Datalake_Parametrage import db_port
from Datalake_Parametrage import db_host

from textblob import TextBlob
from textblob_fr import PatternTagger, PatternAnalyzer
from collections import Counter
from nltk.corpus import stopwords

def Initialization_Database () : 
    con = psycopg2.connect(database="BASE_CURATED_ZONE", user=db_user, password=db_psw, host=db_host, port=db_port)
    cur = con.cursor()
    
    cur.execute("DROP TABLE IF EXISTS DIM_SOCIETE;");
    cur.execute("DROP TABLE IF EXISTS AVIS;");
    cur.execute("DROP TABLE IF EXISTS EMPLOI;");
    cur.execute("DROP TABLE IF EXISTS FAIT_EMPLOIS;");
    cur.execute("DROP TABLE IF EXISTS FAIT_AVIS;");
    
    cur.execute('''CREATE TABLE DIM_SOCIETE
                  (CLE DECIMAL PRIMARY KEY NOT NULL,
                  EMPLACEMENT_SOURCE CHAR(500),
                  DATETIME_INGESTION DATE,
                  PRIVACY_LEVEL CHAR(50),
                  NOM_ENTREPRISE CHAR(500),
                  NRO_AVIS CHAR(500),
                  SITE_WEB CHAR(500),
                  TAILLE CHAR(500),
                  DATE_FONDATION CHAR(500),
                  SECTEUR CHAR(500),
                  REVENU CHAR(500),
                  TYPE_ENTREPRISE CHAR(500)
                  );''')
    print("Table SOCIETE created successfully")
    
    cur.execute('''CREATE TABLE EMPLOI
                  (CLE DECIMAL PRIMARY KEY NOT NULL,
                  EMPLACEMENT_SOURCE CHAR(500),
                  DATETIME_INGESTION DATE,
                  PRIVACY_LEVEL CHAR(50),
                  POSTE CHAR(500),
                  ENTREPRISE CHAR(500),
                  LOCATION CHAR(500),
                  DATE_PUBLICATION CHAR(50),
                  NRO_CANDIDATES INT,
                  DESCRIPTION_JOB CHAR(5000),
                  HIERARCHIE CHAR(5000),
                  TYPE_EMPLOI CHAR(5000),
                  FUNCTION CHAR(5000),
                  SECTEURS CHAR(5000)
                  );''')
    print("Table EMPLOI created successfully")
    
    cur.execute('''CREATE TABLE FAIT_EMPLOIS
                  (ENTREPRISE CHAR(5000),
                  LOCATION CHAR(5000),
                  TYPE_EMPLOI CHAR(5000),
                  HIERARCHIE CHAR(5000),
                  SECTEURS CHAR(5000),
                  FUNCTION CHAR(5000),
                  DATE_PUBLICATION CHAR(5000),
                  NB_POSTES INT,
                  MYN_NB_CANDIDATES FLOAT
                  );''')
    print("Table FAIT_EMPLOIS created successfully")
                        
    cur.execute('''CREATE TABLE AVIS
                  (CLE DECIMAL PRIMARY KEY NOT NULL,
                  EMPLACEMENT_SOURCE CHAR(500),
                  DATETIME_INGESTION DATE,
                  PRIVACY_LEVEL CHAR(50),
                  ENTREPRISE CHAR(500),
                  DATE_AVIS DATE,
                  REVIEW_TITRE FLOAT,
                  STATUS_EMPLOYE CHAR(500),
                  LIEU CHAR(500),
                  RECOMMANDE CHAR(500),
                  COMMENTAIRE FLOAT,
                  AVANTAGE CHAR(5000),
                  INCONVENIENT CHAR(5000)
                  );''')
    print("Table AVIS created successfully")
    
    cur.execute('''CREATE TABLE FAIT_AVIS
                  (ENTREPRISE CHAR(500),
                  DATE_AVIS DATE,
                  STATUS_EMPLOYE CHAR(500),
                  LIEU CHAR(500),
                  RECOMMANDE CHAR(500),
                  MYN_REVIEW_TITRE CHAR(500),
                  MYN_COMMENTAIRE CHAR(500),
                  AVANTAGES CHAR(500),
                  INCONVENIENTS CHAR(500)
                  );''')
    print("Table FAIT_AVIS created successfully")
        
    con.commit()
    con.close()
    return (True)

def Insert_Donnees_SOC() :
    con = psycopg2.connect(database="BASE_CURATED_ZONE", user=db_user, password=db_psw, host=db_host, port=db_port)
    cur = con.cursor()
    myFilePathName = myPathRoot_CURRATEDZONE + "SOC.txt"
    myFilePtr = open(myFilePathName, "r", encoding="utf-8", errors="ignore")
    myFileContents = myFilePtr.readlines()
    del myFileContents[0] 
    
    for myLineRead in myFileContents: 
        line = myLineRead.split(";")
        cle_unique = int(line[0])
        emplacement_source=line[1]
        datetime_ingestion=line[2]
        privacy_level=line[3]
        nom_entreprise=line[4]
        nro_avis=line[5]
        site_web=line[6]
        taille=line[7]
        date_fondation=line[8]
        secteur=line[9]
        revenu=line[10]
        type_entreprise=line[11].replace('\n','')

        cur.execute("INSERT INTO DIM_SOCIETE VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (cle_unique,emplacement_source,datetime_ingestion,privacy_level,nom_entreprise,nro_avis,site_web,taille,date_fondation,secteur,revenu,type_entreprise))
        
    myFilePtr.close()
    con.commit()
    con.close()
    print("Les insertions dans la table societe ont été faits")
    return (True)

def Insert_Donnees_EMP() :
    con = psycopg2.connect(database="BASE_CURATED_ZONE", user=db_user, password=db_psw, host=db_host, port=db_port)
    cur = con.cursor()
    myFilePathName = myPathRoot_CURRATEDZONE + "EMP.txt"
    myFilePtr = open(myFilePathName, "r", encoding="utf-8", errors="ignore")
    myFileContents = myFilePtr.readlines()
    del myFileContents[0] 
    
    nltk.download('stopwords')
    stops = stopwords.words('french')+stopwords.words('english')
     
    for myLineRead in myFileContents: 
        line = myLineRead.split(";")
        cle_unique = int(line[0])
        emplacement_source=line[1]
        datetime_ingestion=line[2]
        privacy_level=line[3]
        poste=line[4]
        entreprise=line[5]
        location=line[6]
        date_publication=line[7]
        nro_candidates=line[8].split()[0]
        mostW = Counter(word for word in filter(str.isalpha,line[9].lower().split()) if word not in stops).most_common(1)
        description_job= mostW.pop(0)[0]
        hierarchie=line[10]
        type_emploi=line[11]
        function=line[12]
        secteurs=line[13]
        
        cur.execute("INSERT INTO EMPLOI VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (cle_unique,emplacement_source,datetime_ingestion,privacy_level,poste,entreprise,location,date_publication,nro_candidates,description_job,hierarchie,type_emploi,function,secteurs))
            
    myFilePtr.close()
    con.commit()
    con.close()
    print("Les insertions dans la table emploi ont été faits")
    return (True)

def Insert_Donnees_AVI() :
    con = psycopg2.connect(database="BASE_CURATED_ZONE", user=db_user, password=db_psw, host=db_host, port=db_port)
    cur = con.cursor()
    myFilePathName = myPathRoot_CURRATEDZONE + "AVI.txt"
    myFilePtr = open(myFilePathName, "r", encoding="utf-8", errors="ignore")
    myFileContents = myFilePtr.readlines()
    del myFileContents[0] 
     
    for myLineRead in myFileContents: 
        line = myLineRead.split(";")
        cle_unique = int(line[0])
        emplacement_source=line[1]
        datetime_ingestion=line[2]
        privacy_level=line[3]
        entreprise=line[4]
        if line[5] == 'NULL' : 
            date = 'May 24, 2020'
        else :
            date = line[5]
        review_titre=TextBlob(line[6], pos_tagger = PatternTagger(), analyzer= PatternAnalyzer()).sentiment[0]
        status_employe=line[7]
        lieu=line[8]
        recommande=line[9]
        commentaire=TextBlob(line[10], pos_tagger = PatternTagger(), analyzer= PatternAnalyzer()).sentiment[0]
        avantage= line[11].lower()
        incovenient= line[12].lower()
        
        cur.execute("INSERT INTO AVIS VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (cle_unique,emplacement_source,datetime_ingestion,privacy_level,entreprise,date,review_titre,status_employe,lieu,recommande,commentaire,avantage,incovenient))
    
        
    myFilePtr.close()
    con.commit()
    con.close()
    print("Les insertions dans la table avis ont été faits")
    return (True)

def Insert_Donnees_FAIT_AVIS() :
    con = psycopg2.connect(database="BASE_CURATED_ZONE", user=db_user, password=db_psw, host=db_host, port=db_port)
    cur = con.cursor()
    cur.execute("select entreprise,date_avis,status_employe,lieu, recommande, avg(review_titre) as myn_rev_titre,avg(commentaire) as myn_commentaire, count(avantage) avantages, count(inconvenient) inconvenients from avis group by entreprise,date_avis,status_employe,lieu,recommande order by lieu")
    rows = cur.fetchall()
    for row in rows:
        cur.execute("INSERT INTO FAIT_AVIS VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]))

    con.commit()
    cur.close()
    con.close()
    print("Les insertions dans la table Fait_Avis ont été faits")
    return (True)

def Insert_Donnees_FAIT_EMPLOIS() :
    con = psycopg2.connect(database="BASE_CURATED_ZONE", user=db_user, password=db_psw, host=db_host, port=db_port)
    cur = con.cursor()
    cur.execute("select entreprise,location,type_emploi,hierarchie, secteurs, function, date_publication, count(poste) as nb_postes, round(avg(nro_candidates)) myn_nb_candidates from emploi group by entreprise,location,type_emploi,hierarchie, secteurs, function, date_publication order by entreprise,location,type_emploi,hierarchie, secteurs, function, date_publication")
    rows = cur.fetchall()
    for row in rows:
        cur.execute("INSERT INTO FAIT_EMPLOIS VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]))
    
    con.commit()
    cur.close()
    con.close()
    print("Les insertions dans la table Fait_Emplois ont été faits")
    return (True)




