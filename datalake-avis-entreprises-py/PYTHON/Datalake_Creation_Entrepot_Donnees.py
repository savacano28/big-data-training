#-- code python pour : Module creation entrepot donnees
# -*- coding: utf-8 -*-

import sys, re, nltk
import psycopg2
from Datalake_Parametrage import myPathRoot_DATASOURCE
from Datalake_Parametrage import myPathRoot_LANDINGZONE
from Datalake_Parametrage import myPathRoot_CURRATEDZONE
from textblob import TextBlob
from textblob_fr import PatternTagger, PatternAnalyzer
from collections import Counter
from nltk.corpus import stopwords

def Initialization_Databse () : 
    con = psycopg2.connect(database="BASE_CURATED_ZONE", user="postgres", password="admin", host="127.0.0.1", port="5433")
    
    cur = con.cursor()
    
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
    
    cur.execute('''CREATE TABLE FAIT_EMPLOI
                  (CLE DECIMAL PRIMARY KEY NOT NULL,
                  ENTREPRISE CHAR(500),
                  LOCATION CHAR(500),
                  TYPE_EMPLOI CHAR(500),
                  HIERARCHIE CHAR(500),
                  MAX_DATE_PUBLICATION DATE,
                  NRO_POSTES CHAR(500),
                  NRO_POSTES_UNIQ CHAR(500),
                  AVG_CANDIDATES INT,
                  MAX_DESCRIPTION_JOB_MOT CHAR(500),
                  MAX_FUNCTION CHAR(500),
                  MAX_SECTEURS CHAR(500)
                  );''')
    print("Table FAIT_EMPLOI created successfully")
                        
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
                  (CLE DECIMAL PRIMARY KEY NOT NULL,
                  ENTREPRISE CHAR(500),
                  DATE_AVIS DATE,
                  STATUS_EMPLOYE CHAR(50),
                  LIEU CHAR(500),
                  AVG_RECOMMANDE CHAR(500),
                  SENT_COMMENTAIRE CHAR(500),
                  SENTI_REVIEW_TITRE CHAR(500),
                  MAX_AVANTAGE CHAR(500),
                  MAX_INCONVENIENT CHAR(500),
                  MIN_AVANTAGE CHAR(500),
                  MIN_INCONVENIENT CHAR(500)
                  );''')
    print("Table FAIT_AVIS created successfully")
        
    con.commit()
    con.close()
    return (True)

def Insert_Donnees_SOC() :
    con = psycopg2.connect(database="BASE_CURATED_ZONE", user="postgres", password="admin", host="127.0.0.1", port="5433")
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
    return (True)

def Insert_Donnees_EMP() :
    con = psycopg2.connect(database="BASE_CURATED_ZONE", user="postgres", password="admin", host="127.0.0.1", port="5433")
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
    return (True)

def Insert_Donnees_AVI() :
    con = psycopg2.connect(database="BASE_CURATED_ZONE", user="postgres", password="admin", host="127.0.0.1", port="5433")
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
    return (True)

def Insert_Donnees_FAIT_AVI() :
    con = psycopg2.connect(database="BASE_CURATED_ZONE", user="postgres", password="admin", host="127.0.0.1", port="5433")
    cur = con.cursor()
    cur.execute("SELECT entreprise, date_avis, status_employe,lieu from AVIS group by (entreprise, date_avis, status_employe,lieu)")
    rows = cur.fetchall()
    
    for row in rows :
        
        
        
        cur.execute("INSERT INTO AVIS VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (entreprise,date,status_employe,lieu,recommande,commentaire,review_titre,max_avantage,max_incovenient,min_avantage,min_incovenient))
    
    
    con.close()
    return (True)



