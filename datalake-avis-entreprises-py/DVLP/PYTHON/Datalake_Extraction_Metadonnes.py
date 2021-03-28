#-- Votre code python pour : Module Extraction des Metadonnes 
# -*- coding: utf-8 -*-

import sys, os, fnmatch, re, random
from bs4 import BeautifulSoup
from Datalake_Parametrage import myPathRoot_DATASOURCE
from Datalake_Parametrage import myPathRoot_LANDINGZONE
from Datalake_Parametrage import myPathRoot_CURRATEDZONE

myPathCuratedZone = myPathRoot_CURRATEDZONE
myPathHtmlSOC = myPathRoot_LANDINGZONE + "/GLASSDOOR/SOC/"
myPathHtmlAVI = myPathRoot_LANDINGZONE + "/GLASSDOOR/AVI/"
myPathHtmlEMP = myPathRoot_LANDINGZONE + "/LINKEDIN/EMP/"

#==============================================================================
#-- Parcourir et faire un traitement sur des fichiers d'un répertoire 
#==============================================================================
myListOfFileSOC = []
myListOfFileAVI = []
myListOfFileEMP = []

#-- ramène tous les noms des fichiers du répertoire 
myListOfFileSOC = os.listdir(myPathHtmlSOC)
myListOfFileAVI = os.listdir(myPathHtmlAVI)
myListOfFileEMP = os.listdir(myPathHtmlEMP)

###############################################################################   
# SUBFUNCTIONS
############################################################################### 
from datetime import datetime
def Get_datetime_ingestion_AVI():
    Result = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    return(Result)

#==============================================================================
#-- Extraction d'information à partir d'un flux ou d'un fichier HTML en python
#==============================================================================

############################################################################### 
#GLASSDOOR (extraction INFOS SUR ENTREPRISE)
###############################################################################
def Generation_Fichiers_avec_Metadonnees_SOC():
    myFilePathName = myPathCuratedZone + "SOC.txt"
    myFilePtr = open(myFilePathName, "w", encoding = "utf-8")
    myListeDeLigneAEcrire = [] 
    #Societe glassdoor
    myListeDeLigneAEcrire.append('cle_unique;emplacement_source;datetime_ingestion;privacy_level;nom_entreprise;nro_avis;site_web;taille;date_fondation;secteur;revenu;type_entreprise'+ '\n')
    
    for myEntry in myListOfFileSOC :  
        f = open(myPathHtmlSOC+myEntry, "r", encoding="utf8")
        myHTMLContents = f.read()
        f.close()
        mySoup = BeautifulSoup(myHTMLContents, 'lxml') 
        if len(mySoup.find_all('div', attrs = {'class':"infoEntity"})) == 7 :
            cle_unique=str(random.getrandbits(128))
            emplacement_source=myEntry
            datetime_ingestion=str(Get_datetime_ingestion_AVI())
            privacy_level="0"
            nom_entreprise=(mySoup.find('h1')['data-company'])
            nro_avis=str(mySoup.find_all('a', attrs = {'data-label':"Avis"})[0].span.contents[0]).replace(";", ",").replace('\n', ' ').replace('\r', '')
            site_web=re.search('>(.+?)<',str(mySoup.find_all('div', attrs = {'class':"infoEntity"})[0].span.contents[0])).group(1)
            taille= str(mySoup.find_all('div', attrs = {'class':"infoEntity"})[2].span.contents[0]).replace(";", ",").replace('\n', ' ').replace('\r', '')
            date_fondation=str(mySoup.find_all('div', attrs = {'class':"infoEntity"})[3].span.contents[0]).replace(";", ",").replace('\n', ' ').replace('\r', '')
            secteur=str(mySoup.find_all('div', attrs = {'class':"infoEntity"})[5].span.contents[0]).replace(";", ",").replace('\n', ' ').replace('\r', '')
            revenu=str(mySoup.find_all('div', attrs = {'class':"infoEntity"})[6].span.contents[0]).replace(";", ",").replace('\n', ' ').replace('\r', '')
            type_entreprise=str(mySoup.find_all('div', attrs = {'class':"infoEntity"})[4].span.contents[0]).replace(";", ",").replace('\n', ' ').replace('\r', '')
    
            myListeDeLigneAEcrire.append(cle_unique+";"+emplacement_source+";"+datetime_ingestion+";"+privacy_level+";"+nom_entreprise+";"+nro_avis+";"+site_web+";"+taille+";"+date_fondation+";"+secteur+";"+revenu+";"+type_entreprise+ '\n')
        
    myFilePtr.writelines(myListeDeLigneAEcrire)
    #close stream    
    myFilePtr.close()
    return (True)
   
###############################################################################
#==============================================================================
#-- GLASSDOOR (AVIS)
#==============================================================================
def Get_nom_entreprise_AVI (Soup):
    myTest = Soup.find_all('div', attrs = {"class":"header cell info"})[0].span.contents[0]
    if (myTest == []) : 
        Result = "NULL"
    else:
        Result = myTest.replace(";", ",").replace('\n', ' ').replace('\r', '')
    return(Result)

def Get_note_moy_entreprise_AVI(Soup):
    myTest = Soup.find_all('div', attrs = {'class':'v2__EIReviewsRatingsStylesV2__ratingNum v2__EIReviewsRatingsStylesV2__large'})[0].contents[0]
    if (myTest == []) : 
        Result = "NULL"
    else:
        Result = myTest.replace(";", ",").replace('\n', ' ').replace('\r', '') 
    return(Result)

def Get_employe_actual(soup2):
    myTest2 = soup2.find_all('span', attrs = {'class':'authorJobTitle middle reviewer'})
    if (myTest2 == []) :        
        return "NULL"
    else :
        return (re.sub(r'<span (.*)">(.*)</span>(.*)', r'\2', str(myTest2[0])).replace(";", ",").replace('\n', ' ').replace('\r', ''))
           
def Get_ville_employe(soup2):
    myTest2 = soup2.find_all('span', attrs = {'class':'authorLocation'}) 
    if (myTest2 == []) :
        return "NULL"
    else :
        return (re.sub(r'<span (.*)">(.*)</span>(.*)', r'\2', str(myTest2[0])).replace(";", ",").replace('\n', ' ').replace('\r', ''))
    
def Get_commentaire(soup2):
    myTest2= soup2.find_all('p', attrs = {'class':'mainText mb-0'}) 
    if (myTest2 == []) :        
        return "NULL"
    else :
        return (myTest2[0].text.replace(";", ",").replace('\n', ' ').replace('\r', ''))
        
def Get_date(soup2):
    myTest2= soup2.find_all('time', attrs = {'class':'date subtle small'}) 
    if (myTest2 == []) :        
        return "NULL"
    else :
        return (myTest2[0].text.replace(";", ",").replace('\n', ' ').replace('\r', ''))

def Get_review_titre(soup2):
    myTest2= soup2.find_all('a', attrs = {'class':'reviewLink'}) 
    if (myTest2 == []) :        
        return "NULL"
    else :
        return (myTest2[0].text.replace(";", ",").replace('\n', ' ').replace('\r', ''))
    
def Get_recommend(soup2):
    myTest2= soup2.find_all('div', attrs = {'class':'row reviewBodyCell recommends'})
    if (myTest2 == []) :        
        return "Ne recommande pas"
    else :
        return (myTest2[0].contents[0].text.replace(";", ",").replace('\n', ' ').replace('\r', ''))
    
def Get_avantages(soup2):
    myTest2= soup2.find_all('div', attrs = {'class':'mt-md common__EiReviewTextStyles__allowLineBreaks'})
    if (myTest2 == []) :        
        return "Ne recommande pas"
    else :
        return (myTest2[0].contents[1].text.replace(";", ",").replace('\n', ' ').replace('\r', ''))
    
def Get_inconvenients(soup2):
    myTest2 = soup2.find_all('div', attrs = {'class':'mt-md common__EiReviewTextStyles__allowLineBreaks'})
    if (myTest2 == []) :        
        return "Pas d’avantages pour les employés"
    else :
        leng = len(myTest2)
        if (leng == 2) : 
            Result = myTest2[1].contents[1].text.replace(";", ",").replace('\n', ' ').replace('\r', '')
        else:
            Result = "Pas d’avantages pour les employés"
        return (Result)

###############################################################################    
# Exemple : GLASSDOOR (extraction AVIS SUR ENTREPRISE)
###############################################################################
# f = open(myPathHtmlAVI+myListOfFileAVI[0], "r", encoding="utf8")
# myHTMLContents = f.read()
# f.close() 
# mySoup = BeautifulSoup(myHTMLContents, 'lxml')
# avis = mySoup.find_all('li', attrs = {'class':'empReview'})
# soup2 = BeautifulSoup(str(avis[1]), 'lxml')
# t = Get_avantages(soup2)
# print(t)

def Generation_Fichiers_avec_Metadonnees_AVI():
    myFilePathName = myPathCuratedZone + "AVI.txt"
    myFilePtr = open(myFilePathName, "w", encoding = "utf-8")
    myListeDeLigneAEcrire = [] 
    #avis glassdoor
    myListeDeLigneAEcrire.append('cle_unique;emplacement_source;datetime_ingestion;privacy_level;entreprise;date;review_titre;status_employe;lieu;recommande;commentaire;avantage;incovenient'+"\n")
    
    for myEntry in myListOfFileAVI :  
        f = open(myPathHtmlAVI+myEntry, "r", encoding="utf8")
        myHTMLContents = f.read()
        f.close() 
        mySoup = BeautifulSoup(myHTMLContents, 'lxml')
        avis = mySoup.find_all('li', attrs = {'class':'empReview'})
        myListTab=[[]]
        if (avis == []) : 
            print("NULL")
        else:
            for x in range(0, len(avis)) :
                if x == 0: 
                    myListTab[0] = ['"0"']
                else:
                    soup2 = BeautifulSoup(str(avis[x]), 'lxml')
                    cle_unique=str(random.getrandbits(128))
                    emplacement_source=myEntry
                    datetime_ingestion=str(Get_datetime_ingestion_AVI())
                    privacy_level="0"
                    entreprise=Get_nom_entreprise_AVI(mySoup)
                    date=Get_date(soup2)
                    review_titre=Get_review_titre(soup2)[1:-1]
                    status_employe=Get_employe_actual(soup2)
                    lieu=Get_ville_employe(soup2)
                    recommande=Get_recommend(soup2)
                    commentaire=Get_commentaire(soup2)
                    avantage=Get_avantages(soup2)
                    incovenient=Get_inconvenients(soup2)
                
                    myListeDeLigneAEcrire.append(cle_unique+";"+emplacement_source+";"+datetime_ingestion+";"+privacy_level+";"+entreprise+";"+date+";"+review_titre+";"+status_employe+";"+lieu+";"+recommande+";"+commentaire+";"+avantage+";"+incovenient+'\n')
        
    myFilePtr.writelines(myListeDeLigneAEcrire)
    myFilePtr.close()
    return (True)
    
###############################################################################
#==============================================================================
#-- LINKEDIN (EMPLOI) 
#==============================================================================
def Get_libelle_emploi_EMP(Soup):
    myTest = Soup.find_all('h1', attrs = {'class':'topcard__title'})
    if (myTest == []) : 
        Result = "NULL"
    else:
        myTest = str(myTest[0].text)
        if (myTest == []) : 
            Result = "NULL"
        else:
            Result = myTest.replace(";", ",").replace('\n', ' ').replace('\r', '')
    return(Result)

def Get_nom_entreprise_EMP(Soup):
    myTest = Soup.find_all('span', attrs = {'class':'topcard__flavor'}) 
    if (myTest == []) : 
        Result = "NULL"
    else:
        myTest = str(myTest[0].text)
        if (myTest == []) : 
            Result = "NULL"
        else :
            Result = myTest.replace(";", ",").replace('\n', ' ').replace('\r', '')
    return(Result)

def Get_ville_emploi_EMP (Soup):
    myTest = Soup.find_all('span', attrs = {'class':'topcard__flavor topcard__flavor--bullet'}) 
    if (myTest == []) : 
        Result = "NULL"
    else:
        myTest = str(myTest[0].text)
        if (myTest == []) : 
            Result = "NULL"
        else:
            Result = myTest.replace(";", ",").replace('\n', ' ').replace('\r', '')
    return(Result)


def Get_date_emploi_EMP (Soup):
    myTest = Soup.find_all('span', attrs = {'class':'topcard__flavor--metadata posted-time-ago__text'})
    if (myTest == []) : 
        Result = "NULL"
    else:
        myTest = str(myTest[0].text)
        if (myTest == []) : 
            Result = "NULL"
        else:
            Result = myTest.replace(";", ",").replace('\n', ' ').replace('\r', '')
    return(Result)

def Get_candidats_emploi_EMP (Soup):
    myTest = Soup.find_all('span', attrs = {'class':'topcard__flavor--metadata topcard__flavor--bullet num-applicants__caption'})
    if (myTest == []) : 
        Result = "0"
    else:
        myTest = str(myTest[0].text)
        if (myTest == []) : 
            Result = "0"
        else:
            Result = myTest.replace(";", ",").replace('\n', ' ').replace('\r', '')
    return(Result)

def Get_texte_emploi_EMP (Soup):
    myTest = Soup.find_all('div', attrs = {"description__text description__text--rich"})
    if (myTest == []) : 
        Result = "NULL"
    else:
        myTest = str(myTest[0].text)
        if (myTest == []) : 
            Result = "NULL"
        else:
            Result = myTest.replace(";", ",").replace('\n', ' ').replace('\r', '')
    return(Result)

###############################################################################   
# LINKEDIN (INFOS EMPLOIS)
############################################################################### 
def Generation_Fichiers_avec_Metadonnees_EMP():
    myFilePathName = myPathCuratedZone + "EMP.txt"
    myFilePtr = open(myFilePathName, "w", encoding = "utf-8")
    myListeDeLigneAEcrire = [] 
    #emp linkied
    myListeDeLigneAEcrire.append('cle_unique;emplacement_source;datetime_ingestion;privacy_level;poste;entreprise;location;date_publication;nro_candidates;description_job;hierarchie;type_emploi;function;secteurs'+"\n")
    
    for myEntry in myListOfFileEMP :  
        f = open(myPathHtmlEMP+myEntry, "r", encoding="utf8")
        myHTMLContents = f.read()
        f.close() 
        mySoup = BeautifulSoup(myHTMLContents, 'lxml')
        cle_unique=str(random.getrandbits(128))
        emplacement_source=myEntry
        datetime_ingestion=str(Get_datetime_ingestion_AVI())
        privacy_level="0"
        poste = Get_libelle_emploi_EMP(mySoup)
        entreprise = Get_nom_entreprise_EMP(mySoup)
        location = Get_ville_emploi_EMP(mySoup)
        date_publication= Get_date_emploi_EMP(mySoup)
        nro_candidates=Get_candidats_emploi_EMP(mySoup)
        description_job=mySoup.find_all('div', attrs = {'class':'description__text description__text--rich'})[0].text
        if len(mySoup.find_all('ul', attrs = {'class':'job-criteria__list'})[0].contents) == 4 :
            hierarchie=str(mySoup.find_all('ul', attrs = {'class':'job-criteria__list'})[0].contents[0].span.contents[0])
            type_emploi=str(mySoup.find_all('ul', attrs = {'class':'job-criteria__list'})[0].contents[1].span.contents[0])
            function=str(mySoup.find_all('ul', attrs = {'class':'job-criteria__list'})[0].contents[2].span.contents[0])
            secteurs=str(mySoup.find_all('ul', attrs = {'class':'job-criteria__list'})[0].contents[3].span.contents[0])
        myListeDeLigneAEcrire.append(cle_unique+";"+emplacement_source+";"+datetime_ingestion+";"+privacy_level+";"+poste+";"+entreprise+";"+location+";"+date_publication+";"+nro_candidates+";"+description_job+";"+hierarchie+";"+type_emploi+";"+function+";"+secteurs + '\n')
    
    myFilePtr.writelines(myListeDeLigneAEcrire)
    myFilePtr.close()
    return (True)