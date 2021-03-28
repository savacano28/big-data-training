# -*- coding: utf-8 -*-

#-- Votre code python pour : Module Acquisition des donnees 

#------------------------------------------------------------------------------
# Importation des bibliotheques utilisee dans ce module
#------------------------------------------------------------------------------
import os, fnmatch
import shutil

#------------------------------------------------------------------------------
# Importation des fonctions , procedure, ... et variables dans ce module
#------------------------------------------------------------------------------
from Datalake_Parametrage import myPathRoot_DATASOURCE
from Datalake_Parametrage import myPathRoot_LANDINGZONE

#==============================================================================
#==============================================================================
#-- Parcours et recuperation des donnees sources en les recopiant à l'identique :
#     tous les fichiers du repertoire de DATA SOURCE WEB 
#     vers les repertoires correspondants de la LANDING ZONE (EMP, SOC, AVI)
#==============================================================================
#==============================================================================

#-- Fonction qui recopie des fichiers DATA SOURCE en LANDING ZONE selon les parametres passes 
def Recuperation_Fichiers_HTML_SOURCE(ChoixDebug=True, TypeDeFichier=None, OrigineDuFichier=None):
    
    #------------------------------------------------------------------------------    
    #-- Exemple de controle fonctionnel des parametres en entre de la fonction
    #------------------------------------------------------------------------------
    if TypeDeFichier not in ('EMP', 'SOC', 'AVI'):
        #- Si le parametre n'est pas conforme a ce que l'on atttend
        #  on sort de la procedure ici avec "return" en renvoyant une chaine d'erreur
        print('ERREUR-001: Fonction : Recuperation_Fichiers_HTML_SOURCE : Parametre "TypeDeFichier" incorrect ou non renseigne')
        return(False)

    if OrigineDuFichier not in ('GLASSDOOR', 'LINKEDIN'):
        #- Si le parametre n'est pas conforme a ce que l'on atttend
        #  on sort de la procedure ici avec "return" en renvoyant une chaine d'erreur
        print('ERREUR-002: Fonction : Recuperation_Fichiers_HTML_SOURCE : Parametre "OrigineDuFichier" incorrect ou non renseigne')
        return(False)


    #------------------------------------------------------------------------------    
    #-- Preparation des donnees contextuelle necessaires dans cette fonction
    #------------------------------------------------------------------------------
    if   TypeDeFichier == 'EMP':
        mySousRepertoire = 'EMP' 
        myFiltre = 'INFO-EMP'
    elif TypeDeFichier == 'SOC':
        mySousRepertoire = 'SOC'
        myFiltre = 'INFO-SOC'
    elif TypeDeFichier == 'AVI':
        mySousRepertoire = 'AVI'
        myFiltre = 'AVIS-SOC'
    else:
        pass #-- Normalement impossible si contraintes fonctionnelles respectees niveau parametre
             #   RQ : "pass" est une instruction qui ne fait rien , 
             #        mais permet de respecter ici la syntaxe du else 

    
    #------------------------------------------------------------------------------    
    #-- On peut commencer l'execution des instruction de cette fonction
    #------------------------------------------------------------------------------    
    myPathHtmlDATASOURCE = myPathRoot_DATASOURCE
    #-- Verification en phase debugage
    print(myPathHtmlDATASOURCE)

    #------------------------------------------------------------------------------
    # Listage les nom des fichier present dans le repertoire a traiter
    #------------------------------------------------------------------------------
    myListOfFileALL = []
    
    #-- La fonction "listdir"  ramène tous les noms des fichiers du répertoire dans un liste
    myListOfFileALL = os.listdir(myPathHtmlDATASOURCE)
    
    #------------------------------------------------------------------------------
    # Creation des listes specifique chaque type de fichier à filtrer :
    #  - EMP pour les Information sur les emplois proposé par les sociétés (LINKEDIN)
    #  - SOC pour les Information sur la  Societes (GLASSDOOR)
    #  - AVI pour les Avis sur la Societes (GLASSDOOR)
    #  - XXX pour d'autre fichiers à venir integrer un jour 
    #------------------------------------------------------------------------------
    
    #------------------------------------------------------------------------------
    #-- Exemple de solution => Filtrage selon le ypde de fichier passe en parametre 
    #   (SOC, AVI, EMP, ... 
    #------------------------------------------------------------------------------
    myPattern = "*" + myFiltre + "*.html"  #-- Pour le parametre 'AVI' par exemple cela generera  :  myPattern = "*" + 'AVI-SOC' + "*.html"
    #-- Verification en phase debugage
    if (ChoixDebug):  print(myPattern)

    myListOfFile = []
    
    #-- Creation de la liste avec selection des nom de fichiers à selectionner
    for myFileName in myListOfFileALL: 
        if fnmatch.fnmatch(myFileName, myPattern):
            myListOfFile.append(myFileName)
    
    #   Exemple de bibliothèque python utilisable : 
    #     -  Importer les bibliotheques Python :  import os, shutil
    #     -  Supprimer un fichier : 
    #           os.remove("nom du fichier")
    #     -  Copier un fichier Supprimer un fichier : 
    #           shutil.copy("repertoire / nom du fichier source" , "repertoire / nom du fichier cible") 
    #        
    #   Remarque : Si vous voulez rendre votre code plus robuste, 
    #              vous pouver gerer le traitement des exceptions avec :
    #              "try / except ..."
            
    
    #-- Parcours de la liste des nom de fichier AVI
    #   pour leur recopie dans l'emplacement en  LANDING ZONE qui correspond au type passe en parametre
    myPathHtmlLANDINGZONE = myPathRoot_LANDINGZONE + "/" + OrigineDuFichier + "/" + mySousRepertoire

    #-- Verification en phase debugage
    if (ChoixDebug): 
        print(myPathHtmlLANDINGZONE )
    
    for myFileNameTmp in myListOfFile: 
        if (ChoixDebug):
            #-- Verification en phase debugage
            print ('shutil.copy ("'+ myPathHtmlDATASOURCE  + '/' + myFileNameTmp + '" , "'  + myPathHtmlLANDINGZONE + '/' + myFileNameTmp + '")', '\n')
        else :
            #-- Recopie relle du fichier en cours
            shutil.copy(myPathHtmlDATASOURCE  + "/" + myFileNameTmp, myPathHtmlLANDINGZONE + "/" + myFileNameTmp)
        
    return(True)    