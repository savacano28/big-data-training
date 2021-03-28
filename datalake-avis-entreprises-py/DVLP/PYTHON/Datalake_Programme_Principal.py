# -*- coding: utf-8 -*-


#------------------------------------------------------------------------------
# Importation des bibliotheques utilisee dans ce module
#------------------------------------------------------------------------------
import time as time2
    


#==============================================================================
#==============================================================================
#==  MAIN (__main__ -> Programme Principal appelle par la command en ligne)
#==============================================================================
#==============================================================================
if __name__ == '__main__':
    #------------------------------------------------------------------------------
    # Importation des fonctions vpresent dans un autre module
    #------------------------------------------------------------------------------
    from Datalake_Acquisition_des_donnees import Recuperation_Fichiers_HTML_SOURCE
    from Datalake_Extraction_Metadonnes import Generation_Fichiers_avec_Metadonnees_SOC
    from Datalake_Extraction_Metadonnes import Generation_Fichiers_avec_Metadonnees_EMP
    from Datalake_Extraction_Metadonnes import Generation_Fichiers_avec_Metadonnees_AVI
    from Datalake_Creation_Entrepot_Donnees import Initialization_Database
    from Datalake_Creation_Entrepot_Donnees import Insert_Donnees_SOC
    from Datalake_Creation_Entrepot_Donnees import Insert_Donnees_EMP
    from Datalake_Creation_Entrepot_Donnees import Insert_Donnees_AVI
    from Datalake_Creation_Entrepot_Donnees import Insert_Donnees_FAIT_AVIS
    from Datalake_Creation_Entrepot_Donnees import Insert_Donnees_FAIT_EMPLOIS
    
    #------------------------------------------------------------------------------
    #--DEBUT CHRONO
    #------------------------------------------------------------------------------
    MyBeginTimeSeconds = time2.time()
    print("**** Debut du traitement en Secondes " + str(MyBeginTimeSeconds) + " ***")
    myDebug = False
    print("Landing Zone")
    Recuperation_Fichiers_HTML_SOURCE(ChoixDebug=myDebug, TypeDeFichier='EMP', OrigineDuFichier='LINKEDIN')
    Recuperation_Fichiers_HTML_SOURCE(ChoixDebug=myDebug, TypeDeFichier='SOC', OrigineDuFichier='GLASSDOOR')
    Recuperation_Fichiers_HTML_SOURCE(ChoixDebug=myDebug, TypeDeFichier='AVI', OrigineDuFichier='GLASSDOOR')
    
    #--CURATED ZONE
    print("Curated Zone")
    Generation_Fichiers_avec_Metadonnees_SOC()
    Generation_Fichiers_avec_Metadonnees_AVI()
    Generation_Fichiers_avec_Metadonnees_EMP()
    
    #--RAFFINAGE ZONE
    print("Rafinnage Zone")
    Initialization_Database()
    Insert_Donnees_SOC()
    Insert_Donnees_EMP()
    Insert_Donnees_AVI()
    Insert_Donnees_FAIT_AVIS()
    Insert_Donnees_FAIT_EMPLOIS()
    
    #------------------------------------------------------------------------------
    #-- FIN CHRONO 
    #------------------------------------------------------------------------------    
    MyEndTimeSeconds = time2.time()
    print("\n\n**** Fin du traitement en Secondes " + str(MyEndTimeSeconds) + " ***\n") #EKLEKL
    MyDeltaTimeSeconds = MyEndTimeSeconds - MyBeginTimeSeconds
    print("**** Duree du traitement en Secondes " + str(MyDeltaTimeSeconds) + " ***\n") #EKLEKL

#    sys.exit(1)
           