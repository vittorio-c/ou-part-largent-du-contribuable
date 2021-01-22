from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from secteurUtilities import *

spark = SparkSession.builder.getOrCreate()




def getSecteurParAnnee(df): 
    
    df = df.select ("Annee", "Secteur" ) 
    
    df = df.groupBy("Annee").agg({'Secteur' : 'count'}).where((df.Annee >= 2013) & (df.Annee <= 2021)).sort(asc("Annee"))
    
    df.show()
                                 
    
                                 
    
    
def montantSecteurAnnee(df):
    
    df = df.select ("Annee", "Secteur" ,"Montant")
    
    #secteurs = selectSecteur(df, df.Secteur);
   
    df = df.groupBy("Annee" , "secteur").agg({'Montant' : 'sum'})
    
    df.show()
    
    
   