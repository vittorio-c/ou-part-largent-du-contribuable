from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
import seaborn as sns
from pyspark.sql.functions import *
import matplotlib.pyplot as plt

# Nombre de bénificiares par an
def nbrBenParAnnee(df):
    df = df.select("Annee", "Benificiaire")
    df = df.groupBy("Annee").agg({'Benificiaire':'count'}).where((df.Annee < 2021) & (df.Annee >= 2010)).sort(desc("Annee"))
    df.show()

# Calculer le montant total accordé par année
def montantAccordeParAnnee(df):
    df = df.select("Annee", "Montant")
    df = df.groupBy("Annee").agg(sum('Montant').alias('Montant_total')).where(
        (df.Annee < 2021) & (df.Annee >= 2010)).sort(desc("Annee"))
    df.show()
    pandasDF = df.toPandas()
    sns.set_theme(style="whitegrid")
    sns.barplot(x="Annee", y="Montant_total", data=pandasDF)

# Nombre de demande par année
def nbrDemandeParAnnee(df):
    df = df.select("Annee", "Num")
    df = df.groupBy("Annee").agg(count('Num').alias('Nombre_de_demandes')).where(
        (df.Annee < 2021) & (df.Annee >= 2010)).sort(desc("Annee"))
    df.show()
    pandasDF = df.toPandas()
    sns.set_theme(style="whitegrid")
    sns.barplot(x="Annee", y="Nombre_de_demandes", data=pandasDF)