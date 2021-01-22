import matplotlib.pyplot as plt
import seaborn as sns
# from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def getAmountSubventionByDirection(df):
    '''
    Permet d'obtenir le montant total des 10 directions qui subventionnent
    le plus les associations.

    '''
    df = df.select ("Annee", "Montant" ,"Direction")
    df = df.groupBy("Direction", "Annee").agg(sum('Montant').alias('Montant_Total')).filter(col("Montant_Total") >= 0).where(df.Annee == input("Renseignez votre année : (2013 - 2020)")).sort(desc("Annee"), desc("Montant_Total"))
    df = df.select("Direction", "Montant_Total").limit(5)

    pandasDF = df.toPandas()

    # sns.set_theme(style="whitegrid")
    # sns.barplot(x="Direction", y="Montant_Total", data=pandasDF)

    labels = pandasDF["Direction"]
    sizes = pandasDF["Montant_Total"]
    explode = (0.1, 0, 0, 0, 0)

    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
            shadow=True, startangle=90)
    ax1.axis('equal')

    plt.title('% des 5 plus grandes sommes total donnée par les 5 directions', y=-0.15)
    plt.show()


def getCountAcceptedSubventionByDirection(df):
    '''
    Permet d'obtenir le nombre total de subvention accordées par les directions.

    '''
    df = df.select ("Annee", "Montant" ,"Direction")
    df = df.groupBy("Direction", "Annee").agg(count('Direction').alias('Comptage_Direction')).filter(col("Comptage_Direction") >= 0).where(df.Annee == input("Renseignez votre année : (2013 - 2020)")).sort(desc("Annee"), desc("Comptage_Direction"))
    df = df.select("Direction", "Comptage_Direction").limit(5)

    pandasDF = df.toPandas()

    sns.set_theme(style="whitegrid")
    sns.barplot(x="Direction", y="Comptage_Direction", data=pandasDF)

    # labels = pandasDF["Direction"]
    # sizes = pandasDF["Comptage_Direction"]
    # explode = (0.1, 0, 0, 0, 0)

    # fig1, ax1 = plt.subplots()
    # ax1.pie(sizes, explode=explode, labels=labels,
    #         shadow=True, startangle=90)
    # ax1.axis('equal')

    # plt.title('% des 5 plus grandes sommes total donnée par les 5 directions', y=-0.15)
    # plt.show()

