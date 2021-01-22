from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType = StringType())
def clean_quotes(colonne):

    return colonne.strip('"')

def subvention_by_actor(df: DataFrame):
    df = df.withColumn("Montant", df["Montant"].cast("numeric"))
    return df.groupBy("Benificiaire").sum('Montant').show()

def subvention_by_actor_ordered(df: DataFrame):
    df = df.withColumn("Montant", df["Montant"].cast("numeric"))
    df = df.orderBy(df["Montant"].desc())

    return df.select("Benificiaire", "Montant").show()

def requests_by_actors_by_year(df: DataFrame):
    df = df.groupBy("Benificiaire", "Annee").agg({"Benificiaire": "count"})
    df = df.filter("annee <= 2020")
    df = df.filter(df["Benificiaire"].isNotNull())
    df = df.withColumn("Benificiaire", clean_quotes(df["Benificiaire"]))
    df = df.orderBy(df["Annee"].desc(), df["Benificiaire"])

    return df.show()

def total_of_actors(df: DataFrame):
    from pyspark.sql import functions as F

    df = df.select(F.countDistinct("Benificiaire"))

    return df.show()

def max_requests_by_actors_by_year(df: DataFrame):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    df = df.groupBy("Benificiaire", "Annee").agg({"Benificiaire": "count"})
    df = df.filter("annee <= 2020")
    df = df.filter(df["Benificiaire"].isNotNull()).filter(df["Annee"] != 0)

    w = Window.partitionBy('Annee')

    df = df.withColumn('MaxDemande', F.max("count(Benificiaire)").over(w)) \
        .where(F.col("count(Benificiaire)") == F.col("MaxDemande")) \
        .drop("MaxDemande")

    df = df.orderBy(df["Annee"].desc(), df["Benificiaire"])

    return df.show()

def max_amount_by_actors_by_year(df: DataFrame):
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    df = df.withColumn("Montant", df["Montant"].cast("numeric"))
    df = df.groupBy("Benificiaire", "Annee").agg({"Montant": "sum"})

    w = Window.partitionBy('Annee')

    df = df.withColumn('MaxSomme', F.max("sum(Montant)").over(w)) \
        .where(F.col("sum(Montant)") == F.col("MaxSomme")) \
        .drop("MaxSomme")

    df = df.orderBy(df["Annee"].desc(), df["Benificiaire"])

    return df.show()
