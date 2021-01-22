from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def getAllSecteur(df):
    '''
        Retourne un array de tous les secteurs
    '''
    secteurColumn= df.select('Secteur')

    dfAllSecteurName = secteurColumn.rdd.flatMap(lambda row: str(row[0]).split(';')) \
                                .map(lambda word: (word, 1)) \
                                .reduceByKey(lambda accumulator, currentValue: accumulator + currentValue) \
                                .collect()
    allSecteurName = []

    for secteurName, key in dfAllSecteurName:
        allSecteurName.append(secteurName)
        
    bannedSecteur = ['None', 'Non précisée', 'Projet']
        
    allSecteurName = filter(lambda x: x not in bannedSecteur, allSecteurName)    

    return sorted(allSecteurName)

def selectSecteur(df, secteursName):
    '''
        Retourne si la colonne `Secteur` contient `secteursName`
    '''

    return df.select('Secteur').where(f"Secteur LIKE '%{secteursName}%'")