from pyspark.sql.functions import *

def myFlatMap(words, amount):
    '''
        Transforme en lowercase, supprime les doublons, retourne (mot, montant)
    '''
    words = map(lambda word: word.lower(), words)
    words = set(words)
    words = map(lambda word: (word, amount), words)
    
    return words

def myReducer(accumulator, currentValue):
    '''
        Convertie et additionne les deux valeurs
    '''
    try:
        accumulator = int(accumulator)
    except:
        accumulator = 0
    try:
        currentValue = int(currentValue)
    except:
        currentValue = 0
    
    return accumulator + currentValue
        

def getMontantByWord(df):
    '''
        Récupérer le `Montant` en fonction des mots clée présent dans `Objet`
    '''
    bannedWords = ('de', 'et', 'la', '-', 'du', 'des', 'pour', 'en', 'dans', 'le', 'à', 'les', 'au', 'aux', 'sur', \
                'd\'un', 'd\'une', ':', 'par', 'avec', '')

    dfObjectWordMontant = df.select("Objet", "Montant")
    return dfObjectWordMontant.rdd.map(lambda row: [str(row[0]).split(' '), row[1]]) \
            .flatMap(lambda row: myFlatMap(row[0], row[1])) \
            .reduceByKey(lambda accumulator, currentValue : myReducer(accumulator, currentValue)) \
            .toDF() \
            .where(f"_1 NOT IN {bannedWords}") \
            .sort(desc('_2'))