# ou-part-largent-du-contribuable

- récupérer le jeu de données et le mettre dans un dataframe
- sanitize le jeu de données si besoin
- partager un volume local avec le container => le volume contenant le jupyter & les datas   
- créer un docker-compose.yml avec le volume bindé + la data + un jupyter vierge             :vittorio

## workflow

- on part sur un juptyer pour la visualisation
- on part sur des fichiers .py pour le code
- le jupyter appelle & exécute les fichiers .py
- a voir : un fichier = une stat, ou un fichier contenant toutes les stats

## liste des insights / Stats possibles

*- Trouver le moyen de parser efficacement parser(;) la colonne `Secteurs d'activités définies par l'association`*
*- Récupérer les différents `Secteurs d'activités définies par l'association` (17)*

*- Quel `Bénéficiaire` demande le plus d'argent par année ?*
*- Quel `Bénéficiaire` demande le plus de fois d'argent par année ?*

*- Quel `Direction` acccepte le plus d'argent par année ?*
*- Quel `Direction` acccepte le plus de fois d'argent par année ?*

## (Exemple de présenation)##
## Bénéficiaire / Direction | 2013 | 2014 | ... ##

*- Combien il y a eu de `Bénéficiaire` par année ?*
*- Combien il y a eu de demandes par année ?*
*- Quel est le montant accordé par année ?*

## (Exemple de présenation)##
## Année | Nombre de Bénéficiaire TOTAUX | Nombre de demandes | Montant TOTAL##

*- Combien il y a eu de `Secteur` par année ?*
*- Quel est le montant accordé par `Secteur` par année ?*

## (Exemple de présenation)##
## Année | Nombre de Secteur TOTAUX | Nombre de Secteur | Montant TOTAL##

*- Quels sont les mots du `objet du dossier` qui permmettent d'obtenir le plus de subventions ?*
*- Recherche d'une corrélation entre secteur d'activité et montant accordé*

