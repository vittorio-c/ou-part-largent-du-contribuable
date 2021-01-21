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

*- Trouver le moyen de parser efficacement parser(;) la colonne `Secteurs d'activités définies par l'association`* : Brandon  
*- Récupérer les différents `Secteurs d'activités définies par l'association` (17)*                                : Brandon

*- Quel `Bénéficiaire` demande le plus d'argent par année ?*           : Vittorio
*- Quel `Bénéficiaire` demande le plus de fois d'argent par année ?*   : Vittorio

*- Quel `Direction` acccepte le plus d'argent par année ?*             : Sébastien
*- Quel `Direction` acccepte le plus de fois d'argent par année ?*     : Sébastien

**(Exemple de présenation)**
**Bénéficiaire / Direction | 2013 | 2014 | ...**

- *Combien il y a eu de `Bénéficiaire` par année ?*  : Raid
- *Combien il y a eu de demandes par année ?*        : Raid
- *Quel est le montant accordé par année ?*          : Raid

**(Exemple de présenation)**
**Année | Nombre de Bénéficiaire TOTAUX | Nombre de demandes | Montant TOTAL**

*- Combien il y a eu de `Secteur` par année ?*              : Wissem
*- Quel est le montant accordé par `Secteur` par année ?*   : Wissem

**(Exemple de présenation)**
**Année | Nombre de Secteur TOTAUX | Nombre de Secteur | Montant TOTAL**

*- Quels sont les mots du `objet du dossier` qui permmettent d'obtenir le plus de subventions ?*    : Sébastien
*- Recherche d'une corrélation entre secteur d'activité et montant accordé*                         : Tout le monde !
*- Compter les mots dans `Noms bénéficiaires` !*                                                    : Tout le monde !


