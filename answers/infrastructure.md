Les données que nous utilisons pour l’instant ne sont pas complètes car il manque des tweets. Combien de jours complets de collecte de données pouvons-nous stocker sur notre infrastructure de test (votre salle de TP) ?
20 machines de 1 To = 20 To
Nombre de Tweets par jour: ~504 millions


Poids pour un tweet: ~5kO


Poids en Tweets par jour:


= 504 000 000 * 5 000


= 2520000000000 octets



Poids en Tweets par jour sur système HDFS (réplication de 3)


= Poids en Tweets par jour * 3


= 7560000000000 octets


Donc résultat == 20 x 10^12 / 7560000000000 = 2.6455 jours de collectes, donc 2 jours complets.



Pour ce nombre de jours de collecte complet, combien de blocs de données seront disponibles sur chaque machine en moyenne?
7560000000000 x 2 (car deux jours) = 15 120 000 000 000 octets
15 120 000 000 000 / 128 000 000 ( car taille de bloc = 128 mo) = 118125
Finalement 118125 / 20 ( 20 machines) = 5906.25 blocs par machines

Afin de planifier nos achats de matériels futurs,calculez le nombre de machines total que nous devons avoir dans notre cluster pour stocker 5 ans de tweets ?
    Soit 5 ans donc 1825 jours
    Soit tweet tailles = 7560000000000  octets
    donc 7560000000000 * 1825 = 1.3797 x 10^16
    Il faut diviser par le stockage par machine == 10^12 octets
    Donc 1.3797 x 10^16 / 10 ^12 = 13797 machines