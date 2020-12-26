# Questions infrastructure


### Les données que nous utilisons pour l’instant ne sont pas complètes car il manque des tweets. Combien de jours complets de collecte de données pouvons-nous stocker sur notre infrastructure de test (votre salle de TP) ?

Espace disponible sur l'infrastructure de la salle 203:
``` 
$ hdfs dfs df 

Filesystem                  Size           Used       Available  Use%
hdfs://data:9000  17973884362752  1409268278424  12835057589104    8%



$ hdfs dfs df -h


Filesystem          Size   Used  Available  Use%
hdfs://data:9000  16.3 T  1.3 T     11.7 T    8%
```

(à simplifier avec des valeurs en To/Go/Mo)

* Nombre de Tweets par jour: ~504 millions
* Poids pour un tweet: ~5kO


* Poids en Tweets par jour:

    * = 504 000 000 * 5 000 

    * = 2520000000000 octets

    * = 2520000000 ko

* Poids en Tweets par jour sur système HDFS (réplication de 3) 

    * = Poids en Tweets par jour * 3

    * = 7560000000000 octets

    * = 7560000000 ko

* Nombre de jours complets:
    * = Taille HDFS / Poids en Tweets par jour sur système HDFS
    * = 17973884362752 / 7560000000
    * = 2377.5
    * __**= 2377 jours complets**__
---

### Pour ce nombre de jour de collecte complet, combien de blocs de données seront disponibles sur chaque machine en moyenne ?

* Nombre de machines: __**??**__
* Taille bloc de donnée: 128Mo
* Taille de données stockées sur hdfs:
    * 2377 * 7560000000
    * 17970120000 Mo

* Nombre de blocs de données:
    * Taille de données stockées / Taille bloc de donnée
    * 17970120000 / 128
    * 140391562,5 blocs

* Nombre de blocs de données par machine:
    * Nombre de blocs de données / Nombre de machines
    * __**Résultat = ??**__

---


### Afin de planifier nos achats de matériels futurs, calculez le nombre de machines total que nous devrons avoir dans notre cluster pour stocker 5 ans de tweets ?

* Espace de stockage par machine du cluster:
    * __**??**__

* Poids en Tweets par années:
    * = Poids en Tweets par jour * 364.25
    * = 2520000000 * 364.25
    * = 917910000000 ko
    * = 917910000 Mo
    * = 917910 Go
    * = 918 To

* Poids en Tweets pour 5 ans (réplication de 1)
    * Poids en Tweets par années * 5
    * 918 To * 5
    * 4590 To

* Poids en Tweets pour 5 ans (réplication de 3)
    * Poids en Tweets pour 5 ans (réplication de 1) * 3
    * 4590 * 3
    * 13770 To

* Nombre de machines total
    * (Poids en Tweets pour 5 ans (réplication de 3)) / (Espace de stockage par machine du cluster)
    * (13770 To) / __**??**__ 

---