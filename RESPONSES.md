QUESTION 1 : Docker Executor Le  docker-compose.yaml  utilise  LocalExecutor . Expliquez la diﬀérence entre
LocalExecutor ,  CeleryExecutor  et  KubernetesExecutor . Dans quel contexte de production RTE devrait-il
utiliser chacun ? Quelles sont les implications en termes de scalabilité et de ressources ?



- LocalExecutor permet d'executer des taches avec des sous processus sur une meme noeud. Il recommandé pour les petites production.

- CeleryExecutor permet de distribuer des taches à des workers via un broker. c'est surtout pour des productions fortes

- KubernetesExecutor permet de lancer de façon dynamique des pods éphémeres. Pour les environnements cloud native




QUESTION 2 : Volumes Docker et persistance des DAGs Le volume  ./dags:/opt/airflow/dags  permet de
modiﬁer les DAGs sans redémarrer le conteneur. Expliquez le mécanisme sous-jacent (bind mount vs volume
nommé). Que se passerait-il si on supprimait ce mapping ? Quel serait l’impact en production sur un cluster
Airﬂow multi-nœuds (plusieurs workers) ?


Sans le Mapping les DAGs n'existent plus dans le container, airflow ne verrait plus aucun DAG. Sur un multi-noeud, les NFS/EFS partagé montent sur tous  les workers.



QUESTION 3 : Idempotence et  catchup  Le DAG a  catchup=False . Expliquez ce que ferait Airﬂow si
catchup=True  et que le DAG est activé aujourd’hui avec un  start_date  au 1er janvier 2024. Qu’est-ce que
l’idempotence d’un DAG et pourquoi est-ce critique pour un pipeline de données énergétiques ? Comment rendre les fonctions  collecter_*  idempotentes ?



Avec catchup=True et start_date=2024-01-01, Airflow calculerait tous les runs manqués depuis le 1er janvier 2024 jusqu'à aujourd'hui. soit environ 825 runs (un par jour à 6h). Il les déclencherait tous en parallèle (limité par max_active_runs), ce qui provoquerait 825 appels simultanés aux APIs Open-Meteo et éCO2mix, probablement un rate limiting ou un ban.

passé le context["ti"] en context["ds"]




QUESTION 4 : Timezone et données temps-réel L’API éCO2mix retourne des données horodatées. Pourquoi le paramètre  timezone=Europe/Paris  est-il essentiel dans le contexte RTE ? Que peut-il se passer lors du passage à l’heure d’été (dernier dimanche de mars) si la timezone n’est pas correctement gérée dans le scheduler Airﬂow et dans les requêtes API ? Donnez un exemple concret de données corrompues ou manquantes.


timezone=Europe/Paris est essentiel car le réseau électrique français fonctionne selon l'heure légale française. La consommation et la production sont analysées par tranches horaires françaises — une heure décalée signifie des données rattachées à la mauvaise plage de consommation.

2026-03-29 01:00 UTC → 02:00 Paris
2026-03-29 02:00 UTC → 03:00 Paris