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


<img width="769" height="557" alt="image" src="https://github.com/user-attachments/assets/e53694d6-6aa0-4d1c-a51f-8df2c8b39f75" />


[2026-04-08T11:54:36.759+0000] {taskinstance.py:1957} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: energie_meteo_dag.generer_rapport_energie manual__2026-04-08T11:54:30.385966+00:00 [queued]>
[2026-04-08T11:54:36.766+0000] {taskinstance.py:1957} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: energie_meteo_dag.generer_rapport_energie manual__2026-04-08T11:54:30.385966+00:00 [queued]>
[2026-04-08T11:54:36.766+0000] {taskinstance.py:2171} INFO - Starting attempt 1 of 3
[2026-04-08T11:54:36.776+0000] {taskinstance.py:2192} INFO - Executing <Task(PythonOperator): generer_rapport_energie> on 2026-04-08 11:54:30.385966+00:00
[2026-04-08T11:54:36.781+0000] {standard_task_runner.py:60} INFO - Started process 179 to run task
[2026-04-08T11:54:36.784+0000] {standard_task_runner.py:87} INFO - Running: ['***', 'tasks', 'run', 'energie_meteo_dag', 'generer_rapport_energie', 'manual__2026-04-08T11:54:30.385966+00:00', '--job-id', '12', '--raw', '--subdir', 'DAGS_FOLDER/energie_meteo_dag.py', '--cfg-path', '/tmp/tmp3dm3ty42']
[2026-04-08T11:54:36.786+0000] {standard_task_runner.py:88} INFO - Job 12: Subtask generer_rapport_energie
[2026-04-08T11:54:36.823+0000] {task_command.py:423} INFO - Running <TaskInstance: energie_meteo_dag.generer_rapport_energie manual__2026-04-08T11:54:30.385966+00:00 [running]> on host 6751d479324b
[2026-04-08T11:54:36.881+0000] {taskinstance.py:2481} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='rte-data-team' AIRFLOW_CTX_DAG_ID='energie_meteo_dag' AIRFLOW_CTX_TASK_ID='generer_rapport_energie' AIRFLOW_CTX_EXECUTION_DATE='2026-04-08T11:54:30.385966+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='manual__2026-04-08T11:54:30.385966+00:00'
[2026-04-08T11:54:36.883+0000] {python.py:201} INFO - Done. Returned value was: None
[2026-04-08T11:54:36.891+0000] {taskinstance.py:1138} INFO - Marking task as SUCCESS. dag_id=energie_meteo_dag, task_id=generer_rapport_energie, execution_date=20260408T115430, start_date=20260408T115436, end_date=20260408T115436
[2026-04-08T11:54:36.917+0000] {local_task_job_runner.py:234} INFO - Task exited with return code 0
[2026-04-08T11:54:36.951+0000] {taskinstance.py:3281} INFO - 0 downstream tasks scheduled from follow-on schedule check

