# Phase 5 — Plan du bac à sable de test des correctifs

> **Statut : plan seulement, rien n'est implémenté.** Conformément à la guideline, rien ne
> sera codé sans accord explicite. Les décisions marquées « proposée » sont à confirmer.

## 1. Objectif

Quand `airflow_diagnose_run` classe une panne en `critical`, la skill `airflow-fix-proposal`
rédige un correctif (diff). Le bac à sable permet à l'agent de **vérifier ce correctif sur une
maquette jetable** avant de le présenter. L'agent présente alors une solution accompagnée d'une
preuve d'exécution. Il **n'applique toujours rien en production**.

## 2. Principes

- **Montée en charge par niveaux** : on n'exécute une tâche que si l'import et la structure
  sont validés, et seulement sur demande.
- **Plusieurs barrières indépendantes** contre les tâches lourdes : conteneur bridé, pas de
  réseau, pas de secrets, échantillon, liste d'opérateurs exclus, budget.
- **Rester dans le pipeline d'approbation de Hermes** : le conteneur est lancé via
  `ctx.dispatch_tool("terminal", ...)`, pas via un `subprocess` maison. La règle 12 du
  catalogue interdit de contourner l'approbation.
- **Ne jamais modifier le code de l'utilisateur** : on travaille sur une copie dans
  `plugin_data_dir("airflow-mlops")/sandbox/<id>/`, supprimée après le test.

## 3. Les niveaux de test

| Niveau | Commande dans le conteneur | Vérifie | Par défaut |
|---|---|---|---|
| `import` | `python -c "from airflow.models import DagBag; ..."` + `airflow dags list-import-errors` | le DAG corrigé se charge | ✅ |
| `structure` | chargement du DAG + contrôle des `task_id`, des dépendances et des `params` comparés à la version actuelle | le correctif ne casse pas le graphe | ✅ (proposé) |
| `task` | `airflow tasks test <dag_id> <task_id> <date> --task-params '{"sample_rows": N}'` | la tâche corrigée s'exécute sur un échantillon | ❌ demande explicite + validation |

`airflow tasks test` exécute **une seule tâche**, sans scheduler ni dépendances, et n'écrit
pas d'état dans une base de production : la maquette utilise une base SQLite locale jetable.

## 4. Barrières contre les tâches lourdes

| Barrière | Mise en œuvre | Ce qu'elle empêche |
|---|---|---|
| Ressources | `--cpus 1 --memory 2g --memory-swap 2g --pids-limit 256` | saturation de la machine ; un OOM donne le code de sortie 137, détecté |
| Durée | `timeout <sandbox_timeout_seconds>` dans le conteneur, plus `docker stop` de sécurité | tâche qui ne se termine pas |
| Disque | `--read-only --tmpfs /tmp:size=512m` | remplissage du disque |
| Réseau | `--network none` | appels à la production, à l'entrepôt, aux API, exfiltration |
| Secrets | aucune variable `AIRFLOW_CONN_*` / `AIRFLOW_VAR_*` réelle, connexions factices seulement | accès aux vraies données |
| Privilèges | `--cap-drop ALL --security-opt no-new-privileges --user 50000` | élévation de privilèges |
| Données | `AIRFLOW_SANDBOX=1` + `sample_rows`, et les XComs d'entrée du run en échec (masqués, tronqués) servis comme fixtures | traitement de gros volumes |
| Opérateurs | analyse avant le niveau `task` : opérateurs « lourds » refusés (liste configurable) | lancement d'un job Spark, d'un pod Kubernetes ou d'une requête d'entrepôt |
| Budget | verrou (un test à la fois), `sandbox_max_tests_per_session` | tests en rafale |
| Sortie | même troncature que les logs (`excerpt_log`) | saturation du contexte de l'agent |
| Traçabilité | chaque test est écrit dans `audit_log` (`tool = airflow_sandbox_test`) | actions invisibles |

Liste proposée par défaut pour `sandbox_heavy_operators` : `SparkSubmitOperator`,
`KubernetesPodOperator`, `DatabricksSubmitRunOperator`, `SnowflakeOperator`,
`BigQueryInsertJobOperator`, `EmrAddStepsOperator`, `BashOperator`, `DockerOperator`,
`SSHOperator`.

## 5. Nouvel outil proposé

`airflow_sandbox_test(dag_id, patch, level="import", task_id=None)`

1. **Garde-fous** : `sandbox_enabled`, allowlist, budget, et niveau ≤ `sandbox_max_level` sauf
   demande explicite.
2. **Préparation** : copie du fichier du DAG (et de ses modules locaux) depuis
   `sandbox_dags_path` vers `plugin_data_dir/sandbox/<id>/`, puis application du diff à cette
   copie. Le diff est refusé s'il touche un fichier hors du dossier copié.
3. **Analyse statique** pour le niveau `task` : opérateur de la tâche comparé à
   `sandbox_heavy_operators`.
4. **Exécution** : `ctx.dispatch_tool("terminal", {"command": "docker run ... <image> timeout ... airflow ..."})`,
   ce qui déclenche la validation humaine de Hermes.
5. **Résultat structuré** :
   ```json
   {"level": "import", "passed": true, "exit_code": 0, "duration_s": 4.2,
    "oom": false, "timed_out": false, "import_errors": [], "log_excerpt": "..."}
   ```
6. **Nettoyage** du dossier temporaire et journalisation.

La skill `airflow-fix-proposal` sera mise à jour : *proposer le diff → le tester (niveaux 1-2)
→ le présenter avec le résultat → proposer le niveau 3 si c'est utile*.

## 6. Réglages proposés (`config_schema`)

| Clé | Type | Défaut proposé |
|---|---|---|
| `sandbox_enabled` | bool | `false` |
| `sandbox_image` | str | `""` (obligatoire si activé) |
| `sandbox_dags_path` | str | `""` (obligatoire si activé) |
| `sandbox_max_level` | str | `structure` |
| `sandbox_cpus` | float | `1` |
| `sandbox_memory` | str | `2g` |
| `sandbox_timeout_seconds` | int | `300` |
| `sandbox_sample_rows` | int | `1000` |
| `sandbox_heavy_operators` | list | voir § 4 |
| `sandbox_max_tests_per_session` | int | `5` |

## 7. Décisions

| # | Question | Décision proposée | Pourquoi |
|---|---|---|---|
| 1 | D'où vient le code du DAG ? | **Dossier local** `sandbox_dags_path`, en lecture seule | L'API Airflow (`dagSources`) ne renvoie que le fichier du DAG, sans ses modules utilitaires : l'import échouerait souvent |
| 2 | Comment avoir les dépendances sans réseau ? | **Image construite une fois** : `FROM apache/airflow:<version>` + `requirements.txt`, via une étape séparée avec réseau et validation | Pas de `pip install` dans le conteneur de test |
| 3 | Limites par défaut ? | **1 CPU, 2 Go, 5 min** | Suffisant pour importer un DAG et tester une tâche sur échantillon |
| 4 | Le terminal Hermes tourne-t-il lui-même dans Docker ? | **À vérifier à l'implémentation**. Si oui, le niveau `task` exigera l'accès au démon Docker de l'hôte, sinon il sera désactivé | Éviter de lancer Docker depuis Docker sans le savoir |

## 8. Tests prévus

- Construction de la commande `docker run` : toutes les limites sont présentes, `--network none`,
  aucun secret transmis.
- Refus : sandbox désactivé, budget épuisé, opérateur lourd, diff qui sort du dossier, DAG hors
  allowlist.
- Interprétation des résultats : succès, erreur d'import, code 137 (OOM), timeout.
- `dispatch_tool` mocké : aucun vrai Docker en test unitaire. Test manuel optionnel avec
  `demo-local-airflow/`.

## 9. Ce qui reste hors périmètre

- Appliquer le correctif dans le dépôt ou en production : toujours un humain.
- Exécuter le DAG entier en maquette.
- Tester avec les vraies données ou les vraies connexions.
