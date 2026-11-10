# Scénario de démo — « FreshCart, une semaine avec Hermes × Airflow »

Démo de 10 minutes du plugin `airflow-mlops`, à rejouer sur l'Airflow de démo
(`demo-local-airflow/`). Chaque scène correspond à une scène de la business story
([ARCHITECTURE.md](../ARCHITECTURE.md)).

| Pipeline de l'histoire | DAG de démo | Ce qui ne va pas |
|---|---|---|
| `demand_forecast_train` (prévision des commandes) | `demo_train_critical` | `train` plante (`KeyError: 'target'`), mais la tâche finale `notify` (`all_done`) marque le run « success » |
| `supplier_prices_fetch` (API fournisseurs) | `demo_fetch_transient` | `fetch` échoue 9 fois sur 10 (`ConnectionResetError`), sans `retries` |
| `stock_dashboard` | `demo_train_ok` | rien : il sert de témoin |

## 0. Préparer (5 minutes, avant le public)

```bash
# Airflow 3.4 de démo (port 8081)
cd demo-local-airflow
docker compose up -d
# déclencher un run de chaque DAG de démo depuis l'interface (http://localhost:8081),
# ou : docker compose exec airflow-apiserver airflow dags trigger demo_train_critical

# Hermes : passerelle relancée, réglages du plugin chargés
hermes gateway restart
```

Réglages conseillés dans `config.yaml` (`plugins.entries.airflow-mlops.settings`) :
`read_only: false`, `sandbox_enabled: true`, `sandbox_image: apache/airflow:3.4.0`,
`sandbox_dags_path: <chemin>/demo-local-airflow/dags`.

Repartir de zéro entre deux répétitions :

```bash
git checkout -- demo-local-airflow/dags/mlops_demo.py   # le DAG de démo d'origine
```

## Scène 1 — Lundi 3 h : la surveillance de nuit (1 min)

> « Personne n'est réveillé pour une coupure réseau, mais un échec caché est signalé. »

Dans Hermes : `/blueprint` → **Airflow night watch** (toutes les 30 min). Pour la démo, lancer
la vérification à la main :

```
Lance la surveillance Airflow (airflow-watch).
```

**Attendu** : une alerte courte : `NEW failed run: demo_fetch_transient`, `NEW hidden failure
(run marked success): demo_train_critical`, avec la catégorie (transient / critical) et la
cause. Une deuxième vérification juste après ne dit rien (`[SILENT]`) : rien de nouveau.

## Scène 2 — Lundi 9 h : l'audit guidé (4 min)

> « Une question entre chaque étape, rien ne se fait sans votre choix. »

```
/airflow-audit
```

1. Le rapport s'affiche (6 sections), puis **Which DAG should I diagnose?** en liste
   numérotée → répondre `1` (`demo_train_critical`, recommandé : échec caché, critique).
2. Plan numéroté : `1.1 train — KeyError: 'target'`, `1.2 run — trigger_rule all_done hides
   the failure`. **Which problem first?** → `1.1`.
3. L'agent lit le code, écrit le correctif et le teste dans le bac à sable (copie jetable,
   sans réseau, niveau `structure`), puis montre la cause, les lignes exactes (avant/après) et
   le résultat. Le bon correctif corrige le producteur (`load` doit fournir `target`), pas
   `frame.get("target")`.
4. **1.1 works. Problems left:** → la relance passagère `2.1` → aperçu de la relance → votre
   accord (validation Hermes) → le run est suivi jusqu'à sa fin.
5. **Stop here** → `🗂️ Audit #n archived` → **Apply 1.1 (your approval required)** : Hermes
   demande la validation du `git apply` du patch testé (verrou du plugin) → **Allow once**.

**Point à montrer** : si l'agent proposait `frame.get("target")`, le bac à sable réussirait
quand même, mais l'audit demanderait d'abord *« it may hide the error instead of fixing it…
What now? »* avec **Revise the fix** recommandé.

## Scène 3 — Mardi : le verrou (1 min)

> « Hermes reste libre partout, sauf sur un DAG non testé. »

```
Modifie directement demo-local-airflow/dags/mlops_demo.py : mets retries=3 sur fetch.
```

**Attendu** : `BLOCKED by airflow-mlops: … this change was not tested`. L'agent propose alors
de tester le changement dans le bac à sable ; une fois le test « structure » réussi, la même
écriture demande votre validation. Dans la même session, écrire un fichier ailleurs
(ex. `notes.md`) fonctionne sans restriction.

## Scène 4 — Mercredi : la revue avant déploiement (2 min)

> « On prévient l'incident de lundi au lieu de le réparer. »

```
Relis le DAG demo_train_critical avant que je le déploie.
```

**Attendu** (`airflow_review_dag`, lignes exactes) :

| Gravité | Ligne | Problème |
|---|---|---|
| high | 41 | `notify` en `trigger_rule="all_done"` : le run est « success » même si `train` échoue |
| medium | 34 | `load`, `train`, `notify` sans retry (défaut d'Airflow : 0) |

…puis la proposition d'une tâche « watcher » (`trigger_rule="one_failed"`), testée dans le
bac à sable avant toute écriture.

## Scène 5 — Vendredi : la fiche bilan (1 min)

> « Qui a validé quoi, quand, avec quelle preuve. »

```
Fais-moi le postmortem de l'incident de lundi.
```

**Attendu** (`airflow_audit_timeline`) : une fiche en anglais avec la chronologie (test en
bac à sable, fichier écrit `matches_tested_fix`, relance), la cause, le correctif, les
validations humaines et les points encore ouverts (1.2). Puis `/airflow-report all 7d` pour le
rapport de la semaine.

## Ce que la démo prouve

| Besoin | Fonction |
|---|---|
| Ne pas réveiller l'équipe pour rien | `airflow-watch` + blueprint, `[SILENT]` |
| Voir les échecs cachés | détection des runs « success » avec une tâche en échec |
| Être guidé, garder la main | `/airflow-audit` : une question par étape, `Stop here` |
| Rien de non testé en production | bac à sable + verrou sur les DAGs + validation humaine |
| Ne pas « réparer » en cachant l'erreur | `masking_warnings`, « Revise the fix » recommandé |
| Prévenir | `airflow-dag-review` |
| Traçabilité | journal d'audit, `airflow-postmortem` |

Les durées sont indicatives ; le modèle cloud gratuit répond en 5 à 10 s par étape.
