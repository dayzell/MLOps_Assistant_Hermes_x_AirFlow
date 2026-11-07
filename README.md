# airflow-mlops — plugin Hermes Agent pour Apache Airflow

Plugin **Hermes Agent** autonome qui transforme l'agent en assistant MLOps pour **Apache Airflow** :
surveillance des DAGs, diagnostic de pannes, relance ciblée et protégée, rapports de monitoring normés.

> **État : Phase 1** — squelette, client REST v1/v2, 7 outils en lecture seule, redaction, tests.
> Diagnostic (phase 2), rapports + skills (phase 3) et remédiation protégée (phase 4) arrivent ensuite.

## Installation

```bash
hermes plugins install <owner>/<repo>      # ou copier ce dossier dans ~/.hermes/plugins/airflow-mlops/
hermes plugins enable airflow-mlops
```

Dépendances Python (déclarées dans `pyproject.toml`, installées par le gestionnaire de plugins
Hermes) : `httpx>=0.27,<0.29`, `PyYAML>=6.0,<7`.

Compatibilité : Hermes `>=0.23`, Airflow 2.x (`/api/v1`) et 3.x (`/api/v2`).

## Configuration

### Identifiants (secrets, `requires_env`)

Un seul jeu est nécessaire selon `auth_mode` :

| `auth_mode` | Airflow | Secrets |
|---|---|---|
| `jwt` (défaut) | 3.x — `POST /auth/token` puis `Bearer <JWT>`, renouvelé automatiquement sur 401 | `AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD` |
| `basic` | 2.x — backend `basic_auth` | `AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD` |
| `token` | 2.x/3.x — jeton statique | `AIRFLOW_TOKEN` |

Les secrets sont saisis à l'installation (`hermes plugins install`) et lus exclusivement via
`agent.secret_scope.get_secret`. Le plugin ne lit jamais `.env` ni `auth.json`.

### Réglages (`config_schema`, sous `plugins.entries.airflow-mlops.settings`)

| Clé | Défaut | Rôle |
|---|---|---|
| `base_url` | `http://localhost:8080` | URL du serveur Airflow (sans `/api/vX`) |
| `api_version` | `v2` | `v1` = Airflow 2.x, `v2` = Airflow 3.x |
| `auth_mode` | `jwt` | `basic` / `token` / `jwt` |
| `verify_tls` | `true` | Vérification du certificat |
| `timeout_seconds` | `15` | Timeout HTTP par requête |
| `max_retries` | `2` | Relances des lectures sur erreur réseau / 502 / 503 / 504 |
| `page_size` | `100` | Taille de page (plafonnée par `[api] maximum_page_limit`) |
| `dag_allowlist` | `["*"]` | Motifs glob des DAGs accessibles ; tout autre DAG est refusé |
| `read_only` | `true` | Masque les outils d'écriture (phase 4) |
| `log_max_lines` / `log_max_chars` | `200` / `20000` | Bornes des logs renvoyés au LLM |
| `error_context_lines` | `40` | Taille max du bloc d'erreur extrait |
| `xcom_max_chars` | `2000` | Taille max d'une valeur XCom renvoyée |
| `data_quality_xcom_key` | `data_quality` | Clé XCom de la convention qualité (voir plus bas) |
| `sla_thresholds` | `{}` | Seuils SLA en secondes : `{"dag_id": 3600, "dag_id.task_id": 600}` |
| `report_default_period` | `24h` | Période par défaut des rapports |
| `auto_remediation_enabled` | `false` | Relance auto des erreurs transitoires (phase 4) |
| `auto_retry_max_attempts` | `2` | Tentatives auto max par `(dag_id, run_id, task_id)` |
| `auto_retry_backoff_seconds` | `300` | Backoff de base (doublé à chaque tentative) |
| `error_patterns_path` | `""` | Règles de classification personnalisées (phase 2) |

Exemple (`~/.hermes/config.yaml`) :

```yaml
plugins:
  enabled: [airflow-mlops]
  entries:
    airflow-mlops:
      settings:
        base_url: https://airflow.example.com
        api_version: v2
        auth_mode: jwt
        dag_allowlist: ["ml_*", "etl_daily"]
```

## Outils (toolset `airflow`)

Tous les outils renvoient une chaîne JSON : `{"ok": true, ...}` ou
`{"ok": false, "error": {"type", "message", "status"}}`. Ils ne lèvent jamais d'exception et
n'exposent jamais de traceback.

| Outil | Rôle |
|---|---|
| `airflow_list_dags` | DAGs (pausé, schedule, tags, prochain run), filtrés par l'allowlist |
| `airflow_get_dag` | Détail d'un DAG, **params** (masqués), tâches (operator, trigger_rule, downstream) |
| `airflow_get_dag_runs` | Runs récents (état, dates, durée) |
| `airflow_get_run_status` | Task instances d'un run : état, tries, durée, attente en file, pool |
| `airflow_get_task_logs` | Log d'un try : tail borné et bloc d'erreur/traceback extrait |
| `airflow_get_xcoms` | XComs d'une tâche, masqués et tronqués |
| `airflow_get_dependencies` | Graphe amont/aval d'une tâche avec `trigger_rule` |

Types d'erreur : `invalid_argument`, `dag_not_allowed`, `config`, `auth`, `forbidden`,
`not_found`, `bad_request`, `timeout`, `network`, `server`, `http`, `internal`.

## Convention XCom « qualité des données »

Airflow n'a pas de notion native de qualité des données. Le rapport (phase 3) lit la convention
suivante : la tâche de contrôle qualité pousse un XCom de clé `data_quality_xcom_key`
(`data_quality` par défaut) contenant :

```json
{"rows": 1200, "null_ratio": 0.01, "freshness": "PT1H", "checks_passed": true}
```

| Champ | Type | Sens |
|---|---|---|
| `rows` | int | Lignes produites / contrôlées |
| `null_ratio` | float 0–1 | Part de valeurs nulles sur les colonnes contrôlées |
| `freshness` | str (durée ISO 8601) ou nombre de secondes | Âge de la donnée la plus récente |
| `checks_passed` | bool | Résultat global des contrôles |

Sans XCom conforme, la section indique « non instrumenté » : le rapport n'invente jamais de
chiffres. Exemple dans [demo-local-airflow/dags/mlops_demo.py](demo-local-airflow/dags/mlops_demo.py).

## Sécurité

- **Redaction** ([redaction.py](redaction.py)) de tout ce qui est renvoyé au LLM (logs, XComs,
  params, messages d'erreur) :
  - mots de passe, tokens, clés d'API et `Authorization` ;
  - identifiants dans les URI (`scheme://user:pass@`) ;
  - variables `AIRFLOW_CONN_*` / `AIRFLOW_VAR_*` ;
  - JWT, clés AWS / GitHub / GitLab / Slack, clés privées PEM ;
  - valeurs des clés sensibles dans les dictionnaires.
- **Allowlist** de DAGs : un DAG hors allowlist est refusé avant tout appel à Airflow.
- **Tailles bornées** : logs (lignes et caractères) et XComs.
- **Aucune écriture** dans le dossier du plugin. L'état durable (phases 2 à 4) ira dans
  `plugin_data_dir("airflow-mlops")` / `plugin_db()`.
- **Divulgation (règle 13 du catalogue)** : le plugin appelle l'API REST du serveur Airflow
  configuré, et rien d'autre. Il ne lance ni commande shell ni processus d'arrière-plan.

## Développement

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements-dev.txt      # (Windows) ou .venv/bin/pip
.venv/Scripts/python -m pytest
.venv/Scripts/ruff check . && .venv/Scripts/ruff format --check .
.venv/Scripts/mypy .
hermes plugins doctor . --ci
hermes plugins validate . --install-deps
```

Les tests mockent l'API Airflow avec `respx`, en v1 et en v2. Ils remplacent les modules hôtes
Hermes par des doublures, uniquement dans `tests/harness.py`.

### Démo locale (optionnelle, hors plugin)

Le dossier [demo-local-airflow/](demo-local-airflow/) contient un Airflow jetable. Il sert uniquement à essayer le plugin à la main et ne fait pas partie du plugin.

```bash
docker compose -f demo-local-airflow/docker-compose.yml up     # Airflow 3 standalone, admin / admin
```

Configurez ensuite `base_url: http://localhost:8080`, `api_version: v2`, `auth_mode: jwt` et
les secrets `AIRFLOW_USERNAME=admin` / `AIRFLOW_PASSWORD=admin`, puis lancez `hermes chat`. Les
DAGs de démonstration couvrent un cas sain, une erreur critique (`KeyError`) et une erreur
transitoire (`ConnectionResetError`).

## Licence

MIT
