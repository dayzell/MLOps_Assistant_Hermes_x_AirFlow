# Mission : plugin Hermes Agent « airflow-mlops »

Tu es un ingénieur Python senior. Tu dois créer un **plugin Hermes Agent** qui transforme l'agent en assistant MLOps pour **Apache Airflow** : surveillance, diagnostic de pannes, relance ciblée, rapports normés.

## 0. Règle n°1 : la documentation Hermes fait foi

Avant d'écrire du code, lis ces pages et base-toi uniquement dessus (n'invente aucune API `ctx.*`) :

- https://hermes-agent.nousresearch.com/docs/developer-guide/plugins (guide principal)
- https://hermes-agent.nousresearch.com/docs/developer-guide/plugins/public-api
- https://hermes-agent.nousresearch.com/docs/developer-guide/plugin-llm-access
- https://hermes-agent.nousresearch.com/docs/developer-guide/plugins/catalog-submission
- https://hermes-agent.nousresearch.com/docs/user-guide/features/hooks (référence des hooks)
- https://hermes-agent.nousresearch.com/docs/developer-guide/creating-skills

Si un point n'est pas couvert par la doc, ne le devine pas : signale-le-moi, ou lis le code source de https://github.com/NousResearch/hermes-agent.

Contraintes issues de la doc, à respecter strictement :

1. **Structure native** : `plugin.yaml` + `__init__.py` avec `register(ctx)` + `schemas.py` (ce que le LLM lit) + `tools.py` (ce qui s'exécute). Ajoute un dossier `skills/` enregistré via `ctx.register_skill()`.
2. **Handlers** : signature `def handler(args: dict, **kwargs) -> str`. Ils retournent **toujours une chaîne JSON**, succès comme erreur, et **ne lèvent jamais d'exception**.
3. **Hooks** : tous les callbacks acceptent `**kwargs`.
4. **Manifest** : liste chaque outil enregistré dans `provides_tools` et chaque hook dans `provides_hooks` (sinon `hermes plugins validate` échoue).
5. **Secrets** : déclare-les dans `requires_env` (format riche, `secret: true`). Ne lis jamais `.env` ni `auth.json` toi-même. Les réglages non secrets vont dans `config_schema` et se lisent avec `ctx.get_config()`.
6. **État durable** : n'écris jamais dans le dossier du plugin. Utilise `plugin_data_dir()` / `plugin_db()` (`plugins.plugin_storage`) ou `ctx.state`.
7. **Singletons** (client HTTP Airflow) : utilise `lazy_singleton` / `SingletonSlot` de `plugins.plugin_utils`, pas un `global` maison.
8. **Dépendances Python** : déclare-les dans un `pyproject.toml` à côté de `plugin.yaml`, avec une borne haute sur chacune.
9. Ne monkey-patche **rien** du cœur de Hermes. C'est refusé par `hermes plugins validate`.
10. Le plugin est un **dépôt autonome** (intégration d'un produit tiers), pas une contribution au cœur de Hermes.
11. Validation obligatoire à chaque étape : `hermes plugins doctor . --ci` et `hermes plugins validate . --install-deps`.

## 1. Contexte et hypothèses par défaut

- Je maîtrise Python : code propre, typé, commenté en anglais, tests `pytest`.
- Intégration via l'**API REST d'Airflow**. Supporte la 2.x (`/api/v1`, auth basic ou token) et la 3.x (`/api/v2`, auth JWT). La version, l'URL de base et le mode d'auth sont configurables. Vérifie les chemins et schémas exacts dans la doc officielle Airflow de la version ciblée.
- Client HTTP : `httpx` (synchrone), timeouts explicites, retries limités sur les erreurs réseau, pagination gérée.
- Les réglages (URL, version API, allowlist de DAGs, seuils, mode lecture seule…) sont dans `config_schema` ; identifiants dans `requires_env` (`AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD` ou `AIRFLOW_TOKEN`).

## 2. Objectifs fonctionnels

### A. Surveillance et lecture (outils en lecture seule)

Outils préfixés `airflow_`, par exemple :

- `airflow_list_dags` : DAGs, état (pausé ou non), schedule, tags, filtrables par allowlist.
- `airflow_get_dag` : détail d'un DAG, **paramètres**, liste des tâches.
- `airflow_get_dag_runs` : runs récents avec état et durée.
- `airflow_get_run_status` : task instances d'un run avec états, tries, durées.
- `airflow_get_task_logs` : logs d'une tâche (try_number choisissable). **Tronqués** (tail, N lignes max configurable) avec extraction automatique du bloc d'erreur/traceback, pour ne pas saturer le contexte du LLM.
- `airflow_get_xcoms` : XComs d'une tâche, valeurs tronquées au-delà d'une taille limite.
- `airflow_get_dependencies` : graphe amont/aval d'une tâche avec `trigger_rule`.

### B. Diagnostic de panne

- `airflow_diagnose_run(dag_id, run_id)` : agrège ce qu'il faut pour interpréter une panne :
  - distingue la **tâche racine en échec** (`failed`) des tâches **impactées** (`upstream_failed`, `skipped`) ;
  - ajoute les logs d'erreur, XComs pertinents, paramètres, dépendances ;
  - **classifie** l'erreur en `transient` (timeout, connexion reset, 429/503, slot de pool saturé, sensor timeout, ressource externe indisponible) ou `critical` (SyntaxError, ImportError, KeyError, incohérence de schéma, échec de contrôle qualité, permission, config ou connexion manquante, OOM récurrent) ou `unknown`. La classification s'appuie sur des règles configurables (regex/mots-clés dans un fichier de données YAML livré avec le plugin) et retourne sa **justification** ;
  - renvoie un JSON structuré. L'interprétation finale en langage naturel reste le travail du LLM, guidé par la skill (voir E).

### C. Remédiation ciblée (outils d'écriture, protégés)

- `airflow_clear_tasks(dag_id, run_id, task_ids, include_downstream, dry_run=True)` : relance **uniquement** les tâches nécessaires (`only_failed`, liste explicite de `task_ids`). **Garde-fous codés en dur :**
  - refuse si `task_ids` est vide ;
  - **ne relance jamais un DAG entier par défaut** ;
  - `dry_run=True` par défaut : renvoie d'abord la liste exacte des task instances qui seraient relancées ;
  - refuse de relancer une erreur classée `critical` sans confirmation explicite.
- **Politique de relance automatique** (désactivée par défaut, activable en config) : seulement pour les erreurs `transient`, avec un compteur par `(dag_id, run_id, task_id)` persisté dans `plugin_db()`, un maximum configurable de tentatives et un backoff. Au-delà : escalade vers l'humain, jamais de boucle.
- Utilise un hook `pre_tool_call` qui retourne `{"action": "approve", ...}` pour faire passer les outils d'écriture par la **validation humaine de Hermes**, sauf si l'utilisateur a explicitement activé l'auto-remédiation. Mode `read_only: true` global dans la config : les outils d'écriture ne sont alors pas exposés (`check_fn`).
- Tous les appels d'écriture sont journalisés (qui, quoi, quand, pourquoi) dans `plugin_db()`.

### D. Rapports de monitoring normés

- `airflow_build_report(scope, period)` : quand tout va bien, produit un rapport **toujours dans la même structure**, retourné en JSON et en Markdown :
  1. Périmètre et période
  2. Santé globale (taux de succès, nombre de runs, incidents)
  3. Performance par tâche (durée p50/p95, tendance par rapport à la ligne de base stockée dans `plugin_db()`, retries, temps de file d'attente, SLA manqués)
  4. Qualité des données (voir hypothèse ci-dessous)
  5. Incidents de la période et actions prises
  6. Recommandations
- **Qualité des données** : Airflow n'a pas de notion native. Hypothèse par défaut : les tâches de contrôle qualité publient un XCom au format convenu (clé configurable, par exemple `data_quality`, avec `rows`, `null_ratio`, `freshness`, `checks_passed`). Le rapport lit cette convention ; sans XCom conforme, la section indique « non instrumenté » au lieu d'inventer des chiffres. Documente la convention dans le README.
- Expose aussi une commande slash `/airflow-report` via `ctx.register_command()`.

### E. Skills embarquées (`skills/`)

Écris des `SKILL.md` en français ou en anglais (suivre le format décrit dans la doc « Creating Skills ») :

- `airflow-triage` : procédure de diagnostic (identifier la racine, lire logs/XComs/dépendances, classifier, proposer), règle « relancer le minimum, jamais le DAG entier ».
- `airflow-report` : gabarit du rapport normé et règles de rédaction.
- `airflow-fix-proposal` : comment formuler une proposition de correction (cause probable, correctif suggéré sous forme de diff, risques, comment vérifier). L'agent **propose**, il n'applique pas de patch sur le code des DAGs.

### F. Phase optionnelle (à ne faire qu'après validation des phases 1 à 4) : test d'un correctif en bac à sable

Proposer un correctif testé dans un conteneur isolé (Docker) avant déploiement. Approche à étudier et à me soumettre sous forme de **plan seulement** : vérification que le DAG modifié s'importe sans erreur (`DagBag` / `airflow dags list-import-errors`), éventuellement `airflow tasks test`, le tout dans un conteneur jetable lancé via `ctx.dispatch_tool("terminal", ...)` pour rester dans le pipeline d'approbation de Hermes. Pas d'implémentation sans mon accord.

## 3. Sécurité et robustesse

- **Redaction** : masque mots de passe, tokens, chaînes de connexion et valeurs de Variables/Connections dans les logs, XComs et paramètres avant de les renvoyer au LLM (expressions régulières testées).
- Allowlist de DAGs configurable ; le plugin ne touche aucun DAG hors allowlist.
- Taille maximale des réponses (logs, XComs) configurable.
- Erreurs réseau, 401/403/404/5xx : réponses JSON claires, jamais de traceback exposé.
- Aucune écriture hors `plugin_data_dir()`.

## 4. Tests et qualité

- `pytest` avec API Airflow mockée (`respx` ou équivalent) : un test par outil, plus des tests dédiés pour :
  - les garde-fous (refus de relancer un DAG entier, `dry_run` par défaut, limite de relances) ;
  - la classification transient/critical ;
  - la redaction ;
  - la forme du rapport.
- `ruff` + `mypy` (ou `pyright`) propres.
- Option : `docker-compose` Airflow pour un test d'intégration manuel, documenté mais non obligatoire.

## 5. Livrables et méthode de travail

Arborescence cible :

```
pluging_hermes_x_airflow/
├── plugin.yaml
├── pyproject.toml
├── __init__.py          # register(ctx)
├── schemas.py
├── tools.py             # handlers fins, logique dans les modules ci-dessous
├── airflow_client.py    # client REST (v2.x / v3.x)
├── diagnosis.py         # classification, agrégation
├── reporting.py
├── redaction.py
├── guardrails.py        # hooks pre_tool_call, politique de relance
├── data/error_patterns.yaml
├── skills/{airflow-triage,airflow-report,airflow-fix-proposal}/SKILL.md
├── tests/
└── README.md            # installation, config, convention XCom qualité, sécurité
```

**Procède par phases et arrête-toi à la fin de chacune pour que je valide :**

1. **Phase 0** : relis la doc, puis propose-moi un plan détaillé (noms d'outils, schémas, config) et liste tes questions ou ambiguïtés. N'écris pas de code avant mon feu vert.
2. **Phase 1** : squelette + outils en lecture seule + redaction + tests + `hermes plugins doctor`.
3. **Phase 2** : diagnostic + classification.
4. **Phase 3** : rapports + commande slash + skills.
5. **Phase 4** : remédiation protégée (clear ciblé, politique de relance, approbation, journal).
6. **Phase 5 (optionnelle)** : plan du bac à sable (F).

À chaque phase : montre-moi les commandes de validation exécutées et leur résultat.
