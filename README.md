# airflow-mlops — plugin Hermes Agent pour Apache Airflow

Plugin **Hermes Agent** autonome qui transforme l'agent en assistant MLOps pour **Apache Airflow** :
surveillance des DAGs, diagnostic de pannes, relance ciblée et protégée, rapports de monitoring normés.

> **État : v1.3.0 — validé sur
> Airflow 2.9.3 et 3.4.0 et dans Hermes v0.23 avec le modèle cloud `meituan/longcat-2.5:free`.**
>
> | Moment | Fonction |
> |---|---|
> | Avant la panne | `airflow-dag-review` : revue d'un DAG avant déploiement (`airflow_review_dag`, lignes exactes) |
> | Pendant, sans vous | `airflow-watch` : surveillance planifiée (modèle « Airflow night watch »), alerte seulement sur la nouveauté |
> | Pendant, avec vous | `/airflow-audit` : rapport → diagnostic → test (relance réelle ou correctif en bac à sable), une question entre chaque étape ; **aucune modification de code pendant l'audit**, archivé à la sortie |
> | Après | `airflow-postmortem` : fiche bilan à partir du dossier et du journal (`airflow_audit_timeline`) |
>
> Sécurité : lecture seule par défaut, relances et écritures validées par un humain, **verrou sur
> les DAGs** (seul le changement testé en bac à sable s'écrit), journal d'audit.
> Architecture et décisions : [ARCHITECTURE.md](ARCHITECTURE.md) ·
> démo : [docs/SCENARIO_DEMO.md](docs/SCENARIO_DEMO.md).

📘 **Vue d'ensemble simple des fonctionnalités, des outils et des réglages :
[docs/GUIDE_UTILISATEUR.md](docs/GUIDE_UTILISATEUR.md).** Ce README est la référence technique.

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
| `read_only` | `true` | Masque et refuse les outils d'écriture (`airflow_clear_tasks`) |
| `log_max_lines` / `log_max_chars` | `200` / `20000` | Bornes des logs renvoyés au LLM |
| `error_context_lines` | `40` | Taille max du bloc d'erreur extrait |
| `xcom_max_chars` | `2000` | Taille max d'une valeur XCom renvoyée |
| `data_quality_xcom_key` | `data_quality` | Clé XCom de la convention qualité (voir plus bas) |
| `sla_thresholds` | `{}` | Seuils SLA en secondes : `{"dag_id": 3600, "dag_id.task_id": 600}` |
| `report_default_period` | `24h` | Période par défaut des rapports |
| `dag_write_guard` | `sandbox_required` | Verrou sur les fichiers des DAGs : seul le changement testé en bac à sable passe, après votre validation (`approval_only`, `off`) |
| `auto_remediation_enabled` | `false` | Relance auto, sans approbation, des erreurs transitoires (bornée, voir « Remédiation ») |
| `auto_retry_max_attempts` | `2` | Tentatives auto max par `(dag_id, run_id, task_id)` |
| `auto_retry_backoff_seconds` | `300` | Backoff de base (doublé à chaque tentative) |
| `error_patterns_path` | `""` | Fichier de règles de classification personnalisé (voir « Diagnostic ») |

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

## Mode opératoire : Hermes avec un modèle local (Ollama)

Cette procédure fait tourner l'agent avec un modèle **gratuit, en local**, sans clé d'API.
Elle a été validée le 08/11/2026 sous Windows 11 avec Hermes Desktop 0.23, Ollama 0.23,
`hermes3:8b` et une RTX 2060 (6 Go).

### 1. Installer le modèle

```powershell
ollama pull hermes3:8b            # ~4,7 Go
ollama show hermes3:8b            # "tools" doit figurer dans les capacités
ollama list                       # liste des modèles installés
```

### 2. Pointer Hermes vers Ollama

La configuration de Hermes se trouve avec `hermes config path`. Sous Windows, avec Hermes
Desktop : `%LOCALAPPDATA%\hermes\config.yaml`, dans un dossier caché (`Windows + R` → coller ce
chemin).

```powershell
Copy-Item "$env:LOCALAPPDATA\hermes\config.yaml" "$env:LOCALAPPDATA\hermes\config.yaml.bak"
```

Ajoutez, ou modifiez s'il existe déjà, le bloc `model:` :

```yaml
model:
  provider: custom                     # Ollama (API compatible OpenAI) ; "ollama" est un alias
  base_url: http://localhost:11434/v1
  default: hermes3:8b
  ollama_num_ctx: 65536                # Hermes exige >= 64 000 tokens pour utiliser les outils
```

> Avec un contexte plus petit, Hermes refuse de répondre : « Hermes needs at least 64,000
> tokens for reliable tool use ».

**Indispensable avec un petit modèle : rendre les outils visibles.** Par défaut, Hermes met
les outils des plugins « de côté », derrière son outil de recherche (`tool_search`) : le modèle
ne voit pas `airflow_list_dags` directement. Un modèle de 8B ne pense pas à les chercher, et
**invente alors une réponse**. Lors des tests, il a listé des DAGs « COVID-19 » ou « Stock
Market » qui n'existaient pas. Ajoutez donc dans `config.yaml` :

```yaml
tools:
  tool_search:
    enabled: off                       # tous les outils visibles directement par le modèle
```

**Avec ce petit modèle local seulement**, limitez Hermes aux outils utiles avec `-t` : le prompt
est alors beaucoup plus court (~850 tokens au lieu de ~8 000), donc la réponse est plus rapide.

```powershell
hermes -z "Utilise l'outil airflow_list_dags et liste mes DAGs Airflow." -t airflow
hermes chat -t airflow,file,terminal
```

Avec un modèle cloud (variante recommandée ci-dessous), `-t` est inutile : un simple
`hermes chat` donne à l'agent les outils de Hermes **et** ceux du plugin.

### 3. Régler Ollama (une seule fois)

Hermes fait aussi des **appels secondaires** au même modèle, par exemple pour générer le titre
de la conversation, sans préciser de contexte. Sans réglage, Ollama recharge alors le modèle en
4096 tokens, puis de nouveau en 65 536. Résultat : des rechargements à chaque tour et des
erreurs `Title generation failed: Request timed out`.

On fixe donc le contexte côté Ollama, et on compresse sa mémoire :

```powershell
[Environment]::SetEnvironmentVariable("OLLAMA_CONTEXT_LENGTH", "65536", "User")  # même contexte pour tous les appels
[Environment]::SetEnvironmentVariable("OLLAMA_FLASH_ATTENTION", "1", "User")     # requis pour compresser le contexte
[Environment]::SetEnvironmentVariable("OLLAMA_KV_CACHE_TYPE", "q8_0", "User")    # mémoire du contexte divisée par 2
```

Puis **redémarrez Ollama** : quittez-le depuis l'icône de la barre des tâches, puis relancez-le.
Sur macOS, l'équivalent est `launchctl setenv OLLAMA_CONTEXT_LENGTH 65536`, et de même pour les
deux autres variables, avant de relancer l'application.

### 4. Vérifier

```powershell
hermes status                                      # Model: hermes3:8b, Provider: custom
hermes -z "Réponds uniquement par le mot : OK"     # un seul message, affiche la réponse
ollama ps                                          # CONTEXT doit afficher 65536
```

### 5. Brancher le plugin

```powershell
# Lien vers le dossier du projet : les modifications du code sont prises en compte sans réinstaller
New-Item -ItemType Directory -Force "$env:LOCALAPPDATA\hermes\plugins"
New-Item -ItemType Junction -Path "$env:LOCALAPPDATA\hermes\plugins\airflow-mlops" `
         -Target "C:\chemin\vers\pluging_hermes_x_airflow"
hermes plugins enable airflow-mlops
hermes plugins list
```

Si le plugin n'apparaît pas, `$env:HERMES_PLUGINS_DEBUG=1; hermes plugins list` affiche le
détail. Autre solution : l'installer depuis GitHub, avec
`hermes plugins install <url-du-dépôt> --enable`.

Ajoutez ensuite les réglages du plugin dans `config.yaml`, sous `plugins.entries.airflow-mlops.settings`
(voir « Configuration » plus haut et la démo Airflow 2.9 : `api_version: v1`, `auth_mode: basic`).
Placez les identifiants dans **`%LOCALAPPDATA%\hermes\.env`**, et non dans le dossier du projet :

```powershell
Set-Content -Path "$env:LOCALAPPDATA\hermes\.env" -Encoding utf8 `
            -Value "AIRFLOW_USERNAME=admin`nAIRFLOW_PASSWORD=admin"
hermes plugins doctor airflow-mlops
```

### Variante recommandée : modèle cloud gratuit (Nous Portal)

Pour une démonstration fluide, un modèle cloud fait nettement mieux qu'un petit modèle local.
Hermes est fourni avec un **accès gratuit à Nous Portal** (offre « welcome », sans compte) :

```powershell
hermes model        # 1. "Nous · free tier"  →  2. un modèle ":free"  →  3. reasoning "medium"
hermes status       # Provider: Nous Portal
hermes -z "Liste mes DAGs Airflow." -t airflow
```

`hermes model` réécrit le bloc `model:` de `config.yaml`. Gardez `tools.tool_search.enabled`
désactivé et l'option `-t airflow`. Validé le 08/11/2026 avec `meituan/longcat-2.5:free` :
l'agent appelle seul `airflow_list_dags`, **sans qu'on lui nomme l'outil**, et cite les 3 vrais
DAGs. Évitez les modèles « stealth », dont on ne connaît ni la provenance ni l'usage fait des
échanges. Les limites d'usage de l'offre gratuite sont fixées par Nous ; avec des données
réelles d'entreprise, tenez compte du fait que les échanges passent par leurs serveurs.

### Performances constatées et limites

| Configuration | Empreinte locale | Appel de l'outil | Temps pour « liste mes DAGs » |
|---|---|---|---|
| `hermes3:8b` local, PC RTX 2060 6 Go | ~13 Go (65 % CPU / 35 % GPU) | ❌ invente une réponse, ou écrit l'appel sans le déclencher | 20 s à 4 min |
| `meituan/longcat-2.5:free` via Nous Portal | aucune | ✅ appel réel, bons résultats | **~7 s** (2 appels au modèle) |

- Les 64 000 tokens exigés par Hermes ne tiennent pas dans 6 Go de mémoire graphique : une
  grande partie tourne sur le processeur. **C'est fonctionnel mais lent.** Une tâche avec
  plusieurs appels d'outils prend plusieurs minutes.
- Un modèle de 8B se trompe parfois : il a répondu « 11 » à « 3+4 » lors d'un test.
  `hermes3:8b` choisit bien l'outil, mais oublie les balises `<tool_call>` qu'Ollama attend pour
  déclencher l'appel : Hermes reçoit alors un simple texte.
- Un Mac Apple Silicon avec 16 Go de mémoire unifiée, ou un modèle hébergé, donne une
  expérience nettement plus fluide.

### Dépannage

| Symptôme | Cause | Solution |
|---|---|---|
| « Hermes needs at least 64,000 tokens » | `ollama_num_ctx` trop petit | `ollama_num_ctx: 65536` (étape 2) |
| `Title generation failed: Request timed out` dans `logs/agent.log` | rechargements du modèle | `OLLAMA_CONTEXT_LENGTH=65536`, puis redémarrer Ollama (étape 3) |
| `ollama ps` affiche `CONTEXT 4096` | variables non prises en compte | redémarrer **complètement** Ollama, y compris l'icône de la barre des tâches |
| Une modification du **code Python** du plugin n'est pas prise en compte, même après avoir relancé `hermes chat` (une modification de **skill**, `SKILL.md`, est en revanche visible dès le chat suivant) | le plugin est chargé par la **passerelle** de Hermes, qui tourne en arrière-plan et garde le code en mémoire, et non par le chat | redémarrer la passerelle. Si `hermes gateway status` indique « Running manually », `hermes gateway restart` ne suffit pas : utiliser `hermes gateway stop` puis `hermes gateway ensure`, ou quitter **complètement** puis rouvrir l'application Hermes. Le PID affiché par `hermes gateway status` doit changer |
| `hermes chat -t airflow` répond `Error: invalid_params`, et le journal indique « unknown or invalid toolset name: 'airflow' » | le plugin est désactivé (`hermes plugins list` affiche « not enabled »), ce qui a été constaté après une réinstallation de la passerelle | `hermes plugins enable airflow-mlops`, puis `hermes gateway restart` |
| L'agent répond sans appeler d'outil, ou invente des DAGs | outils du plugin cachés derrière `tool_search` (journal : « 0 core/visible tools kept, 9 deferred ») | `tools.tool_search.enabled: off` et `-t airflow` (étape 2) ; vérifier que la réponse cite de vrais noms de DAGs |
| Pendant la réponse : carte graphique peu utilisée, ~9 Go de RAM pris par `ollama.exe` | modèle et contexte de 64k trop gros pour 6 Go de mémoire graphique : le calcul se fait surtout sur le processeur (~3 tokens/s) | normal sur cette machine ; voir « Performances » |
| `ollama: command not found` dans Git Bash (VS Code) | PATH réécrit par l'extension Python | utiliser PowerShell, ou relancer la fenêtre VS Code |
| Revenir en arrière | — | restaurer `config.yaml.bak` et supprimer les 3 variables `OLLAMA_*` |

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
| `airflow_get_dag_source` | Code source du DAG (secrets masqués) : lu dans le dossier local `sandbox_dags_path` s'il est configuré, sinon via l'API Airflow. Signale si le code local diffère du code déployé. Plage de lignes possible |
| `airflow_diagnose_run` | Diagnostic complet d'un run en échec : racine vs impactées, logs, XComs, classification justifiée |
| `airflow_build_report` | Rapport de monitoring normé (6 sections, JSON + Markdown) |
| `airflow_sandbox_test` | Teste un correctif (diff) dans un conteneur Docker jetable, sans réseau ni secrets ; masqué si `sandbox_enabled: false` |
| `airflow_clear_tasks` | **Écriture.** Relance ciblée de tâches d'un run (dry run par défaut, protégée) ; masquée si `read_only: true` |
| `airflow_review_dag` | Revue d'un DAG avant déploiement : `trigger_rule` final qui masque les échecs, secret dans le code, travail lourd au chargement, exception avalée, tâches sans retry, `catchup` ; lignes exactes |
| `airflow_watch_check` | Surveillance planifiée : seulement ce qui a changé depuis la dernière vérification (`silent: true` sinon) |
| `airflow_audit_plan` / `_select` / `_progress` | Audit guidé : plan numéroté, sélection tapée, étapes et question suivante |
| `airflow_audit_timeline` | Faits d'un postmortem : dossier de l'audit et journal du plugin, dans l'ordre |

Types d'erreur : `invalid_argument`, `dag_not_allowed`, `config`, `auth`, `forbidden`,
`not_found`, `bad_request`, `timeout`, `network`, `server`, `http`, `internal`.

## Audit guidé (`/airflow-audit`)

**Règle : un audit ne modifie jamais le code.** Il fait trois choses, dans l'ordre, avec une
question entre chaque étape (4 choix au plus, le recommandé en premier, `Stop here` toujours
possible), posée en texte à la fin de la réponse :

1. **Rapport** : l'état des lieux, dans la réponse de la commande, suivi de la première
   question (« Which DAG should I diagnose? »).
2. **Diagnostic** des DAGs choisis : problèmes numérotés (1.1, 1.2, 2.1…), puis « Which problem
   first? ». Sélection tapée possible : `1.1, 2.3`, `DAG 2`, `all critical`, `all except 2.1`.
3. **Test de chaque proposition** :
   - erreur passagère → **vraie relance** (aperçu d'abord, puis votre validation), puis suivi
     du run ;
   - erreur de code → correctif **testé dans le bac à sable** (niveau `structure`, qui
     comprend l'import) puis montré : une copie, vos fichiers ne changent pas.

   Test réussi → les problèmes restants sont proposés, ou `Stop here`. Test échoué →
   « Revise the fix » / « Next problem » / `Stop here`. Un correctif qui masque l'erreur
   (`masking_warnings`) est signalé avant tout. Une erreur passagère sur un DAG qui a aussi
   une erreur de code n'est pas relancée pendant l'audit (le code n'y change pas).

**Sortie** (`Stop here`, ou tout est testé) : l'audit est **archivé**, sans reprise ; le
prochain `/airflow-audit` repart du rapport. **Si des correctifs ont réussi leur test**, une
dernière question propose « Apply 1.1 + 1.2 (your approval required) » ou « Not now » : Hermes
applique alors le patch testé (validation demandée), puis propose la relance ciblée. Le bilan (résultats, patchs testés et commande pour
les appliquer) reste connu de Hermes **dans la même conversation** pendant 3 heures. Ensuite,
si vous demandez « applique le correctif 1.1 », Hermes applique le patch testé : le verrou ne
laisse passer que lui, après votre validation.

**Pendant un audit, le verrou bloque toute écriture dans un fichier de DAG**, même d'un
correctif testé (`BLOCKED by airflow-mlops: an audit is in progress…`).
La conversation de l'audit reste verrouillée jusqu'à sa fin ; les autres conversations le
sont tant que l'audit est actif (30 minutes sans étape le libèrent).

**Comment l'audit pose ses questions.** `/airflow-audit` répond tout de suite : le
rapport, puis la première question en liste numérotée (« Which DAG should I diagnose? »). Il
n'ouvre aucun menu, n'appelle pas le modèle et n'envoie pas de message à part. Répondez dans la
conversation (un numéro, un nom de DAG, « stop ») : l'agent continue avec les outils
`airflow_audit_*`, et chaque réponse de l'agent se termine par la question suivante du plugin,
en texte.

Pourquoi ce choix :

- le Desktop abandonne une commande au bout de 30 s, puis la relance : une commande qui
  attendait un menu tournait deux fois ;
- dans une commande, Hermes ne distingue pas le Desktop d'un terminal (les deux sont la
  surface `local`) ;
- un modèle qui appelle un outil de menu (`clarify`) juste après son texte envoie souvent
  l'appel seul : le texte restait dans son raisonnement et n'apparaissait pas.

Dans un même tour, l'agent fait d'abord tous ses appels d'outils, puis écrit le résultat et la
question. Le correctif d'une erreur de code est testé au niveau `structure` du bac à sable, qui
comprend la vérification d'import.

Outils : `airflow_audit_plan(dag_ids)`, `airflow_audit_select(selection)`,
`airflow_audit_progress(item, state)` (`fix_proposed`, `retried`, `passed`, `failed`,
`skipped`, `stop`), `airflow_audit_timeline` (postmortem). Le résultat du bac à sable et la vraie
relance sont enregistrés automatiquement. `/airflow-status` montre l'audit en cours, ou le
bilan du dernier.

**Tester en une seule commande** : `hermes -z` (une question, une réponse) **saute les
validations humaines** (choix de Hermes pour les scripts). Pour vérifier les validations,
utilisez `hermes chat` ou l'application Desktop.

## Diagnostic (`airflow_diagnose_run`)

L'outil agrège en un seul appel les éléments nécessaires pour interpréter une panne.

**Structure du run**

- `root_failures` : tâches `failed` sans aucun ancêtre en échec. Ce sont les causes racines.
- `secondary_failures` : tâches `failed` qui ont un ancêtre en échec, par exemple une tâche
  `trigger_rule: all_done` qui échoue à cause de la racine.
- `impacted` : tâches `upstream_failed` / `skipped`, avec `caused_by`, la ou les racines
  responsables. Un `skipped` sans racine en amont est signalé comme possiblement volontaire
  (branchement).
- `retrying` : tâches `up_for_retry`.

**Pour chaque racine**

- le bloc d'erreur ou traceback extrait ;
- la fin du log ;
- les XComs de la tâche et de ses producteurs amont directs, masqués et tronqués ;
- ses dépendances avec leur `trigger_rule` ;
- sa **classification**.

**Résumé**

- `category` ;
- `retry_candidates` : racines `transient` uniquement ;
- `requires_human` ;
- `guidance`.

Les paramètres du DAG sont joints, masqués. L'interprétation en langage naturel reste le travail
du LLM, guidé par la skill `airflow-triage`.

### Classification

Les règles sont dans [data/error_patterns.yaml](data/error_patterns.yaml) : expressions
régulières et mots-clés, avec un `id`, une `category` et une `description`.

| Catégorie | Exemples |
|---|---|
| `transient` | timeout, connexion reset ou refusée, HTTP 429/502/503/504, pool saturé, timeout de sensor, DNS / verrou de base de données, zombie / SIGTERM |
| `critical` | SyntaxError, ImportError, KeyError / TypeError…, incohérence de schéma, échec d'un contrôle qualité, permission, connexion ou variable manquante, OOM récurrent |
| `unknown` | aucune règle ne correspond : revue humaine |

Fonctionnement :

- **Zone analysée** : le bloc d'erreur est analysé d'abord. La fin du log n'est utilisée que si le
  bloc d'erreur ne correspond à aucune règle.
- **Priorité** : une règle `critical` l'emporte toujours. Les signaux `transient` restent listés
  pour la transparence.
- **`min_tries` et `below_min_tries_category`** : ces champs gèrent les erreurs récurrentes. Un
  premier OOM est `transient`, un OOM à partir du 2e essai est `critical`.
- **Justification** : chaque classification renvoie une `justification` et les `matched_rules`
  (règle, extrait de la ligne fautive, source).
- **Règles personnalisées** : `error_patterns_path` pointe vers votre propre fichier, au même
  format. S'il est illisible ou vide, le plugin revient aux règles livrées et le signale dans
  `rules_warnings`.

Les classifications sont mises en cache dans la base SQLite du plugin (`plugin_db`). Le
mécanisme d'approbation de la phase 4 s'appuie sur ce cache.

## Rapports (`airflow_build_report`, `/airflow-report`)

`airflow_build_report(scope, period)` produit un rapport **toujours dans la même structure**.
Il renvoie un JSON (`report`, avec 6 clés fixes) et un Markdown (`markdown`, gabarit fixe).

**Le rapport est rédigé en anglais**, quelle que soit la langue de la conversation. Ses sections s'intitulent `1. Scope and period`, `2. Overall health`, `3. Performance per task`, `4. Data quality`, `5. Incidents and actions` et `6. Recommendations`. Ses tableaux sont alignés avec des espaces pour rester lisibles dans le terminal : `/airflow-report` affiche le Markdown en texte brut. Chaque statut est un emoji suivi d'un mot (`✅ healthy`, `🟠 degraded`, `🔴 failing`, `⚪ no data`, `⚠️` pour un avertissement, `ℹ️` pour une information), réunis dans `display.py`. Les raisons de statut sont listées sous le tableau, et non dans les cellules. L'agent, lui, répond dans la langue de l'utilisateur.

- `scope` : `all` (par défaut), un `dag_id` ou `tag:<tag>`. L'allowlist s'applique toujours.
- `period` : un nombre suivi de `m`, `h`, `d` ou `w` (`24h`, `7d`…), au maximum 90 jours. Par
  défaut, `report_default_period`.

| # | Section | Contenu |
|---|---|---|
| 1 | Périmètre et période | DAGs couverts, fenêtre UTC, DAGs échantillonnés ou illisibles |
| 2 | Santé globale | **un statut par DAG** avec sa raison, et un statut global **expliqué** (voir ci-dessous), runs, taux de succès, incidents |
| 3 | Performance par tâche | durées p50 / p95, ligne de base et tendance, retries, temps d'attente en file p50 / p95, SLA manqués |
| 4 | Qualité des données | lue uniquement depuis la convention XCom ci-dessous ; sinon « not instrumented » |
| 5 | Incidents et actions | runs en échec, tâches en échec, diagnostic en cache, actions du journal d'audit |
| 6 | Recommandations | règles simples (échecs, lenteur > +25 %, retries, file d'attente, SLA, qualité), triées par gravité |

**Comment les chiffres sont calculés**

- **Statut d'un DAG**, calculé sur ses propres runs, car les DAGs sont indépendants :
  - `failing` : il est cassé **maintenant**. Son dernier run a échoué, ou il cache une tâche en
    échec alors qu'il est marqué success ;
  - `degraded` : il a eu des incidents sur la période, mais son dernier run est sain ;
  - `healthy` : aucun incident ;
  - `no_data` : aucun run sur la période.
- **Statut global**, déduit des statuts des DAGs, et non des runs mélangés :
  - `failing` si **au moins la moitié** des DAGs avec des runs sont en échec ;
  - `degraded` si au moins un DAG a un problème ;
  - `healthy` sinon.

  Il est toujours suivi de sa **raison** : par exemple « 2 DAG(s) sur 3 en échec : … », avec
  le nom de chaque DAG concerné et son motif. Un seul DAG en difficulté parmi plusieurs DAGs
  sains donne donc `degraded`, pas `failing`.

- **Ligne de base** : stockée dans `plugin_db()` par couple `(dag_id, task_id)`. Elle est mise à
  jour à chaque rapport par moyenne mobile exponentielle, avec un poids de 30 % pour les
  nouvelles mesures et à partir de 3 exécutions. `update_baseline=false` laisse la base
  intacte. La tendance compare le p50 à la base : `stable` (±10 %), `slower`, `faster` ou
  `no_baseline`.
- **SLA** : calculés seulement si `sla_thresholds` est configuré :
  - `"dag_id": secondes` s'applique à la durée du run ;
  - `"dag_id.task_id": secondes` s'applique à la durée de la tâche.
- **Bornes** : au plus 50 DAGs, 200 runs par DAG, et le détail des tâches des 30 runs les plus
  récents. Les DAGs concernés sont signalés comme échantillonnés.

**Commande slash** : `/airflow-report [all|<dag_id>|tag:<tag>] [24h|7d]` renvoie directement le
Markdown. Un argument qui commence par un chiffre est lu comme la période.

## Skills embarquées (`skills/`)

Elles sont enregistrées via `ctx.register_skill` et accessibles sous le nom
`airflow-mlops:<nom>`.

| Skill | Rôle |
|---|---|
| `airflow-triage` | Procédure de diagnostic : trouver la racine, lire logs / XComs / dépendances, classifier, proposer. Relancer le minimum, jamais le DAG entier, toujours un *dry run* d'abord. Avec **plusieurs problèmes**, l'agent présente d'abord un **plan numéroté par DAG** (1.1, 1.2, 2.1…), avec une correction proposée pour chacun, puis demande par lequel commencer. |
| `airflow-report` | Présentation du rapport normé : résumé exécutif, puis les 6 sections inchangées. Aucun chiffre inventé. |
| `airflow-audit` | Le chef d'orchestre de l'audit guidé : rapport, choix, plan numéroté, correctif, test, application, relance, avec une question du plugin entre chaque étape. |
| `airflow-dag-review` | Revue d'un DAG avant déploiement (`airflow_review_dag`), puis correctifs testés en bac à sable et appliqués sous le verrou. |
| `airflow-watch` | Surveillance planifiée : `[SILENT]` quand rien n'a changé, sinon une alerte courte (diagnostic, relance des erreurs passagères selon la politique). Lancée par le modèle « Airflow night watch » (`/blueprint`). |
| `airflow-postmortem` | Fiche bilan d'incident en anglais, uniquement à partir des faits enregistrés (chronologie, cause, correctif et preuve du test, validations, prévention). |
| `airflow-fix-proposal` | Proposition de correctif pour une erreur critique : l'agent **lit le code** (`airflow_get_dag_source`), montre le correctif (fichier, lignes, avant / après, risques), **propose** de le tester en bac à sable, puis **s'arrête**. Les outils du plugin n'écrivent jamais vos fichiers ; si vous demandez ensuite la modification, Hermes la fait avec ses outils de code habituels, que la skill ne bride pas. |

## Remédiation protégée (`airflow_clear_tasks`)

`airflow_clear_tasks(dag_id, run_id, task_ids, include_downstream=true, only_failed=true,
dry_run=true, reason, confirm_critical=false)` relance **uniquement** les task instances
nécessaires d'**un** run. L'outil n'est exposé que si `read_only: false`.

### Garde-fous codés en dur

Aucun réglage ne permet de les désactiver.

| Règle | Comportement |
|---|---|
| `task_ids` vide | refusé ; il n'existe pas de « clear » de DAG entier |
| Tâches déjà réussies | jamais relancées par défaut : `only_failed=true` (avec `include_downstream=true`) relance la racine et les tâches qu'elle a bloquées (`upstream_failed`), et rien d'autre |
| DAG entier | refusé dès que la relance couvrirait toutes les tâches du DAG **en ré-exécutant des tâches déjà réussies** (DAG de 2 tâches ou plus) |
| Portée | un seul run (`dag_run_id`) ; jamais d'amont, de passé, de futur ni de plage de dates ; 20 tâches au maximum |
| `dry_run=true` par défaut | renvoie la liste exacte calculée par le *dry run* d'Airflow, sans rien modifier |
| Erreur `critical` | relance réelle refusée sans `confirm_critical=true` ; le dry run reste possible, avec un avertissement |
| `reason` | obligatoire pour une relance réelle ; elle est journalisée |
| Allowlist / `read_only` | vérifiés dans l'outil **et** dans le hook |

La classification utilisée est celle du diagnostic en cache. À défaut, le dernier essai de la
tâche est classé à la volée. Une relance réelle commence toujours par un *dry run* Airflow.
Elle utilise ensuite `reset_dag_runs=true`, pour que le run en échec redevienne exécutable par
le scheduler : seules les tâches relancées sont ré-exécutées, les autres gardent leur état.

### Validation humaine

Le hook `pre_tool_call` intercepte chaque appel de `airflow_clear_tasks` avec `dry_run=false` :

- DAG hors allowlist ou mode `read_only` : `{"action": "block"}` ;
- sinon `{"action": "approve", "message": <résumé>, "rule_key": "airflow_clear_tasks:<dag>:<run>"}`,
  qui envoie l'appel vers la **validation humaine de Hermes** (once / session / always / deny) ;
- seule exception : la politique de relance automatique ci-dessous l'autorise ;
- si le hook plante, l'appel est bloqué.

Sans humain disponible (cron, gateway sans pont d'approbation), Hermes refuse l'action.

### Politique de relance automatique (désactivée par défaut)

Avec `auto_remediation_enabled: true`, une relance réelle passe **sans** validation humaine
seulement si toutes ces conditions sont remplies :

1. chaque tâche a un diagnostic en cache classé `transient` (lancer `airflow_diagnose_run` d'abord) ;
2. `only_failed=true` ;
3. moins de `auto_retry_max_attempts` relances automatiques ont déjà eu lieu pour
   `(dag_id, run_id, task_id)`. Le compteur est persisté dans `plugin_db()` ;
4. le backoff est écoulé : `auto_retry_backoff_seconds × 2^(n-1)` après la n-ième relance
   automatique.

Au-delà, l'appel repasse par la validation humaine : **escalade, jamais de boucle**.

### Journal d'audit

Chaque appel est enregistré dans la table `audit_log` de `plugin_db()`. Cela couvre les dry
runs, les refus, les relances et les erreurs Airflow. Chaque entrée contient :

- **qui** : `session_id` / `conversation_id` Hermes ;
- **quoi** : DAG, run, tâches, options, task instances touchées ;
- **quand** : horodatage ;
- **pourquoi** : `reason` ;
- le mode (manuel ou automatique) et le résultat.

Une relance réelle est d'abord journalisée comme `pending`. **Si le journal est indisponible,
la relance est refusée.** Le rapport (section 5) affiche ces actions.

## Bac à sable (`airflow_sandbox_test`)

`airflow_sandbox_test(dag_id, patch, level="import", task_id?, run_id?)` teste un correctif
avant qu'il soit proposé.

**Déroulé**

1. Le diff unifié est appliqué à une **copie** de `sandbox_dags_path`, dans
   `plugin_data_dir("airflow-mlops")/sandbox/<id>/`. Cette copie est supprimée après le test.
2. Le script [sandbox_runner/check_dag.py](sandbox_runner/check_dag.py) est lancé dans un
   conteneur jetable, via `ctx.dispatch_tool("terminal", ...)`. L'appel passe donc par le
   pipeline d'approbation de Hermes.

**Le conteneur**

- `--network none`, aucun secret ;
- `--cpus` / `--memory` sans swap / `--pids-limit 256` ;
- système de fichiers `--read-only`, avec un `/tmp` limité ;
- `--cap-drop ALL`, `no-new-privileges`, utilisateur non root ;
- `timeout --signal KILL`.

**Les niveaux**

- `import` : `DagBag`, aucune erreur d'import ;
- `structure` : tâches et dépendances comparées à la production ;
- `task` : `airflow tasks test` d'**une** tâche, en mode échantillon.

**Les refus et protections**

- Le niveau `task` est refusé pour les `sandbox_heavy_operators`.
- Un niveau au-delà de `sandbox_max_level` déclenche la validation humaine (hook
  `pre_tool_call`).
- Les tests sont limités : un à la fois, et `sandbox_max_tests_per_session` au total.
- Le diff est refusé s'il sort du dossier, supprime un fichier, ou si ses lignes de contexte
  sont introuvables.
- Chaque test est journalisé (`audit_log`).

**Le résultat** est un JSON structuré : `passed`, `stages`, `exit_code`, `oom`, `timed_out`,
`log_excerpt`, `patched_files`, `limits`. Il contient aussi, pour présenter le correctif :

- `changes` : pour chaque fichier, le **chemin complet** et les **lignes exactes**, avant et
  après. Elles sont calculées à partir des fichiers, et non des numéros souvent faux écrits
  par le modèle ;
- `patch_file` : le correctif testé, enregistré comme **fichier `.patch` standard** dans
  `plugin_data_dir`, jamais dans ton dépôt ;
- `apply_commands` : les commandes `git apply --check`, `git apply` et `git apply -R`
  (annulation) à lancer **après relecture** ;
- `your_files_modified: false` : le rappel que seule une copie a été modifiée.

**Prérequis** : Docker accessible au terminal **local** de Hermes, et une image
`sandbox_image` contenant Airflow et les dépendances des DAGs. Le conteneur n'a pas de réseau,
donc pas de `pip install` possible. Mise en place et convention d'échantillon :
[guide utilisateur § 6](docs/GUIDE_UTILISATEUR.md).

## Convention XCom « qualité des données »

Airflow n'a pas de notion native de qualité des données. Le rapport lit la convention
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

Sans XCom conforme, la section indique « not instrumented » : le rapport n'invente jamais de
chiffres. Exemple dans [demo-local-airflow/dags/mlops_demo.py](demo-local-airflow/dags/mlops_demo.py).

## Revue, surveillance et fiche bilan (plan v2 étapes 4 et 5)

**Revue avant déploiement** — `airflow_review_dag(dag_id)` lit le code (dossier local d'abord) et
les tâches, et signale avec la ligne exacte : tâche finale en `all_done` sans « watcher »
(`high`), secret écrit dans le code (`high`, repéré par le masquage du plugin), travail lourd au
chargement du fichier (`requests.get`, `Variable.get`… hors d'une tâche), exception avalée,
tâches sans retry, `catchup=True`. Ce sont des heuristiques : elles montrent les lignes à revoir,
elles ne modifient rien.

**Surveillance planifiée** — `airflow_watch_check` compare le dernier run de chaque DAG autorisé à
la vérification précédente (table `watch_state`) : nouveau run en échec, échec caché (run
« success » avec une tâche en échec), retour au vert. Rien de nouveau → `silent: true`, et la
tâche planifiée répond `[SILENT]` (aucun message). Le plugin propose le modèle **« Airflow night
watch »** dans la galerie de Hermes (`/blueprint`) : fréquence 15, 30 ou 60 minutes, livraison où
vous voulez (chat d'origine, local, Telegram…). Les erreurs passagères peuvent être relancées
selon la politique `auto_remediation_enabled` ; une erreur critique n'est jamais relancée.

**Fiche bilan** — `airflow_audit_timeline(audit_id?)` renvoie le dossier (points numérotés et leurs
états) et le journal du plugin pour ses DAGs, dans l'ordre : tests en bac à sable, fichiers de DAG
écrits (`matches_tested_fix`), relances, et qui a validé. La skill `airflow-postmortem` en fait une
fiche en anglais, sans rien inventer.

## Verrou sur les DAGs (`dag_write_guard`, plan v2 étape 3)

Hermes reste un agent de code complet ; le plugin ajoute **un seul verrou**, sur les fichiers
du dossier des DAGs (`sandbox_dags_path`) : **seul le changement exact qui a réussi un test en
bac à sable peut y être écrit, et Hermes demande votre validation**. Partout ailleurs, ses
outils de fichiers et son terminal fonctionnent normalement.

| Hermes veut… (dans le dossier des DAGs) | Décision |
|---|---|
| `write_file` / `patch` sans test réussi | **bloqué** : « testez d'abord avec airflow_sandbox_test » |
| `write_file` / `patch` qui produit exactement le fichier testé | **votre validation** est demandée |
| `write_file` / `patch` qui produit autre chose | **bloqué**, avec la commande qui applique le correctif testé |
| le fichier a changé depuis le test | **bloqué** : refaire le test sur le fichier actuel |
| `git apply` du `.patch` testé (terminal) | **votre validation** est demandée (`git apply -R` pour annuler aussi) |
| `git apply` d'un autre patch | **bloqué** |
| autre commande du terminal ou code Python qui écrit dans ce dossier | **votre validation**, avec un avertissement (le plugin ne peut pas la vérifier) |
| lire (`read_file`, `cat`, `grep`, `git diff`, `git apply --check`), ou tout autre dossier | libre |

- Chaque test réussi enregistre l'empreinte du fichier avant et après le correctif (table
  `tested_changes`, gardée 7 jours). Les fins de ligne (CRLF / LF) sont ignorées.
- Il faut **au moins le niveau `structure`** : `import` seul prouve que le fichier se charge,
  pas que les tâches et dépendances sont intactes.
- **Un correctif qui masque l'erreur** (`x["k"]` devenu `x.get("k")`, exception avalée, valeur
  par défaut, `raise` / `assert` supprimé) passe tous les niveaux du bac à sable. Le résultat du
  test le signale dans `masking_warnings`, et l'audit demande alors « it may hide the error…
  What now? » avec « Revise the fix » recommandé, avant toute application.
- Après l'écriture, le hook `post_tool_call` vérifie le fichier, l'inscrit au journal
  (`dag_write` : `matches_tested_fix` ou `differs_from_tested_fix`) et fait passer le point de
  l'audit à « applied ».
- La validation demandée par le plugin porte sur **ce changement-là** : répondre « always »
  n'autorise pas les changements suivants.
- Réglage `dag_write_guard` : `sandbox_required` (défaut), `approval_only` (une validation
  humaine pour toute écriture, sans test), `off`. Si le bac à sable est désactivé, le verrou
  passe de lui-même en `approval_only`, puisque rien ne peut être testé.
- **Limite connue** : en mode « yolo » de Hermes, les validations humaines sont sautées (choix
  de Hermes) ; les blocages, eux, restent actifs.

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
- **Aucune écriture** dans le dossier du plugin. L'état durable (cache de diagnostic, et plus
  tard journal et compteurs) est dans `plugin_db("airflow-mlops")`. Son schéma est versionné par
  `PRAGMA user_version`.
- **Divulgation (règle 13 du catalogue)** : le seul service réseau appelé est l'API REST du
  serveur Airflow configuré. Aucun processus d'arrière-plan, aucune télémétrie.
  - **Commandes shell** : uniquement le `docker run` du bac à sable (ci-dessous). Il passe par
    `ctx.dispatch_tool("terminal")` : les contrôles de commandes dangereuses du terminal de
    Hermes s'appliquent, pas les hooks `pre_tool_call`. La validation humaine est donc faite
    sur l'outil `airflow_sandbox_test` lui-même, au-dessus de `sandbox_max_level`.
  - **Lectures hors des données du plugin** : le dossier local des DAGs (`sandbox_dags_path`),
    lu pour afficher le code et copié pour le bac à sable, jamais modifié par le plugin.
  - **Contexte injecté** : pendant un audit, `pre_llm_call` ajoute un court rappel du dossier
    au message de l'utilisateur, seulement dans la conversation de l'audit.
  - Il ne **modifie** Airflow que par `airflow_clear_tasks`. Cet outil est désactivé par défaut
    (`read_only: true`), passe par la validation humaine de Hermes et est journalisé.
  - La relance automatique sans validation est une option explicite, désactivée par défaut
    (`auto_remediation_enabled`). Elle est bornée aux erreurs transitoires diagnostiquées, à
    `auto_retry_max_attempts` tentatives par tâche avec un délai croissant, puis escalade vers
    un humain. Le hook ne considère comme aperçu qu'un `dry_run` réellement vrai.
  - Le bac à sable (`sandbox_enabled`, désactivé par défaut) lance `docker run` via l'outil
    `terminal` de Hermes. Le conteneur est jetable, sans réseau ni secrets. Il lit une copie
    temporaire des DAGs, placée dans `plugin_data_dir`.

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

## Tests

Le projet a trois niveaux de vérification, du plus rapide au plus réaliste :

| Niveau | Commande | Durée | Ce qui est réel |
|---|---|---|---|
| 1. Tests unitaires | `pytest` | ~10 s | la logique du plugin ; Airflow et Docker sont simulés |
| 2. Validation Hermes | `hermes plugins doctor . --ci` et `hermes plugins validate .` | ~30 s | le chargement par le vrai Hermes |
| 3. Test Docker du bac à sable | `python demo-local-airflow/sandbox_smoke_test.py` | ~2–3 min | de vrais conteneurs Airflow |
| 4. Test de bout en bout | `python demo-local-airflow/e2e_walkthrough.py` | ~1 min | un vrai serveur Airflow et ses vraies réponses |

### 1. Tests unitaires (`pytest`, 496 tests)

Ils simulent l'API Airflow avec `respx`, au format exact des versions **v1** (Airflow 2.x) et
**v2** (Airflow 3.x). La plupart des tests tournent sur les deux versions. Les modules hôtes de
Hermes (`plugin_storage`, `plugin_utils`, `secret_scope`) sont remplacés par des doublures,
uniquement dans [tests/harness.py](tests/harness.py) : aucun Hermes, aucun Airflow ni aucun
Docker n'est nécessaire.

| Fichier | Tests | Ce qui est vérifié |
|---|---|---|
| [test_tools.py](tests/test_tools.py) | 38 | Un test par outil de lecture, allowlist, pagination, encodage des `run_id`, erreurs HTTP en JSON clair, aucun traceback exposé |
| [test_client.py](tests/test_client.py) | 18 | Authentification basic / token / JWT (et son renouvellement), relances réseau, codes HTTP, lecture des logs v1 et v2, réglages |
| [test_redaction.py](tests/test_redaction.py) | 32 | Masquage de chaque forme de secret, et le texte normal laissé intact |
| [test_classification.py](tests/test_classification.py) | 33 | 22 erreurs réelles classées `transient` / `critical`, priorité du critique, OOM récurrent, règles personnalisées |
| [test_diagnosis.py](tests/test_diagnosis.py) | 20 | Diagnostic de bout en bout : racine, échecs secondaires, tâches impactées, échecs masqués, XComs, cache |
| [test_reporting.py](tests/test_reporting.py) | 64 | **Forme du rapport** (6 sections, toujours dans le même ordre), échecs masqués, calculs, ligne de base, SLA, qualité des données, section 5 résumée, commande `/airflow-report` |
| [test_guardrails.py](tests/test_guardrails.py) | 47 | **Garde-fous** : aucune tâche réussie relancée, refus du DAG entier, `dry_run` par défaut, refus du critique, limite et backoff des relances automatiques, hook d'approbation, journal obligatoire |
| [test_dag_source.py](tests/test_dag_source.py) | 18 | Lecture du code d'un DAG : API v1/v2, dossier local prioritaire, écart avec le code déployé, plages de lignes, masquage, et un diff construit à partir du code lu qui s'applique bien |
| [test_sandbox.py](tests/test_sandbox.py) | 47 | Application des diffs, chemins interdits, commande Docker bridée, lecture des résultats (OOM, timeout, réseau), budget, nettoyage |
| [test_storage.py](tests/test_storage.py) | 3 | Base SQLite : migrations et cache des diagnostics |
| [test_skills.py](tests/test_skills.py) | 13 | Format des 7 skills, et outils cités qui existent bien |
| [test_workflow.py](tests/test_workflow.py) | 29 | **Audit guidé** (règle simple) : rapport → diagnostic → test, problèmes restants ou `Stop here`, aucune relance d'un DAG qui a une erreur de code, archivage sans reprise, bilan avec les patchs testés |
| [test_audit.py](tests/test_audit.py) | 36 | Audit de bout en bout : la commande répond tout de suite (rapport + question en texte, aucun menu, aucun message à part), questions jamais posées par un outil de menu, choix gardés pour la réponse, rappel limité à la conversation de l'audit, résultat du bac à sable = test, `Stop here` archive |
| [test_batch.py](tests/test_batch.py) | 6 | **Traitement par lot** : correctifs d'un même DAG testés en un seul passage, relances groupées, barre d'avancement |
| [test_dag_guard.py](tests/test_dag_guard.py) | 17 | **Verrou sur les DAGs** : écriture non testée bloquée, changement testé soumis à validation (CRLF compris), changement différent bloqué avec la solution, fichier modifié depuis le test, `git apply` du patch testé ou non, commandes qui écrivent, modes, journal après écriture |
| [test_masking.py](tests/test_masking.py) | 9 | Correctifs qui masquent l'erreur (`.get()`, exception avalée, valeur par défaut, `raise` supprimé) repérés ; l'audit demande avant d'appliquer |
| [test_dag_review.py](tests/test_dag_review.py) | 8 | Revue de DAG : `all_done` sans watcher, secret, travail au chargement, exception avalée, retries, `catchup`, lignes du bon DAG |
| [test_watch.py](tests/test_watch.py) | 13 | Surveillance : nouveautés seulement, `[SILENT]`, retour au vert, allowlist ; faits du postmortem ; modèle « night watch » |
| [test_display.py](tests/test_display.py) | 3 | Emoji des statuts, aucune séquence de couleur dans les commandes et les rapports, quelle que soit la surface |
| [test_regressions.py](tests/test_regressions.py) | 29 | Un test par bug corrigé lors de l'audit du 10/11 : `dry_run` en texte, verrou et commandes relatives / liens, patch sans effet, journal tronqué, pagination plafonnée, allowlist mal formée, `none_failed`, sélection `extract-and-load`, masquage du code source, fenêtre du rapport sur Airflow 2, classification d'un ancien essai, audit oublié qui ne verrouille plus les autres conversations, secret changé, tâches mappées |
| [test_register.py](tests/test_register.py) | 7 | Manifest identique à ce que `register()` enregistre, aucun appel réseau au chargement, aucune lecture de `.env` |
| [test_textutils.py](tests/test_textutils.py) | 6 | Extraction du bloc d'erreur et troncature |

```bash
.venv/Scripts/python -m pytest        # résumé
.venv/Scripts/python -m pytest -v     # une ligne par test
```

### 2. Validation Hermes

- `hermes plugins doctor . --ci` charge le plugin avec le vrai Hermes, dans un environnement
  temporaire. Il vérifie les noms des hooks, les `**kwargs` et les enregistrements.
- `hermes plugins validate .` exécute les contrôles d'admission au catalogue : outils et hooks
  déclarés = enregistrés, analyse de sécurité, absence de monkey-patching, dépendances.

### 3. Test Docker du bac à sable (manuel)

[demo-local-airflow/sandbox_smoke_test.py](demo-local-airflow/sandbox_smoke_test.py) fait
passer **le vrai code du bac à sable** par 6 scénarios sur de vrais conteneurs. Seul l'outil
`terminal` de Hermes est remplacé par un appel direct à bash. Le script nécessite Docker et une
image Airflow locale ; il ne fait pas partie de `pytest`.

```bash
.venv/Scripts/python demo-local-airflow/sandbox_smoke_test.py                      # apache/airflow:2.9.3
.venv/Scripts/python demo-local-airflow/sandbox_smoke_test.py --image apache/airflow:3.4.0
```

Résultat du 08/11/2026, identique avec `apache/airflow:2.9.3` et `apache/airflow:3.4.0` (durées
indiquées pour la 2.9.3 ; la 3.4.0 met ~10 s de plus aux scénarios D à F) :

| Scénario | Attendu | Obtenu |
|---|---|---|
| A. Correctif d'un `KeyError`, niveau `import` | le DAG corrigé se charge | ✅ (11 s) |
| B. Même correctif, niveau `structure` | même structure qu'en production | ✅ 3 tâches, aucun changement (11 s) |
| C. Correctif cassé | erreur détectée | ✅ `SyntaxError: '(' was never closed` (9 s) |
| D. Lancement réel d'une tâche | la tâche réussit | ✅ elle renvoie `{'rows': 1200}` (33 s) |
| E. Tâche de 3 Go avec une limite de 512 Mo | tuée, `oom: true` | ✅ (32 s) |
| F. Tâche qui appelle Internet | bloquée, explication donnée à l'agent | ✅ (47 s) |

Le fichier DAG d'origine n'est pas modifié, et aucune copie temporaire ne reste après le test.

### 4. Test de bout en bout sur un vrai Airflow (manuel)

[demo-local-airflow/e2e_walkthrough.py](demo-local-airflow/e2e_walkthrough.py) appelle les
outils du plugin, exactement comme Hermes, sur un vrai serveur Airflow. Il suit le déroulé des
skills : lister, diagnostiquer, lire les logs et XComs, tester un correctif en bac à sable,
relancer (aperçu puis relance), produire le rapport. Seul le LLM est absent : l'ordre des appels
est fixé dans le script.

```bash
# Airflow 2.9.3, API v1, auth basic (http://localhost:8080)
docker compose -f demo-local-airflow/docker-compose.airflow2.yml up -d
.venv/Scripts/python demo-local-airflow/e2e_walkthrough.py

# Airflow 3.4.0, API v2, auth JWT (http://localhost:8081) : tourne à côté de la 2.9
docker compose -f demo-local-airflow/docker-compose.yml up -d
.venv/Scripts/python demo-local-airflow/e2e_walkthrough.py --base-url http://localhost:8081 \
    --api-version v2 --auth jwt --image apache/airflow:3.4.0
```

Avant de lancer le script, activez et déclenchez les 3 DAGs de démo, dans l'interface web
(`admin` / `admin`) ou via l'API.

Ce test, lancé le 08/11/2026 sur Airflow 2.9.3, a révélé **deux comportements réels
d'Airflow** que les tests simulés ne pouvaient pas montrer. Les deux sont corrigés et couverts
par des tests :

| Constat sur le vrai Airflow | Correction |
|---|---|
| Une tâche finale `trigger_rule="all_done"` fait passer le run en **`success` malgré une tâche en échec** : l'échec est masqué | le rapport compte et liste les **échecs masqués**, recommande de revoir les `trigger_rule` ; `airflow_diagnose_run` signale `masked_failure` ; la skill de triage ne se fie plus au seul état du run |
| Relancer seulement la tâche racine laisse les tâches en aval en **`upstream_failed`** : Airflow ne les relance pas tout seul et le run reste `failed` | `include_downstream=true` par défaut, avec `only_failed=true`. Cela relance la racine et les tâches qu'elle a bloquées, **jamais une tâche réussie** (vérifié sur le vrai Airflow) |

Après correction, le run bloqué a été entièrement réparé : seule `publish` a été relancée, et
le run est passé à `success`.

Le même test, relancé le 08/11/2026 sur **Airflow 3.4.0** (la dernière version publiée), a
validé l'API v2, l'authentification JWT, les logs structurés et les XComs natifs. Il a aussi
révélé **quatre différences d'Airflow 3**, toutes corrigées et couvertes par des tests :

| Constat sur Airflow 3.4 | Correction |
|---|---|
| Le serveur d'API ouvre le fichier des mots de passe **en écriture** : un montage en lecture seule le fait planter | `docker-compose.yml` monte `passwords.json` en lecture-écriture |
| `DagBag` n'accepte plus l'argument `include_examples` : le script du bac à sable plantait | il n'utilise cet argument que si la version d'Airflow le connaît. En cas de plantage, il renvoie désormais **l'erreur réelle** au lieu de « no result line » |
| **`airflow tasks test` sort avec le code 0 même quand la tâche échoue** : le bac à sable aurait déclaré « réussi » un correctif dont la tâche échoue | le résultat se base sur l'**état final annoncé** par Airflow (`new_state=…` en 3.x, `Marking task as …` en 2.x), puis seulement sur le code de sortie |
| Un run manuel n'a **pas de `logical_date`**, et sa `start_date` change à chaque relance : le rapport l'écartait de la période | le rapport se base sur `logical_date`, puis sur **`run_after`**, qui est stable, puis sur `start_date` |

### Démo locale (optionnelle, hors plugin)

Le dossier [demo-local-airflow/](demo-local-airflow/) contient un Airflow jetable. Il sert uniquement à essayer le plugin à la main et ne fait pas partie du plugin.

```bash
docker compose -f demo-local-airflow/docker-compose.yml up -d          # Airflow 3.4.0, port 8081
docker compose -f demo-local-airflow/docker-compose.airflow2.yml up -d # Airflow 2.9.3, port 8080
```

Les deux tournent côte à côte : chaque fichier compose a son propre nom de projet et son propre
port. Réglages du plugin :

| Démo | `base_url` | `api_version` | `auth_mode` |
|---|---|---|---|
| Airflow 3.4.0 | `http://localhost:8081` | `v2` | `jwt` |
| Airflow 2.9.3 | `http://localhost:8080` | `v1` | `basic` |

Secrets : `AIRFLOW_USERNAME=admin` / `AIRFLOW_PASSWORD=admin`. Lancez ensuite `hermes chat`. Les
DAGs de démonstration couvrent un cas sain, une erreur critique (`KeyError`) et une erreur
transitoire (`ConnectionResetError`).

## Licence

MIT
