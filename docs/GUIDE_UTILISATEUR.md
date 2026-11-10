# Guide utilisateur — plugin `airflow-mlops`

Ce guide explique **ce que l'agent sait faire**, **les outils qu'il utilise** et **les réglages
que vous pouvez changer**, sans entrer dans le code. Pour les détails techniques, voir le
[README](../README.md).

> Version décrite : **1.3.0**.

---

## 1. En bref

Le plugin transforme l'agent Hermes en assistant pour **Apache Airflow**. Il sait faire :

| Fonction | En une phrase | Modifie quelque chose ? |
|---|---|---|
| 🔍 **Surveiller** | Lister les DAGs, leurs runs, leurs tâches, leurs logs | Non |
| 🩺 **Diagnostiquer** | Trouver la tâche à l'origine d'une panne et dire si l'erreur est passagère ou grave | Non |
| 📊 **Rapporter** | Produire un rapport de santé, toujours dans le même format | Non |
| 🧭 **Audit guidé** | `/airflow-audit` : une question entre chaque étape (répondez par un numéro), `Stop here` toujours possible | Non, sauf ce que vous validez |
| 🔁 **Relancer** | Relancer uniquement les tâches en échec, après votre accord | **Airflow**, désactivé par défaut |
| 🧪 **Tester un correctif** | Essayer un correctif dans un conteneur Docker jetable avant de vous le proposer | Non, désactivé par défaut |
| 🔒 **Verrou sur les DAGs** | Seul le changement exact qui a réussi le test peut être écrit dans un DAG, après votre validation | Vos fichiers, seulement après validation |
| 🔎 **Revoir un DAG** | Avant déploiement : `all_done` qui cache les échecs, secret dans le code, retries… avec la ligne exacte | Non |
| 🌙 **Surveillance de nuit** | Tâche planifiée « Airflow night watch » : un message seulement quand il y a du nouveau | Non (relance des erreurs passagères si vous l'autorisez) |
| 📝 **Fiche bilan** | Postmortem d'un incident : chronologie, cause, correctif, qui a validé quoi | Non |

**Par défaut, le plugin est en lecture seule** : il ne peut rien modifier dans Airflow.

Les réponses du plugin utilisent les mêmes repères visuels : `✅ healthy`, `🟠 degraded`,
`🔴 failing`, `⚠️` pour un avertissement. L'audit pose ses questions en liste numérotée :
répondez par un numéro ou un nom.

---

## 2. Démarrage rapide

1. **Installer et activer le plugin**
   ```bash
   hermes plugins install <owner>/<repo>
   hermes plugins enable airflow-mlops
   ```
2. **Donner les identifiants Airflow.** Hermes les demande à l'installation. Il en faut **un
   seul** des deux jeux suivants :
   - `AIRFLOW_USERNAME` + `AIRFLOW_PASSWORD`,
   - ou `AIRFLOW_TOKEN`.
3. **Indiquer où est Airflow**, dans `~/.hermes/config.yaml` :
   ```yaml
   plugins:
     entries:
       airflow-mlops:
         settings:
           base_url: https://airflow.mon-entreprise.com
           api_version: v2        # v2 = Airflow 3.x, v1 = Airflow 2.x
           auth_mode: jwt         # jwt (Airflow 3), basic (Airflow 2), token
   ```
4. Lancer `hermes chat` et demander : *« Quels DAGs sont en échec aujourd'hui ? »*

---

## 3. Ce que vous pouvez demander

Vous parlez normalement ; l'agent choisit les bons outils.

| Vous demandez… | L'agent utilise |
|---|---|
| « Liste mes DAGs ML » | `airflow_list_dags` |
| « Que fait le DAG `train_model` ? » | `airflow_get_dag` |
| « Les 10 derniers runs de `etl_daily` » | `airflow_get_dag_runs` |
| « Pourquoi le run d'hier a échoué ? » | `airflow_diagnose_run`, puis logs, XComs, dépendances |
| « Montre-moi le log de la tâche `load` » | `airflow_get_task_logs` |
| « Rapport de la semaine » ou `/airflow-report all 7d` | `airflow_build_report` |
| « Relance la tâche en échec » | `airflow_clear_tasks` : aperçu d'abord, puis votre accord |
| « Comment corriger cette erreur ? » | skill `airflow-fix-proposal` : le correctif (fichier, lignes, avant / après), puis l'offre de le tester dans le bac à sable |
| « Applique le correctif » (après le test) | `git apply` du patch testé : le **verrou** le laisse passer et Hermes vous demande de valider ; toute autre écriture dans un DAG non testée est bloquée |
| « Vérifie que ton correctif marche » | `airflow_sandbox_test`, qui le teste dans le bac à sable |
| « Qu'est-ce qui est cassé ? Corrige mes DAGs » | un **plan numéroté par DAG** (1.1, 1.2, 2.1…), avec une correction proposée pour chaque problème ; l'agent demande ensuite par lequel commencer |
| « Relis mon DAG avant que je le déploie » | skill `airflow-dag-review` : `airflow_review_dag` (lignes exactes), puis correctifs testés |
| « Surveille mes DAGs la nuit » ou `/blueprint` → « Airflow night watch » | skill `airflow-watch` : `airflow_watch_check`, silencieux quand rien ne change |
| « Fais-moi le postmortem de l'incident » | skill `airflow-postmortem` : `airflow_audit_timeline` (dossier + journal) |

**L'audit guidé : `/airflow-audit`** — rapport → diagnostic → test, **sans jamais modifier le code**

```
/airflow-audit
→ le rapport, puis : Which DAG should I diagnose?  1. demo_train_critical  2. demo_fetch_transient
                     3. All (2)  4. Stop here                         vous : 1
→ le diagnostic, puis : Which problem first?  1. 1.1 train — KeyError 'target'  2. …    vous : 1
→ le correctif testé (fichier, lignes avant/après, bac à sable structure ✅), puis les problèmes
  restants                                                              vous : stop
→ 🗂️ Audit #7 archived — le bilan, puis : Apply 1.1 (your approval required) ?  vous : oui
```

1. **Rapport** : toujours en premier.
2. **Diagnostic** des DAGs choisis : problèmes numérotés (1.1, 1.2, 2.1…).
3. **Test de chaque proposition**, fait par l'agent :
   - une erreur passagère est **relancée pour de vrai**, après un aperçu et votre accord ;
   - une erreur de code reçoit un correctif **testé dans le bac à sable** (une copie). Si le
     test échoue, « Revise the fix » demande un nouveau correctif. Après chaque test réussi, les problèmes restants vous sont proposés, ou `Stop here`.
4. **Sortie** : l'audit est **archivé** (pas de reprise). Hermes garde le bilan dans la
   conversation : dites ensuite « applique le correctif 1.1 », et il applique le patch testé,
   après votre validation.

Pendant l'audit, **aucun fichier de DAG ne peut être modifié** : le verrou le bloque.

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

Lancez simplement `hermes chat` (ou l'application Desktop) : l'agent a les outils de fichiers
et le terminal de Hermes, en plus des outils du plugin. L'option `-t airflow,file,terminal` ne
sert qu'avec un petit modèle local, pour alléger le prompt.

**Commande rapide :** `/airflow-report [périmètre] [période]`

- périmètre : `all`, un nom de DAG, ou `tag:<étiquette>` ;
- période : `24h`, `7d`, `2w`, au maximum 90 jours.

Exemples : `/airflow-report`, `/airflow-report train_model 7d`, `/airflow-report tag:ml 24h`.

---

## 4. Les outils

### Lecture (sans risque)

| Outil | À quoi il sert | Paramètres principaux |
|---|---|---|
| `airflow_list_dags` | Liste des DAGs : en pause ou non, planification, étiquettes, prochain run | `tags`, `paused`, `limit` |
| `airflow_get_dag` | Détail d'un DAG : paramètres (secrets masqués), liste des tâches | `dag_id` |
| `airflow_get_dag_runs` | Derniers runs : état, durée | `dag_id`, `limit`, `state` |
| `airflow_get_run_status` | État de chaque tâche d'un run : essais, durée, attente | `dag_id`, `run_id` |
| `airflow_get_task_logs` | Fin du log d'une tâche et **bloc d'erreur extrait** | `dag_id`, `run_id`, `task_id`, `try_number` |
| `airflow_get_xcoms` | Données échangées entre tâches (masquées, tronquées) | `dag_id`, `run_id`, `task_id`, `key` |
| `airflow_get_dependencies` | Tâches en amont et en aval, avec leur règle de déclenchement | `dag_id`, `task_id`, `depth` |
| `airflow_get_dag_source` | Le code du DAG (secrets masqués), pour écrire un correctif juste | `dag_id`, `start_line`, `end_line` |
| `airflow_diagnose_run` | **Diagnostic complet d'une panne en un appel** (voir § 5.1) | `dag_id`, `run_id` |
| `airflow_build_report` | **Rapport normé en 6 sections** (voir § 5.2) | `scope`, `period` |

### Écriture (protégée)

| Outil | À quoi il sert | Paramètres principaux |
|---|---|---|
| `airflow_clear_tasks` | Relancer certaines tâches d'un run (voir § 5.3) | `dag_id`, `run_id`, `task_ids`, `dry_run` (vrai par défaut), `reason` |

Cet outil est **invisible pour l'agent** tant que `read_only` vaut `true`.

### Bac à sable (ne modifie jamais Airflow)

| Outil | À quoi il sert | Paramètres principaux |
|---|---|---|
| `airflow_sandbox_test` | Tester un correctif (diff) dans un conteneur jetable (voir § 6) | `dag_id`, `patch`, `level`, `task_id` |

Cet outil est **invisible pour l'agent** tant que `sandbox_enabled` vaut `false`.

### Skills (méthodes de travail de l'agent)

| Skill | Rôle |
|---|---|
| `airflow-triage` | La méthode de diagnostic : trouver la cause racine, lire les preuves, relancer le minimum |
| `airflow-report` | La présentation du rapport : résumé, puis les 6 sections, sans chiffre inventé |
| `airflow-fix-proposal` | La proposition de correctif : **lecture du code**, cause, diff, **test en bac à sable**, risques, vérification. **Jamais appliquée par l'agent.** |

---

## 5. Comment ça marche

### 5.1 Le diagnostic

```mermaid
flowchart LR
    A[Run en échec] --> B[Tâche racine<br/>première en échec]
    A --> C[Tâches impactées<br/>upstream_failed, skipped]
    B --> D[Bloc d'erreur du log]
    D --> E{Règles d'erreurs}
    E -->|timeout, réseau, 503...| F[transient<br/>une relance suffit]
    E -->|bug, schéma, permission...| G[critical<br/>il faut corriger]
    E -->|aucune règle| H[unknown<br/>revue humaine]
```

- Le plugin distingue **la cause** (la tâche racine) des **conséquences** (les tâches bloquées
  derrière).
- Chaque classification est **justifiée** : quelle règle a reconnu quelle ligne du log.
- Les règles sont dans [`data/error_patterns.yaml`](../data/error_patterns.yaml). Vous pouvez y
  ajouter vos propres erreurs métier (voir `error_patterns_path`, § 7).

### 5.2 Le rapport

Toujours les **mêmes 6 sections, dans le même ordre**, pour pouvoir comparer d'une semaine à
l'autre. **Le rapport est en anglais**, et ses tableaux restent alignés même dans le terminal :

1. **Périmètre et période** : quels DAGs, quelle fenêtre de temps.
2. **Santé globale** : un **statut par DAG** (sain, dégradé, en échec), puis un statut global
   **expliqué** : il nomme les DAGs en cause et la raison. Un seul DAG en difficulté parmi
   plusieurs DAGs sains ne met pas tout en échec. On y trouve aussi le nombre de runs, le taux
   de succès et les incidents.
3. **Performance** : durée de chaque tâche comparée à sa durée habituelle, relances, attente,
   SLA.
4. **Qualité des données** : lue depuis un XCom `data_quality` publié par vos DAGs (voir le
   README). Sans cet XCom, la section affiche **« not instrumented »** : jamais de chiffres
   inventés.
5. **Incidents et actions** : runs en échec et relances effectuées.
6. **Recommandations** : classées par gravité.

La « durée habituelle » (ligne de base) s'apprend toute seule, rapport après rapport.

### 5.3 La relance protégée

```mermaid
flowchart TD
    A[L'agent veut relancer] --> B[Aperçu : dry_run]
    B --> C{Garde-fous}
    C -->|DAG entier, tâches vides,<br/>erreur critique non confirmée| X[Refusé]
    C -->|OK| D{Relance auto autorisée ?}
    D -->|Non, cas par défaut| E[Vous validez dans Hermes]
    D -->|Oui : erreur passagère,<br/>sous la limite, backoff écoulé| F[Relance]
    E -->|Accepté| F
    E -->|Refusé| X
    F --> G[Journal d'audit]
    X --> G
```

Les règles qu'**aucun réglage ne peut désactiver** :

- jamais une tâche déjà réussie, ni le DAG entier : seules la tâche en échec et les tâches
  qu'elle a bloquées sont relancées ;
- toujours un aperçu d'abord ;
- jamais en dehors de l'allowlist ;
- pas de relance d'une erreur grave sans confirmation explicite ;
- une raison obligatoire ;
- **pas de relance si le journal ne peut pas l'enregistrer**.

### 5.4 Le journal d'audit

Chaque tentative est notée : aperçu, refus, relance ou erreur. Le journal enregistre **qui**
(session Hermes), **quoi** (DAG, run, tâches), **quand** et **pourquoi** (la raison donnée). Il
apparaît dans la section 5 du rapport.

---

## 6. Le bac à sable

**But :** une fois le correctif montré, l'agent vous **propose de le tester sur une maquette**
dans un conteneur Docker jetable. Si vous acceptez, il vous rend le résultat avec la preuve,
puis la procédure s'arrête : votre code n'a pas changé, et c'est vous qui décidez de la suite.

```mermaid
flowchart LR
    A[Erreur critique] --> B[Correctif proposé<br/>sous forme de diff]
    B --> C[Copie de vos DAGs<br/>+ correctif]
    C --> D[Conteneur Docker jetable<br/>limité, sans réseau]
    D --> E[Niveau 1 : le DAG se charge ?]
    E --> F[Niveau 2 : structure OK ?]
    F -.->|avec votre accord| G[Niveau 3 : la tâche tourne<br/>sur un échantillon ?]
    E --> H[Résultat + preuve]
    F --> H
    G --> H
```

### Les trois niveaux

| Niveau | Ce qui est vérifié | Coût | Votre accord ? |
|---|---|---|---|
| `import` | Le DAG corrigé se charge sans erreur | quelques secondes | non |
| `structure` | Les tâches et dépendances, comparées à la production | quelques secondes | non |
| `task` | **Une seule tâche** tourne, sur un **petit échantillon** | limité | **oui**, toujours demandé (par défaut) |

Le niveau au-delà duquel votre accord est demandé se règle avec `sandbox_max_level`. La valeur
par défaut est `structure`.

### Pourquoi il ne peut pas lancer une tâche trop lourde

- **Conteneur bridé** : 1 CPU, 2 Go de mémoire sans swap, 256 processus, 5 minutes au maximum.
  Tout est réglable. Au-delà, le conteneur est tué et le résultat indique « mémoire » ou
  « temps » dépassé.
- **Aucun réseau** (`--network none`) et **aucun identifiant** : impossible de joindre la
  production, l'entrepôt de données ou un cluster. Une tâche qui essaierait échoue tout de suite.
- **Système de fichiers en lecture seule**, sauf un `/tmp` limité à 512 Mo.
- **Opérateurs lourds jamais exécutés** : Spark, Kubernetes, Snowflake, BigQuery, Bash, Docker,
  SSH… restent aux niveaux `import` et `structure`. La liste est réglable.
- **Un seul test à la fois**, et au maximum 5 par session Hermes. C'est réglable.
- **Votre code n'est jamais modifié** : le test travaille sur une copie, supprimée juste après.
- **Une correction facile à retrouver** : l'agent indique le **chemin complet** du fichier et
  les **lignes exactes** (avant / après), calculées par le bac à sable.
- **La procédure s'arrête après le test.** Ensuite, c'est vous qui décidez : demander à
  Hermes de modifier le fichier (ses outils de code habituels), ou lancer vous-même la
  commande `git apply` fournie, avec sa commande d'annulation.
- **Chaque test est journalisé** et apparaît dans le rapport (section 5).

### Pour que la tâche travaille sur un échantillon (niveau `task`)

Le conteneur fournit trois variables d'environnement à vos tâches :

| Variable | Valeur | Usage conseillé dans votre DAG |
|---|---|---|
| `AIRFLOW_SANDBOX` | `1` | « je suis en test » : ne rien écrire hors de `/tmp` |
| `AIRFLOW_SANDBOX_SAMPLE_ROWS` | `1000` (réglable) | limiter le volume lu, ex. `LIMIT 1000` ou `df.head(n)` |
| `AIRFLOW_SANDBOX_FIXTURES` | chemin d'un fichier JSON | les XComs d'entrée du run en échec (masqués), à utiliser à la place de `xcom_pull` |

Exemple :
```python
import os

rows = int(os.environ.get("AIRFLOW_SANDBOX_SAMPLE_ROWS", 0)) or None
df = read_table(limit=rows)  # lit tout en production, 1000 lignes en test
```

### Mise en place (une seule fois)

1. **Docker** doit tourner sur la machine de Hermes, et le terminal de Hermes doit être local
   (cas par défaut).
2. **Construire une image** avec Airflow et les dépendances de vos DAGs. Le conteneur de test
   n'a pas de réseau et ne peut donc pas faire de `pip install` :
   ```dockerfile
   FROM apache/airflow:3.1.0          # même version que votre production
   COPY requirements.txt /tmp/requirements.txt
   RUN pip install --no-cache-dir -r /tmp/requirements.txt
   ```
   ```bash
   docker build -t mon-airflow-sandbox:1 .
   ```
3. **Activer** dans `config.yaml` :
   ```yaml
   sandbox_enabled: true
   sandbox_image: mon-airflow-sandbox:1
   sandbox_dags_path: C:/chemin/vers/mes/dags     # copié à chaque test, jamais modifié
   ```

## 7. Les réglages modifiables

Ils se placent dans `~/.hermes/config.yaml`, sous
`plugins.entries.airflow-mlops.settings`. Seuls les identifiants (§ 2) sont des secrets ; tout le
reste se règle ici.

### Connexion à Airflow

| Réglage | Défaut | À changer quand… |
|---|---|---|
| `base_url` | `http://localhost:8080` | toujours : l'adresse de votre Airflow |
| `api_version` | `v2` | vous êtes sur Airflow 2.x → `v1` |
| `auth_mode` | `jwt` | Airflow 2 avec login → `basic` ; jeton fixe → `token` |
| `verify_tls` | `true` | certificat interne non reconnu (déconseillé de le désactiver) |
| `timeout_seconds` | `15` | votre Airflow répond lentement |
| `max_retries` | `2` | réseau instable (lectures uniquement) |
| `page_size` | `100` | rarement |

### Périmètre et sécurité

| Réglage | Défaut | À changer quand… |
|---|---|---|
| `dag_allowlist` | `["*"]` (tous) | vous voulez limiter l'agent à certains DAGs, ex. `["ml_*", "etl_daily"]` |
| `read_only` | `true` | vous voulez autoriser les relances → `false` |

### Taille des réponses (pour ne pas noyer l'agent)

| Réglage | Défaut | À changer quand… |
|---|---|---|
| `log_max_lines` | `200` | vous voulez plus ou moins de log |
| `log_max_chars` | `20000` | idem, en caractères |
| `error_context_lines` | `40` | les tracebacks sont très longs |
| `xcom_max_chars` | `2000` | vos XComs sont volumineux |

### Diagnostic

| Réglage | Défaut | À changer quand… |
|---|---|---|
| `error_patterns_path` | vide (règles livrées) | vous avez votre propre fichier de règles d'erreurs |

### Rapports

| Réglage | Défaut | À changer quand… |
|---|---|---|
| `report_default_period` | `24h` | vous préférez un rapport hebdomadaire → `7d` |
| `data_quality_xcom_key` | `data_quality` | vos tâches qualité utilisent une autre clé XCom |
| `sla_thresholds` | `{}` (aucun) | vous voulez suivre des SLA, ex. `{"etl_daily": 3600, "etl_daily.load": 600}` (en secondes) |

### Relance automatique

Ces réglages n'ont d'effet que si `read_only: false`.

| Réglage | Défaut | À changer quand… |
|---|---|---|
| `auto_remediation_enabled` | `false` | vous acceptez que les erreurs **passagères** soient relancées sans vous demander |
| `auto_retry_max_attempts` | `2` | nombre de relances automatiques par tâche avant de vous demander |
| `auto_retry_backoff_seconds` | `300` | délai minimal entre deux relances automatiques (doublé à chaque fois) |

### Bac à sable

| Réglage | Défaut | À changer quand… |
|---|---|---|
| `sandbox_enabled` | `false` | vous voulez que l'agent teste ses correctifs → `true` |
| `sandbox_image` | vide | toujours, si activé : votre image Airflow + dépendances |
| `sandbox_dags_path` | vide | toujours, si activé : le dossier local de vos DAGs |
| `sandbox_max_level` | `structure` | `import` pour encore moins d'exécution ; `task` pour ne plus être sollicité au niveau 3 (déconseillé) |
| `sandbox_cpus` | `1` | vos tâches ont besoin de plus (maximum 8) |
| `sandbox_memory` | `2g` | de `256m` à `16g` |
| `sandbox_timeout_seconds` | `300` | de 30 à 540 secondes |
| `sandbox_sample_rows` | `1000` | taille de l'échantillon proposé à vos tâches |
| `sandbox_heavy_operators` | Spark, Kubernetes, Databricks, Snowflake, BigQuery, EMR, Bash, Docker, SSH | ajouter vos propres opérateurs lourds |
| `sandbox_max_tests_per_session` | `5` | budget de tests par session Hermes |

---

## 8. Sécurité en 6 points

1. **Secrets masqués** : mots de passe, tokens et chaînes de connexion sont remplacés par
   `[REDACTED]` avant d'être montrés à l'agent.
2. **Allowlist** : un DAG hors liste est refusé avant tout appel à Airflow.
3. **Lecture seule par défaut.**
4. **Validation humaine** pour toute relance, sauf la relance automatique bornée si vous
   l'activez.
5. **Journal** de toutes les actions.
6. **Aucune donnée écrite dans le dossier du plugin** : tout est dans la base SQLite privée du
   plugin, gérée par Hermes.

---

## 9. Que veulent dire les erreurs ?

| Type d'erreur | Signification | Que faire |
|---|---|---|
| `config` | identifiants manquants pour le mode d'authentification choisi | vérifier `auth_mode` et les secrets |
| `auth` / `forbidden` | Airflow refuse l'accès (401 / 403) | vérifier l'utilisateur et ses droits |
| `network` / `timeout` | Airflow injoignable ou lent | vérifier `base_url`, le VPN, `timeout_seconds` |
| `not_found` | DAG, run ou tâche inexistant | vérifier l'identifiant |
| `dag_not_allowed` | DAG hors `dag_allowlist` | l'ajouter à la liste si c'est voulu |
| `guardrail` | une règle de sécurité a refusé la relance | lire le message : il explique quoi faire |
| `sandbox` | le bac à sable a refusé ou n'a pas pu préparer le test (désactivé, budget, opérateur lourd, diff qui ne s'applique pas) | lire le message |
| `invalid_argument` | paramètre mal formé | reformuler la demande |
