# Architecture du plugin `airflow-mlops`

Plugin Hermes Agent autonome (Hermes ≥ 0.23) qui parle à l'API REST d'Apache Airflow 2.x
(`/api/v1`) et 3.x (`/api/v2`). Ce document décrit la structure et les décisions qui ne se
lisent pas dans le code. Le fonctionnement pour l'utilisateur est dans le
[README](README.md) et le [guide](docs/GUIDE_UTILISATEUR.md).

## Flux

```
Modèle ─┬─ outils airflow_*  ──▶ tools.py / audit_tools.py ──▶ modules métier ──▶ airflow_client.py ──▶ Airflow
        │                                                                     └─▶ storage.py (plugin_db)
        ├─ hooks Hermes ─────▶ pre_tool_call  : guardrails.py (relances, bac à sable) + dag_guard.py (verrou DAGs)
        │                     post_tool_call : dag_guard.py (journal de l'écriture)
        │                     pre_llm_call   : audit_tools.AuditTools.context (rappel du dossier d'audit)
Utilisateur ─ /airflow-report, /airflow-audit, /airflow-status ──▶ tools.py / audit_tools.py
```

Le plugin ne touche ni au flux du modèle ni au rendu : la séparation raisonnement / texte et
l'affichage sont faits par Hermes. Le plugin agit sur ce qu'il renvoie au modèle (JSON des
outils, consignes, rappel `pre_llm_call`) et sur le texte de ses commandes.

## Modules

| Couche | Module | Rôle |
|---|---|---|
| Entrée | `__init__.py` | `register(ctx)` : outils, hooks, commandes, skills, blueprint. Aucun appel réseau. |
| | `schemas.py` | Ce que lit le modèle : noms, descriptions, paramètres des 18 outils. |
| | `tools.py` | Handlers de lecture, de relance et du bac à sable : validation des arguments, JSON en retour, jamais d'exception (`safe_handler`). |
| | `audit_tools.py` | Outils `airflow_audit_*`, commandes `/airflow-audit` et `/airflow-status`, rappel `pre_llm_call`, observateurs du bac à sable et des relances. |
| Métier | `diagnosis.py` | Racine / échecs secondaires / tâches impactées, classification `transient` / `critical` / `unknown` par règles YAML (`data/error_patterns.yaml`). |
| | `reporting.py` | Rapport normé en 6 sections, JSON + Markdown, lignes de base. |
| | `guardrails.py` | Garde-fous codés en dur de `airflow_clear_tasks`, politique de relance automatique, hook d'approbation. |
| | `sandbox.py` + `sandbox_runner/check_dag.py` | Application du diff sur une copie, `docker run` borné, lecture du résultat, détection des correctifs qui masquent l'erreur. |
| | `dag_guard.py` | Verrou des fichiers de DAG : seul le changement testé s'écrit, après validation humaine. |
| | `dag_review.py`, `dag_source.py`, `watch.py` | Revue avant déploiement, lecture du code d'un DAG, surveillance planifiée. |
| | `workflow.py` | Modèle de l'audit (items numérotés), persistance, sélection tapée, questions entre les étapes. N'appelle jamais Airflow. |
| Socle | `airflow_client.py` | Client httpx : auth basic / token / JWT, relances des GET, pagination, normalisation v1/v2, erreurs `AirflowError`. |
| | `config.py`, `runtime.py` | Réglages typés (valeurs par défaut ici, reflétées dans `plugin.yaml`), client HTTP unique (`SingletonSlot`). |
| | `storage.py` | Schéma SQLite versionné (`PRAGMA user_version`) : cache de diagnostic, lignes de base, journal, compteurs de relance, audits. |
| | `redaction.py`, `textutils.py`, `views.py`, `display.py` | Masquage des secrets (texte libre et code source), troncature, XComs et graphe de tâches, icônes. |

## Décisions

- **Une question se pose en texte, à la fin de la réponse.** Jamais par l'outil de menu
  `clarify`, et jamais d'appel d'outil après le texte. Un modèle qui écrit puis appelle un outil
  envoie souvent l'appel seul : la réponse restait dans son raisonnement (session réelle du
  10/11/2026, 14 tours sans texte visible). Dans un tour : outils d'abord, texte ensuite.
- **Le rapport est une réponse complète** : `airflow_build_report` ne renvoie pas de question.
- **Les commandes slash répondent tout de suite**, sans menu (`ask_choice`), sans appel au
  modèle, sans message à part (`post_message`). Le Desktop abandonne une commande au bout de
  30 s puis la relance, et Hermes renvoie la surface `local` pour le Desktop comme pour un
  terminal. Aucune couleur ANSI pour la même raison.
- **Un seul moteur d'audit**, piloté par le modèle avec les outils `airflow_audit_*` ; le plugin
  calcule chaque question. L'ancien moteur qui enchaînait les menus dans la commande a été
  supprimé.
- **Un audit ne modifie jamais le code** : rapport → diagnostic → test (relance réelle d'une
  erreur passagère, ou correctif testé dans le bac à sable), puis archivage. Le correctif testé
  s'applique ensuite, sur demande, sous le verrou.
- **Pas de `ctx.inject_message`** : un tour lancé par le plugin s'exécutait sans s'afficher et
  pouvait rester bloqué.
- **Rappel `pre_llm_call` limité à la conversation de l'audit.**
- **Bac à sable** : niveau `structure` (il comprend l'import) avant toute écriture ; un patch
  sans effet est refusé ; un correctif qui masque l'erreur est signalé (`masking_warnings`).
  Docker est lancé par `ctx.dispatch_tool("terminal")` : les contrôles du terminal de Hermes
  s'appliquent, pas les hooks ; la validation humaine porte sur `airflow_sandbox_test`.
- **Sécurité fermée par défaut** : lecture seule, allowlist mal formée = rien d'autorisé, seul
  un `dry_run` réellement vrai passe pour un aperçu, relance automatique désactivée et bornée.
- **Emoji dans les réponses** (choix de l'utilisateur), centralisés dans `display.py`.

## Contraintes Hermes respectées

API publiques seulement (pas de modification du cœur), handlers `handler(args, **kwargs) -> str`
qui ne lèvent jamais, hooks avec `**kwargs`, outils et hooks déclarés dans `plugin.yaml`, secrets
par `requires_env` et `get_secret`, état dans `plugin_db` / `plugin_data_dir`, dépendances
bornées dans `pyproject.toml`. Contrôles : `hermes plugins validate . --install-deps` et
`hermes plugins doctor . --ci`.

## Limites connues

- Le verrou sur les commandes shell reste heuristique (analyse du texte de la commande).
- Un `try_number` absent du cache d'un diagnostic ancien est accepté tel quel.
- Les tâches mappées partagent leur classification en cache (`task_id` sans `map_index`) ;
  les garde-fous de relance, eux, regardent chaque instance.
- Les lignes de base bougent au plus une fois par heure et par tâche.
- Le dossier d'audit est réécrit en entier à chaque étape : deux étapes simultanées sur le même
  audit (rare : un seul agent le pilote) gardent la dernière.
- Les erreurs des outils sont un objet (`type`, `message`, `status`) : Hermes n'en affiche
  qu'un extrait sur la ligne de l'outil ; le modèle reçoit tout.
- Verrou d'audit : la conversation de l'audit reste verrouillée jusqu'à la fin de l'audit ;
  les autres le sont tant qu'il est actif (30 min).
