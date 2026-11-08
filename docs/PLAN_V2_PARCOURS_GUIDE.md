# Plan v2 — parcours guidé, sécurisé et scalable

Ce document fixe la **ligne directrice de la v2** du plugin `airflow-mlops`. Il complète
[guide_line.md](../guide_line.md) (règles d'origine) et [phase5_sandbox_plan.md](phase5_sandbox_plan.md)
(bac à sable). En cas de doute pendant l'implémentation, c'est ce document qui fait foi.

## 1. Vision

Hermes reste **un agent de code complet**. Le plugin ne lui retire rien : il **ajoute** un parcours
Airflow guidé (audit → diagnostic → correctif → test en bac à sable → application → relance)
et **un seul verrou de sécurité** :

> Aucun fichier de DAG n'est modifié sans un test en bac à sable réussi **sur ce changement exact**,
> puis une **validation humaine**. Partout ailleurs, Hermes fonctionne comme d'habitude.

Objectifs :

- **Guidé** : un questionnaire entre chaque étape, l'option recommandée en premier, `Stop here`
  toujours possible.
- **Scalable** : le même parcours marche avec 1 ou 100 erreurs (groupes, sélection multiple,
  traitement par lot, barre d'avancement).
- **Sûr** : lecture seule par défaut, relances et écritures validées par un humain, tout est journalisé.
- **Déterministe** : c'est le plugin qui calcule les questions et les options, le modèle se contente
  de les afficher (indispensable avec un modèle gratuit ou local).

## 2. Constats qui motivent la v2

| Constat (test réel du 08/11/2026) | Conséquence |
|---|---|
| La sortie de `/airflow-report` n'entre pas dans le contexte du modèle | À « yes go », le modèle a deviné et relancé `fetch` |
| La question de fin de rapport était ouverte (« tell me which DAG(s)… ») | Réponse ambiguë |
| La section 5 listait 20 lignes de tests en bac à sable | Rapport difficile à lire |
| La skill de correctif interdisait toute écriture, et `-t airflow` masquait les outils de fichiers | Les outils de code de Hermes étaient bridés |
| Les emoji dans les réponses font peu professionnel | Style final choisi (option B), appliqué à la fin (section 7) |

## 3. Le parcours en questionnaire

Le plugin calcule chaque question. Depuis une commande (`/airflow-audit`), elle passe par
`ctx.ask_choice`. Pendant la conversation, chaque outil renvoie un champ `next_question`
(question, options, recommandée) que la skill fait afficher avec l'outil `clarify` de Hermes,
**sans changer les options**.

Contraintes Hermes vérifiées dans le code v0.23 : `ask_choice` = un seul choix, options illimitées ;
`clarify` = choix simple ou multiple, **4 options au maximum** (+ « Other » ajouté automatiquement).
Toutes les questions ont donc au plus 4 options ; au-delà, on propose des **groupes**, et « Other »
permet de taper une sélection précise.

| Après… | Question | Options (la 1re est recommandée) |
|---|---|---|
| Le rapport | Which DAG should I diagnose? | `<DAG le plus grave> (raison)` · `<DAG suivant>` · `All (n)` · `Stop here` |
| Le rapport, beaucoup d'erreurs | What should I handle? | `All critical (n)` · `All retries (n)` · `Pick…` · `Stop here` |
| Le diagnostic d'un DAG | Which problem first? | `1.1 …` · `1.2 …` · `All of this DAG` · `Stop here` |
| Le correctif affiché | Test this fix in the sandbox? | `Test it (copy, no network)` · `Show the raw diff` · `Skip the test` · `Stop here` |
| Test réussi | Apply the fix to your file? | `Apply it (your approval required)` · `I'll apply it myself (git commands)` · `Not now` · `Stop here` |
| Test échoué | The sandbox test failed. | `Revise the fix` · `Show the sandbox log` · `Stop here` |
| Correctif appliqué | Retry the failed tasks? | `Preview the retry (dry run)` · `Not now` · `Stop here` |
| Erreur passagère | Retry? | `Preview the retry (dry run)` · `Diagnose deeper` · `Stop here` |
| Un point terminé | What next? | `Continue with <n>` · `Next DAG: <dag> (k left)` · `Stop here` |

**Sélection tapée** (« Other », Telegram, cron) — interprétée par le plugin, jamais par le modèle :
`1.1, 2.3` · `1.1-1.4` · `DAG 2` · `all critical` · `all transient` · `all except 3.2`.

**`Stop here`** enregistre l'avancement et affiche un bilan :

```
Stopped. Done: 1.1 ✅ fixed, tested, applied, retried
Left: 1.2 (critical), 2.1 (transient). Resume any time with /airflow-audit
```

## 4. Barre d'avancement et traitement par lot

Quand plusieurs points sont traités d'affilée, **un seul message mis à jour sur place**
(`ctx.post_message` puis `.edit()`) :

```
[###----] 3/7 OK · testing 1.2 in sandbox...
```

- Relances : regroupées dans **un seul aperçu**, **une seule validation**.
- Correctifs : regroupés **par fichier** — un test en bac à sable par fichier, une validation par fichier.

## 5. Le dossier d'incident

Mémoire du parcours, stockée dans `plugin_db` (nouvelle table). Chaque point a un état :

```
new → diagnosed → fix_proposed → sandbox_passed | sandbox_failed → applied → retried → verified
                                                                                   (ou skipped)
```

- Le hook `pre_llm_call` rappelle au modèle, avant chaque réponse, où en est le dossier
  (court résumé) : un « yes » ou un « 2 » est toujours compris.
- `/airflow-status` affiche le dossier et l'avancement ; `/airflow-audit` propose `Resume (k left)`.
- Le correctif testé y est enregistré : fichier, empreinte avant, empreinte attendue après,
  fichier `.patch`, niveaux réussis. C'est ce qui alimente le verrou.

## 6. Le verrou sur les DAGs

Ne concerne **que** les fichiers du dossier des DAGs.

| Outil Hermes visant le dossier des DAGs | Décision |
|---|---|
| `write_file` / `patch` sans test réussi | Bloqué : « test it in the sandbox first » |
| `write_file` / `patch` identique au changement testé | Validation humaine demandée |
| `write_file` / `patch` différent du changement testé | Bloqué : « test it again » |
| `terminal` / `execute_code` qui écrit dans ce dossier | Validation humaine, avec avertissement (repérage sur le texte de la commande, pas infaillible) |
| Après écriture (`post_tool_call`) | Vérifie que le fichier correspond exactement au changement testé |
| Lecture (`read_file`, `cat`, `grep`, `git diff`), autres dossiers | Libre |

Réglage : `dag_write_guard: sandbox_required` (défaut) | `approval_only` | `off`.
À vérifier à l'étape 3 : le comportement des validations si Hermes tourne en mode « tout valider ».

**Écart assumé avec guide_line.md** (« l'agent propose, il n'applique pas ») : le **plugin** n'écrit
toujours rien lui-même ; c'est **Hermes** qui applique, seulement après un test réussi et une
validation humaine. À noter dans le README.

## 7. Règles d'affichage

- **Les emoji restent dans l'interface de Hermes** (appels d'outils et de skills : `⚡ Airflow_…`,
  `📚 Skill …`) : c'est Hermes qui les affiche.
- **Pour l'instant, les emoji d'origine restent** dans les réponses (✅ 🟠 🔴 ⚠️…). Ils sont
  regroupés dans `display.py` pour pouvoir changer de style à un seul endroit.
- **Finition prévue à la fin du projet** (décision du 08/11/2026) : **option B**, pas d'emoji dans
  les réponses, mais un symbole et un mot **colorés ensemble** dans le terminal : `✓ healthy` /
  `✓ passed` en vert, `✗ failing` / `✗ failed` en rouge, `! degraded` / `! warning` en orange,
  `– not run` / `· info` grisés. Les emoji de Hermes devant les appels d'outils et de skills restent.
  Couleurs seulement là où la surface les affiche (réglage `color_output: auto | always | never`).
- Réponses toutes faites du plugin : **100 % en anglais**. Le modèle répond dans la langue de
  l'utilisateur.
- Tableaux alignés pour le terminal brut.

## 8. Les skills (7)

| Skill | État | Rôle | Moment |
|---|---|---|---|
| `airflow-audit` | nouvelle | Chef d'orchestre : rapport → questions → diagnostic → correctif → test → application → relance, tient le dossier | Pendant la panne, avec l'utilisateur |
| `airflow-report` | adaptée | Rapport en 6 sections + 1re question | Étape 1 du parcours |
| `airflow-triage` | adaptée | Diagnostic, plan numéroté, « Which problem first? » | Étape 2 |
| `airflow-fix-proposal` | adaptée | Correctif, test en bac à sable, « Apply the fix? » | Étapes 3 à 5 |
| `airflow-dag-review` | nouvelle | Revue d'un DAG avant déploiement (all_done, secret en dur, retries, code lourd) + bac à sable | Avant la panne |
| `airflow-watch` | nouvelle | Surveillance planifiée (modèle de tâche Hermes), relance auto des erreurs passagères si autorisé, alertes | Pendant la panne, sans l'utilisateur |
| `airflow-postmortem` | nouvelle | Fiche bilan en anglais : chronologie, cause, preuve du test, validations | Après la panne |

## 9. Fonctions de Hermes utilisées

| Fonction | Usage |
|---|---|
| `ctx.ask_choice` | Questions depuis `/airflow-audit` |
| outil `clarify` | Questions pendant la conversation (options venant de `next_question`) |
| `ctx.inject_message` | Transmettre au modèle le choix fait dans une commande, avec son contexte |
| `ctx.post_message` + `.edit()` | Barre d'avancement |
| hook `pre_llm_call` | Résumé du dossier avant chaque réponse |
| hooks `pre_tool_call` / `post_tool_call` | Verrou sur les DAGs, validations |
| hook `pre_verify` | Après une modification de DAG : rappeler la vérification et la relance ciblée |
| `ctx.register_automation_blueprint` | Modèle de surveillance nocturne prêt à installer |
| `ctx.register_approval_transport` | (option) principe des quatre yeux : validation par un responsable sur Slack |
| `ctx.register_redaction_patterns` | Masquage des secrets Airflow dans tout Hermes |
| `ctx.register_context_reference` | (option) références `@incident:` avec autocomplétion |

Lancement conseillé : `hermes chat -t airflow,file,terminal,clarify`.

## 10. Étapes de réalisation

Arrêt pour validation après chaque étape (tests, ruff, mypy, `hermes plugins doctor . --ci`,
`hermes plugins validate .`, puis essai dans Hermes).

| Étape | Contenu | Validation par l'utilisateur |
|---|---|---|
| **0. Prototypes** (livrée ; l'affichage est reporté à la fin) | Étiquettes `OK`/`FAIL`/`WARN` à la place des emoji dans les réponses ; commande `/airflow-ui-check` qui essaie `ask_choice`, la barre d'avancement, la couleur, `inject_message` et `pre_llm_call` | Ce qui marche dans Hermes Desktop et dans le terminal |
| **1. Questionnaire et dossier** (livrée, v0.8.0) | Moteur de questions, table du dossier, `/airflow-audit`, `/airflow-status`, `pre_llm_call`, section 5 résumée, `next_question`, sélection tapée | Parcours complet sur la démo, `Stop here` puis reprise |
| **2. Barre d'avancement et lots** | Message mis à jour, relances groupées, tests groupés par fichier | `All` sur plusieurs erreurs |
| **3. Verrou sur les DAGs** | `pre_tool_call` / `post_tool_call` sur les outils de fichiers, `dag_write_guard`, question « Apply the fix? » | Écriture non testée bloquée, écriture testée validée |
| **4. Skills** | `airflow-audit`, mise à jour des 3 skills, `airflow-dag-review` | Réponses de l'agent sur la démo |
| **5. Cas métier** | `airflow-watch` (blueprint + alertes), `airflow-postmortem`, `docs/SCENARIO_DEMO.md` | Démo de bout en bout |

## 11. Business story (résumé)

FreshCart, livraison de courses ; trois pipelines (= les DAGs de démo).

| Moment | Skill | Situation | Valeur |
|---|---|---|---|
| Lundi 3 h | `airflow-watch` | Coupure de l'API fournisseur → relance auto ; échec masqué du modèle → alerte | Personne réveillé pour rien ; échec silencieux détecté |
| Lundi 9 h | `airflow-audit` | Rapport → choix → diagnostic → correctif testé → appliqué avec validation → relance | Incident réglé en minutes ; modèle de prévision à nouveau entraîné |
| Mardi | verrou | « Modifie vite le DAG » sans test → bloqué ; le reste du code reste libre | Rien de non testé en production |
| Mercredi | `airflow-dag-review` | Nouveau DAG relu avant la PR : secret en dur, all_done | L'incident de lundi ne se reproduit pas |
| Vendredi | `airflow-postmortem` + `/airflow-report all 7d` | Fiche d'incident et rapport de la semaine | Traçabilité, conformité |

Les durées et gains de l'histoire sont illustratifs ; la démo mesure les vrais temps.

## 12. Règles d'origine toujours en vigueur

- Secrets via `requires_env` + `get_secret`, jamais de lecture de `.env` / `auth.json`.
- Écritures du plugin uniquement dans `plugin_data_dir()` / `plugin_db()`.
- Pas de monkey-patching ; uniquement les API publiques de Hermes.
- Handlers : retournent du JSON, ne lèvent jamais d'exception.
- Jamais de relance du DAG entier ; `dry_run` d'abord ; relance automatique bornée.
- Commits et push faits par l'utilisateur ; pas de CI.
