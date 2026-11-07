# Démo locale — Airflow jetable pour essayer le plugin

**Ce dossier ne fait pas partie du plugin.** Hermes ne le charge pas : le plugin n'en importe
rien. Il sert uniquement à lancer un Airflow 3 local pour tester `airflow-mlops` à la main.

## Contenu

- `docker-compose.yml` : un conteneur Airflow 3 en mode `standalone` (SQLite,
  SimpleAuthManager). Ne pas utiliser en production.
- `passwords.json` : identifiants de démo `admin` / `admin`.
- `dags/mlops_demo.py` : trois DAGs de démonstration.
  - `demo_train_ok` : cas sain, qui publie l'XCom qualité `data_quality`.
  - `demo_train_critical` : erreur critique (`KeyError`).
  - `demo_fetch_transient` : erreur transitoire (`ConnectionResetError`).

## Utilisation

```bash
docker compose -f demo-local-airflow/docker-compose.yml up
```

Configure ensuite le plugin :

- `base_url: http://localhost:8080`
- `api_version: v2`
- `auth_mode: jwt`
- secrets `AIRFLOW_USERNAME=admin` et `AIRFLOW_PASSWORD=admin`

Déclenche les DAGs dans l'UI, puis lance `hermes chat`.
