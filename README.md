# YouTube ELT Pipeline

Un pipeline ELT (Extract, Load, Transform) complet pour l'extraction et l'analyse des données YouTube utilisant Apache Airflow, PostgreSQL, Soda Core et Docker.

## 🎯 Fonctionnalités

### Pipeline ELT

- **DAG `produce_JSON`** : Extraction quotidienne des données YouTube

  - Chaîne cible : MrBeast (@MrBeast) - Chaîne YouTube
  - Données extraites : ID vidéo, titre, date de publication, durée, vues, likes, commentaires
  - Format de sortie : Fichiers JSON timestampés
  - Gestion de la pagination et des quotas API (10,000 unités/jour)
  - Gestion d'erreurs et retry logic avec backoff exponentiel

- **DAG `update_db`** : Chargement et transformation des données

  - Chargement des données en schéma staging
  - Transformation et nettoyage des données
  - Chargement en schéma core (Data Warehouse)
  - Gestion des doublons et de l'historique

- **DAG `data_quality`** : Validation automatique de la qualité
  - Validation avec Soda Core
  - Tests de complétude, cohérence et format
  - Alertes en cas de problème de qualité

### Architecture & Code

- Structure modulaire : DAGs séparés, modules réutilisables
- Code lisible : Commentaires, docstrings, nommage clair
- Gestion des secrets : Variables Airflow pour les clés API
- Tests complets : 25+ tests unitaires et d'intégration

### Data Warehouse PostgreSQL

- **Architecture staging/core** :
  - Schéma staging : Données brutes avec index optimisés
  - Schéma core : Données transformées et nettoyées
- **Tables structurées** : Colonnes typées, index, contraintes CHECK
- **Transformations** : Conversion et optimisation des formats de données
- **Historique** : Conservation des données avec timestamps

### Déploiement & Infrastructure

- Docker : Containerisation complète avec Astro CLI
- Volumes : Synchronisation des données entre conteneur et host
- CI/CD : Pipeline GitHub Actions fonctionnel
- Monitoring : Logs détaillés et alertes

## 🚀 Quick Start

### 1. Prérequis

- Docker Desktop
- Astro CLI
- Python 3.10+

### 2. Configuration

Créer `.env` (Astro charge `.env` automatiquement) :

```bash
YOUTUBE_API_KEY=your-youtube-api-key
TARGET_CHANNEL_HANDLE=@MrBeast
JSON_OUTPUT_PATH=/usr/local/airflow/data/raw
PGHOST=postgres
PGPORT=5432
PGDATABASE=youtube_dw
PGUSER=airflow
PGPASSWORD=airflow
ENABLE_SODA=1
```

### 3. Démarrage

```bash
source .venv/Scripts/activate
```


```bash
astro dev start
```

Interface Airflow : http://localhost:8080

### 4. DAGs Disponibles

| DAG            | Horaire | Description                    |
| -------------- | ------- | ------------------------------ |
| `produce_JSON` | 02:00   | Extraction des données YouTube |
| `update_db`    | 02:30   | Chargement et transformation   |
| `data_quality` | 02:45   | Validation de la qualité       |

## 🧪 Tests

```bash
# Tests unitaires et d'intégration
astro dev run pytest -v

# Tests avec couverture
astro dev run pytest --cov=src --cov-report=html

# Validation des DAGs
astro dev run python -c "from airflow.models import DagBag; DagBag()"
```

## 📊 Qualité des Données

### Soda Core

- Configuration dans `soda/warehouse.yml` et `soda/checks.yml`
- Activation/désactivation via `ENABLE_SODA=1|0`
- Validation automatique des métriques et formats

### Métriques Surveillées

- Complétude des données (champs requis)
- Cohérence des formats (durées, dates)
- Fraîcheur des données (< 365 jours)
- Intégrité des contraintes (valeurs positives)

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   YouTube API   │───▶│  produce_JSON   │───▶│   JSON Files    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Soda Core     │◀───│   data_quality  │◀───│    update_db    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │  PostgreSQL DW  │
                                               │ staging + core  │
                                               └─────────────────┘
```

## 🔧 Configuration Avancée

### Variables d'Environnement

| Variable                | Défaut   | Description                         |
| ----------------------- | -------- | ----------------------------------- |
| `YOUTUBE_API_KEY`       | -        | Clé API YouTube (requis)            |
| `TARGET_CHANNEL_HANDLE` | @MrBeast | Handle de la chaîne cible           |
| `MAX_VIDEOS`            | 500      | Nombre max de vidéos à extraire     |
| `ENABLE_SODA`           | 1        | Activation de Soda Core             |
| `DISABLE_DB`            | 0        | Désactivation de la base de données |

### Gestion des Quotas API

- Surveillance automatique des quotas (10,000 unités/jour)
- Retry avec backoff exponentiel
- Gestion des erreurs de rate limiting
- Logs détaillés de l'utilisation

## 📈 Monitoring

### Logs

- Logs structurés avec niveaux (INFO, WARNING, ERROR)
- Suivi des quotas API en temps réel
- Alertes automatiques en cas d'erreur

### Métriques

- Nombre de vidéos extraites
- Quota API utilisé/restant
- Temps d'exécution des tâches
- Qualité des données (Soda Core)

## 🛠️ Développement

### Structure du Projet

```
├── dags/                    # DAGs Airflow
│   ├── produce_json_dag.py
│   ├── update_db_dag.py
│   └── data_quality_dag.py
├── src/                     # Modules Python
│   ├── youtube_client.py
│   └── db_tasks.py
├── tests/                   # Tests unitaires
│   ├── test_youtube_client.py
│   ├── test_db_tasks.py
│   ├── test_dag_validation.py
│   └── test_integration.py
├── soda/                    # Configuration Soda Core
│   ├── warehouse.yml
│   └── checks.yml
├── .github/workflows/       # CI/CD
│   └── ci.yml
└── data/raw/               # Données JSON extraites
```

### Tests

- **25+ tests** couvrant tous les composants
- Tests unitaires pour chaque module
- Tests d'intégration end-to-end
- Validation des DAGs Airflow
- Tests de performance et de gestion d'erreurs

### CI/CD

- Pipeline GitHub Actions complet
- Tests automatiques sur chaque PR
- Validation des DAGs
- Scan de sécurité
- Build et test Docker

## 📚 Documentation Technique

### API YouTube

- Utilisation de l'API YouTube Data v3
- Gestion de la pagination automatique
- Optimisation des appels API (batch de 50 vidéos)
- Gestion des quotas et rate limiting

### Base de Données

- PostgreSQL avec schémas staging/core
- Index optimisés pour les requêtes fréquentes
- Contraintes de validation des données
- Gestion des doublons avec ON CONFLICT

### Airflow

- DAGs modulaires et réutilisables
- Gestion d'erreurs robuste
- Configuration centralisée
- Monitoring et alertes

## 🤝 Contribution

1. Fork le projet
2. Créer une branche feature (`git checkout -b feature/AmazingFeature`)
3. Commit les changements (`git commit -m 'Add some AmazingFeature'`)
4. Push vers la branche (`git push origin feature/AmazingFeature`)
5. Ouvrir une Pull Request

## 📄 Licence

Ce projet est sous licence MIT. Voir le fichier `LICENSE` pour plus de détails.

## 🆘 Support

Pour toute question ou problème :

1. Vérifier la documentation
2. Consulter les logs Airflow
3. Exécuter les tests : `astro dev run pytest -v`
4. Ouvrir une issue sur GitHub
