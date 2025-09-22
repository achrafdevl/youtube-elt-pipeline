# 🧪 Guide de Test - YouTube ELT Pipeline

Ce guide vous explique comment tester le projet YouTube ELT Pipeline pour vérifier qu'il fonctionne correctement.

## 🚀 Tests Rapides

### 1. Test Automatique Complet

```bash
python test_project.py
```

**Résultat attendu :** Tous les tests doivent passer (6/6)

### 2. Test des Modules Individuels

```bash
# Test du client YouTube
python -c "from src.youtube_client import YouTubeClient, iso_duration_to_seconds; print('✅ YouTube Client OK')"

# Test des tâches DB
python -c "from src.db_tasks import load_to_staging, transform_core; print('✅ DB Tasks OK')"

# Test de conversion de durée
python -c "from src.youtube_client import iso_duration_to_seconds; print('✅ Duration:', iso_duration_to_seconds('PT2M30S'))"
```

### 3. Test de Syntaxe Python

```bash
python -m py_compile dags/*.py src/*.py
```

**Résultat attendu :** Aucune erreur de syntaxe

## 🔧 Tests Avancés

### 1. Tests Unitaires (si Airflow installé)

```bash
# Installer Airflow d'abord
pip install apache-airflow==2.7.0

# Puis tester
python -m pytest tests/test_duration.py -v
```

### 2. Test avec Docker (recommandé)

```bash
# Construire l'image
docker build -t youtube-elt-pipeline .

# Tester l'image
docker run --rm youtube-elt-pipeline python test_project.py
```

### 3. Test avec Astro CLI (recommandé)

```bash
# Démarrer l'environnement
astro dev start

# Dans un autre terminal, tester
astro dev run python test_project.py
```

## 📊 Validation des Fonctionnalités

### 1. Client YouTube

- ✅ Initialisation avec clé API
- ✅ Gestion des quotas (10,000 unités/jour)
- ✅ Conversion des durées ISO 8601
- ✅ Gestion d'erreurs et retry logic

### 2. Structure des DAGs

- ✅ `produce_JSON` : Extraction des données
- ✅ `update_db` : Chargement et transformation
- ✅ `data_quality` : Validation de qualité

### 3. Base de Données

- ✅ Schémas staging et core
- ✅ Index et contraintes
- ✅ Gestion des doublons

### 4. Qualité des Données

- ✅ Configuration Soda Core
- ✅ Tests de complétude
- ✅ Validation des formats

## 🐛 Dépannage

### Problèmes Courants

#### 1. Erreurs d'import Airflow

```bash
# Solution : Utiliser Docker ou Astro CLI
docker run --rm youtube-elt-pipeline python test_project.py
```

#### 2. Conflits de dépendances

```bash
# Solution : Environnement virtuel
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

#### 3. Erreurs de base de données

```bash
# Solution : Utiliser les variables d'environnement
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=youtube_dw
export PGUSER=airflow
export PGPASSWORD=airflow
```

## ✅ Checklist de Validation

### Fonctionnalités de Base

- [ ] Imports des modules réussis
- [ ] Client YouTube initialisé
- [ ] Conversion des durées fonctionne
- [ ] Structure des fichiers complète
- [ ] Syntaxe Python valide
- [ ] JSON existant bien formé

### Fonctionnalités Avancées

- [ ] DAGs Airflow chargés (avec Airflow)
- [ ] Tests unitaires passent
- [ ] Pipeline CI/CD fonctionne
- [ ] Docker build réussi
- [ ] Base de données accessible

### Production

- [ ] Variables d'environnement configurées
- [ ] Clé API YouTube valide
- [ ] Base de données PostgreSQL accessible
- [ ] Soda Core configuré
- [ ] Monitoring activé

## 📈 Métriques de Succès

### Tests Automatiques

- **6/6 tests** du script `test_project.py` passent
- **0 erreur** de syntaxe Python
- **Tous les imports** réussis

### Tests Fonctionnels

- **Client YouTube** : Quota géré, erreurs gérées
- **Base de données** : Schémas créés, données chargées
- **Pipeline** : DAGs exécutés, données transformées

### Tests d'Intégration

- **End-to-end** : Extraction → Transformation → Chargement
- **Qualité** : Validation Soda Core
- **Monitoring** : Logs et alertes

## 🎯 Résultats Attendus

### Test Réussi ✅

```
📈 SUMMARY: 6/6 tests passed
🎉 All tests passed! Project is ready.
```

### Test Échoué ❌

```
📈 SUMMARY: X/6 tests passed
⚠️  Some tests failed. Please check the issues above.
```

## 🚀 Prochaines Étapes

1. **Si tous les tests passent** : Le projet est prêt pour la production
2. **Si des tests échouent** : Corriger les problèmes identifiés
3. **Pour la production** : Configurer les variables d'environnement et déployer

## 📞 Support

En cas de problème :

1. Vérifier les logs d'erreur
2. Consulter la documentation
3. Exécuter `python test_project.py` pour diagnostiquer
4. Vérifier les prérequis (Docker, Astro CLI, etc.)
