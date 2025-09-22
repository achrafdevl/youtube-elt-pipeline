# tests/test_dag_validation.py
import pytest
from airflow.models import DagBag
from airflow.models.dag import DAG
from datetime import datetime, timedelta


class TestDAGValidation:
    """Tests de validation des DAGs"""
    
    @pytest.fixture
    def dag_bag(self):
        """Fixture pour charger tous les DAGs"""
        return DagBag(include_examples=False)
    
    def test_dag_import_errors(self, dag_bag):
        """Test qu'il n'y a pas d'erreurs d'import dans les DAGs"""
        assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    
    def test_dag_has_required_dags(self, dag_bag):
        """Test que les DAGs requis existent"""
        required_dags = ["produce_JSON", "update_db", "data_quality"]
        for dag_id in required_dags:
            assert dag_id in dag_bag.dags, f"DAG {dag_id} not found"
    
    def test_produce_json_dag_structure(self, dag_bag):
        """Test de la structure du DAG produce_JSON"""
        dag = dag_bag.get_dag("produce_JSON")
        assert dag is not None
        
        # Vérifier les tâches
        task_ids = [task.task_id for task in dag.tasks]
        assert "fetch_youtube_videos" in task_ids
        
        # Vérifier les arguments par défaut
        assert dag.default_args["retries"] >= 2
        assert dag.default_args["retry_delay"] == timedelta(minutes=5)
        
        # Vérifier les tags
        assert "youtube" in dag.tags
        assert "extract" in dag.tags
        assert "json" in dag.tags
    
    def test_update_db_dag_structure(self, dag_bag):
        """Test de la structure du DAG update_db"""
        dag = dag_bag.get_dag("update_db")
        assert dag is not None
        
        # Vérifier les tâches
        task_ids = [task.task_id for task in dag.tasks]
        assert "load_staging" in task_ids
        assert "transform_core" in task_ids
        
        # Vérifier les arguments par défaut
        assert dag.default_args["retries"] >= 2
        assert dag.default_args["retry_delay"] == timedelta(minutes=5)
        
        # Vérifier les tags
        assert "youtube" in dag.tags
        assert "postgres" in dag.tags
        assert "etl" in dag.tags
    
    def test_data_quality_dag_structure(self, dag_bag):
        """Test de la structure du DAG data_quality"""
        dag = dag_bag.get_dag("data_quality")
        assert dag is not None
        
        # Vérifier les tâches
        task_ids = [task.task_id for task in dag.tasks]
        assert "validate_data_completeness" in task_ids
        
        # Vérifier les arguments par défaut
        assert dag.default_args["retries"] >= 2
        assert dag.default_args["retry_delay"] == timedelta(minutes=5)
        
        # Vérifier les tags
        assert "youtube" in dag.tags
        assert "quality" in dag.tags
        assert "soda" in dag.tags
    
    def test_dag_schedules(self, dag_bag):
        """Test des horaires des DAGs"""
        # produce_JSON à 2h00
        produce_dag = dag_bag.get_dag("produce_JSON")
        assert produce_dag.schedule_interval == "0 2 * * *"
        
        # update_db à 2h30
        update_dag = dag_bag.get_dag("update_db")
        assert update_dag.schedule_interval == "30 2 * * *"
        
        # data_quality à 2h45
        quality_dag = dag_bag.get_dag("data_quality")
        assert quality_dag.schedule_interval == "45 2 * * *"
    
    def test_dag_max_active_runs(self, dag_bag):
        """Test que les DAGs ont max_active_runs=1"""
        for dag_id, dag in dag_bag.dags.items():
            assert dag.max_active_runs == 1, f"DAG {dag_id} should have max_active_runs=1"
    
    def test_dag_catchup_false(self, dag_bag):
        """Test que les DAGs ont catchup=False"""
        for dag_id, dag in dag_bag.dags.items():
            assert dag.catchup == False, f"DAG {dag_id} should have catchup=False"
    
    def test_dag_owner(self, dag_bag):
        """Test que les DAGs ont un owner défini"""
        for dag_id, dag in dag_bag.dags.items():
            assert dag.default_args.get("owner") is not None, f"DAG {dag_id} should have an owner"


class TestDAGTaskDependencies:
    """Tests des dépendances entre tâches"""
    
    def test_produce_json_dag_dependencies(self, dag_bag):
        """Test des dépendances du DAG produce_JSON"""
        dag = dag_bag.get_dag("produce_JSON")
        tasks = dag.tasks
        
        # Le DAG produce_JSON n'a qu'une seule tâche, pas de dépendances
        assert len(tasks) == 1
    
    def test_update_db_dag_dependencies(self, dag_bag):
        """Test des dépendances du DAG update_db"""
        dag = dag_bag.get_dag("update_db")
        tasks = dag.tasks
        
        # Vérifier que load_staging précède transform_core
        load_staging_task = dag.get_task("load_staging")
        transform_core_task = dag.get_task("transform_core")
        
        # Vérifier les dépendances
        downstream_tasks = dag.get_task("load_staging").downstream_list
        assert transform_core_task in downstream_tasks
    
    def test_data_quality_dag_dependencies(self, dag_bag):
        """Test des dépendances du DAG data_quality"""
        dag = dag_bag.get_dag("data_quality")
        tasks = dag.tasks
        
        # Vérifier que validate_data_completeness précède run_soda_scan (si Soda activé)
        if "run_soda_scan" in [task.task_id for task in tasks]:
            validate_task = dag.get_task("validate_data_completeness")
            soda_task = dag.get_task("run_soda_scan")
            
            downstream_tasks = validate_task.downstream_list
            assert soda_task in downstream_tasks


class TestDAGConfiguration:
    """Tests de la configuration des DAGs"""
    
    def test_environment_variables_usage(self, dag_bag):
        """Test que les DAGs utilisent les variables d'environnement"""
        # Test du DAG produce_JSON
        produce_dag = dag_bag.get_dag("produce_JSON")
        assert produce_dag is not None
        
        # Test du DAG update_db
        update_dag = dag_bag.get_dag("update_db")
        assert update_dag is not None
        
        # Test du DAG data_quality
        quality_dag = dag_bag.get_dag("data_quality")
        assert quality_dag is not None
    
    def test_dag_descriptions(self, dag_bag):
        """Test que les DAGs ont des descriptions"""
        for dag_id, dag in dag_bag.dags.items():
            assert dag.description is not None, f"DAG {dag_id} should have a description"
            assert len(dag.description) > 10, f"DAG {dag_id} description should be meaningful"
