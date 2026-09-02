"""
Script de Sincronização: Airflow ↔ Databricks Asset Bundles

Lê databricks.yml e gera/valida o DAG do Airflow para garantir consistência.
Agora suporta workflow pai com dependências complexas.
"""
import yaml
import os
from pathlib import Path
from typing import Dict, List, Any


class AirflowDAGGenerator:
    """Gera DAG do Airflow a partir de databricks.yml com suporte a dependências"""
    
    def __init__(self, databricks_yml_path: str = "databricks.yml"):
        self.databricks_yml_path = databricks_yml_path
        self.bundle_config = self._load_databricks_yml()
        
    def _load_databricks_yml(self) -> Dict:
        """Carrega e valida o arquivo databricks.yml"""
        with open(self.databricks_yml_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        if not config or 'resources' not in config:
            raise ValueError("databricks.yml inválido ou sem recursos")
        
        return config
    
    def _extract_jobs(self) -> List[Dict]:
        """Extrai jobs do databricks.yml (incluindo workflow pai)"""
        jobs = []
        resources = self.bundle_config.get('resources', {})
        
        # Extrair workflow pai com dependências
        if 'pipeline_completo' in resources.get('jobs', {}):
            workflow = resources['jobs']['pipeline_completo']
            jobs.append({
                'id': 'pipeline_completo',
                'name': workflow.get('name', 'Pipeline Completo'),
                'description': workflow.get('description', ''),
                'tasks': workflow.get('tasks', []),
                'schedule': workflow.get('schedule', {}),
                'tags': workflow.get('tags', {}),
                'is_master_workflow': True  # Marcador especial
            })
        
        # Extrair jobs individuais (sem schedule)
        if 'jobs' in resources:
            for job_id, job_config in resources['jobs'].items():
                if job_id == 'pipeline_completo':
                    continue  # Pular workflow pai
                    
                jobs.append({
                    'id': job_id,
                    'name': job_config.get('name', job_id),
                    'description': job_config.get('description', ''),
                    'tasks': job_config.get('tasks', []),
                    'schedule': job_config.get('schedule', {}),
                    'tags': job_config.get('tags', {}),
                    'is_master_workflow': False
                })
        
        return jobs
    
    def _generate_dag_code(self, jobs: List[Dict]) -> str:
        """Gera código completo do DAG com dependências complexas"""
        
        # Verificar se existe workflow pai
        master_workflow = next((j for j in jobs if j.get('is_master_workflow')), None)
        
        if master_workflow:
            return self._generate_dag_with_dependencies(master_workflow)
        else:
            return self._generate_dag_simple(jobs)
    
    def _generate_dag_with_dependencies(self, workflow: Dict) -> str:
        """Gera DAG com dependências complexas baseado no workflow pai"""
        
        # Header do DAG
        dag_header = '''"""
DAG: Pipeline Corretora Santander (Sincronizado com Databricks Asset Bundles)
Gerado automaticamente via scripts/sync_airflow_from_databricks.py

 NÃO EDITE MANUALMENTE - Alterações devem ser feitas em databricks.yml
Este DAG reflete as dependências do workflow pai pipeline_completo
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.task_group import TaskGroup

# Configurações do Databricks (lidas de variáveis de ambiente)
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "0401-150803-wefgy1hc")
REPO_PATH = os.getenv("DATABRICKS_REPO_PATH", "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")
ENVIRONMENT = os.getenv("ENVIRONMENT", "hk")

default_args = {
    "owner": "santander",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def databricks_task(task_id: str, job_path: str) -> DatabricksSubmitRunOperator:
    """Cria task do Airflow para executar job no Databricks"""
    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id="databricks_default",
        json={
            "existing_cluster_id": CLUSTER_ID,
            "spark_python_task": {
                "python_file": f"{REPO_PATH}/{job_path}",
            },
        },
    )


with DAG(
    dag_id="pipeline_corretora_santander",
    default_args=default_args,
    description=f"Pipeline de dados financeiros — Corretora Santander (Ambiente: {ENVIRONMENT})",
    schedule_interval="0 6 * * *",  # 06:00 Brasília
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["santander", "databricks", "financeiro", "synced", ENVIRONMENT],
) as dag:
'''
        
        # Gerar tasks baseado no workflow pai
        tasks_code = self._generate_tasks_from_workflow(workflow)
        
        # Dependências (geradas a partir do workflow)
        dependencies_code = self._generate_dependencies_from_workflow(workflow)
        
        # Footer
        dag_footer = '''
# Este DAG foi gerado automaticamente a partir de databricks.yml
# Reflete as dependências do workflow pai pipeline_completo
# Para regerar: python scripts/sync_airflow_from_databricks.py
# Para modificar: Edite databricks.yml e rode o script novamente
'''
        
        return dag_header + tasks_code + dependencies_code + dag_footer
    
    def _generate_dag_simple(self, jobs: List[Dict]) -> str:
        """Gera DAG simples (sem workflow pai)"""
        
        # Header do DAG
        dag_header = '''"""
DAG: Pipeline Corretora Santander (Sincronizado com Databricks Asset Bundles)
Gerado automaticamente via scripts/sync_airflow_from_databricks.py

 NÃO EDITE MANUALMENTE - Alterações devem ser feitas em databricks.yml
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.task_group import TaskGroup

# Configurações do Databricks (lidas de variáveis de ambiente)
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "0401-150803-wefgy1hc")
REPO_PATH = os.getenv("DATABRICKS_REPO_PATH", "/Workspace/Users/diego.silva0001@gmail.com/case-santander-data-master")
ENVIRONMENT = os.getenv("ENVIRONMENT", "hk")

default_args = {
    "owner": "santander",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def databricks_task(task_id: str, job_path: str) -> DatabricksSubmitRunOperator:
    """Cria task do Airflow para executar job no Databricks"""
    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id="databricks_default",
        json={
            "existing_cluster_id": CLUSTER_ID,
            "spark_python_task": {
                "python_file": f"{REPO_PATH}/{job_path}",
            },
        },
    )


with DAG(
    dag_id="pipeline_corretora_santander",
    default_args=default_args,
    description=f"Pipeline de dados financeiros — Corretora Santander (Ambiente: {ENVIRONMENT})",
    schedule_interval="0 6 * * *",  # 06:00 Brasília
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["santander", "databricks", "financeiro", "synced", ENVIRONMENT],
) as dag:
'''
        
        # Gerar tasks simples
        tasks_code = "    #  Tasks Sincronizadas do Databricks Asset Bundles \n"
        
        for job in jobs:
            tasks_code += self._generate_databricks_task(job, 0)
        
        # Dependências (simples - todas em paralelo)
        dependencies_code = '''
    #  Dependências 
    # Todas as tasks executam em paralelo por padrão
    # Para adicionar dependências específicas, use workflow pai no databricks.yml
    
    pass  # Tasks definidas acima
'''
        
        # Footer
        dag_footer = '''
# Este DAG foi gerado automaticamente a partir de databricks.yml
# Para regerar: python scripts/sync_airflow_from_databricks.py
# Para modificar: Edite databricks.yml e rode o script novamente
'''
        
        return dag_header + tasks_code + dependencies_code + dag_footer
    
    def _generate_tasks_from_workflow(self, workflow: Dict) -> str:
        """Gera código de tasks baseado no workflow pai"""
        tasks = workflow.get('tasks', [])
        code = ""
        
        for task in tasks:
            task_key = task.get('task_key', '')
            description = task.get('description', '')
            
            # Extrair informações da task
            wheel_task = task.get('python_wheel_task', {})
            entry_point = wheel_task.get('entry_point', '')
            
            # job_path vem do proprio entry_point, nao de um mapeamento
            # separado. O mapeamento hardcoded (_map_task_to_job_path) tinha
            # duas falhas: ignorava o valor real de entry_point (uma task
            # cujo entry_point mudasse no databricks.yml continuava apontando
            # para o job antigo) e nao conhecia task_keys novas -- uma task
            # adicionada ao bundle desaparecia do DAG em silencio, sem erro.
            job_path = f"jobs/{entry_point}.py" if entry_point else ""
            
            if job_path:
                code += f'''
    # {description}
    {task_key} = databricks_task(
        task_id="{task_key}",
        job_path="{job_path}"
    )
'''
        
        return code
    
    
    def _generate_dependencies_from_workflow(self, workflow: Dict) -> str:
        """Gera código de dependências baseado no workflow pai"""
        tasks = workflow.get('tasks', [])
        code = "\n    #  Dependências (Sincronizadas do databricks.yml) \n"
        
        # Criar mapeamento de dependências
        task_deps = {}
        for task in tasks:
            task_key = task.get('task_key', '')
            depends_on = task.get('depends_on', [])
            if depends_on:
                task_deps[task_key] = [d.get('task_key') for d in depends_on]
        
        # Gerar código de dependências
        for task_key, dependencies in task_deps.items():
            dep_list = ", ".join(dependencies)
            code += f"    [{dep_list}] >> {task_key}\n"
        
        if not task_deps:
            code += "    pass  # Sem dependências configuradas\n"
        
        return code
    
    def _generate_databricks_task(self, job: Dict, task_index: int) -> str:
        """Gera código Python para uma task do Airflow (legado)"""
        job_id = job['id']
        job_name = job['name']
        
        # Extrair configuração da task
        if not job['tasks']:
            return ""
        
        task_config = job['tasks'][0]  # Assume primeira task
        task_key = task_config.get('task_key', job_id)
        
        # Extrair python_wheel_task
        wheel_task = task_config.get('python_wheel_task', {})
        entry_point = wheel_task.get('entry_point', '')
        package_name = wheel_task.get('package_name', 'case_santander')
        
        # Extrair cluster_id
        cluster_id = task_config.get('existing_cluster_id', '${var.cluster_id}')
        
        # Extrair timeout e retries
        timeout = task_config.get('timeout_seconds', 3600)
        max_retries = task_config.get('max_retries', 2)
        
        # Gerar código da task
        task_code = f"""
    {job_id} = databricks_task(
        task_id="{job_id}",
        job_path="jobs/{entry_point}.py"
    )
"""
        return task_code
    
    def generate_dag(self, output_path: str = "dags/dag_pipeline_santander.py") -> str:
        """Gera o arquivo do DAG"""
        jobs = self._extract_jobs()
        
        if not jobs:
            print("[WARN] Nenhum job encontrado no databricks.yml")
            return ""
        
        dag_code = self._generate_dag_code(jobs)
        
        # Escrever arquivo com UTF-8 encoding
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(dag_code)
        
        print(f"[SUCCESS] DAG gerado: {output_path}")
        print(f"[SUCCESS] {len(jobs)} jobs sincronizados")
        
        return dag_code
    
    def validate_consistency(self, dag_path: str = "dags/dag_pipeline_santander.py") -> bool:
        """Valida se o DAG atual está consistente com databricks.yml"""
        if not os.path.exists(dag_path):
            print(f"[ERROR] DAG não encontrado: {dag_path}")
            return False
        
        jobs = self._extract_jobs()
        
        # Ler DAG atual
        with open(dag_path, 'r', encoding='utf-8') as f:
            current_dag = f.read()
        
        # Verificar se contém marca de sincronização
        sync_marker = "Gerado automaticamente via scripts/sync_airflow_from_databricks.py"
        
        if sync_marker not in current_dag:
            print("[WARN] DAG não foi gerado pelo script de sincronização")
            print("[WARN] Consistência não pode ser validada automaticamente")
            return False
        
        # Verificar se usa workflow pai
        master_workflow = next((j for j in jobs if j.get('is_master_workflow')), None)
        
        if master_workflow:
            # Validar se DAG contém dependências
            has_deps = ">>" in current_dag
            if not has_deps:
                print(f"[ERROR] DAG não contém dependências mas databricks.yml tem workflow pai")
                return False
        
        # Gerar DAG esperado
        expected_dag = self._generate_dag_code(jobs)
        
        # Comparar (simplificado - verifica apenas número de tasks)
        current_tasks = current_dag.count('databricks_task(')
        expected_tasks = expected_dag.count('databricks_task(')
        
        if current_tasks != expected_tasks:
            print(f"[ERROR] Inconsistência detectada:")
            print(f"   Tasks no DAG atual: {current_tasks}")
            print(f"   Jobs no databricks.yml: {expected_tasks}")
            return False
        
        print(f"[SUCCESS] DAG consistente com databricks.yml ({expected_tasks} tasks)")
        return True


def main():
    """Função principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Sincroniza Airflow DAG com Databricks Asset Bundles")
    parser.add_argument(
        "--generate",
        action="store_true",
        help="Gera o DAG do Airflow a partir de databricks.yml"
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Valida consistência entre DAG atual e databricks.yml"
    )
    parser.add_argument(
        "--databricks-yml",
        default="databricks.yml",
        help="Caminho para o arquivo databricks.yml"
    )
    parser.add_argument(
        "--output",
        default="dags/dag_pipeline_santander.py",
        help="Caminho de saída para o DAG gerado"
    )
    
    args = parser.parse_args()
    
    if not args.generate and not args.validate:
        # Padrão: validar e gerar se necessário
        args.validate = True
        args.generate = True
    
    generator = AirflowDAGGenerator(args.databricks_yml)
    
    if args.validate:
        print("[INFO] Validando consistência...")
        is_consistent = generator.validate_consistency(args.output)
        
        if not is_consistent:
            print("[WARN] DAG desincronizado - regerando...")
            args.generate = True
    
    if args.generate:
        print("[INFO] Gerando DAG...")
        generator.generate_dag(args.output)
        print("[SUCCESS] Sincronização concluída")


if __name__ == "__main__":
    main()
