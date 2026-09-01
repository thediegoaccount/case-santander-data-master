"""
Dynamic Pipeline Framework

Pipeline flexível com auto-discovery de jobs.
"""

from typing import Dict, List, Callable, Any
from dataclasses import dataclass
from pathlib import Path
import importlib.util
import sys
from src.config.logging import info, error, warning


@dataclass
class JobDefinition:
    """Definição de um job"""
    name: str
    function: Callable
    dependencies: List[str] = None
    priority: int = 0
    enabled: bool = True
    parameters: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
        if self.parameters is None:
            self.parameters = {}


class DynamicPipeline:
    """Pipeline dinâmico com auto-discovery de jobs"""
    
    def __init__(self, pipeline_name: str, auto_discover: bool = True):
        self.pipeline_name = pipeline_name
        self.jobs: Dict[str, JobDefinition] = {}
        
        if auto_discover:
            self.auto_discover_jobs()
    
    def auto_discover_jobs(self, jobs_dir: str = "jobs"):
        """
        Auto-descobre jobs no diretório jobs/
        
        Args:
            jobs_dir: Diretório onde procurar jobs
        """
        jobs_path = Path(jobs_dir)
        
        if not jobs_path.exists():
            warning(self.pipeline_name, f"Diretório {jobs_dir} não encontrado")
            return
        
        # Encontrar todos os arquivos job_*.py
        job_files = list(jobs_path.glob("job_*.py"))
        
        info(self.pipeline_name, f"Auto-descobrindo {len(job_files)} jobs em {jobs_dir}")
        
        for job_file in job_files:
            job_name = job_file.stem  # job_clientes_ordens
            
            # Registrar job (assum função main())
            try:
                self.register_job(
                    name=job_name,
                    function=lambda: self._execute_job_file(job_file),
                    dependencies=[],  # Dependências serão configuradas manualmente
                    enabled=True
                )
            except Exception as e:
                error(self.pipeline_name, f"Erro ao registrar job {job_name}: {str(e)}")
    
    def _execute_job_file(self, job_file: Path):
        """
        Executa um job de um arquivo Python
        
        Args:
            job_file: Caminho do arquivo do job
        """
        # Importar módulo dinamicamente
        spec = importlib.util.spec_from_file_location(job_file.stem, job_file)
        module = importlib.util.module_from_spec(spec)
        sys.modules[job_file.stem] = module
        spec.loader.exec_module(module)
        
        # Executar função main()
        if hasattr(module, 'main'):
            module.main()
        else:
            error(self.pipeline_name, f"Job {job_file.stem} não tem função main()")
    
    def load_from_databricks_yml(self, databricks_yml_path: str = "databricks.yml"):
        """
        Carrega jobs e dependências do databricks.yml
        
        Args:
            databricks_yml_path: Caminho do arquivo databricks.yml
        """
        import yaml
        
        try:
            with open(databricks_yml_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Extrair jobs do databricks.yml
            if 'resources' in config and 'jobs' in config['resources']:
                jobs_config = config['resources']['jobs']
                
                for job_name, job_config in jobs_config.items():
                    # Dependências (via tasks)
                    dependencies = []
                    if 'tasks' in job_config:
                        for task in job_config['tasks']:
                            if 'depends_on' in task:
                                dependencies.extend(task['depends_on'])
                    
                    # Registrar job
                    self.register_job(
                        name=job_name,
                        function=lambda: None,  # Placeholder - execução via Databricks
                        dependencies=dependencies,
                        enabled=True
                    )
                
                info(self.pipeline_name, f"Carregados {len(jobs_config)} jobs do databricks.yml")
        except Exception as e:
            error(self.pipeline_name, f"Erro ao carregar databricks.yml: {str(e)}")
    
    def register_job(
        self,
        name: str,
        function: Callable,
        dependencies: List[str] = None,
        priority: int = 0,
        enabled: bool = True,
        parameters: Dict[str, Any] = None
    ):
        """
        Registra um job no pipeline
        
        Args:
            name: Nome do job
            function: Função a executar
            dependencies: Lista de jobs dos quais este depende
            priority: Prioridade de execução (menor = maior prioridade)
            enabled: Se o job está habilitado
            parameters: Parâmetros para o job
        """
        job = JobDefinition(
            name=name,
            function=function,
            dependencies=dependencies,
            priority=priority,
            enabled=enabled,
            parameters=parameters
        )
        
        self.jobs[name] = job
        info(self.pipeline_name, f"Job registrado: {name}")
    
    def unregister_job(self, name: str):
        """Remove um job do pipeline"""
        if name in self.jobs:
            del self.jobs[name]
            info(self.pipeline_name, f"Job removido: {name}")
    
    def get_job(self, name: str) -> JobDefinition:
        """Retorna definição de um job"""
        return self.jobs.get(name)
    
    def get_all_jobs(self) -> Dict[str, JobDefinition]:
        """Retorna todos os jobs"""
        return self.jobs
    
    def get_enabled_jobs(self) -> Dict[str, JobDefinition]:
        """Retorna apenas jobs habilitados"""
        return {k: v for k, v in self.jobs.items() if v.enabled}
    
    def get_execution_order(self) -> List[str]:
        """
        Calcula ordem de execução baseada em dependências
        
        Returns:
            Lista de nomes de jobs em ordem de execução
        """
        enabled_jobs = self.get_enabled_jobs()
        
        # Topological sort para resolver dependências
        execution_order = []
        visited = set()
        
        def visit(job_name: str):
            if job_name in visited:
                return
            
            visited.add(job_name)
            job = enabled_jobs.get(job_name)
            
            if job:
                # Visitar dependências primeiro
                for dep in job.dependencies:
                    if dep in enabled_jobs:
                        visit(dep)
                
                execution_order.append(job_name)
        
        # Visitar todos os jobs
        for job_name in enabled_jobs:
            visit(job_name)
        
        info(self.pipeline_name, f"Ordem de execução: {execution_order}")
        return execution_order
    
    def execute_job(self, name: str):
        """Executa um job específico"""
        job = self.get_job(name)
        
        if not job:
            error(self.pipeline_name, f"Job não encontrado: {name}")
            return False
        
        if not job.enabled:
            warning(self.pipeline_name, f"Job desabilitado: {name}")
            return False
        
        try:
            info(self.pipeline_name, f"Executando job: {name}")
            
            if job.parameters:
                result = job.function(**job.parameters)
            else:
                result = job.function()
            
            info(self.pipeline_name, f"Job concluído: {name}")
            return True
        except Exception as e:
            error(self.pipeline_name, f"Job falhou: {name} - {str(e)}")
            return False
    
    def execute_pipeline(self):
        """Executa todo o pipeline na ordem correta"""
        execution_order = self.get_execution_order()
        
        results = {}
        
        for job_name in execution_order:
            success = self.execute_job(job_name)
            results[job_name] = success
            
            if not success:
                error(self.pipeline_name, f"Pipeline interrompido devido a falha em: {job_name}")
                break
        
        all_success = all(results.values())
        
        if all_success:
            info(self.pipeline_name, "Pipeline executado com sucesso")
        else:
            error(self.pipeline_name, f"Pipeline executado com falhas: {results}")
        
        return results
    
    def generate_dag_code(self) -> str:
        """
        Gera código de DAG Airflow dinamicamente
        
        Returns:
            Código Python do DAG
        """
        execution_order = self.get_execution_order()
        
        dag_code = f'''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from jobs import {", ".join(self.jobs.keys())}

default_args = {{
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}}

dag = DAG(
    "{self.pipeline_name}",
    default_args=default_args,
    description="Pipeline gerado dinamicamente",
    schedule_interval="@daily",
    catchup=False,
)

'''
        
        # Gerar tasks
        for job_name in execution_order:
            job = self.get_job(job_name)
            dag_code += f'''
{job_name}_task = PythonOperator(
    task_id="{job_name}",
    python_callable={job_name},
    dag=dag,
)

'''
        
        # Gerar dependências
        for job_name in execution_order:
            job = self.get_job(job_name)
            
            for dep in job.dependencies:
                if dep in self.jobs:
                    dag_code += f'''
{dep}_task >> {job_name}_task
'''
        
        return dag_code
    
    def validate_pipeline(self) -> bool:
        """
        Valida se o pipeline está configurado corretamente
        
        Returns:
            True se válido, False caso contrário
        """
        # Verificar dependências circular
        execution_order = self.get_execution_order()
        
        # Verificar se todos os jobs dependem de jobs existentes
        for job_name, job in self.jobs.items():
            for dep in job.dependencies:
                if dep not in self.jobs:
                    error(self.pipeline_name, f"Dependência inválida: {job_name} depende de {dep} (não existe)")
                    return False
        
        info(self.pipeline_name, "Pipeline validado com sucesso")
        return True


# Pipeline global singleton
_global_pipeline = None


def get_pipeline(pipeline_name: str = "default", auto_discover: bool = True) -> DynamicPipeline:
    """
    Retorna pipeline global com auto-discovery
    
    Args:
        pipeline_name: Nome do pipeline
        auto_discover: Se deve auto-descobrir jobs
    
    Returns:
        DynamicPipeline configurado
    """
    global _global_pipeline
    
    if _global_pipeline is None:
        _global_pipeline = DynamicPipeline(pipeline_name, auto_discover=auto_discover)
    
    return _global_pipeline


def auto_generate_dag(databricks_yml_path: str = "databricks.yml", output_path: str = "dags/dag_pipeline_santander.py"):
    """
    Gera DAG Airflow automaticamente do databricks.yml
    
    Args:
        databricks_yml_path: Caminho do databricks.yml
        output_path: Caminho de saída do DAG
    """
    pipeline = DynamicPipeline("auto_generated", auto_discover=False)
    pipeline.load_from_databricks_yml(databricks_yml_path)
    
    dag_code = pipeline.generate_dag_code()
    
    with open(output_path, 'w') as f:
        f.write(dag_code)
    
    info("auto_generate_dag", f"DAG gerado automaticamente: {output_path}")


def reset_pipeline():
    """Reseta pipeline global"""
    global _global_pipeline
    _global_pipeline = None
