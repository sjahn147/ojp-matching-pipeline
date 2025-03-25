from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

# Import correct function names from respective modules
from csv_ingestion import run_csv_ingestion
from drop_duplicate import drop_duplicates
from company_matching import process_matching_file
from kobert_classification import run_kobert_classification
from match_skills import match_skills
from process_skill_data import process_skill_data
from create_index import create_index

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 1),
    'retries': 1,
}

with DAG(
    'ojp_processing',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1. CSV 데이터 적재
    csv_ingestion_task = PythonOperator(
        task_id='csv_ingestion',
        python_callable=run_csv_ingestion,
    )

    # 2. Elasticsearch 인덱스 생성
    create_index_task = PythonOperator(
        task_id='create_index',
        python_callable=create_index,
    )

    # 3. 중복 제거
    drop_duplicate_task = PythonOperator(
        task_id='drop_duplicate',
        python_callable=drop_duplicates,
    )

    # 4. 회사 정보 매칭
    company_matching_task = PythonOperator(
        task_id='company_matching',
        python_callable=process_matching_file,
    )

    # 5. KoBERT 직종 분류
    kobert_classification_task = PythonOperator(
        task_id='kobert_classification',
        python_callable=run_kobert_classification,
    )

    # 6. 스킬 매칭 병렬화
    # 병렬 작업 시작과 종료를 위한 더미 태스크
    start_skill_matching = DummyOperator(task_id='start_skill_matching')
    end_skill_matching = DummyOperator(task_id='end_skill_matching')
    
    # 병렬 스킬 매칭 태스크 생성
    num_shards = 8  # 병렬 처리 수 
    for i in range(num_shards):
        match_skills_task = PythonOperator(
            task_id=f'match_skills_{i}',
            python_callable=match_skills,
            op_kwargs={'shard_id': i, 'total_shards': num_shards},
        )
        # 각 병렬 태스크를 시작과 종료 더미 태스크에 연결
        start_skill_matching >> match_skills_task >> end_skill_matching

    # 7. 스킬 데이터 처리
    process_skill_data_task = PythonOperator(
        task_id='process_skill_data',
        python_callable=process_skill_data,
    )

    # Task Dependencies
    csv_ingestion_task >> create_index_task >> drop_duplicate_task >> company_matching_task >> kobert_classification_task >> start_skill_matching
    end_skill_matching >> process_skill_data_task