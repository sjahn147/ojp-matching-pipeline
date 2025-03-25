import subprocess
import os
import psycopg2
import pandas as pd
import requests
import json
import time
from io import StringIO

# PostgreSQL 연결 정보
DB_PARAMS = {
    "dbname": "your_db_name",
    "user": "your_user_name",
    "password": "your_password",
    "host": "your_host_name", 
    "port": "5432"

# API 서버 설정
API_HOST = "model_server2"  # Docker 컨테이너 이름
API_PORT = 5000
API_BASE_URL = f"http://{API_HOST}:{API_PORT}"

# 파일 경로 설정
AIRFLOW_MODELS_PATH = "/opt/airflow/models"

def load_keco_mapping():
    """KECO 코드 매핑 정보 로드"""
    try:
        keco_file_path = "/opt/airflow/data/job_code/target_keco_t.csv"
        print(f"Loading KECO code mapping from {keco_file_path}")
        
        if not os.path.exists(keco_file_path):
            print(f"Error: KECO mapping file not found at {keco_file_path}")
            return None
        
        # CSV 파일에서 매핑 정보 로드
        keco_df = pd.read_csv(keco_file_path)
        
        # 필요한 컬럼이 있는지 확인
        required_columns = ['target', 'keco_4', 'keco_t']
        for col in required_columns:
            if col not in keco_df.columns:
                print(f"Error: Required column '{col}' not found in KECO mapping file")
                return None
        
        print(f"Loaded {len(keco_df)} KECO code mappings")
        return keco_df
    
    except Exception as e:
        print(f"Error loading KECO mapping: {e}")
        return None

def ensure_path_exists(path):
    """경로가 존재하는지 확인하고, 없으면 생성합니다."""
    os.makedirs(path, exist_ok=True)
    print(f"Ensuring path exists: {path}")
    return os.path.exists(path)

def check_and_add_column():
    """merged_table에 필요한 컬럼이 있는지 확인하고, 없으면 추가합니다."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        # 테이블이 존재하는지 확인
        cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'merged_table'
        );
        """)
        
        if not cursor.fetchone()[0]:
            print("Error: 'merged_table' does not exist in database")
            cursor.close()
            conn.close()
            return False
        
        # 확인할 컬럼 목록
        columns_to_check = ['target', 'keco_4', 'keco_t']
        
        # 각 컬럼의 존재 여부 확인 및 추가
        for column_name in columns_to_check:
            cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='merged_table' AND column_name='{column_name}';
            """)
            
            if not cursor.fetchone():
                # 컬럼이 없으면 추가
                print(f"Adding '{column_name}' column to 'merged_table'...")
                try:
                    if column_name == 'target':
                        cursor.execute("""
                        ALTER TABLE merged_table 
                        ADD COLUMN target INTEGER;
                        """)
                    elif column_name == 'keco_4':
                        cursor.execute("""
                        ALTER TABLE merged_table 
                        ADD COLUMN keco_4 TEXT;
                        """)
                    elif column_name == 'keco_t':
                        cursor.execute("""
                        ALTER TABLE merged_table 
                        ADD COLUMN keco_t TEXT;
                        """)
                    conn.commit()
                    print(f"Successfully added '{column_name}' column")
                except Exception as e:
                    print(f"Error adding column {column_name}: {e}")
                    conn.rollback()
                    cursor.close()
                    conn.close()
                    return False
            else:
                print(f"'{column_name}' column already exists in 'merged_table'")
        
        # predicted_occupation 컬럼이 있는지 확인하고 target으로 마이그레이션
        cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name='merged_table' AND column_name='predicted_occupation';
        """)
        
        if cursor.fetchone():
            print("Found legacy 'predicted_occupation' column, migrating data to 'target'...")
            try:
                cursor.execute("""
                UPDATE merged_table 
                SET target = predicted_occupation
                WHERE predicted_occupation IS NOT NULL AND target IS NULL;
                """)
                conn.commit()
                print("Successfully migrated data from 'predicted_occupation' to 'target'")
            except Exception as e:
                print(f"Error migrating data: {e}")
                conn.rollback()
            
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Database error in check_and_add_column: {e}")
        return False


def fetch_data_for_kobert(batch_size=1000):
    """PostgreSQL에서 KoBERT 모델이 사용할 데이터를 배치로 가져옴"""
    try:
        # 경로 확인
        ensure_path_exists(AIRFLOW_MODELS_PATH)
        
        # 데이터베이스 연결
        print("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        # 총 레코드 수 확인
        cursor.execute("SELECT COUNT(*) FROM merged_table WHERE pst_detail IS NOT NULL")
        total_records = cursor.fetchone()[0]
        print(f"Total records to process: {total_records}")
        
        if total_records == 0:
            print("No job descriptions found for classification.")
            cursor.close()
            conn.close()
            return None
        
        # 첫 번째 배치 가져오기
        cursor.execute("SELECT pst_id, pst_detail AS job_desc FROM merged_table WHERE pst_detail IS NOT NULL LIMIT %s", (batch_size,))
        rows = cursor.fetchall()
        
        # 배치 데이터를 DataFrame으로 변환
        df = pd.DataFrame(rows, columns=['pst_id', 'job_desc'])
        
        # pst_id를 문자열로 변환 (정수 변환 오류 방지)
        df['pst_id'] = df['pst_id'].astype(str)
        
        cursor.close()
        conn.close()
        
        print(f"Retrieved {len(df)} records (batch) for classification")
        return df
    
    except Exception as e:
        print(f"Error fetching data for KoBERT classification: {e}")
        return None

def check_api_health():
    """API 서버 상태 확인"""
    max_retries = 5
    retry_delay = 5  # 초
    
    for i in range(max_retries):
        try:
            response = requests.get(f"{API_BASE_URL}/health", timeout=10)
            if response.status_code == 200:
                health_data = response.json()
                if health_data.get("model_loaded", False):
                    print("API server is healthy and model is loaded")
                    return True
                else:
                    print("API server is starting, model not loaded yet")
            else:
                print(f"API server returned status code: {response.status_code}")
        except requests.RequestException as e:
            print(f"Error connecting to API server (attempt {i+1}/{max_retries}): {e}")
        
        if i < max_retries - 1:
            print(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
    
    print("Failed to connect to API server after multiple attempts")
    return False

def process_data_with_csv_api(df, temp_csv_path=None, verbose=False):
    """CSV 파일을 API에 보내고 결과 받기 - 로깅 최적화"""
    if temp_csv_path is None:
        temp_csv_path = os.path.join(AIRFLOW_MODELS_PATH, f"temp_data_{int(time.time())}.csv")
    
    try:
        # CSV 파일로 저장
        df.to_csv(temp_csv_path, index=False, mode='w', chunksize=100)
        if verbose:
            print(f"Saved {len(df)} records to temporary CSV: {temp_csv_path}")
        
        # 파일 열기
        with open(temp_csv_path, 'rb') as f:
            files = {'file': (os.path.basename(temp_csv_path), f, 'text/csv')}
            
            # API 호출
            if verbose:
                print("Sending CSV to API...")
            start_time = time.time()
            
            response = requests.post(
                f"{API_BASE_URL}/predict_csv",
                files=files,
                timeout=1800  # 30분 타임아웃
            )
            
            elapsed_time = time.time() - start_time
            if verbose:
                print(f"CSV processing request took {elapsed_time:.2f} seconds")
            
            if response.status_code == 200:
                response_data = response.json()
                csv_result = response_data.get("csv_result")
                count = response_data.get("count", 0)
                if verbose:
                    print(f"Received {count} predictions from CSV API")
                
                # CSV 문자열을 DataFrame으로 변환
                from io import StringIO
                results_df = pd.read_csv(StringIO(csv_result), dtype={'pst_id': str, 'predicted_occupation': int})
                return results_df
            else:
                print(f"Error response from API (HTTP {response.status_code}): {response.text}")
                return None
    
    except Exception as e:
        print(f"Error processing data with CSV API: {e}")
        return None
    finally:
        # 임시 파일 삭제
        if temp_csv_path and os.path.exists(temp_csv_path):
            try:
                os.remove(temp_csv_path)
                if verbose:
                    print(f"Removed temporary CSV file: {temp_csv_path}")
            except Exception as e:
                print(f"Error removing temporary file: {e}")

def process_data_in_batches(df, batch_size=100):
    """데이터를 배치로 처리하여 API 호출 (CSV 방식 실패 시 대체 방법)"""
    results = []
    total_records = len(df)
    total_batches = (total_records + batch_size - 1) // batch_size
    
    print(f"Processing {total_records} records in {total_batches} batches of size {batch_size}")
    
    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, total_records)
        batch_df = df.iloc[start_idx:end_idx]
        
        print(f"Processing batch {batch_idx+1}/{total_batches} ({start_idx}-{end_idx})")
        
        # API 요청 데이터 준비
        batch_data = []
        for _, row in batch_df.iterrows():
            batch_data.append({
                "pst_id": int(row["pst_id"]),
                "job_desc": str(row["job_desc"])
            })
        
        try:
            # API 호출
            start_time = time.time()
            response = requests.post(
                f"{API_BASE_URL}/predict", 
                json={"data": batch_data},
                timeout=300  # 5분 타임아웃
            )
            
            elapsed_time = time.time() - start_time
            print(f"Batch {batch_idx+1} request took {elapsed_time:.2f} seconds")
            
            if response.status_code == 200:
                response_data = response.json()
                batch_results = response_data.get("results", [])
                print(f"Received {len(batch_results)} predictions for batch {batch_idx+1}")
                results.extend(batch_results)
            else:
                print(f"Error response from API (HTTP {response.status_code}): {response.text}")
        
        except requests.RequestException as e:
            print(f"Error calling API for batch {batch_idx+1}: {e}")
    
    print(f"Completed processing with {len(results)} total predictions")
    return results

def update_database_with_results(results, verbose=False):
    """결과를 데이터베이스에 업데이트하고 KECO 코드 매핑 적용"""
    # 결과가 없는 경우 체크
    if results is None:
        if verbose:
            print("No results to update in database")
        return False
    
    # DataFrame 여부 확인 및 빈 DataFrame 체크
    if isinstance(results, pd.DataFrame):
        if results.empty:
            if verbose:
                print("Empty DataFrame results, nothing to update")
            return False
        results_df = results
    else:  # list인 경우
        if not results:  # 빈 리스트 체크
            if verbose:
                print("Empty list results, nothing to update")
            return False
        results_df = pd.DataFrame(results)
    
    # KECO 코드 매핑 정보 로드
    keco_mapping = load_keco_mapping()
    if keco_mapping is None:
        print("Failed to load KECO mapping. Will update target values only.")
    else:
        # 결과에 KECO 코드 매핑 정보 추가
        # predicted_occupation을 target으로 변환
        if 'predicted_occupation' in results_df.columns and 'target' not in results_df.columns:
            results_df = results_df.rename(columns={'predicted_occupation': 'target'})
        
        # target을 정수로 변환
        results_df['target'] = pd.to_numeric(results_df['target'], errors='coerce').fillna(-1).astype(int)
        
        # 매핑 정보 추가
        results_df = results_df.merge(keco_mapping, on='target', how='left')
        
        if verbose:
            print(f"Added KECO code mapping to {len(results_df)} predictions")
    
    if verbose:
        print(f"Updating database with {len(results_df)} predictions")
    
    try:
        # 데이터베이스 연결
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        # pst_id의 데이터 타입 확인
        cursor.execute("SELECT data_type FROM information_schema.columns WHERE table_name='merged_table' AND column_name='pst_id'")
        pst_id_type = cursor.fetchone()[0]
        if verbose:
            print(f"pst_id column data type: {pst_id_type}")
        
        # 배치 처리
        batch_size = 1000
        total_records = len(results_df)
        total_batches = (total_records + batch_size - 1) // batch_size
        
        updated_count = 0
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, total_records)
            batch_df = results_df.iloc[start_idx:end_idx]
            
            if verbose:
                print(f"Updating database batch {batch_idx+1}/{total_batches} ({start_idx}-{end_idx})")
            
            try:
                for _, row in batch_df.iterrows():
                    if keco_mapping is not None and 'keco_4' in batch_df.columns and 'keco_t' in batch_df.columns:
                        # KECO 매핑 정보가 있는 경우
                        update_sql = """
                        UPDATE merged_table 
                        SET target = %s, keco_4 = %s, keco_t = %s 
                        WHERE pst_id = %s
                        """
                        
                        # pst_id 데이터 타입에 따라 처리
                        pst_id = int(row['pst_id']) if pst_id_type.lower() not in ('text', 'varchar', 'character varying') else str(row['pst_id'])
                        
                        cursor.execute(update_sql, (
                            int(row['target']), 
                            row.get('keco_4', ''), 
                            row.get('keco_t', ''),
                            pst_id
                        ))
                    else:
                        # KECO 매핑 정보가 없는 경우 target만 업데이트
                        update_sql = """
                        UPDATE merged_table 
                        SET target = %s 
                        WHERE pst_id = %s
                        """
                        
                        # pst_id 데이터 타입에 따라 처리
                        pst_id = int(row['pst_id']) if pst_id_type.lower() not in ('text', 'varchar', 'character varying') else str(row['pst_id'])
                        
                        target_col = 'predicted_occupation' if 'predicted_occupation' in row else 'target'
                        cursor.execute(update_sql, (int(row[target_col]), pst_id))
                
                conn.commit()
                updated_count += len(batch_df)
                if verbose:
                    print(f"Database batch {batch_idx+1} completed: {len(batch_df)} records updated")
                
            except Exception as e:
                print(f"Error processing database batch {batch_idx+1}: {e}")
                conn.rollback()
                continue
        
        cursor.close()
        conn.close()
        
        if verbose:
            print(f"Database update completed: {updated_count} of {total_records} records updated")
        return updated_count > 0
        
    except Exception as e:
        print(f"Error updating database with results: {e}")
        return False
        
def run_kobert_classification():
    """DAG에서 호출하는 메인 함수 - NULL인 레코드만 처리하고 성능 유지 작업 추가"""
    try:
        # 1. 테이블 컬럼 확인 및 추가
        if not check_and_add_column():
            print("Failed to verify or add required database column. Cannot proceed.")
            return
        
        # 2. API 서버 상태 확인
        if not check_api_health():
            print("API server is not available. Cannot proceed.")
            return
        
        # 배치 크기 및 처리된 레코드 수 초기화
        batch_size = 2000
        processed_count = 0
        batch_num = 1
        
        # 데이터베이스에서 처리해야 할 레코드 수 확인 (target이 NULL인 항목만)
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM merged_table WHERE pst_detail IS NOT NULL AND target IS NULL")
        total_records = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        print(f"Total NULL records to process: {total_records}")
        
        # 진행 모니터링을 위한 변수
        start_time = time.time()
        last_log_time = start_time
        last_maintenance_time = start_time
        
        # 더 처리할 레코드가 있는 동안 반복
        while processed_count < total_records:
            # 현재 시간 확인
            current_time = time.time()
            
            # 10초마다 요약 정보 로깅
            if current_time - last_log_time >= 10:
                elapsed_seconds = current_time - start_time
                if processed_count > 0:
                    seconds_per_record = elapsed_seconds / processed_count
                    remaining_records = total_records - processed_count
                    estimated_remaining_seconds = remaining_records * seconds_per_record
                    
                    # 시간 형식 변환
                    remaining_hours = int(estimated_remaining_seconds // 3600)
                    remaining_minutes = int((estimated_remaining_seconds % 3600) // 60)
                    
                    print(f"Progress: {processed_count}/{total_records} records ({processed_count/total_records*100:.1f}%) - "
                          f"Elapsed: {int(elapsed_seconds//60)} min {int(elapsed_seconds%60)} sec - "
                          f"Est. remaining: {remaining_hours}h {remaining_minutes}m - "
                          f"Speed: {processed_count/elapsed_seconds*60:.0f} records/min")
                
                last_log_time = current_time
            
            # 5분(300초)마다 시스템 유지보수 작업 수행
            if current_time - last_maintenance_time >= 3000:
                print("Performing system maintenance...")
                
                # 1. 메모리 정리
                import gc
                collected = gc.collect()
                print(f"Garbage collection: {collected} objects collected")
                
                # 2. 짧은 휴식 시간
                time.sleep(3)
                
                # 3. API 서버 상태 재확인
                check_api_health()
                
                # 4. 데이터베이스 연결 리셋 (테스트 쿼리 실행)
                try:
                    conn = psycopg2.connect(**DB_PARAMS)
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                    conn.close()
                    print("Database connection reset successful")
                except Exception as e:
                    print(f"Database connection reset failed: {e}")
                
                last_maintenance_time = time.time()
            
            # 현재 오프셋에서 NULL인 레코드만 배치로 가져오기
            conn = psycopg2.connect(**DB_PARAMS)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT pst_id, pst_detail AS job_desc FROM merged_table "
                "WHERE pst_detail IS NOT NULL AND target IS NULL "
                "ORDER BY pst_id LIMIT %s", 
                (batch_size,)
            )
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            
            if not rows:
                print("No more NULL records to process. Task completed.")
                break
                
            # 배치 데이터를 DataFrame으로 변환
            df = pd.DataFrame(rows, columns=['pst_id', 'job_desc'])
            df['pst_id'] = df['pst_id'].astype(str)  # 문자열로 변환
            
            batch_records = len(df)
            
            # 현재 배치 처리 (상세 로깅 없이)
            results_df = process_data_with_csv_api(df, verbose=False)
            
            if results_df is None or (isinstance(results_df, pd.DataFrame) and results_df.empty):
                results = process_data_in_batches(df, batch_size=100, verbose=False)
                if results is None or (isinstance(results, list) and len(results) == 0):
                    print(f"Failed to process batch {batch_num}. Skipping {batch_records} records.")
                    processed_count += batch_records
                    batch_num += 1
                    continue
            else:
                results = results_df
            
            # 결과를 데이터베이스에 업데이트 (상세 로깅 없이)
            update_success = update_database_with_results(results, verbose=False)
            if not update_success:
                print(f"Failed to update database with batch {batch_num}. Skipping {batch_records} records.")
            
            # 처리된 레코드 수 업데이트
            processed_count += batch_records
            batch_num += 1
            
            # 메모리 정리
            del df
            del results
            if results_df is not None:
                del results_df
            
            # 배치 처리 후 짧은 휴식 (속도 조절 및 부하 분산)
            if batch_num % 10 == 0:  # 10배치마다 짧은 휴식
                time.sleep(1)
        
        # 처리 완료 후 최종 요약
        total_elapsed = time.time() - start_time
        hours = int(total_elapsed // 3600)
        minutes = int((total_elapsed % 3600) // 60)
        seconds = int(total_elapsed % 60)
        
        print(f"KoBERT classification completed: {processed_count}/{total_records} NULL records processed")
        print(f"Total processing time: {hours}h {minutes}m {seconds}s")
        print(f"Average speed: {processed_count/total_elapsed*60:.0f} records/min")
        
    except Exception as e:
        print(f"Error running KoBERT classification with API: {e}")