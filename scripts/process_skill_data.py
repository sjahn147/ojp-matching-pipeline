import logging
import time
import psycopg2
import os

# PostgreSQL 연결 정보
DB_PARAMS = {
    "dbname": "your_db_name",
    "user": "your_user_name",
    "password": "your_password",
    "host": "your_host_name", 
    "port": "5432"
}

# CSV 내보내기 경로 설정
CSV_EXPORT_PATH = "/opt/airflow/data/output/expanded_skills.csv"

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_expanded_skills_table(conn):
    """expanded_skills 테이블 생성 (기존 테이블 삭제 후)"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            DROP TABLE IF EXISTS expanded_skills;
            
            CREATE TABLE expanded_skills (
                pst_id TEXT,
                snr TEXT,
                rnk TEXT,
                edu TEXT,
                typ TEXT,
                sal TEXT,
                time TEXT,
                co_nm TEXT,
                pr_maj TEXT,
                co_ind TEXT,
                qual TEXT,
                source TEXT,
                year TEXT,  
                month TEXT, 
                matched_company_name TEXT,
                bzno TEXT,
                skill_code TEXT,
                keco_4 TEXT,
                keco_t TEXT,
                skill_name TEXT,
                main TEXT,
                sub TEXT,
                detail TEXT,
                code0 TEXT,
                code1 TEXT,
                code2 TEXT,
                code3 TEXT,
                softskill TEXT,
                ksic TEXT,
                ksic9 TEXT
            );
        """)
        conn.commit()
        logging.info("expanded_skills 테이블이 성공적으로 생성되었습니다.")
        
        return True
    except Exception as e:
        logging.error(f"expanded_skills 테이블 생성 중 오류 발생: {e}")
        conn.rollback()
        return False

def execute_skill_expansion_query(conn):
    """SQL을 사용하여 스킬 확장 및 결합을 수행하는 함수"""
    try:
        cursor = conn.cursor()
        
        # 스킬 확장 쿼리 수행
        logging.info("PostgreSQL에서 스킬 데이터 확장 쿼리 실행 중...")
        
        expand_query = """
        INSERT INTO expanded_skills
        WITH skill_codes_expanded AS (
            SELECT 
                m.pst_id, m.snr, m.rnk, m.edu, m.typ, m.sal, m.time, m.co_nm, 
                m.pr_maj, m.co_ind, m.qual, m.source, m.year, m.month, 
                m.matched_company_name, m.bzno, m.keco_4, m.keco_t, m.softskill,
                m.ksic, m.ksic9,
                trim(skill_item) AS skill_code
            FROM 
                merged_table m,
                unnest(string_to_array(m.skill_code, ',')) AS skill_item
            WHERE 
                m.skill_code IS NOT NULL AND m.skill_code <> ''
        )
        SELECT 
            s.pst_id, s.snr, s.rnk, s.edu, s.typ, s.sal, s.time, s.co_nm, 
            s.pr_maj, s.co_ind, s.qual, s.source, s.year, s.month, 
            s.matched_company_name, s.bzno, s.skill_code, s.keco_4, s.keco_t,
            sd.skill_name, sd.main, sd.sub, sd.detail, 
            sd.code0, sd.code1, sd.code2, sd.code3,
            s.softskill,
            s.ksic, s.ksic9
        FROM 
            skill_codes_expanded s
        LEFT JOIN 
            skill_dictionary sd ON s.skill_code = sd.skill_code;
        """
        
        # 쿼리 실행
        cursor.execute(expand_query)
        
        # 인덱스 생성
        logging.info("인덱스 생성 중...")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_expanded_skills_pst_id ON expanded_skills(pst_id);
            CREATE INDEX IF NOT EXISTS idx_expanded_skills_skill_code ON expanded_skills(skill_code);
            CREATE INDEX IF NOT EXISTS idx_expanded_skills_year_month ON expanded_skills(year, month);
        """)
        
        conn.commit()
        
        # 결과 확인
        cursor.execute("SELECT COUNT(*) FROM expanded_skills;")
        count = cursor.fetchone()[0]
        logging.info(f"확장된 스킬 데이터 {count}개 행이 성공적으로 생성되었습니다.")
        
        cursor.close()
        return count
    except Exception as e:
        logging.error(f"스킬 확장 쿼리 실행 중 오류 발생: {e}")
        conn.rollback()
        raise

def export_to_csv(conn, output_path):
    """확장된 스킬 데이터를 CSV 파일로 내보내는 함수"""
    try:
        cursor = conn.cursor()
        
        # 출력 디렉토리 확인 및 생성
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logging.info(f"출력 디렉토리 생성: {output_dir}")
        
        # COPY 명령 실행
        cursor.execute(f"""
            COPY expanded_skills TO STDOUT WITH CSV HEADER ENCODING 'UTF8'
        """)
        
        # 파일로 저장
        with open(output_path, 'w', encoding='utf-8') as f:
            conn.cursor_factory = None  # Reset cursor factory to default
            cursor.copy_expert(f"COPY expanded_skills TO STDOUT WITH CSV HEADER ENCODING 'UTF8'", f)
        
        logging.info(f"CSV 파일 내보내기 완료: {output_path}")
        cursor.close()
        return True
    except Exception as e:
        logging.error(f"CSV 파일 내보내기 중 오류 발생: {e}")
        return False

def process_skill_data():
    """PostgreSQL에서 직접 처리하여 스킬 데이터를 확장하는 메인 함수"""
    start_time = time.time()
    
    try:
        # 데이터베이스 연결
        conn = psycopg2.connect(**DB_PARAMS)
        
        # expanded_skills 테이블 생성
        if not create_expanded_skills_table(conn):
            logging.error("expanded_skills 테이블 생성에 실패하여 작업을 중단합니다.")
            conn.close()
            return
        
        # SQL 쿼리로 스킬 확장 수행
        rows_affected = execute_skill_expansion_query(conn)
        
        # CSV로 내보내기
        csv_path = os.environ.get('EXPANDED_SKILLS_CSV_PATH', CSV_EXPORT_PATH)
        export_success = export_to_csv(conn, csv_path)
        
        # 연결 종료
        conn.close()
        
        # 처리 결과 요약
        elapsed_time = time.time() - start_time
        logging.info(f"총 처리 시간: {elapsed_time:.2f}초")
        logging.info(f"스킬 데이터 처리 완료: {rows_affected}개 레코드 생성")
        
        if export_success:
            logging.info(f"CSV 내보내기 완료: {csv_path}")
        else:
            logging.warning("CSV 내보내기 실패")
        
    except Exception as e:
        logging.error(f"process_skill_data 오류: {e}")
        logging.error(f"오류 발생 전까지 총 경과 시간: {time.time() - start_time:.2f}초")

if __name__ == "__main__":
    process_skill_data()