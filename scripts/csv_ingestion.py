import os
import pandas as pd
import psycopg2
from tqdm import tqdm
import logging
import csv
import sys
from datetime import datetime
import re
from typing import Optional, Tuple

# PostgreSQL 연결 정보
DB_PARAMS = {
    "dbname": "your_db_name",
    "user": "your_user_name",
    "password": "your_password",
    "host": "your_host_name", 
    "port": "5432"
}

# 데이터 디렉토리 설정
DATA_PATHS = {
    "jk": "/opt/airflow/data/2024_jk",
    "jkhd": "/opt/airflow/data/2024_jkhd",
    "srm": "/opt/airflow/data/2024_srm",
    "company_reference": "/opt/airflow/data/keyvar/company_info.csv",
    "skill_dictionary": "/opt/airflow/data/skilldict/korskilldict.xlsx"
}

# 테이블 스키마 정의
TABLE_SCHEMAS = {
    "jk": ["pst_id", "pst_title", "co_nm", "loc", "snr", "rnk", "pr_item", "pr_maj", "edu", "typ", "sal", "time", "pst_strt", "pst_end", "co_ind", "co_hr", "pst_detail", "corp_addr"],
    "jkhd": ["pst_id", "typ", "cl_info", "wc", "qual", "co_hr", "co_nm", "pst_detail"],
    "srm": ["pst_id", "pst_title", "co_nm", "loc", "snr", "rnk", "pr_item", "edu", "typ", "sal", "time", "pst_strt", "pst_end", "pst_detail", "rep", "corp_addr"]
}

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_company_reference(test: bool = False):
    """회사 레퍼런스 데이터 적재"""
    logger.info("Starting company reference data ingestion...")
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        # 회사 레퍼런스 테이블 생성
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS company_reference (
                bzno TEXT,
                title TEXT,
                address TEXT,
                phone TEXT,
                region TEXT,
                bonid TEXT,
                bon_add TEXT,
                bon_address TEXT,
                bon_phone TEXT,
                bon_title TEXT,
                bon_info TEXT,
                ksic TEXT,
                ksic9 TEXT
            );
        """)
        conn.commit()
        
        # 기존 데이터 삭제
        cursor.execute("TRUNCATE TABLE company_reference")
        conn.commit()
        
        file_path = DATA_PATHS["company_reference"]
        df = pd.read_csv(file_path, delimiter='|', encoding='utf-8')
        
        if test:
            df = df
        
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Loading company reference data"):
            sql = """
                INSERT INTO company_reference 
                (bzno, title, address, phone, region, bonid, bon_add, bon_address, 
                 bon_phone, bon_title, bon_info, ksic, ksic9)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(sql, tuple(row))
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Company reference data loaded successfully")
        
    except Exception as e:
        logger.error(f"Error loading company reference data: {e}")
        raise

def load_skill_dictionary(test: bool = False):
    """스킬 사전 데이터 적재"""
    logger.info("Starting skill dictionary data ingestion...")
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        # 스킬 사전 테이블 생성 (이미 존재하면 생성하지 않음)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS skill_dictionary (
                skill_code VARCHAR(20) PRIMARY KEY,
                code0 VARCHAR(5),
                code1 VARCHAR(5),
                code2 VARCHAR(5),
                code3 VARCHAR(5),
                main VARCHAR(100),
                sub VARCHAR(100),
                detail VARCHAR(100),
                skill_name VARCHAR(255),
                softskill INT,
                ext_key TEXT
            );
        """)
        conn.commit()
        
        # Excel 파일 읽기
        df = pd.read_excel(DATA_PATHS["skill_dictionary"])
        
        if test:
            df = df
        
        for _, row in tqdm(df.iterrows(), desc="Loading skill dictionary", total=len(df)):
            sql = """
                INSERT INTO skill_dictionary 
                (skill_code, code0, code1, code2, code3, main, sub, detail, 
                 skill_name, softskill, ext_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (skill_code) DO NOTHING;
            """
            values = (
                row['skill_code'], row['code0'], row['code1'], row['code2'],
                row['code3'], row['main'], row['sub'], row['detail'],
                row['skill_name'], row.get('softskill', None), row['ext_key']
            )
            cursor.execute(sql, values)
            
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Skill dictionary loaded successfully (with conflict resolution)")
        
    except Exception as e:
        logger.error(f"Error loading skill dictionary: {e}")
        raise


def setup_tables(conn, cursor):
    """테이블 스키마 설정 - Primary Key 제약조건 추가"""
    # jk 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jk (
            pst_id TEXT PRIMARY KEY,  -- Primary Key 추가
            pst_title TEXT,
            co_nm TEXT,
            loc TEXT,
            snr TEXT,
            rnk TEXT,
            pr_item TEXT,
            pr_maj TEXT,
            edu TEXT,
            typ TEXT,
            sal TEXT,
            time TEXT,
            pst_strt TEXT,
            pst_end TEXT,
            co_ind TEXT,
            co_hr TEXT,
            pst_detail TEXT,
            corp_addr TEXT,
            year INT,
            month INT
        );
    """)
    
    # srm 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS srm (
            pst_id TEXT PRIMARY KEY,  -- Primary Key 추가
            pst_title TEXT,
            co_nm TEXT,
            loc TEXT,
            snr TEXT,
            rnk TEXT,
            pr_item TEXT,
            edu TEXT,
            typ TEXT,
            sal TEXT,
            time TEXT,
            pst_strt TEXT,
            pst_end TEXT,
            pst_detail TEXT,
            rep TEXT,
            corp_addr TEXT,
            year INT,
            month INT
        );
    """)
    
    # jkhd 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jkhd (
            pst_id TEXT PRIMARY KEY,  -- Primary Key 추가
            typ TEXT,
            cl_info TEXT,
            wc TEXT,
            qual TEXT,
            co_hr TEXT,
            co_nm TEXT,
            pst_detail TEXT,
            year INT,
            month INT
        );
    """)
    
    conn.commit()

def deduplicate_source_tables(conn, cursor):
    """각 소스 테이블의 중복 데이터 제거"""
    try:
        # jk 테이블 중복 제거
        cursor.execute("""
            CREATE TABLE jk_dedup AS
            SELECT DISTINCT ON (pst_id) *
            FROM jk
            ORDER BY pst_id, pst_strt DESC NULLS LAST;
            
            DROP TABLE jk;
            ALTER TABLE jk_dedup RENAME TO jk;
        """)
        
        # srm 테이블 중복 제거
        cursor.execute("""
            CREATE TABLE srm_dedup AS
            SELECT DISTINCT ON (pst_id) *
            FROM srm
            ORDER BY pst_id, pst_strt DESC NULLS LAST;
            
            DROP TABLE srm;
            ALTER TABLE srm_dedup RENAME TO srm;
        """)
        
        # jkhd 테이블 중복 제거
        cursor.execute("""
            CREATE TABLE jkhd_dedup AS
            SELECT DISTINCT ON (pst_id) *
            FROM jkhd
            ORDER BY pst_id;
            
            DROP TABLE jkhd;
            ALTER TABLE jkhd_dedup RENAME TO jkhd;
        """)
        
        conn.commit()
        
    except Exception as e:
        print(f"Error during source table deduplication: {e}")
        conn.rollback()
        raise

def extract_year_month(date_str: str) -> Tuple[Optional[int], Optional[int]]:
    """날짜 문자열에서 연도와 월을 추출"""
    if pd.isna(date_str) or not isinstance(date_str, str) or date_str.strip() == '':
        return None, None
    
    try:
        # jk: 2024.02.01(목)
        match = re.search(r'(\d{4})\.(\d{2})\.\d{2}\(\w+\)', date_str)
        if match:
            return int(match.group(1)), int(match.group(2))
            
        # srm: 2024.03.11 14:31
        match = re.search(r'(\d{4})\.(\d{2})\.\d{2}\s+\d{2}:\d{2}', date_str)
        if match:
            return int(match.group(1)), int(match.group(2))
            
        return None, None
    except Exception as e:
        logger.error(f"Error parsing date: {date_str} -> {e}")
        return None, None

def get_selected_files(base_dir: str, folder_name: str) -> list:
    """범위에 따른 처리할 파일 목록 반환"""
    all_files = sorted([f for f in os.listdir(base_dir) if f.endswith('.csv')])
    
    # 폴더별 파일 범위 설정
    folder_ranges = {
        "2024_jk": (0, 17),
        "2024_jkhd": (0, 2),
        "2024_srm": (0, 14)
    }
    
    start_num, end_num = folder_ranges.get(folder_name, (None, None))
    if (start_num, end_num) == (None, None):
        logger.info(f"Skipping all files in {folder_name}")
        return []
        
    selected_files = []
    for file in all_files:
        try:
            file_num = int(''.join(filter(str.isdigit, file)))
            if (start_num is None or file_num >= start_num) and (end_num is None or file_num <= end_num):
                selected_files.append(os.path.join(base_dir, file))
        except ValueError:
            continue
            
    return selected_files

def load_job_postings(table_name: str, file_path: str, test: bool = False, force: bool = False):
    """
    특정 테이블(jk, jkhd, srm)에 대응하는 CSV 파일을 읽어 DB에 적재
    
    Args:
        table_name (str): 테이블 이름 (jk, jkhd, srm)
        file_path (str): 파일 경로
        test (bool): 테스트 모드 여부
        force (bool): True인 경우 기존 데이터를 삭제하고 새로 적재
    """
    try:
        # CSV 필드 크기 제한 증가
        csv.field_size_limit(sys.maxsize)
        
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # 테이블 스키마가 없을 경우 생성 (중복 실행해도 OK)
        setup_tables(conn, cursor)

        # 적재 이력 테이블 생성
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_ingestion_history (
                file_path TEXT PRIMARY KEY,
                table_name TEXT,
                processed_at TIMESTAMP,
                status TEXT
            );
        """)
        conn.commit()

        # FORCE가 True인 경우, 기존 데이터 삭제
        if force:
            logger.info(f"FORCE mode is enabled. Truncating table {table_name}...")
            cursor.execute(f"TRUNCATE TABLE {table_name};")
            # 적재 이력도 삭제
            cursor.execute(f"DELETE FROM file_ingestion_history WHERE table_name = %s;", (table_name,))
            conn.commit()

        folder_name = os.path.basename(file_path)  # 예: 2024_jk, 2024_jkhd, 2024_srm
        selected_files = get_selected_files(file_path, folder_name)

        if not selected_files:
            logger.info(f"No matching files found for {table_name} in {folder_name}")
            cursor.close()
            conn.close()
            return

        logger.info(f"Processing files in {folder_name}: {len(selected_files)} files found")
        
        processed_count = 0
        for csv_file in selected_files:
            # 파일 처리 이력 확인
            cursor.execute(
                "SELECT status FROM file_ingestion_history WHERE file_path = %s", 
                (csv_file,)
            )
            result = cursor.fetchone()
            
            if result and result[0] == 'COMPLETED' and not force:
                logger.info(f"File {csv_file} already processed. Skipping...")
                processed_count += 1
                continue
                
            logger.info(f"Processing file: {csv_file}")
            
            # 처리 중 상태로 기록
            cursor.execute(
                """
                INSERT INTO file_ingestion_history (file_path, table_name, processed_at, status)
                VALUES (%s, %s, NOW(), 'PROCESSING')
                ON CONFLICT (file_path) DO UPDATE 
                SET processed_at = NOW(), status = 'PROCESSING'
                """, 
                (csv_file, table_name)
            )
            conn.commit()
            
            try:
                rows = []
                with open(csv_file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f, delimiter='\t')
                    header = next(reader)
                    for idx, row_data in enumerate(reader):
                        if test and idx >= 1000:
                            break
                        # 줄바꿈 문자 제거 및 NUL(0x00) 문자 제거
                        row_data = [
                            field.replace("\n", " ").replace("\r", " ").replace("\x00", "") 
                            if isinstance(field, str) else field 
                            for field in row_data
                        ]
                        if table_name == 'jk':
                            year, month = extract_year_month(row_data[12])
                        elif table_name == 'srm':
                            year, month = extract_year_month(row_data[11])
                        else:
                            year, month = datetime.now().year, datetime.now().month
                        rows.append(row_data + [year, month])

                # DB Insert
                success_count = 0
                error_count = 0
                for row_insert in tqdm(rows, desc=f"Uploading {os.path.basename(csv_file)}"):
                    columns = TABLE_SCHEMAS[table_name] + ['year', 'month']
                    placeholders = ', '.join(['%s'] * len(row_insert))
                    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders});"
                    try:
                        cursor.execute(sql, tuple(row_insert))
                        success_count += 1
                    except psycopg2.errors.UniqueViolation:
                        conn.rollback()
                        error_count += 1
                        continue
                    except Exception as e:
                        conn.rollback()
                        logger.warning(f"Error inserting row: {e}")
                        error_count += 1
                        continue

                conn.commit()
                
                # 처리 완료 상태로 기록
                cursor.execute(
                    """
                    UPDATE file_ingestion_history 
                    SET processed_at = NOW(), status = 'COMPLETED'
                    WHERE file_path = %s
                    """, 
                    (csv_file,)
                )
                conn.commit()
                
                logger.info(f"File {csv_file} processed. Success: {success_count}, Errors: {error_count}")
                processed_count += 1
                
            except Exception as e:
                # 처리 실패 상태로 기록
                cursor.execute(
                    """
                    UPDATE file_ingestion_history 
                    SET processed_at = NOW(), status = 'FAILED'
                    WHERE file_path = %s
                    """, 
                    (csv_file,)
                )
                conn.commit()
                logger.error(f"Error processing file {csv_file}: {e}")
                raise

        logger.info(f"Processed {processed_count}/{len(selected_files)} files for {table_name}")

        # 모든 파일이 처리되었으면 소스 테이블 중복 제거
        if processed_count == len(selected_files):
            logger.info(f"Deduplicating {table_name} table...")
            deduplicate_source_tables(conn, cursor)

        cursor.close()
        conn.close()
        logger.info(f"Data inserted into {table_name} and deduplicated")

    except Exception as e:
        logger.error(f"Error loading file {file_path}: {e}")
        if 'cursor' in locals() and cursor is not None:
            try:
                # cursor.query가 있고 None이 아닐 때만 decode 시도
                query_str = cursor.query.decode() if hasattr(cursor, 'query') and cursor.query is not None else "No query available"
                logger.error(f"\nLast executed query: {query_str}")
            except Exception as query_error:
                logger.error(f"Error retrieving query: {query_error}")
        raise

def create_merged_table(conn) -> None:
    """통합 테이블 생성 및 데이터 병합"""
    with conn.cursor() as cursor:
        # 1. 기존 테이블 삭제
        cursor.execute("DROP TABLE IF EXISTS merged_table;")
        
        # 2. 새 테이블 생성 (총 29개 컬럼)
        cursor.execute("""
            CREATE TABLE merged_table (
                -- 공통 필드 (1~13)
                pst_id TEXT PRIMARY KEY,
                pst_title TEXT,
                co_nm TEXT,
                loc TEXT,
                snr TEXT,
                rnk TEXT,
                edu TEXT,
                typ TEXT,
                sal TEXT,
                time TEXT,
                pst_strt TIMESTAMP,
                pst_end TIMESTAMP,
                pst_detail TEXT,
                
                -- JK 특화 필드 (14~16)
                pr_maj TEXT,
                co_ind TEXT,
                co_hr TEXT,
                
                -- JKHD 특화 필드 (17~19)
                cl_info TEXT,
                wc TEXT,
                qual TEXT,
                
                -- SRM 특화 필드 (20~22)
                pr_item TEXT,
                rep TEXT,
                corp_addr TEXT,
                
                -- 메타데이터 (23~25)
                source VARCHAR(10),
                year INT,
                month INT
            );
        """)

        # 3. JK 데이터 삽입
        #
        # (1) INSERT 절에 merged_table 컬럼 29개 순서와
        # (2) SELECT 절에 가져오는 컬럼(또는 NULL)의 개수가 일치해야 합니다.
        cursor.execute("""
            INSERT INTO merged_table (
                pst_id, pst_title, co_nm, loc, snr, rnk, edu, typ, sal, time,
                pst_strt, pst_end, pst_detail,
                pr_maj, co_ind, co_hr,
                cl_info, wc, qual,
                pr_item, rep, corp_addr,
                source, year, month
            )
            SELECT 
                pst_id,
                pst_title,
                co_nm,
                loc,
                snr,
                rnk,
                edu,
                typ,
                sal,
                time,
                CASE 
                    WHEN pst_strt ~ E'^\\d{4}\\.\\d{2}\\.\\d{2}\\(\\w+\\)$' 
                         THEN TO_TIMESTAMP(regexp_replace(pst_strt, '\\(\\w+\\)$', ''), 'YYYY.MM.DD')
                    WHEN pst_strt ~ E'^\\d{4}\\.\\d{2}\\.\\d{2}\\s+\\d{2}:\\d{2}$'
                         THEN TO_TIMESTAMP(pst_strt, 'YYYY.MM.DD HH24:MI')
                    ELSE NULL
                END as pst_strt,
                CASE 
                    WHEN pst_end ~ E'^\\d{4}\\.\\d{2}\\.\\d{2}\\(\\w+\\)$' 
                         THEN TO_TIMESTAMP(regexp_replace(pst_end, '\\(\\w+\\)$', ''), 'YYYY.MM.DD')
                    WHEN pst_end ~ E'^\\d{4}\\.\\d{2}\\.\\d{2}\\s+\\d{2}:\\d{2}$'
                         THEN TO_TIMESTAMP(pst_end, 'YYYY.MM.DD HH24:MI')
                    ELSE NULL
                END as pst_end,
                pst_detail,
                pr_maj,
                co_ind,
                co_hr,
                NULL as cl_info,
                NULL as wc,
                NULL as qual,
                pr_item,
                NULL as rep,
                corp_addr,
                'jk' as source,
                year,
                month
            FROM jk
            WHERE LENGTH(pst_detail) > 10;
        """)

        # 4. JKHD 데이터 삽입
        cursor.execute("""
            INSERT INTO merged_table (
                pst_id, pst_title, co_nm, loc, snr, rnk, edu, typ, sal, time,
                pst_strt, pst_end, pst_detail,
                pr_maj, co_ind, co_hr,
                cl_info, wc, qual,
                pr_item, rep, corp_addr,
                source, year, month
            )
            SELECT 
                pst_id,
                NULL,      -- pst_title
                co_nm,
                NULL,      -- loc
                NULL,      -- snr
                NULL,      -- rnk
                NULL,      -- edu
                typ,
                NULL,      -- sal
                NULL,      -- time
                NULL::TIMESTAMP,  -- pst_strt
                NULL::TIMESTAMP,  -- pst_end
                pst_detail,
                NULL,      -- pr_maj
                NULL,      -- co_ind
                co_hr,
                cl_info,
                wc,
                qual,
                NULL,      -- pr_item
                NULL,      -- rep
                NULL,      -- corp_addr
                'jkhd' as source,
                year,
                month
            FROM jkhd
            WHERE LENGTH(pst_detail) > 10
              AND NOT EXISTS (
                SELECT 1 FROM merged_table m WHERE m.pst_id = jkhd.pst_id
              );
        """)

        # 5. SRM 데이터 삽입
        cursor.execute("""
            INSERT INTO merged_table (
                pst_id, pst_title, co_nm, loc, snr, rnk, edu, typ, sal, time,
                pst_strt, pst_end, pst_detail,
                pr_maj, co_ind, co_hr,
                cl_info, wc, qual,
                pr_item, rep, corp_addr,
                source, year, month
            )
            SELECT 
                pst_id,
                pst_title,
                co_nm,
                loc,
                snr,
                rnk,
                edu,
                typ,
                sal,
                time,
                CASE 
                    WHEN pst_strt ~ E'^\\d{4}\\.\\d{2}\\.\\d{2}\\(\\w+\\)$' 
                         THEN TO_TIMESTAMP(regexp_replace(pst_strt, '\\(\\w+\\)$', ''), 'YYYY.MM.DD')
                    WHEN pst_strt ~ E'^\\d{4}\\.\\d{2}\\.\\d{2}\\s+\\d{2}:\\d{2}$'
                         THEN TO_TIMESTAMP(pst_strt, 'YYYY.MM.DD HH24:MI')
                    ELSE NULL
                END as pst_strt,
                CASE 
                    WHEN pst_end ~ E'^\\d{4}\\.\\d{2}\\.\\d{2}\\(\\w+\\)$' 
                         THEN TO_TIMESTAMP(regexp_replace(pst_end, '\\(\\w+\\)$', ''), 'YYYY.MM.DD')
                    WHEN pst_end ~ E'^\\d{4}\\.\\d{2}\\.\\d{2}\\s+\\d{2}:\\d{2}$'
                         THEN TO_TIMESTAMP(pst_end, 'YYYY.MM.DD HH24:MI')
                    ELSE NULL
                END as pst_end,
                pst_detail,
                NULL,      -- pr_maj
                NULL,      -- co_ind
                NULL,      -- co_hr
                NULL,      -- cl_info
                NULL,      -- wc
                NULL,      -- qual
                pr_item,
                rep,
                corp_addr,
                'srm' as source,
                year,
                month
            FROM srm
            WHERE LENGTH(pst_detail) > 10
              AND NOT EXISTS (
                SELECT 1 FROM merged_table m WHERE m.pst_id = srm.pst_id
              );
        """)

        # 6. 인덱스 생성 (이미 존재하는 경우 생성하지 않음)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_merged_year_month ON merged_table(year, month);
            CREATE INDEX IF NOT EXISTS idx_merged_source ON merged_table(source);
            CREATE INDEX IF NOT EXISTS idx_merged_co_nm ON merged_table(co_nm);
        """)
        
        # 7. 커밋
        conn.commit()


def check_table_exists_and_has_data(table_name: str) -> bool:
    """
    테이블이 존재하고 데이터가 있는지 확인
    
    Args:
        table_name (str): 확인할 테이블 이름
        
    Returns:
        bool: 테이블이 존재하고 데이터가 있으면 True, 그렇지 않으면 False
    """
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        # 테이블 존재 여부 확인
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            return False
            
        # 데이터 존재 여부 확인
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return count > 0
        
    except Exception as e:
        logger.error(f"Error checking table {table_name}: {e}")
        return False

def create_backup_tables():
   try:
       conn = psycopg2.connect(**DB_PARAMS)
       cursor = conn.cursor()

       # 백업 테이블 생성
       for table in ['jk', 'srm', 'jkhd']:
           cursor.execute(f"""
               CREATE TABLE IF NOT EXISTS {table}_backup AS 
               SELECT * FROM {table};
           """)

       conn.commit()
       cursor.close()
       conn.close()
       print("Backup tables created successfully")

   except Exception as e:
       print(f"Error creating backup tables: {e}")


def run_csv_ingestion(load_jobs: bool = False,
                      load_companies: bool = False,
                      load_skills: bool = False,
                      test: bool = False,
                      force: bool = False):
    """
    ETL 프로세스를 실행하는 메인 함수.
    
    Args:
        load_jobs (bool): 구인공고 데이터 적재 여부
        load_companies (bool): 회사 레퍼런스 데이터 적재 여부
        load_skills (bool): 스킬 사전 데이터 적재 여부
        test (bool): True이면 각 파일당 최대 1000개의 샘플만 불러옵니다.
        force (bool): True이면 기존 데이터를 삭제하고 새로 적재합니다.
    """
    try:
        # 1. Job 데이터 적재
        if load_jobs:
            # 각 테이블(jk, jkhd, srm)에 대해 로딩
            for tbl in ["jk", "jkhd", "srm"]:
                logger.info(f"Loading job postings for {tbl}...")
                load_job_postings(tbl, DATA_PATHS[tbl], test=test, force=force)
        else:
            logger.info("Job postings ingestion skipped.")

        # 2. 회사 레퍼런스 적재
        if load_companies:
            load_company_reference(test=test)

        # 3. 스킬 사전 적재
        if load_skills:
            load_skill_dictionary(test=test)

        # 4. 병합 테이블 생성
        #    (병합 테이블 생성 및 중복 제거는 최종에 한 번만 해도 되므로,
        #     DB 연결해서 실행)
        if force or not check_table_exists_and_has_data('merged_table'):
            logger.info("Creating merged_table and merging data...")
            with psycopg2.connect(**DB_PARAMS) as merge_conn:
                create_merged_table(merge_conn)
        else:
            logger.info("merged_table already exists and has data. Skipping creation.")

        logger.info("CSV ingestion and processing completed successfully")

    except Exception as e:
        logger.error(f"Error during CSV ingestion: {e}")
        raise