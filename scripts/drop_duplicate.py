import logging
import time
import sys
import gc
import warnings
import resource
import argparse
from typing import Set, List, Tuple, Dict
import psycopg2
from elasticsearch import Elasticsearch
import pandas as pd
from tqdm import tqdm
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Elasticsearch 관련 로거 비활성화
logging.getLogger('elasticsearch').setLevel(logging.ERROR)
logging.getLogger('elastic_transport').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)

# PostgreSQL 연결 정보
DB_PARAMS = {
    "dbname": "your_db_name",
    "user": "your_user_name",
    "password": "your_password",
    "host": "your_host_name", 
    "port": "5432"
}

# Elasticsearch 설정
ES_CONFIG = {
    "hosts": ["http://elasticsearch:9200"],
    "request_timeout": 60,
    "max_retries": 5,
    "retry_on_timeout": True,
    "http_compress": True,
    "sniff_on_start": False,
    "sniff_on_connection_fail": False,
    "sniffer_timeout": 0
}

# 중복 탐지 임계값
SIMILARITY_THRESHOLD = 0.95  # 정밀 중복 검출 임계값 상향

# 배치 크기
BATCH_SIZE = 5000  # 정확한 중복 제거를 위한 큰 배치 
DELETE_BATCH_SIZE = 10000  # 삭제 일괄 처리 증가
MAX_DUPLICATES_BEFORE_DELETE = 5000  # 중간 삭제 임계값 증가

# 병렬 처리 설정
MAX_WORKERS = 14

# tqdm 출력을 로거로 리디렉션하는 클래스
class TqdmToLogger:
    def __init__(self, logger, level=logging.INFO):
        self.logger = logger
        self.level = level
        self.last_msg = ""
        self.last_print_time = 0
        self.min_interval = 5  # 최소 로깅 간격(초)

    def write(self, buf):
        buf = buf.strip()
        if buf and buf != self.last_msg and time.time() - self.last_print_time >= self.min_interval:
            self.logger.log(self.level, buf)
            self.last_msg = buf
            self.last_print_time = time.time()

    def flush(self):
        pass

# 로깅에 최적화된 진행 표시줄 생성
def create_progress_bar(total, desc, unit="건"):
    tqdm_out = TqdmToLogger(logger)
    return tqdm(
        total=total, 
        desc=desc, 
        unit=unit,
        bar_format='{desc}: |{bar}| {percentage:3.1f}% [{n_fmt}/{total_fmt}] {rate_fmt} 예상: {remaining}',
        file=tqdm_out,
        mininterval=5.0
    )

# 리소스 제한 해제 (Linux 환경에서만 작동)
def disable_memory_limits():
    try:
        resource.setrlimit(resource.RLIMIT_AS, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
        resource.setrlimit(resource.RLIMIT_DATA, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
        logger.info("메모리 제한 해제 완료")
    except (ValueError, resource.error):
        logger.warning("메모리 제한을 해제할 수 없습니다. 플랫폼 제한이 유지됩니다.")
    except NameError:
        logger.warning("Windows 환경에서는 메모리 제한 해제가 지원되지 않습니다.")

# 텍스트 일부만 추출하여 비교에 사용
def get_text_preview(text, max_length=2000):
    if text is None:
        return ""
    return text[:max_length]

@contextmanager
def get_db_connection():
    """데이터베이스 연결 컨텍스트 매니저"""
    conn = psycopg2.connect(**DB_PARAMS)
    try:
        yield conn
    finally:
        conn.close()

def setup_elasticsearch() -> Elasticsearch:
    """Elasticsearch 설정 및 연결"""
    es = Elasticsearch(**ES_CONFIG)
    if not es.ping():
        raise ConnectionError("Elasticsearch 연결 실패")
    return es

def check_elasticsearch_index(es: Elasticsearch) -> bool:
    """Elasticsearch 인덱스 확인"""
    return es.indices.exists(index="job_posts")

def verify_index_content(es: Elasticsearch) -> bool:
    """인덱스 내용 검증"""
    try:
        count = es.count(index="job_posts")
        if count["count"] == 0:
            return False
        
        result = es.search(index="job_posts", body={"query": {"match_all": {}}, "size": 1})
        return result["hits"]["total"]["value"] > 0
    except Exception as e:
        logger.error(f"인덱스 검증 오류: {e}")
        return False

def remove_exact_duplicates_by_month(conn, month: int) -> int:
    """단일 월 내 정확한 중복 제거 (PostgreSQL만 사용)"""
    logger.info(f"월:{month} - 정확한 중복 검색 시작 (PostgreSQL)")
    
    # 해당 월 내 전체 레코드 수 확인
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM merged_table WHERE month = %s", (month,))
        total_records = cursor.fetchone()[0]
    
    if total_records == 0:
        logger.info(f"월:{month} - 데이터 없음, 건너뜀")
        return 0
    
    # 해당 월 내 소스 목록 확인
    with conn.cursor() as cursor:
        cursor.execute("SELECT DISTINCT source FROM merged_table WHERE month = %s", (month,))
        sources = [row[0] for row in cursor.fetchall()]
    
    if len(sources) <= 1:
        logger.info(f"월:{month} - 소스가 1개 이하, 건너뜀")
        return 0
    
    # srm 소스를 우선으로 설정 (있는 경우)
    priority_source = "srm" if "srm" in sources else sources[0]
    logger.info(f"월:{month} - 우선 소스: {priority_source}")
    
    # 정확한 중복 제거를 위한 임시 테이블 생성 및 중복 식별
    with conn.cursor() as cursor:
        # 임시 테이블 생성
        cursor.execute("""
        CREATE TEMP TABLE exact_duplicates_month AS
        WITH ranked_records AS (
            SELECT 
                pst_id,
                md5(pst_detail) AS content_hash,
                source,
                ROW_NUMBER() OVER (
                    PARTITION BY md5(pst_detail)
                    ORDER BY 
                        CASE WHEN source = %s THEN 0 ELSE 1 END,
                        source,
                        pst_id
                ) AS row_num
            FROM merged_table
            WHERE month = %s AND pst_detail IS NOT NULL
        )
        SELECT pst_id
        FROM ranked_records
        WHERE row_num > 1
        """, (priority_source, month))
        
        # 중복 수 가져오기
        cursor.execute("SELECT COUNT(*) FROM exact_duplicates_month")
        duplicate_count = cursor.fetchone()[0]
        
        if duplicate_count > 0:
            # 중복 제거
            cursor.execute("""
            DELETE FROM merged_table 
            WHERE pst_id IN (SELECT pst_id FROM exact_duplicates_month)
            """)
            
            # 임시 테이블 삭제
            cursor.execute("DROP TABLE exact_duplicates_month")
            
            # 변경사항 커밋
            conn.commit()
            
            logger.info(f"월:{month} - {duplicate_count:,}개 정확한 중복 제거됨 ({duplicate_count/total_records*100:.1f}%)")
        else:
            cursor.execute("DROP TABLE exact_duplicates_month")
            logger.info(f"월:{month} - 정확한 중복 없음")
    
    return duplicate_count

def remove_exact_duplicates_across_months(conn, source: str) -> int:
    """단일 소스 내 월간 정확한 중복 제거 (PostgreSQL만 사용)"""
    logger.info(f"소스:{source} - 월 간 정확한 중복 검색 시작 (PostgreSQL)")
    
    # 해당 소스의 월 목록 가져오기
    with conn.cursor() as cursor:
        cursor.execute("SELECT DISTINCT month FROM merged_table WHERE source = %s ORDER BY month", (source,))
        months = [row[0] for row in cursor.fetchall() if row[0] is not None]
    
    if len(months) <= 1:
        logger.info(f"소스:{source} - 월이 1개 이하, 건너뜀")
        return 0
    
    # 전체 소스 레코드 수
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM merged_table WHERE source = %s", (source,))
        total_records = cursor.fetchone()[0]
    
    # 월간 중복 제거 수행
    with conn.cursor() as cursor:
        # 임시 테이블 생성 및 중복 식별
        cursor.execute("""
        CREATE TEMP TABLE exact_duplicates_source AS
        WITH ranked_records AS (
            SELECT 
                pst_id,
                md5(pst_detail) AS content_hash,
                month,
                ROW_NUMBER() OVER (
                    PARTITION BY md5(pst_detail)
                    ORDER BY month, pst_id
                ) AS row_num
            FROM merged_table
            WHERE source = %s AND pst_detail IS NOT NULL
        )
        SELECT pst_id
        FROM ranked_records
        WHERE row_num > 1
        """, (source,))
        
        # 중복 수 가져오기
        cursor.execute("SELECT COUNT(*) FROM exact_duplicates_source")
        duplicate_count = cursor.fetchone()[0]
        
        if duplicate_count > 0:
            # 중복 제거
            cursor.execute("""
            DELETE FROM merged_table 
            WHERE pst_id IN (SELECT pst_id FROM exact_duplicates_source)
            """)
            
            # 임시 테이블 삭제
            cursor.execute("DROP TABLE exact_duplicates_source")
            
            # 변경사항 커밋
            conn.commit()
            
            logger.info(f"소스:{source} - {duplicate_count:,}개 정확한 월간 중복 제거됨 ({duplicate_count/total_records*100:.1f}%)")
        else:
            cursor.execute("DROP TABLE exact_duplicates_source")
            logger.info(f"소스:{source} - 정확한 월간 중복 없음")
    
    return duplicate_count

def process_month_fuzzy_duplicates(es: Elasticsearch, conn, month: int) -> int:
    """단일 월 내 모든 소스 간 유사도 기반 중복 처리 (Elasticsearch 사용)"""
    logger.info(f"월:{month} - 유사도 기반 소스 간 중복 검색 시작")
    
    # 해당 월 내 전체 레코드 수 확인
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM merged_table WHERE month = %s", (month,))
        total_records = cursor.fetchone()[0]
    
    if total_records == 0:
        logger.info(f"월:{month} - 데이터 없음, 건너뜀")
        return 0
    
    # 해당 월 내 소스 목록 확인
    with conn.cursor() as cursor:
        cursor.execute("SELECT DISTINCT source FROM merged_table WHERE month = %s", (month,))
        sources = [row[0] for row in cursor.fetchall()]
    
    if len(sources) <= 1:
        logger.info(f"월:{month} - 소스가 1개 이하, 건너뜀")
        return 0
    
    # srm 소스를 우선으로 설정 (있는 경우)
    priority_source = "srm" if "srm" in sources else sources[0]
    logger.info(f"월:{month} - 우선 소스: {priority_source}")
    
    # 중복 제거 로직
    duplicates = set()
    batch_size = 100
    
    # 진행률 표시
    progress_bar = create_progress_bar(total_records, f"월:{month} 유사도 중복 검색")
    
    # 배치 처리를 위한 오프셋
    offset = 0
    
    while offset < total_records:
        # 배치 데이터 가져오기
        query = """
        SELECT pst_id, pst_detail, source
        FROM merged_table
        WHERE month = %s
        ORDER BY 
            CASE WHEN source = %s THEN 0 ELSE 1 END,  -- 우선 소스 먼저
            source, 
            pst_id
        LIMIT %s OFFSET %s
        """
        
        with conn.cursor() as cursor:
            cursor.execute(query, (month, priority_source, batch_size, offset))
            batch_records = cursor.fetchall()
        
        if not batch_records:
            break
        
        # 처리된 문서 ID 추적
        processed_ids = set()
        
        # 배치 내에서 비교
        for i, (pst_id, pst_detail, source) in enumerate(batch_records):
            # 이미 처리된 문서는 건너뜀
            if pst_id in processed_ids:
                continue
            
            # 현재 문서를 처리된 것으로 표시
            processed_ids.add(pst_id)
            
            # 우선 소스면 다른 소스에서 중복 찾음
            is_priority = (source == priority_source)
            
            # 텍스트 미리보기만 사용
            preview_text = get_text_preview(pst_detail)
            
            # 검색 쿼리: 현재 문서와 유사한 문서 검색
            search_query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {  # more_like_this 대신 match 사용
                                    "pst_detail": preview_text
                                }
                            }
                        ],
                        "must_not": [{"term": {"pst_id": pst_id}}],
                        "filter": [
                            {"term": {"month": month}}
                        ]
                    }
                },
                "_source": ["pst_id", "source"],
                "size": 10
            }
            
            # 같은 월 내 검색
            try:
                response = es.search(
                    index="job_posts", 
                    body=search_query,
                    request_timeout=30
                )
                
                # 중복 처리
                for hit in response["hits"]["hits"]:
                    if hit["_score"] > SIMILARITY_THRESHOLD:
                        hit_source = hit["_source"].get("source", "")
                        hit_id = hit["_source"]["pst_id"]
                        
                        # 우선 소스에서 비교하는 경우 다른 소스의 중복 표시
                        if is_priority:
                            duplicates.add(hit_id)
                            processed_ids.add(hit_id)
                        # 비우선 소스에서 비교하는 경우 우선 소스의 중복은 보존
                        elif hit_source != priority_source:
                            duplicates.add(hit_id)
                            processed_ids.add(hit_id)
            
            except Exception as e:
                logger.warning(f"검색 오류: {e}")
            
        # 진행률 업데이트
        progress_bar.update(len(batch_records))
        
        # 다음 배치로 이동
        offset += batch_size
        
        # 메모리 정리
        gc.collect()
        
        # 일정 개수 이상 중복 발견시 중간 삭제
        if len(duplicates) >= MAX_DUPLICATES_BEFORE_DELETE:
            # 중복 삭제
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM merged_table WHERE pst_id = ANY(%s)", (list(duplicates),))
            conn.commit()
            
            logger.info(f"월:{month} - 중간 {len(duplicates):,}개 유사도 중복 제거")
            duplicates = set()
    
    progress_bar.close()
    
    # 남은 중복 삭제
    if duplicates:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM merged_table WHERE pst_id = ANY(%s)", (list(duplicates),))
        conn.commit()
        logger.info(f"월:{month} - 최종 {len(duplicates):,}개 유사도 중복 제거")
    
    # 월별 중복 제거 후 총 제거된 개수 반환
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM merged_table WHERE month = %s", (month,))
        new_count = cursor.fetchone()[0]
        
    removed = total_records - new_count
    logger.info(f"월:{month} - 총 {removed:,}개 유사도 중복 제거됨 ({removed/total_records*100:.1f}%)")
    
    return removed

def process_source_fuzzy_months(es: Elasticsearch, conn, source: str) -> int:
    """단일 소스 내 인접 월 간 유사도 기반 중복 처리 (Elasticsearch 사용)"""
    logger.info(f"소스:{source} - 월 간 유사도 중복 검색 시작")
    
    # 해당 소스의 월 목록 가져오기
    with conn.cursor() as cursor:
        cursor.execute("SELECT DISTINCT month FROM merged_table WHERE source = %s ORDER BY month", (source,))
        months = [row[0] for row in cursor.fetchall() if row[0] is not None]
    
    if len(months) <= 1:
        logger.info(f"소스:{source} - 월이 1개 이하, 건너뜀")
        return 0
    
    # 전체 소스 레코드 수
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM merged_table WHERE source = %s", (source,))
        total_records = cursor.fetchone()[0]
    
    # 중복 제거 추적
    removed_count = 0
    
    # 진행률 표시
    progress_bar = create_progress_bar(total_records, f"소스:{source} 월간 유사도 중복 검색")
    
    # 진행 상황 추적
    processed = 0
    
    # 각 월을 다른 모든 월과 비교 (인접 월에 한정하지 않음)
    for i, current_month in enumerate(months):
        # 현재 월의 문서 수
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM merged_table WHERE source = %s AND month = %s", 
                         (source, current_month))
            current_month_count = cursor.fetchone()[0]
        
        if current_month_count == 0:
            continue
        
        # 다른 모든 월과 비교할 것이므로, 전체 다른 월 목록 구성
        other_months = [m for m in months if m != current_month]
        
        # 배치 처리
        batch_size = 50
        offset = 0
        
        # 현재 월 배치 처리
        while offset < current_month_count:
            # 배치 데이터 가져오기
            query = """
            SELECT pst_id, pst_detail
            FROM merged_table
            WHERE source = %s AND month = %s
            ORDER BY pst_id
            LIMIT %s OFFSET %s
            """
            
            with conn.cursor() as cursor:
                cursor.execute(query, (source, current_month, batch_size, offset))
                batch_records = cursor.fetchall()
            
            if not batch_records:
                break
            
            # 배치 내 각 문서 처리
            duplicates = set()
            
            for pst_id, pst_detail in batch_records:
                # 텍스트 미리보기만 사용
                preview_text = get_text_preview(pst_detail)
                
                # 검색 쿼리: 현재 문서와 유사하고 다른 월에 있는 문서 검색
                search_query = {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "match": {
                                        "pst_detail": preview_text
                                    }
                                }
                            ],
                            "must_not": [{"term": {"pst_id": pst_id}}],
                            "filter": [
                                {"term": {"source": source}},
                                {"terms": {"month": other_months}}
                            ]
                        }
                    },
                    "_source": ["pst_id", "month"],
                    "size": 5
                }
                
                try:
                    response = es.search(
                        index="job_posts", 
                        body=search_query,
                        request_timeout=30
                    )
                    
                    # 중복 문서 ID 수집 - 나중 월의 것만 중복으로 간주
                    for hit in response["hits"]["hits"]:
                        if hit["_score"] > SIMILARITY_THRESHOLD:
                            hit_month = hit["_source"].get("month", 0)
                            hit_id = hit["_source"]["pst_id"]
                            
                            # 나중 월의 것만 중복으로 처리
                            if hit_month > current_month:
                                duplicates.add(hit_id)
                
                except Exception as e:
                    logger.warning(f"검색 오류: {e}")
            
            # 진행률 업데이트
            processed += len(batch_records)
            progress_bar.update(len(batch_records))
            
            # 중복 삭제
            if duplicates:
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM merged_table WHERE pst_id = ANY(%s)", (list(duplicates),))
                conn.commit()
                
                removed_count += len(duplicates)
                
                # 로깅
                if len(duplicates) > 10:
                    logger.info(f"소스:{source} 월:{current_month} - {len(duplicates):,}개 유사도 중복 제거")
            
            # 다음 배치로 이동
            offset += batch_size
            
            # 메모리 정리
            gc.collect()
    
    progress_bar.close()
    
    logger.info(f"소스:{source} - 총 {removed_count:,}개 유사도 중복 제거됨")
    return removed_count

def drop_duplicates(enable_fuzzy_dedup=False):
    """중복 제거 프로세스 실행
    
    Args:
        enable_fuzzy_dedup (bool): Elasticsearch를 사용한 유사도 기반 중복 제거 활성화 여부.
                                   기본값은 False로, 정확한 중복만 제거합니다.
    """
    try:
        logger.info("중복 제거 프로세스 시작")
        if enable_fuzzy_dedup:
            logger.info("정밀 중복 제거 (Elasticsearch 유사도 비교) 활성화됨")
        else:
            logger.info("정밀 중복 제거 비활성화 - 정확한 중복만 제거합니다")
            
        start_time = time.time()
        
        # 메모리 제한 해제
        disable_memory_limits()
        
        # 경고 무시 설정
        warnings.filterwarnings('ignore', category=ResourceWarning)
        
        # Elasticsearch 설정 (정밀 중복 제거 사용 시에만)
        es = None
        if enable_fuzzy_dedup:
            es = setup_elasticsearch()
            logger.info("Elasticsearch 연결 성공")
            
            # 인덱스 검증
            if not check_elasticsearch_index(es):
                raise RuntimeError("Elasticsearch 인덱스 'job_posts'가 없습니다. 인덱스 생성을 먼저 실행하세요.")
                
            if not verify_index_content(es):
                raise RuntimeError("Elasticsearch 인덱스가 비어있거나 유효하지 않습니다.")
            logger.info("Elasticsearch 인덱스 검증 완료")
        
        with get_db_connection() as conn:
            try:
                # 시작 전 레코드 수 확인
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM merged_table")
                    start_count = cursor.fetchone()[0]
                    logger.info(f"처리 시작: 총 {start_count:,}개 레코드")
                
                #----- 1단계: 월별 정확한 중복 제거 (PostgreSQL) -----
                logger.info("1단계: 월별 정확한 중복 제거 시작 (PostgreSQL)")
                
                # 모든 월 정보 가져오기
                with conn.cursor() as cursor:
                    cursor.execute("SELECT DISTINCT month FROM merged_table WHERE month IS NOT NULL ORDER BY month")
                    month_list = [row[0] for row in cursor.fetchall()]
                
                logger.info(f"총 {len(month_list)}개 월 병렬 처리 시작")
                
                # 병렬 처리 함수
                def remove_month_duplicates_with_new_connection(month):
                    with get_db_connection() as thread_conn:
                        return remove_exact_duplicates_by_month(thread_conn, month)
                
                # 월별 병렬 처리
                exact_stage1_removed = 0
                with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(month_list))) as executor:
                    # 각 월별로 작업 제출
                    futures = {}
                    for month in month_list:
                        future = executor.submit(remove_month_duplicates_with_new_connection, month)
                        futures[future] = month
                    
                    # 결과 수집
                    for future in as_completed(futures):
                        month = futures[future]
                        try:
                            removed = future.result()
                            exact_stage1_removed += removed
                            if removed > 0:
                                logger.info(f"월:{month} - 병렬 처리 완료, {removed:,}개 정확한 중복 제거")
                        except Exception as e:
                            logger.error(f"월:{month} 처리 중 오류: {e}")
                
                # 1단계 후 중간 통계
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM merged_table")
                    after_stage1_count = cursor.fetchone()[0]
                    exact_stage1_removed_count = start_count - after_stage1_count
                    
                    logger.info(f"1단계 완료: {exact_stage1_removed_count:,}개 정확한 중복 제거됨 ({exact_stage1_removed_count/start_count*100:.1f}%)")
                    logger.info(f"남은 레코드 수: {after_stage1_count:,}개")
                
                #----- 2단계: 소스별 월간 정확한 중복 제거 (PostgreSQL) -----
                logger.info("2단계: 소스별 월간 정확한 중복 제거 시작 (PostgreSQL)")
                
                # 모든 소스 목록 가져오기
                with conn.cursor() as cursor:
                    cursor.execute("SELECT DISTINCT source FROM merged_table ORDER BY source")
                    source_list = [row[0] for row in cursor.fetchall()]
                
                logger.info(f"총 {len(source_list)}개 소스 병렬 처리 시작")
                
                # 소스별 병렬 처리 함수
                def remove_source_duplicates_with_new_connection(source):
                    with get_db_connection() as thread_conn:
                        return remove_exact_duplicates_across_months(thread_conn, source)
                
                # 소스별 병렬 처리
                exact_stage2_removed = 0
                with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(source_list))) as executor:
                    # 각 소스별로 작업 제출
                    futures = {}
                    for source in source_list:
                        future = executor.submit(remove_source_duplicates_with_new_connection, source)
                        futures[future] = source
                    
                    # 결과 수집
                    for future in as_completed(futures):
                        source = futures[future]
                        try:
                            removed = future.result()
                            exact_stage2_removed += removed
                            if removed > 0:
                                logger.info(f"소스:{source} - 병렬 처리 완료, {removed:,}개 정확한 월간 중복 제거")
                        except Exception as e:
                            logger.error(f"소스:{source} 처리 중 오류: {e}")
                
                # 2단계 후 중간 통계
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM merged_table")
                    after_stage2_count = cursor.fetchone()[0]
                    exact_stage2_removed_count = after_stage1_count - after_stage2_count
                    total_exact_removed = start_count - after_stage2_count
                    
                    logger.info(f"2단계 완료: {exact_stage2_removed_count:,}개 정확한 월간 중복 제거됨 ({exact_stage2_removed_count/after_stage1_count*100:.1f}%)")
                    logger.info(f"정확한 중복 제거 총량: {total_exact_removed:,}개 ({total_exact_removed/start_count*100:.1f}%)")
                    logger.info(f"남은 레코드 수: {after_stage2_count:,}개")
                
                # 정밀 중복 제거가 비활성화된 경우 여기서 종료
                if not enable_fuzzy_dedup:
                    logger.info("정밀 중복 제거가 비활성화되어 있습니다. 정확한 중복 제거만 완료했습니다.")
                    
                    # 소스별 통계 - 새로운 커서 사용
                    with conn.cursor() as stat_cursor:
                        stat_cursor.execute("SELECT source, COUNT(*) FROM merged_table GROUP BY source ORDER BY COUNT(*) DESC")
                        source_stats = stat_cursor.fetchall()
                        logger.info("소스별 레코드 분포:")
                        for source, count in source_stats:
                            logger.info(f"- {source}: {count:,}개 ({count/after_stage2_count*100:.1f}%)")
                    
                    elapsed_time = time.time() - start_time
                    minutes, seconds = divmod(elapsed_time, 60)
                    hours, minutes = divmod(minutes, 60)
                    
                    if hours > 0:
                        time_str = f"{int(hours)}시간 {int(minutes)}분 {int(seconds)}초"
                    elif minutes > 0:
                        time_str = f"{int(minutes)}분 {int(seconds)}초"
                    else:
                        time_str = f"{elapsed_time:.1f}초"
                    
                    logger.info(f"중복 제거 프로세스 완료 (총 소요 시간: {time_str})")
                    return True
                
                #----- 3단계: 월별 유사도 기반 중복 제거 (Elasticsearch) -----
                logger.info("3단계: 월별 유사도 기반 중복 제거 시작 (Elasticsearch)")
                fuzzy_stage1_removed = 0
                
                # 월별 병렬 처리 함수
                def process_month_fuzzy_with_new_connection(month):
                    with get_db_connection() as thread_conn:
                        return process_month_fuzzy_duplicates(es, thread_conn, month)
                
                # 월별 병렬 처리
                with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(month_list))) as executor:
                    # 각 월별로 작업 제출
                    futures = {}
                    for month in month_list:
                        future = executor.submit(process_month_fuzzy_with_new_connection, month)
                        futures[future] = month
                    
                    # 결과 수집
                    for future in as_completed(futures):
                        month = futures[future]
                        try:
                            removed = future.result()
                            fuzzy_stage1_removed += removed
                            if removed > 0:
                                logger.info(f"월:{month} - 유사도 병렬 처리 완료, {removed:,}개 유사도 중복 제거")
                        except Exception as e:
                            logger.error(f"월:{month} 유사도 처리 중 오류: {e}")
                
                # 3단계 후 중간 통계
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM merged_table")
                    after_stage3_count = cursor.fetchone()[0]
                    fuzzy_stage1_removed_count = after_stage2_count - after_stage3_count
                    
                    logger.info(f"3단계 완료: {fuzzy_stage1_removed_count:,}개 유사도 기반 중복 제거됨 ({fuzzy_stage1_removed_count/after_stage2_count*100:.1f}%)")
                    logger.info(f"남은 레코드 수: {after_stage3_count:,}개")
                
                #----- 4단계: 소스별 월간 유사도 기반 중복 제거 (Elasticsearch) -----
                logger.info("4단계: 소스별 월간 유사도 기반 중복 제거 시작 (Elasticsearch)")
                fuzzy_stage2_removed = 0
                
                # 소스별 병렬 처리 함수
                def process_source_fuzzy_with_new_connection(source):
                    with get_db_connection() as thread_conn:
                        return process_source_fuzzy_months(es, thread_conn, source)
                
                # 소스별 병렬 처리
                with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(source_list))) as executor:
                    # 각 소스별로 작업 제출
                    futures = {}
                    for source in source_list:
                        future = executor.submit(process_source_fuzzy_with_new_connection, source)
                        futures[future] = source
                    
                    # 결과 수집
                    for future in as_completed(futures):
                        source = futures[future]
                        try:
                            removed = future.result()
                            fuzzy_stage2_removed += removed
                            if removed > 0:
                                logger.info(f"소스:{source} - 유사도 병렬 처리 완료, {removed:,}개 유사도 월간 중복 제거")
                        except Exception as e:
                            logger.error(f"소스:{source} 유사도 처리 중 오류: {e}")
                
                # 최종 통계
                with conn.cursor() as final_cursor:
                    final_cursor.execute("SELECT COUNT(*) FROM merged_table")
                    final_count = final_cursor.fetchone()[0]
                    fuzzy_stage2_removed_count = after_stage3_count - final_count
                    total_fuzzy_removed = after_stage2_count - final_count
                    total_removed = start_count - final_count
                    
                    logger.info(f"4단계 완료: {fuzzy_stage2_removed_count:,}개 유사도 월간 중복 제거됨 ({fuzzy_stage2_removed_count/after_stage3_count*100:.1f}%)")
                    
                    logger.info(f"처리 결과 요약:")
                    logger.info(f"- 최초 레코드 수: {start_count:,}개")
                    logger.info(f"- 정확한 중복 제거 (월별): {exact_stage1_removed_count:,}개 ({exact_stage1_removed_count/start_count*100:.1f}%)")
                    logger.info(f"- 정확한 중복 제거 (소스별 월간): {exact_stage2_removed_count:,}개 ({exact_stage2_removed_count/after_stage1_count*100:.1f}%)")
                    logger.info(f"- 유사도 중복 제거 (월별): {fuzzy_stage1_removed_count:,}개 ({fuzzy_stage1_removed_count/after_stage2_count*100:.1f}%)")
                    logger.info(f"- 유사도 중복 제거 (소스별 월간): {fuzzy_stage2_removed_count:,}개 ({fuzzy_stage2_removed_count/after_stage3_count*100:.1f}%)")
                    logger.info(f"- 총 제거된 중복: {total_removed:,}개 ({total_removed/start_count*100:.1f}%)")
                    logger.info(f"  - 정확한 중복: {total_exact_removed:,}개 ({total_exact_removed/start_count*100:.1f}%)")
                    logger.info(f"  - 유사도 중복: {total_fuzzy_removed:,}개 ({total_fuzzy_removed/after_stage2_count*100:.1f}%)")
                    logger.info(f"- 최종 레코드 수: {final_count:,}개")
                    
                    # 소스별 통계
                    final_cursor.execute("SELECT source, COUNT(*) FROM merged_table GROUP BY source ORDER BY COUNT(*) DESC")
                    source_stats = final_cursor.fetchall()
                    logger.info("소스별 레코드 분포:")
                    for source, count in source_stats:
                        logger.info(f"- {source}: {count:,}개 ({count/final_count*100:.1f}%)")
                
            except Exception as e:
                logger.error(f"데이터베이스 작업 중 오류 발생: {e}")
                raise
        
        elapsed_time = time.time() - start_time
        minutes, seconds = divmod(elapsed_time, 60)
        hours, minutes = divmod(minutes, 60)
        
        if hours > 0:
            time_str = f"{int(hours)}시간 {int(minutes)}분 {int(seconds)}초"
        elif minutes > 0:
            time_str = f"{int(minutes)}분 {int(seconds)}초"
        else:
            time_str = f"{elapsed_time:.1f}초"
        
        logger.info(f"중복 제거 프로세스 완료 (총 소요 시간: {time_str})")
        return True
        
    except Exception as e:
        logger.error(f"중복 제거 프로세스 오류: {e}")
        raise