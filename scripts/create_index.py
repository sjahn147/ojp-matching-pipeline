import logging
from typing import Dict, Any
import psycopg2
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
import pandas as pd
import numpy as np
import time
from tqdm import tqdm

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# PostgreSQL 연결 정보
DB_PARAMS = {
    "dbname": "your_db_name",
    "user": "your_user_name",
    "password": "your_password",
    "host": "your_host_name", 
    "port": "5432"
}

# Elasticsearch 설정 - 타임아웃 대폭 증가
ES_CONFIG = {
    "hosts": ["http://elasticsearch:9200"],
    "max_retries": 20,              # 재시도 횟수 증가
    "retry_on_timeout": True,
    "timeout": 60                  # 기본 타임아웃도 증가
}

# 인덱스 설정
INDEX_SETTINGS = {
    "job_posts": {
        "settings": {
            "index": {
                "number_of_shards": 3,                  # 샤드 수 증가
                "number_of_replicas": 0,                # 복제본 제거
                "refresh_interval": "60s",              # 리프레시 간격 늘림
                "mapping.ignore_malformed": True,       # 일부 필드 오류 무시
                "mapping.total_fields.limit": 2000,     # 필드 수 제한 증가
                "blocks.read_only_allow_delete": False, # 읽기 전용 모드 방지
                "codec": "best_compression",            # 데이터 압축으로 디스크 공간 절약
                "routing.allocation.disk.threshold_enabled": False  # 디스크 임계값 비활성화
            },
            "analysis": {
                "analyzer": {
                    "job_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "stop", "trim"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                # 1~13 공통 필드
                "pst_id": {"type": "keyword"},
                "pst_title": {"type": "text"},
                "co_nm": {
                    "type": "text",
                    "fields": {
                        "raw": {"type": "keyword"}
                    }
                },
                "loc": {"type": "text"},
                "snr": {"type": "keyword"},
                "rnk": {"type": "keyword"},
                "edu": {"type": "keyword"},
                "typ": {"type": "keyword"},
                "sal": {"type": "text"},
                "time": {"type": "text"},
                "pst_strt": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                "pst_end": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                "pst_detail": {
                    "type": "text",
                    "analyzer": "job_analyzer",
                    # raw 필드 제거
                },
                # 14~16 (JK 특화)
                "pr_maj": {"type": "text"},
                "co_ind": {"type": "text"},
                "co_hr": {"type": "text"},
                # 17~19 (JKHD 특화)
                "cl_info": {"type": "text"},
                "wc": {"type": "text"},
                "qual": {"type": "text"},
                # 20~22 (SRM 특화)
                "pr_item": {"type": "text"},
                "rep": {"type": "text"},
                "corp_addr": {"type": "text"},
                # 23~25 (메타데이터)
                "source": {"type": "keyword"},
                "year": {"type": "integer"},
                "month": {"type": "integer"},
            }
        }
    },
    "company_info": {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "60s",
                "blocks.read_only_allow_delete": False,
                "codec": "best_compression",
                "routing.allocation.disk.threshold_enabled": False
            },
            "analysis": {
                "analyzer": {
                    "company_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "stop", "trim"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "bzno": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "analyzer": "company_analyzer",
                    "fields": {
                        "raw": {"type": "keyword"}
                    }
                },
                "address": {"type": "text"},
                "phone": {"type": "keyword"},
                "region": {"type": "keyword"},
                "bonid": {"type": "keyword"},
                "bon_add": {"type": "text"},
                "bon_address": {"type": "text"},
                "bon_phone": {"type": "keyword"},
                "bon_title": {
                    "type": "text",
                    "fields": {
                        "raw": {"type": "keyword"}
                    }
                },
                "bon_info": {"type": "text"},
                "ksic": {"type": "keyword"},
                "ksic9": {"type": "keyword"}                
            }
        }
    }
}

def preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Elasticsearch로 인덱싱하기 전에 데이터 정리"""
    df = df.replace({np.nan: None})  # NaN을 None으로 변환

    df["year"] = df["year"].fillna(0).astype(int)  # NaN → 0 변환 후 정수형 변환
    df["month"] = df["month"].fillna(0).astype(int)

    # 문자열 필드의 NaN을 빈 문자열("")로 변환
    text_columns = [
        "pst_title", "co_nm", "loc", "snr", "rnk", "edu", "typ", "sal", "time",
        "pst_detail", "pr_maj", "co_ind", "co_hr", "cl_info", "wc", "qual",
        "pr_item", "rep", "corp_addr", "source"
    ]
    for col in text_columns:
        df[col] = df[col].fillna("")  # NaN → 빈 문자열

    # 날짜 필드 변환 (None → `null`, NaN → `null`)
    date_columns = ["pst_strt", "pst_end"]
    for col in date_columns:
        df[col] = df[col].apply(lambda x: None if pd.isna(x) or x in ["None", "nan", "NaT"] else x)
    
    # 긴 텍스트 필드 자르기 (32KB 이상)
    if "pst_detail" in df.columns:
        # 32KB 초과하는 텍스트 필드를 32KB로 제한
        max_length = 32000  # 안전하게 32000자로 설정
        df["pst_detail"] = df["pst_detail"].apply(
            lambda x: x[:max_length] if isinstance(x, str) and len(x) > max_length else x
        )

    return df

def setup_elasticsearch() -> Elasticsearch:
    """Elasticsearch 연결 설정 및 디스크 공간 부족 문제 해결"""
    es = Elasticsearch(**ES_CONFIG)
    if not es.ping():
        raise ConnectionError("Could not connect to Elasticsearch")

    # 모든 디스크 관련 제한 비활성화
    try:
        # 클러스터 설정 업데이트
        es.cluster.put_settings(
            body={
                "persistent": {
                    "cluster.routing.allocation.disk.threshold_enabled": False,
                    "cluster.routing.allocation.disk.watermark.low": "95%",
                    "cluster.routing.allocation.disk.watermark.high": "97%",
                    "cluster.routing.allocation.disk.watermark.flood_stage": "99%"
                }
            }
        )
        
        # 읽기 전용 모드 해제
        es.indices.put_settings(
            index="_all",
            body={"index.blocks.read_only_allow_delete": None}
        )
        logger.info("디스크 공간 관련 제한 해제 완료")
    except Exception as e:
        logger.warning(f"디스크 공간 관련 설정 변경 실패: {e}")

    return es

def create_indices(es: Elasticsearch, force: bool = False) -> None:
    """인덱스 생성"""
    for index_name, settings in INDEX_SETTINGS.items():
        if force and es.indices.exists(index=index_name):
            logger.info(f"기존 인덱스 삭제 중: {index_name}")
            es.indices.delete(index=index_name)
            
        if not es.indices.exists(index=index_name):
            logger.info(f"인덱스 생성 중: {index_name}")
            es.indices.create(index=index_name, body=settings)
        else:
            logger.info(f"인덱스가 이미 존재합니다: {index_name}")

def fetch_job_posts(conn) -> pd.DataFrame:
    """채용공고 데이터 조회"""
    logger.info("채용공고 데이터 조회 중...")
    query = """
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
            pst_strt,
            pst_end,
            pst_detail,
            pr_maj,
            co_ind,
            co_hr,
            cl_info,
            wc,
            qual,
            pr_item,
            rep,
            corp_addr,
            source,
            year,
            month
        FROM merged_table
    """
    return pd.read_sql(query, conn)

def fetch_company_reference(conn) -> pd.DataFrame:
    """회사 정보 조회"""
    logger.info("회사 정보 조회 중...")
    query = """
        SELECT 
            bzno,
            title,
            address,
            phone,
            region,
            bonid,
            bon_add,
            bon_address,
            bon_phone,
            bon_title,
            bon_info,
            ksic,
            ksic9
        FROM company_reference
    """
    return pd.read_sql(query, conn)

def index_job_posts(es: Elasticsearch, df: pd.DataFrame) -> None:
    """채용공고 인덱싱 (진행 상황 추적 개선)"""
    if df.empty:
        logger.warning("인덱싱할 채용공고 데이터가 없습니다.")
        return

    # 데이터 전처리
    start_time = time.time()
    logger.info("데이터 전처리 중...")
    df = preprocess_dataframe(df)
    logger.info(f"데이터 전처리 완료 (소요시간: {time.time() - start_time:.2f}초)")
    
    # 총 문서 수 및 배치 계산
    total_docs = len(df)
    logger.info(f"총 {total_docs:,}개 문서 인덱싱 시작")
    
    # 배치 크기 및 카운터 설정
    batch_size = 500  # 배치 크기 감소
    success_count = 0
    failed_count = 0
    
    # 진행상황 표시를 위한 설정
    progress_interval = max(1, total_docs // 100)  # 1% 단위로 진행상황 출력
    last_progress = 0
    start_time = time.time()
    
    # 데이터프레임을 배치로 분할하여 처리
    for i in range(0, total_docs, batch_size):
        # 현재 배치 범위
        end_idx = min(i + batch_size, total_docs)
        batch_df = df.iloc[i:end_idx]
        
        # 문서 준비
        batch_docs = []
        for _, row in batch_df.iterrows():
            doc_source = {col: row[col] for col in row.index if not pd.isna(row[col])}
            # None 값은 빈 문자열로 처리 (문자열 필드만)
            for col in row.index:
                if col not in ["pst_strt", "pst_end", "year", "month"] and (doc_source.get(col) is None):
                    doc_source[col] = ""
            
            batch_docs.append({
                "_index": "job_posts",
                "_id": row["pst_id"],
                "_source": doc_source
            })
        
        # 인덱싱 시도
        try:
            success, failed = bulk(
                es,
                batch_docs,
                refresh=False,  # 성능 향상을 위해 배치별 refresh 비활성화
                chunk_size=50,  # 청크 크기 감소
                max_retries=10,  # 재시도 횟수 증가
                initial_backoff=2,  # 초기 백오프 시간 설정
                max_backoff=600,  # 최대 백오프 시간 설정
                raise_on_error=False  # 에러가 있어도 계속 진행
            )
            success_count += success
            failed_count += len(failed) if failed else 0
            
            # 진행상황 출력
            if (i + batch_size) // progress_interval > last_progress // progress_interval:
                last_progress = i + batch_size
                elapsed = time.time() - start_time
                progress_pct = min(100, (i + batch_size) / total_docs * 100)
                docs_per_sec = (i + batch_size) / elapsed if elapsed > 0 else 0
                
                remaining = (total_docs - (i + batch_size)) / docs_per_sec if docs_per_sec > 0 else 0
                remaining_str = f"{int(remaining // 60)}분 {int(remaining % 60)}초" if remaining > 0 else "계산 중..."
                
                logger.info(f"진행률: {progress_pct:.1f}% ({i + batch_size:,}/{total_docs:,}) | "
                           f"성공: {success_count:,} | "
                           f"실패: {failed_count:,} | "
                           f"속도: {docs_per_sec:.1f}개/초 | "
                           f"예상 남은 시간: {remaining_str}")
                
        except BulkIndexError as e:
            failed_count += len(e.errors)
            logger.error(f"배치 인덱싱 일부 실패 (문서 {i}~{end_idx-1}): {len(e.errors)}개 실패")
        except Exception as e:
            logger.error(f"배치 인덱싱 중 오류 발생 (문서 {i}~{end_idx-1}): {e}")
            failed_count += len(batch_docs)
            
            # 일시적인 오류일 수 있으므로 잠시 대기 후 계속 진행
            logger.info("오류 발생 후 10초 대기 중...")
            time.sleep(10)
    
    # 최종 결과 출력
    total_time = time.time() - start_time
    logger.info(f"채용공고 인덱싱 완료 (소요시간: {total_time:.2f}초)")
    logger.info(f"총 {total_docs:,}개 문서 중 {success_count:,}개 성공, {failed_count:,}개 실패")
    logger.info(f"평균 속도: {total_docs / total_time:.1f}개/초")
    
    # 최종 refresh 실행
    try:
        logger.info("인덱스 refresh 중...")
        es.indices.refresh(index="job_posts")
    except Exception as e:
        logger.warning(f"최종 refresh 중 오류: {e}")

def index_companies(es: Elasticsearch, df: pd.DataFrame) -> None:
    """회사 정보 인덱싱 (진행 상황 추적 개선)"""
    if df.empty:
        logger.warning("인덱싱할 회사 정보가 없습니다.")
        return
    
    total_companies = len(df)
    logger.info(f"총 {total_companies:,}개 회사 정보 인덱싱 시작")
    
    # 회사 정보는 상대적으로 적으므로 더 큰 배치 크기 사용
    batch_size = 1000
    success_count = 0
    failed_count = 0
    start_time = time.time()
    
    # 배치 단위로 처리
    for i in range(0, total_companies, batch_size):
        end_idx = min(i + batch_size, total_companies)
        batch_df = df.iloc[i:end_idx]
        
        # 문서 준비
        batch_docs = []
        for _, row in batch_df.iterrows():
            doc_source = {col: row[col] for col in row.index if not pd.isna(row[col])}
            # 문자열 필드의 None 값은 빈 문자열로 처리
            for col in row.index:
                if doc_source.get(col) is None and col != "bonid":
                    doc_source[col] = ""
            
            batch_docs.append({
                "_index": "company_info",
                "_id": row["bzno"],  # 사업자번호를 ID로 사용
                "_source": doc_source
            })
        
        # 인덱싱 시도
        try:
            success, failed = bulk(
                es,
                batch_docs,
                refresh=False,
                chunk_size=100,
                max_retries=10,
                raise_on_error=False
            )
            success_count += success
            failed_count += len(failed) if failed else 0
            
            # 진행상황 출력
            progress_pct = min(100, (i + batch_size) / total_companies * 100)
            logger.info(f"회사 정보 인덱싱 진행률: {progress_pct:.1f}% ({i + batch_size:,}/{total_companies:,})")
        
        except Exception as e:
            logger.error(f"회사 정보 인덱싱 중 오류 발생: {e}")
            failed_count += len(batch_docs)
            time.sleep(5)  # 오류 후 잠시 대기
    
    # 최종 결과 출력
    total_time = time.time() - start_time
    logger.info(f"회사 정보 인덱싱 완료 (소요시간: {total_time:.2f}초)")
    logger.info(f"총 {total_companies:,}개 회사 중 {success_count:,}개 성공, {failed_count:,}개 실패")
    
    # 최종 refresh
    try:
        es.indices.refresh(index="company_info")
    except Exception as e:
        logger.warning(f"회사 정보 인덱스 refresh 중 오류: {e}")

def verify_indices(es: Elasticsearch) -> bool:
    """인덱스 검증"""
    logger.info("인덱스 검증 중...")
    all_indices_valid = True
    
    for index_name in INDEX_SETTINGS.keys():
        if not es.indices.exists(index=index_name):
            logger.error(f"인덱스가 존재하지 않습니다: {index_name}")
            all_indices_valid = False
            continue
            
        try:
            # 인덱스 상태 확인
            health = es.cluster.health(index=index_name)
            status = health.get("status")
            
            # 문서 수 확인
            count_resp = es.count(index=index_name)
            doc_count = count_resp.get("count", 0)
            
            logger.info(f"인덱스 '{index_name}' 상태: {status}, 문서 수: {doc_count:,}개")
            
            if doc_count == 0:
                logger.warning(f"인덱스 '{index_name}'에 문서가 없습니다.")
                all_indices_valid = False
        except Exception as e:
            logger.error(f"인덱스 '{index_name}' 검증 중 오류: {e}")
            all_indices_valid = False
    
    return all_indices_valid

def create_index(force: bool = False) -> bool:
    """인덱스 생성 프로세스 실행 (진행 상황 추적 개선)"""
    start_time = time.time()
    logger.info("인덱스 생성 프로세스 시작")
    
    try:
        # Elasticsearch 연결 설정
        logger.info("Elasticsearch 연결 중...")
        es = setup_elasticsearch()
        logger.info("Elasticsearch 연결 완료")
        
        # 인덱스 존재 여부 확인
        indices_exist = all(es.indices.exists(index=idx) for idx in INDEX_SETTINGS.keys())
        
        # force=False이고 인덱스가 이미 존재하면 전체 작업 스킵
        if indices_exist and not force:
            logger.info("인덱스가 이미 존재하며 force=False로 설정되어 있어 인덱스 생성 및 데이터 인덱싱을 건너뜁니다.")
            
            # 인덱스 내 문서 수 확인 및 로그 출력
            for index_name in INDEX_SETTINGS.keys():
                count = es.count(index=index_name)
                logger.info(f"인덱스 '{index_name}'에 {count['count']:,}개의 문서가 이미 존재합니다.")
            
            total_time = time.time() - start_time
            logger.info(f"인덱스 생성 프로세스 스킵 완료 (소요시간: {total_time:.2f}초)")
            return True
        
        # 기존 인덱스 삭제 후 생성 (force=True일 때)
        logger.info("인덱스 생성 설정 중...")
        create_indices(es, force)
        
        with psycopg2.connect(**DB_PARAMS) as conn:
            # 채용공고 인덱싱
            job_posts_df = fetch_job_posts(conn)
            
            if not job_posts_df.empty:
                logger.info(f"채용공고 {len(job_posts_df):,}개 로드 완료, 인덱싱 시작...")
                index_job_posts(es, job_posts_df)
            else:
                logger.warning("인덱싱할 채용공고가 없습니다.")
            
            # 메모리 최적화를 위해 데이터프레임 제거
            del job_posts_df
            
            # 회사 정보 인덱싱
            company_df = fetch_company_reference(conn)
            
            if not company_df.empty:
                logger.info(f"회사 정보 {len(company_df):,}개 로드 완료, 인덱싱 시작...")
                index_companies(es, company_df)
            else:
                logger.warning("인덱싱할 회사 정보가 없습니다.")
        
        # 인덱스 검증
        verify_indices(es)
        
        # 최종 결과
        total_time = time.time() - start_time
        hours, remainder = divmod(total_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        logger.info(f"인덱스 생성 프로세스 완료 (소요시간: {int(hours)}시간 {int(minutes)}분 {int(seconds)}초)")
        return True
        
    except Exception as e:
        logger.error(f"인덱스 생성 중 오류 발생: {e}", exc_info=True)
        # 오류 발생해도 프로세스 계속 진행하도록 False 반환
        return False

if __name__ == "__main__":
    # 강제 인덱스 재생성을 원하면 force=True로 설정
    create_index(force=False)