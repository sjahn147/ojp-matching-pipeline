def match_skills(shard_id=0, total_shards=1):
    """PostgreSQL에서 스킬 매칭을 수행하는 메인 함수 (Elasticsearch 기반)"""
    import pandas as pd
    import numpy as np
    import re
    import logging
    from rapidfuzz import fuzz
    import time
    from functools import wraps
    import psycopg2
    import threading
    import queue
    import gc
    from elasticsearch import Elasticsearch, helpers
    from io import StringIO

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
        "max_retries": 20,
        "retry_on_timeout": True,
        "timeout": 60
    }
    
    # 로깅 설정
    logging.basicConfig(level=logging.INFO,
                       format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # 샤드 정보 로깅 - 함수 시작시 명확히 출력
    logger.info(f"Starting skill matching with sharding: shard {shard_id} of {total_shards} total shards")

    # 성능 최적화 설정
    CHUNK_SIZE = 5000      # DB에서 한 번에 가져올 레코드 수
    BULK_SIZE = 1000       # ES에 한 번에 색인할 문서 수
    SEARCH_BATCH = 500     # 한 번에 검색할 문서 수
    UPDATE_BATCH = 1000     # PostgreSQL에 일괄 업데이트할 크기
    
    # 함수 실행 시간 측정 데코레이터
    def log_time(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            logger.info(f"{func.__name__} took {end_time - start_time:.2f} seconds")
            return result
        return wrapper

    def preprocess_job_desc(text):
        """채용 공고 텍스트 전처리"""
        if pd.isna(text):
            return ""
        if not isinstance(text, str):
            text = str(text)
        
        patterns_to_remove = [
            r"기업\s*소개\s*또는\s*채용\s*안내를\s*작성해\s*보세요\s*불필요시\s*'?소개글'?을\s*OFF하면\s*소개\s*영역이\s*숨겨집니다\.?",
            r"\*{3}\s*입사\s*지원",
            r"\*{3}\s*온라인\s*이력서",
            r"유의\s*사항\s*[ㆍ·]\s*학력[,\s]*성별[,\s]*연령을\s*보지\s*않는\s*블라인드\s*채용입니다\.?",
            r"입사\s*지원\s*서류에\s*허위\s*사실이\s*발견될\s*경우[,\s]*채용\s*확정\s*이후라도\s*채용이\s*취소될\s*수\s*있습니다\.?",
            r"모집\s*분야별로\s*마감일이\s*상이할\s*수\s*있으니\s*유의하시길\s*바랍니다\.?",
            r"상세\s*사항\s*전화\s*문의\s*요망",
            r"접수된\s*지원서는\s*최초\s*접수일로부터\s*1년간\s*보관되며\s*1년이\s*경과된\s*뒤에는\s*자동\s*파기",
            r"급여\s*및\s*복리후생은\s*당사\s*규정에\s*준하여\s*적용",
            r"3개월\s*근무\s*평가\s*후\s*정규직\s*전환\s*면접\s*실시",
            r"이력서\s*등\s*제출\s*서류에\s*허위\s*사실이\s*있을\s*시\s*채용\s*취소",
            r"자세한\s*상세\s*요강은\s*반드시\s*채용\s*홈페이지에서\s*직접\s*확인해\s*주시기\s*바랍니다"
        ]
        
        for pattern in patterns_to_remove:
            text = re.sub(pattern, "", text, flags=re.IGNORECASE)
        
        base_keywords = [
            r'혜택\s*및\s*복지',
            r'근무\s*조건',
            r'전형\s*절차',
            r'접수\s*기간',
            r'지원\s*방법',
            r'지원\s*자격',
            r'채용\s*절차',
            r'제출\s*서류',
            r'기타\s*사항',
            r'복지\s*제도',
            r'급여\s*및',
            r'근무\s*형태',
            r'근무\s*환경',
            r'복리\s*후생',
            r'서류\s*전형',
            r'접수된\s*지원서',
        ]
        
        cut_keywords = '|'.join(base_keywords)
        pattern = fr'(^|\s|\S)({cut_keywords})'
        
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            text = text[:match.start() + 1]
        
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def setup_database(conn, force=False):
        """데이터베이스에 필요한 컬럼 추가 및 인덱스 생성"""
        cursor = conn.cursor()
        
        try:
            # skill_code 및 skill_name 컬럼이 없으면 추가
            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'merged_table' 
                        AND column_name = 'skill_code'
                    ) THEN
                        ALTER TABLE merged_table ADD COLUMN skill_code TEXT;
                    END IF;
                    
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'merged_table' 
                        AND column_name = 'skill_name'
                    ) THEN
                        ALTER TABLE merged_table ADD COLUMN skill_name TEXT;
                    END IF;
                END $$;
            """)
            
            # pg_trgm 확장 활성화
            cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            
            # 인덱스 생성 또는 재생성
            if force:
                logger.info("Force mode: Dropping existing indexes before recreating")
                # 기존 인덱스 삭제
                cursor.execute("""
                    DO $$
                    DECLARE
                        idx_name text;
                    BEGIN
                        FOR idx_name IN 
                            SELECT indexname FROM pg_indexes 
                            WHERE tablename = 'merged_table' AND 
                            indexname LIKE 'idx_merged_table_%'
                        LOOP
                            EXECUTE 'DROP INDEX IF EXISTS ' || idx_name;
                        END LOOP;
                    END $$;
                """)
                logger.info("Existing indexes dropped")
            
            # 인덱스 생성
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_merged_table_pst_id ON merged_table(pst_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_merged_table_skill_code ON merged_table(skill_code);")
            
            # 전처리된 텍스트 필드에 GIN 인덱스 생성 (전처리된 필드가 있는 경우)
            cursor.execute("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'merged_table' 
                        AND column_name = 'processed_pst_detail'
                    ) THEN
                        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_merged_table_processed_pst_detail 
                                ON merged_table USING gin(processed_pst_detail gin_trgm_ops)';
                    END IF;
                    
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'merged_table' 
                        AND column_name = 'processed_co_nm'
                    ) THEN
                        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_merged_table_processed_co_nm 
                                ON merged_table USING gin(processed_co_nm gin_trgm_ops)';
                    END IF;
                END $$;
            """)
            
            # 자주 필터링하는 조건에 대한 인덱스
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_merged_table_skill_matching 
                ON merged_table((pst_detail IS NOT NULL), (skill_code IS NULL))
                WHERE pst_detail IS NOT NULL;
            """)
            
            conn.commit()
            logger.info("Database setup and indexing completed")
        
        except Exception as e:
            logger.error(f"Error setting up database: {str(e)}")
            raise
        
        finally:
            cursor.close()

    def create_skill_index(es, force=False):
        """korskilldict 인덱스 생성 또는 확인"""
        index_name = "korskilldict"
        
        # force 모드에서는 인덱스 삭제 후 재생성
        if force and es.indices.exists(index=index_name):
            logger.info(f"Force mode: Deleting existing index {index_name}")
            es.indices.delete(index=index_name)
            logger.info(f"Index {index_name} deleted")
        
        # 한국어 처리를 위한 분석기 설정
        settings = {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "5s",
                "codec": "best_compression"
            },
            "analysis": {
                "analyzer": {
                    "skill_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "stop"]
                    }
                }
            }
        }
        
        # nori 플러그인이 설치되어 있는지 확인하고 있으면 추가
        try:
            plugins = es.cat.plugins(format="json")
            if any("analysis-nori" in p["component"] for p in plugins):
                logger.info("Nori plugin detected, adding Korean analysis")
                settings["analysis"]["analyzer"]["korean_analyzer"] = {
                    "type": "custom",
                    "tokenizer": "nori_tokenizer",
                    "filter": ["lowercase", "stop"]
                }
                settings["analysis"]["tokenizer"] = {
                    "nori_tokenizer": {
                        "type": "nori_tokenizer",
                        "decompound_mode": "mixed"
                    }
                }
        except Exception as e:
            logger.warning(f"Could not check for Nori plugin: {str(e)}")
        
        mappings = {
            "properties": {
                "skill_code": {
                    "type": "keyword",
                    "doc_values": True,
                    "eager_global_ordinals": True
                },
                "skill_name": {
                    "type": "text",
                    "analyzer": "skill_analyzer",
                    "fields": {
                        "keyword": {"type": "keyword"}
                    }
                },
                "pattern": {"type": "keyword"},
                "category": {"type": "keyword"}  # 대분류 카테고리 추가
            }
        }
        
        # 인덱스가 이미 존재하는지 확인
        if es.indices.exists(index=index_name):
            logger.info(f"Index {index_name} already exists, checking document count")
            # 기존 문서 수 확인
            stats = es.indices.stats(index=index_name)
            doc_count = stats['_all']['primaries']['docs']['count']
            
            if doc_count > 0:
                logger.info(f"Index {index_name} already contains {doc_count} documents")
                
                # 인덱스 최적화
                try:
                    # 필터링 및 집계 성능 향상을 위한 설정
                    settings_update = {
                        "index": {
                            "refresh_interval": "10s",
                        }
                    }
                    
                    es.indices.put_settings(index=index_name, body=settings_update)
                    
                    # 매핑 업데이트 (category 필드 추가)
                    mapping_update = {
                        "properties": {
                            "category": {
                                "type": "keyword"
                            }
                        }
                    }
                    
                    es.indices.put_mapping(index=index_name, body=mapping_update)
                    logger.info(f"Optimized {index_name} index settings")
                except Exception as e:
                    logger.warning(f"Could not optimize skill index settings: {str(e)}")
                
                return index_name
            else:
                logger.info(f"Index {index_name} exists but is empty, updating settings")
                # 인덱스 삭제 후 재생성 (매핑 변경이 어려울 수 있으므로)
                es.indices.delete(index=index_name)
                es.indices.create(
                    index=index_name,
                    body={"settings": settings, "mappings": mappings}
                )
        else:
            # 새 인덱스 생성
            es.indices.create(
                index=index_name,
                body={"settings": settings, "mappings": mappings}
            )
        
        logger.info(f"Created/verified index: {index_name}")
        return index_name

    def load_skill_dict(es, skill_index_name, conn):
        """PostgreSQL에서 스킬 데이터를 로드하여 Elasticsearch에 색인"""
        logger.info("Loading skill dictionary from database")
        
        try:
            query = """
                SELECT skill_code, skill_name, ext_key 
                FROM skill_dictionary 
                WHERE ext_key IS NOT NULL;
            """
            df = pd.read_sql(query, conn)
            total_skills = len(df)
            logger.info(f"Loaded {total_skills} skills from PostgreSQL")
            
            # 인덱스 문서 수 확인
            stats = es.indices.stats(index=skill_index_name)
            doc_count = stats['_all']['primaries']['docs']['count']
            
            # 이미 모든 스킬이 인덱싱되어 있으면 스킵
            if doc_count >= total_skills:
                logger.info(f"All {total_skills} skills already indexed in Elasticsearch")
                
                # 스킬 사전 정보를 메모리에 로드
                skill_dict = {}
                for _, row in df.iterrows():
                    pattern = row['ext_key']
                    if isinstance(pattern, str):
                        if pattern.startswith("r'") and pattern.endswith("'"):
                            pattern = pattern[2:-1]
                        if pattern.startswith('(?i)'):
                            pattern = pattern[4:]
                            flags = re.IGNORECASE
                        else:
                            flags = 0
                        try:
                            skill_dict[row['skill_code']] = {
                                'pattern': re.compile(pattern, flags),
                                'skill_name': row['skill_name']
                            }
                        except re.error:
                            logger.error(f"Invalid regex pattern for skill_code {row['skill_code']}: {pattern}")
                
                logger.info(f"Loaded {len(skill_dict)} skills with valid patterns")
                return skill_dict
            
            # 벌크 색인 작업 수행
            bulk_data = []
            for _, row in df.iterrows():
                pattern = row['ext_key']
                skill_code = row['skill_code']
                
                if isinstance(pattern, str):
                    if pattern.startswith("r'") and pattern.endswith("'"):
                        pattern = pattern[2:-1]
                    if pattern.startswith('(?i)'):
                        pattern = pattern[4:]
                    
                    # 대분류 카테고리 추출 (skill_code의 첫 2글자)
                    category = skill_code[:2] if len(skill_code) >= 2 else "00"
                    
                    bulk_data.append({
                        "_index": skill_index_name,
                        "_id": skill_code,
                        "_source": {
                            "skill_code": skill_code,
                            "skill_name": row['skill_name'],
                            "pattern": pattern,
                            "category": category
                        }
                    })
            
            # Elasticsearch에 색인
            if bulk_data:
                success, failed = helpers.bulk(
                    es, 
                    bulk_data, 
                    refresh=True,
                    stats_only=True
                )
                logger.info(f"Indexed {success} skills to Elasticsearch")
                if failed:
                    logger.warning(f"Failed to index {failed} skills")
            
            # 스킬 사전 정보를 메모리에 로드 (원래 코드와 동일한 방식)
            skill_dict = {}
            for _, row in df.iterrows():
                pattern = row['ext_key']
                if isinstance(pattern, str):
                    if pattern.startswith("r'") and pattern.endswith("'"):
                        pattern = pattern[2:-1]
                    if pattern.startswith('(?i)'):
                        pattern = pattern[4:]
                        flags = re.IGNORECASE
                    else:
                        flags = 0
                    try:
                        skill_dict[row['skill_code']] = {
                            'pattern': re.compile(pattern, flags),
                            'skill_name': row['skill_name']
                        }
                    except re.error:
                        logger.error(f"Invalid regex pattern for skill_code {row['skill_code']}: {pattern}")
            
            logger.info(f"Loaded {len(skill_dict)} skills with valid patterns")
            return skill_dict
            
        except Exception as e:
            logger.error(f"Error loading skill dictionary: {str(e)}")
            raise

    def check_job_posts_index(es):
        """job_posts 인덱스 확인 및 검증"""
        job_index_name = "job_posts"
        
        # 인덱스 존재 여부 확인
        if not es.indices.exists(index=job_index_name):
            logger.error(f"Index {job_index_name} does not exist. Please run create_index task first.")
            return None
        
        # 문서 수 확인
        try:
            count_result = es.count(index=job_index_name)
            doc_count = count_result["count"]
            
            if doc_count == 0:
                logger.error(f"Index {job_index_name} exists but contains no documents.")
                return None
                
            logger.info(f"Found existing index {job_index_name} with {doc_count} documents.")
            return job_index_name
            
        except Exception as e:
            logger.error(f"Error checking job_posts index: {str(e)}")
            return None

    def optimize_job_posts_index(es, job_index_name, force=False):
        """job_posts 인덱스 매핑 최적화"""
        if not es.indices.exists(index=job_index_name):
            logger.error(f"Index {job_index_name} does not exist")
            return False
        
        try:
            # 분석기 및 성능 최적화 설정
            index_settings = {
                "index": {
                    "refresh_interval": "30s",
                    "max_result_window": 50000
                }
            }
            
            # 설정 업데이트 (refresh_interval만 변경하기 때문에 닫을 필요 없음)
            es.indices.put_settings(index=job_index_name, body=index_settings)
            
            # 매핑 업데이트 시도 (기존 필드에 추가되는 부분)
            try:
                mapping_update = {
                    "properties": {
                        "pst_detail_keywords": {
                            "type": "text",
                            "index": True
                        }
                    }
                }
                
                es.indices.put_mapping(index=job_index_name, body=mapping_update)
                logger.info(f"Updated mappings for {job_index_name}")
            except Exception as e:
                logger.warning(f"Could not update mapping (this is expected for some fields): {e}")
            
            logger.info(f"Optimized {job_index_name} index settings")
            return True
            
        except Exception as e:
            logger.error(f"Error optimizing job_posts index: {e}")
            return False

    def match_skills_for_text(text, skill_dict, top_n=20, min_similarity=70):
        """텍스트에서 스킬 패턴 매칭 - 원래 코드와 동일하게 유지"""
        if pd.isna(text) or not isinstance(text, str) or text == "":
            return {}
        
        matched_skills = {}
        for skill_code, skill_info in skill_dict.items():
            if skill_info['pattern'].search(text):
                similarity = fuzz.partial_ratio(skill_info['skill_name'].lower(), text.lower())
                if similarity >= min_similarity:
                    matched_skills[skill_code] = {
                        'similarity': similarity,
                        'skill_name': skill_info['skill_name']
                    }
        
        # similarity 기준으로 정렬하여 상위 top_n개만 유지
        return dict(sorted(matched_skills.items(), key=lambda x: x[1]['similarity'], reverse=True)[:top_n])

    # 배치 단위로 처리할 유효한 ID 그룹 (메모리 효율성을 위해)
    def process_job_posts(es, job_index_name, skill_dict, conn):
        cursor = conn.cursor()
        
        try:
            # 샤딩을 위한 조건 - 문자열 해시 기반 샤딩 
            # PostgreSQL의 hashtext 함수를 사용하여 문자열을 정수로 변환 후 모듈러 연산
            shard_condition = f"AND (hashtext(pst_id) % {total_shards} = {shard_id})"
            
            # 처리할 레코드 수 확인
            cursor.execute(f"""
                SELECT COUNT(*) FROM merged_table 
                WHERE pst_detail IS NOT NULL 
                AND (skill_code IS NULL OR skill_code = '')
                {shard_condition}
            """)
            total_count = cursor.fetchone()[0]
            
            if total_count == 0:
                logger.info(f"Shard {shard_id+1}/{total_shards}: No records need processing.")
                return 0
                    
            logger.info(f"Shard {shard_id+1}/{total_shards}: Total records to process: {total_count:,}")
            
            # 대용량 메모리 활용을 위한 설정
            CHUNK_SIZE = 2000  # 한 번에 가져올 레코드 수 (32GB RAM 활용)
            ES_QUERY_SIZE = 200  # Elasticsearch 쿼리 크기
            UPDATE_BATCH = 200  # PostgreSQL 업데이트 크기
            
            # 스킬 카테고리화 (메모리 사용 최적화)
            skill_by_category = {}
            for skill_code, skill_info in skill_dict.items():
                category = skill_code[:2] if len(skill_code) >= 2 else "00"
                if category not in skill_by_category:
                    skill_by_category[category] = {}
                skill_by_category[category][skill_code] = skill_info
            
            processed = 0
            updated = 0
            start_time = time.time()
            last_log_time = start_time
            last_processed = 0
            
            # 청크 단위로 처리
            for offset in range(0, total_count, CHUNK_SIZE):
                # ID만 효율적으로 가져오기 - 샤딩 조건 동일하게 추가
                cursor.execute(f"""
                    SELECT pst_id FROM merged_table 
                    WHERE pst_detail IS NOT NULL AND (skill_code IS NULL OR skill_code = '')
                    {shard_condition}
                    ORDER BY pst_id 
                    LIMIT {CHUNK_SIZE} OFFSET {offset}
                """)
                chunk_ids = [row[0] for row in cursor.fetchall()]
                logger.info(f"Shard {shard_id+1}/{total_shards} - Processing chunk {offset//CHUNK_SIZE + 1}: {len(chunk_ids):,} records")
                
                if not chunk_ids:
                    continue
                    
                # 대규모 ES 쿼리 실행 (ES 내부 메모리 제한 고려)
                all_docs = []
                for i in range(0, len(chunk_ids), ES_QUERY_SIZE):
                    batch_ids = chunk_ids[i:i+ES_QUERY_SIZE]
                    query = {
                        "query": {"terms": {"pst_id": batch_ids}},
                        "_source": ["pst_id", "pst_detail"],
                        "size": len(batch_ids)
                    }
                    
                    response = es.search(index=job_index_name, body=query)
                    all_docs.extend(response["hits"]["hits"])
                
                # 대량 업데이트 준비
                updates = []
                
                # 문서 처리
                for doc in all_docs:
                    pst_id = doc["_source"]["pst_id"]
                    text = doc["_source"].get("pst_detail", "")
                    
                    if not text:
                        continue
                        
                    # 텍스트 전처리 (원래 함수 사용)
                    processed_text = preprocess_job_desc(text)
                    
                    if processed_text:
                        # 모든 카테고리의 매칭 결과를 수집
                        all_matches = {}
                        
                        # 각 카테고리별로 매칭 수행
                        for category_skills in skill_by_category.values():
                            category_matches = match_skills_for_text(processed_text, category_skills)
                            all_matches.update(category_matches)
                        
                        # 최종 스코어로 정렬하여 상위 20개만 선택
                        top_skills = sorted(
                            all_matches.items(),
                            key=lambda x: x[1]['similarity'],
                            reverse=True
                        )[:20]
                        
                        # 스킬 코드와 이름 분리
                        if top_skills:
                            skill_codes = [code for code, _ in top_skills]
                            skill_names = [info['skill_name'] for _, info in top_skills]
                            
                            updates.append((
                                pst_id, 
                                ','.join(skill_codes), 
                                ','.join(skill_names)
                            ))
                    
                    processed += 1
                    
                    # 10초마다 로깅 추가
                    current_time = time.time()
                    if current_time - last_log_time >= 10:
                        # 마지막 로그 이후 처리된 건수
                        new_processed = processed - last_processed
                        
                        # 기간 동안의 처리 속도
                        interval = current_time - last_log_time
                        current_rate = new_processed / interval if interval > 0 else 0
                        
                        # 전체 진행 상황
                        total_elapsed = current_time - start_time
                        overall_rate = processed / total_elapsed if total_elapsed > 0 else 0
                        progress = min(100, processed / total_count * 100)
                        
                        # ETA 계산
                        eta_seconds = (total_count - processed) / overall_rate if overall_rate > 0 else 0
                        eta = f"{int(eta_seconds//3600)}h {int((eta_seconds%3600)//60)}m"
                        
                        logger.info(f"Shard {shard_id+1}/{total_shards} - [10s update] Processed: +{new_processed} records | "
                                   f"Current rate: {current_rate:.1f}/sec | "
                                   f"Overall: {processed:,}/{total_count:,} ({progress:.1f}%) | "
                                   f"Updated: {updated:,} | Overall rate: {overall_rate:.1f}/sec | ETA: {eta}")
                        
                        # 로그 타임스탬프 및 처리 건수 업데이트
                        last_log_time = current_time
                        last_processed = processed
                
                # 효율적인 일괄 업데이트 (COPY 메서드)
                if updates:
                    # 임시 테이블 생성
                    temp_table = f"temp_skills_{int(time.time())}"
                    cursor.execute(f"""
                        CREATE TEMP TABLE {temp_table} (
                            pst_id TEXT,
                            skill_code TEXT,
                            skill_name TEXT
                        ) ON COMMIT DROP
                    """)
                    
                    # StringIO를 사용한 빠른 데이터 로드
                    from io import StringIO
                    buffer = StringIO()
                    for item in updates:
                        buffer.write(f"{item[0]}\t{item[1]}\t{item[2]}\n")
                    buffer.seek(0)
                    
                    # COPY로 데이터 삽입
                    cursor.copy_from(buffer, temp_table, null='', columns=('pst_id', 'skill_code', 'skill_name'), sep='\t')
                    
                    # 한 번의 업데이트로 모든 레코드 처리
                    cursor.execute(f"""
                        UPDATE merged_table m
                        SET skill_code = t.skill_code,
                            skill_name = t.skill_name
                        FROM {temp_table} t
                        WHERE m.pst_id = t.pst_id
                    """)
                    
                    conn.commit()
                    updated += len(updates)
                
                # 청크 완료 로깅
                current_time = time.time()
                elapsed = current_time - start_time
                overall_rate = processed / elapsed if elapsed > 0 else 0
                progress = min(100, processed / total_count * 100)
                eta_seconds = (total_count - processed) / overall_rate if overall_rate > 0 else 0
                eta = f"{int(eta_seconds//3600)}h {int((eta_seconds%3600)//60)}m"
                
                logger.info(f"Shard {shard_id+1}/{total_shards} - Chunk complete: {processed:,}/{total_count:,} ({progress:.1f}%) | Updated: {updated:,} | "
                           f"Overall rate: {overall_rate:.1f}/sec | ETA: {eta}")
                
                # 마지막 로그 시간 업데이트
                last_log_time = current_time
                last_processed = processed
                
                # 메모리 정리 (SIGKILL 방지)
                del all_docs
                del updates
                gc.collect()
            
            logger.info(f"Shard {shard_id+1}/{total_shards} - Completed: Processed {processed:,} records, updated {updated:,} records")
            return updated
            
        except Exception as e:
            logger.error(f"Shard {shard_id+1}/{total_shards} - Error processing job posts: {str(e)}")
            conn.rollback()
            raise
        finally:
            cursor.close()

    @log_time
    def process_data_with_elasticsearch(force=False):
        """메인 처리 함수 - 메모리 최적화 버전"""
        # 메모리 한계 무시 설정 (Linux 환경에서만 동작)
        try:
            import resource
            # 메모리 제한 해제 시도
            resource.setrlimit(resource.RLIMIT_AS, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
            logger.info(f"Memory limits disabled")
        except (ImportError, ValueError, resource.error):
            logger.warning("Could not disable memory limits")
        
        conn = None
        es = None
        
        try:
            # 연결 설정
            conn = psycopg2.connect(**DB_PARAMS)
            es = Elasticsearch(**ES_CONFIG)
            
            if not es.ping():
                logger.error("Failed to connect to Elasticsearch")
                return False
                
            # 필수 인덱스 확인
            skill_index_name = create_skill_index(es, force=force)
            skill_dict = load_skill_dict(es, skill_index_name, conn)
            
            job_index_name = check_job_posts_index(es)
            if job_index_name is None:
                logger.error("Job posts index not found")
                return False
            
            # 스킬 매칭 실행
            process_job_posts(es, job_index_name, skill_dict, conn)
            
            return True
            
        except Exception as e:
            logger.error(f"Processing error: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()

    # 함수 실행
    start_time = time.time()
    success = process_data_with_elasticsearch(force=False)
    end_time = time.time()

    if success:
        logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")
    else:
        logger.error(f"Processing failed")

    return success