import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from elasticsearch import Elasticsearch
import pandas as pd
import psycopg2
from typing import List, Dict, Any, Tuple, Set, Optional
from tqdm import tqdm
import json
from datetime import datetime 
import sys
import os
import gc

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TqdmToLogger:
    """
    tqdm의 출력을 로깅 시스템으로 리디렉션하는 클래스
    - WARNING 중복 출력 방지
    """
    def __init__(self, logger, level=logging.INFO):
        self.logger = logger
        self.level = level
        self.last_msg = ""

    def write(self, buf):
        buf = buf.strip()
        if buf and buf != self.last_msg:  # 중복 메시지 방지
            self.last_msg = buf
            self.logger.log(self.level, buf)

    def flush(self):
        pass

# 로깅 설정 개선
def setup_enhanced_logging():
    """향상된 로깅 설정"""
    logger = logging.getLogger(__name__)
    
    # 이미 핸들러가 설정되어 있으면 중복 설정 방지
    if not logger.handlers:
        # 표준 로그 포맷: 시간, 로그 레벨, 메시지
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(log_format)
        
        # 콘솔 핸들러 추가 (Airflow 로그에 표시)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # 로그 레벨 설정
        logger.setLevel(logging.INFO)
        
        # 부모 로거로부터 로그 이벤트 전파 방지
        logger.propagate = False
        
    return logger

def is_processing_needed(cursor, type_name="전처리", force=False):
    """
    특정 처리(전처리 또는 인덱싱)가 필요한지 확인
    - 이전 처리 여부를 DB에 기록하고 확인
    - force=True이면 항상 처리 필요로 판단
    """
    if force:
        return True
        
    # 처리 상태 테이블 확인 및 필요시 생성
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'processing_status'
        )
    """)
    table_exists = cursor.fetchone()[0]
    
    if not table_exists:
        cursor.execute("""
            CREATE TABLE processing_status (
                type_name TEXT PRIMARY KEY,
                last_processed TIMESTAMP,
                count INTEGER
            )
        """)
        return True  # 테이블이 없었으면 처리 필요
    
    # 처리 상태 확인
    cursor.execute("""
        SELECT last_processed, count FROM processing_status
        WHERE type_name = %s
    """, (type_name,))
    
    result = cursor.fetchone()
    
    # 처리 기록이 없으면 처리 필요
    if result is None:
        return True
        
    # # 처리 기록과 매칭되지 않은 레코드 수 확인
    # last_processed, last_count = result
    
    # # 매칭되지 않은 레코드 수 확인
    # cursor.execute("SELECT COUNT(*) FROM merged_table WHERE matched_company_name IS NULL")
    # current_count = cursor.fetchone()[0]
    
    # # 이전과 다르면 처리 필요
    # if current_count != last_count:
        # return True
        
    # 마지막 처리 후 24시간이 지났으면 처리 필요 (선택적)
    # time_diff = datetime.now() - last_processed
    # if time_diff.total_seconds() > 86400:  # 24시간 (초 단위)
    #    return True
    
    return False

def update_processing_status(cursor, conn, type_name, count):
    """처리 상태 업데이트"""
    cursor.execute("""
        INSERT INTO processing_status (type_name, last_processed, count)
        VALUES (%s, %s, %s)
        ON CONFLICT (type_name) 
        DO UPDATE SET last_processed = %s, count = %s
    """, (type_name, datetime.now(), count, datetime.now(), count))
    conn.commit()


def create_progress_bar(total, desc, unit="건", disable_tqdm=False):
    """
    진행 상황 표시용 tqdm 객체 생성
    - 로깅 시스템으로 출력 리디렉션
    - 환경에 따라 비활성화 옵션 제공
    """
    logger = logging.getLogger(__name__)
    
    # 진행 바 비활성화 여부 확인 (환경변수 또는 매개변수)
    disable = disable_tqdm or os.environ.get('DISABLE_TQDM', '0') == '1'
    
    # tqdm을 로깅 시스템으로 리디렉션
    tqdm_out = TqdmToLogger(logger, level=logging.INFO)
    
    # 진행 바 생성
    return tqdm(
        total=total,
        desc=desc,
        unit=unit,
        file=tqdm_out,
        disable=disable,
        ncols=100
    )

# 매칭 진행 상황 추적기
class MatchingProgressTracker:
    def __init__(self, total_items, log_interval=1000, match_detail_interval=100):
        """매칭 진행 상황 추적기 초기화"""
        self.logger = logging.getLogger(__name__)
        self.total_items = total_items
        self.processed_items = 0
        self.matched_items = 0
        self.log_interval = log_interval
        self.match_detail_interval = match_detail_interval
        self.start_time = datetime.now()
        self.matching_stats = {
            "name_only_match": 0,
            "name_region_match": 0,
            "name_address_match": 0,
            "name_region_address_match": 0,
            "detail_only_match": 0,
            "detail_region_match": 0,
            "detail_address_match": 0,
            "detail_region_address_match": 0
        }
        self.confidence_sum = 0
        
        # 진행률 표시를 위한 tqdm 설정 - 로깅 시스템으로 리디렉션
        self.progress_bar = create_progress_bar(
            total_items, 
            "회사 매칭 진행"
        )
        
        # 초기 로그 출력
        self.logger.info(f"매칭 프로세스 시작: 총 {total_items}건 처리 예정")
    
    def update(self, result=None):
        """진행 상황 업데이트
        
        Args:
            result: 매칭 결과 (성공 시 딕셔너리, 실패 시 None)
        """
        self.processed_items += 1
        self.progress_bar.update(1)
        
        # 매칭 성공 시 통계 업데이트
        if result:
            self.matched_items += 1
            match_type = result.get('match_type', 'unknown')
            if match_type in self.matching_stats:
                self.matching_stats[match_type] += 1
            
            # 신뢰도 점수 합산
            self.confidence_sum += result.get('confidence', 0)
        
        # 로그 간격마다 또는 마지막 항목에서 상세 로그 출력
        if (self.processed_items % self.log_interval == 0) or (self.processed_items == self.total_items):
            elapsed = datetime.now() - self.start_time
            match_rate = (self.matched_items / self.processed_items) * 100 if self.processed_items > 0 else 0
            
            # 현재 시점 기준 통계
            self.logger.info(f"진행 상황: {self.processed_items}/{self.total_items} 처리 완료 ({match_rate:.1f}% 매칭 성공)")
            
            # 마지막 또는 5000건마다 상세 통계 출력
            if self.processed_items % (self.log_interval * 5) == 0 or self.processed_items == self.total_items:
                avg_confidence = self.confidence_sum / self.matched_items if self.matched_items > 0 else 0
                stats_summary = ", ".join([f"{k}: {v}" for k, v in self.matching_stats.items() if v > 0])
                self.logger.info(f"매칭 통계: 총 {self.matched_items}건 매칭, 평균 신뢰도: {avg_confidence:.2f}")
                self.logger.info(f"매칭 유형: {stats_summary}")
    
    def log_match_detail(self, row, result):
        """매칭 상세 정보 로깅 (일정 간격으로)
        
        Args:
            row: 원본 데이터 행
            result: 매칭 결과
        """
        # 일정 간격으로만 출력
        if self.matched_items % self.match_detail_interval != 0:
            return
            
        co_nm = row.get('co_nm', '정보 없음')
        matched_name = result.get('matched_company_name', '정보 없음')
        
        # pst_detail 전처리 (200자 제한)
        pst_detail = row.get('processed_pst_detail', '')
        if not pst_detail and 'pst_detail' in row:
            pst_detail = row.get('pst_detail', '')[:200]
        else:
            pst_detail = pst_detail[:200] if pst_detail else '정보 없음'
        
        # 매칭 정보 출력
        match_type = result.get('match_type', 'unknown')
        confidence = result.get('confidence', 0)
        
        self.logger.info(f"매칭 예시 #{self.matched_items}:")
        self.logger.info(f"  원본: {co_nm}")
        self.logger.info(f"  매칭: {matched_name} (유형: {match_type}, 신뢰도: {confidence:.2f})")
        self.logger.info(f"  상세: {pst_detail}")
    
    def finish(self):
        """작업 완료 처리"""
        elapsed = datetime.now() - self.start_time
        hours, remainder = divmod(elapsed.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # 최종 통계 출력
        match_rate = (self.matched_items / self.total_items) * 100 if self.total_items > 0 else 0
        self.logger.info(f"매칭 완료: 총 {self.total_items}건 중 {self.matched_items}건 매칭 성공 ({match_rate:.1f}%)")
        self.logger.info(f"소요 시간: {int(hours)}시간 {int(minutes)}분 {int(seconds)}초")
        
        # 상세 매칭 통계
        if self.matched_items > 0:
            avg_confidence = self.confidence_sum / self.matched_items
            self.logger.info(f"평균 매칭 신뢰도: {avg_confidence:.2f}")
            
            # 매칭 유형별 통계
            self.logger.info("매칭 유형별 통계:")
            for match_type, count in self.matching_stats.items():
                if count > 0:
                    percent = (count / self.matched_items) * 100
                    self.logger.info(f"  - {match_type}: {count}건 ({percent:.1f}%)")
        
        # tqdm 진행 바 닫기
        self.progress_bar.close()


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
    "request_timeout": 30,
    "max_retries": 10,
    "retry_on_timeout": True
}

# 회사명으로 인식하면 안 되는 일반 단어 블록리스트
COMPANY_NAME_BLOCKLIST = {
    # 일반 단어
    "개인", "주", "담당", "가능", "접수", "채용", "모집", "인턴", "지원", "임금", 
    "법정", "준수", "기간", "입사", "경력", "신입", "정규직", "계약직", "파견", "아르바이트",
    "공고", "채용공고", "지원서", "이력서", "지원자", "모집요강", "급여", "복지",
    
    # 직종 관련
    "사원", "대리", "과장", "차장", "부장", "팀장", "매니저", "사무직", "영업직", "기술직",
    "포지션", "직무", "직책", "직위", "담당자", "전문가", "바리스타", "디자이너", "개발자",
    
    # 회사 관련 일반 단어
    "상사", "기업", "업체", "회사", "주식회사", "법인", "그룹", "컴퍼니", "유한회사", "합자회사",
    "주식회사의", "인재", "비전", "안내", "문의", "채용", "안내", "설명회", "상세",
    
    # 지역명 (주요 도시, 구)
    "한남", "서현", "여수", "구로", "강남", "강서", "송파", "마포", "용산", "종로", "성동", "동작",
    "영등포", "금천", "관악", "동대문", "서대문", "성북", "강북", "도봉", "노원", "중랑", "광진",
    "인천", "부산", "대구", "광주", "대전", "울산", "세종", "경기", "강원", "충북", "충남", "전북", "전남",
    
    # 특수 케이스 (요청에 따른 추가)
    "준수(주)", "주", "주식회사의", "포지션", "회사", "부동산", "25퍼센트", "이미지", "바리스타", 
    "인천", "구내식당", "이미지"
}

# 점진적 매칭 정책 설정
MATCHING_POLICIES = {
    "strict": {
        "short_name_length": 3,         # 짧은 이름으로 간주할 길이
        "very_short_name_length": 4,    # 지역/주소 일치가 필요한 짧은 이름 길이
        "similarity_threshold_short": 0.8,
        "similarity_threshold_normal": 0.7,
        "score_threshold_short": 0.85,
        "score_threshold_normal": 0.75,
        "fuzziness": "1",
        "prefix_length": 2,
        "name_weight": 0.6,
        "region_weight": 0.3,
        "address_weight": 0.2,
        "pst_detail": {
            "prefix_length": 200,             # 앞부분 텍스트 길이
            "min_threshold_short": 0.65,      # 2글자 이하 임계값
            "min_threshold_medium": 0.55,     # 3글자 임계값
            "min_threshold_normal": 0.35,     # 일반 임계값
            "name_weight_short": 0.4,         # 짧은 이름 매칭 가중치
            "region_weight_short": 0.4,       # 짧은 이름 지역 가중치
            "address_weight_short": 0.3,      # 짧은 이름 주소 가중치
            "name_weight": 0.5,               # 일반 이름 매칭 가중치
            "region_weight": 0.35,            # 일반 이름 지역 가중치
            "address_weight": 0.25            # 일반 이름 주소 가중치
        }                
    },
    "moderate": {
        "short_name_length": 2,
        "very_short_name_length": 3,
        "similarity_threshold_short": 0.7,
        "similarity_threshold_normal": 0.6,
        "score_threshold_short": 0.7,
        "score_threshold_normal": 0.65,
        "fuzziness": "2", 
        "prefix_length": 1,
        "name_weight": 0.65,
        "region_weight": 0.2,
        "address_weight": 0.15,
        "pst_detail": {
            "prefix_length": 250,             # 앞부분 텍스트 길이 증가
            "min_threshold_short": 0.6,       # 2글자 이하 임계값 완화
            "min_threshold_medium": 0.5,      # 3글자 임계값 완화
            "min_threshold_normal": 0.3,      # 일반 임계값 완화
            "name_weight_short": 0.45,        # 가중치 조정
            "region_weight_short": 0.35,
            "address_weight_short": 0.25,
            "name_weight": 0.55,
            "region_weight": 0.3,
            "address_weight": 0.2
        }
    },
    "relaxed": {
        "short_name_length": 2,
        "very_short_name_length": 2,
        "similarity_threshold_short": 0.55,
        "similarity_threshold_normal": 0.45,
        "score_threshold_short": 0.6,
        "score_threshold_normal": 0.5,
        "fuzziness": "AUTO",
        "prefix_length": 1,
        "name_weight": 0.7,
        "region_weight": 0.2,
        "address_weight": 0.1,
        "pst_detail": {
            "prefix_length": 300,             # 앞부분 텍스트 길이 추가 증가
            "min_threshold_short": 0.5,       # 임계값 추가 완화
            "min_threshold_medium": 0.4,
            "min_threshold_normal": 0.25,
            "name_weight_short": 0.5,         # 가중치 조정
            "region_weight_short": 0.3,
            "address_weight_short": 0.2,
            "name_weight": 0.75,
            "region_weight": 0.15,
            "address_weight": 0.1
        }
    },
    "very_relaxed": {
        "short_name_length": 2,  
        "very_short_name_length": 2,  
        "similarity_threshold_short": 0.4,  # 유사도 임계값 완화
        "similarity_threshold_normal": 0.3,
        "score_threshold_short": 0.5,  # 점수 임계값 완화
        "score_threshold_normal": 0.4,
        "fuzziness": "AUTO:4,7",  # 퍼지 매칭 허용 범위 확대
        "prefix_length": 1,
        "name_weight": 0.8,  # 이름 가중치 증가
        "region_weight": 0.15,  # 지역 가중치 감소
        "address_weight": 0.05,  # 주소 가중치 감소
        "pst_detail": {
            "prefix_length": 400,
            "min_threshold_short": 0.4,
            "min_threshold_medium": 0.3,
            "min_threshold_normal": 0.2,
            "name_weight_short": 0.6,
            "region_weight_short": 0.25,
            "address_weight_short": 0.15,
            "name_weight": 0.7,
            "region_weight": 0.2,
            "address_weight": 0.1
        }
    }
}    

# ===== 전처리 함수 =====

def normalize_company_name(text):
    """
    회사명 정규화 함수
    - 회사 법인 형태 표시 제거 ((주), 주식회사 등)
    - 특수기호 제거
    - 공백 정규화
    """
    if not text:
        return ""
    
    patterns = [
        "\\(주\\)",
        "\\(유\\)",
        "\\(무\\)",
        "\\(의\\)",
        "\\(조\\)",
        "\\(합\\)",
        "\\(쭈\\)",
        "\\(재\\)",
        "\\(사\\)",
        "\\(재단법인\\)",
        "\\(사단법인\\)",
        "주식회사",
        "유한회사",
        "합자회사",
        "합명회사",
        "유한책임회사",
        "사단법인",
        "재단법인",
        "㈜",
        "㈔",
        "\\[본사\\]"
    ]
    combined_pattern = "|".join(patterns)
    text = re.sub(combined_pattern, '', text)
    
    # 추가로 남아 있는 특수기호 제거
    text = re.sub(r'[^\w\s]', '', text)
    # 공백 통일
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def normalize_region(text):
    """
    지역명 정규화 함수 (광역시/도 수준으로 통일)
    - 다양한 지역명 표기를 표준화된 형식으로 변환
    """
    if not text:
        return ""
    
    # 지역명 표준화 매핑
    region_mapping = {
        "서울특별시": "서울",
        "서울시": "서울",
        "서울 ": "서울",
        "부산광역시": "부산",
        "부산시": "부산",
        "인천광역시": "인천",
        "인천시": "인천",
        "대구광역시": "대구",
        "대구시": "대구",
        "대전광역시": "대전",
        "대전시": "대전",
        "광주광역시": "광주",
        "광주시": "광주",
        "울산광역시": "울산",
        "울산시": "울산",
        "세종특별자치시": "세종",
        "세종시": "세종",
        "경기도": "경기",
        "강원도": "강원",
        "충청북도": "충북",
        "충북도": "충북",
        "충청남도": "충남",
        "충남도": "충남",
        "전라북도": "전북",
        "전북도": "전북",
        "전라남도": "전남",
        "전남도": "전남",
        "경상북도": "경북",
        "경북도": "경북",
        "경상남도": "경남",
        "경남도": "경남",
        "제주특별자치도": "제주",
        "제주도": "제주"
    }
    
    # 전처리 텍스트
    processed_text = text
    
    # 지역명 표준화
    for old, new in region_mapping.items():
        if old in processed_text:
            return new
    
    # 기본 지역명 추출 패턴
    regions = [
        "서울", "부산", "인천", "대구", "대전", "광주", "울산", "세종",
        "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주"
    ]
    
    for region in regions:
        if region in processed_text:
            return region
    
    return ""

def extract_address_for_matching(text):
    """
    주소 정보에서 매칭용 주소 추출 (광역시/도 + 시/군/구)
    - 지역 추출 후 시/군/구 패턴 추가
    """
    if not text:
        return ""
    
    # 지역명 추출
    region = normalize_region(text)
    if not region:
        return ""
    
    # 시/군/구 패턴 찾기
    local_match = re.search(r'([가-힣]+시|[가-힣]+군|[가-힣]+구)', text)
    
    if local_match:
        local = local_match.group(1)
        return f"{region} {local}"
    
    return region

def preprocess_text(text):
    """
    통합된 텍스트 전처리 함수
    - HTML 제거
    - 앞부분 추출 (회사명은 주로 앞에 위치하기 때문)
    - 특정 키워드 이후 텍스트 절삭
    - 불필요한 패턴 제거
    """
    if not text:
        return ""
    
    # 1. HTML 태그 제거
    text = re.sub(r'<[^>]+>', ' ', text)
    
    # 2. 기본 공백 정규화
    text = re.sub(r'\s+', ' ', text).strip()
    
    # 3. 앞부분 추출 (회사명은 주로 앞에 위치)
    # 최대 1000자로 제한 (성능 및 관련성 고려)
    max_length = min(len(text) // 3, 200)
    shortened_text = text[:max_length]
    
    # 4. 특정 키워드 이후 텍스트 절사
    cut_keywords = [
        "포지션", "모집부문", "모집분야", "채용분야", "담당업무", "모집직종", 
        "직무내용", "업무내용", "지원자격", "지원방법", "지원서류", "근무조건",
        "자격요건", "자격조건", "우대사항", "주요업무", "근무지역", "근무시간",
        "급여조건", "복리후생", "접수방법", "전형방법", "지원기간", "접수기간",
        "모집요강", "채용공고", "공고기간", "지원내용", "채용내용", "모집내용"
    ]
    
    for keyword in cut_keywords:
        pos = shortened_text.find(keyword)
        if pos > 50:  # 최소 50자는 유지
            shortened_text = shortened_text[:pos]
    
    # 5. 불필요한 패턴 제거
    patterns_to_remove = [
        r'채용제목을?\s*입력해?주세요',
        r'또는\s*개인\s*양식\s*이력서\s*접수\s*가능',
        r'인턴\s*기간\s*\d+개월\s*법정\s*임금\s*준수',
        r'[가-힣]+점\s*채용',
        r'채용공고',
        r'지원서\s*접수',
        r'이력서\s*접수',
        r'모집공고',
        r'채용안내',
        r'☞\s*.*\s*바로가기\s*☜',
        r'내용을\s*입력해?주세요',
        r'홈페이지\s*바로가기',
        r'블로그\s*바로가기',
        r'\[안내\]',
        r'\[공지\]',
        r'\[모집\]',
        r'\[채용\]'
    ]
    
    for pattern in patterns_to_remove:
        shortened_text = re.sub(pattern, ' ', shortened_text)
    
    # 6. 최종 공백 정규화
    cleaned_text = re.sub(r'\s+', ' ', shortened_text).strip()
    
    return cleaned_text

# ===== 데이터베이스 및 Elasticsearch 연결 함수 =====

def get_elasticsearch_client() -> Elasticsearch:
    """
    Elasticsearch 클라이언트 설정 및 연결 확인
    """
    es = Elasticsearch(**ES_CONFIG)
    if not es.ping():
        raise ConnectionError("Could not connect to Elasticsearch")
    return es

def ensure_matched_columns_exist():
    """merged_table에 필요한 컬럼 추가"""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # 컬럼 존재 여부 확인 (기존 컬럼들)
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'merged_table' AND column_name IN 
            ('matched_company_name', 'bzno', 'match_score', 'match_type', 'matching_attempt', 'matching_policy', 'ksic', 'ksic9')
        """)
        existing_columns = {row[0] for row in cursor.fetchall()}

        # 기존 컬럼 추가 (코드 유지)
        if "matched_company_name" not in existing_columns:
            cursor.execute("ALTER TABLE merged_table ADD COLUMN matched_company_name TEXT;")
            logger.info("Added column 'matched_company_name' to merged_table.")

        if "bzno" not in existing_columns:
            cursor.execute("ALTER TABLE merged_table ADD COLUMN bzno TEXT;")
            logger.info("Added column 'bzno' to merged_table.")
            
        if "match_score" not in existing_columns:
            cursor.execute("ALTER TABLE merged_table ADD COLUMN match_score FLOAT;")
            logger.info("Added column 'match_score' to merged_table.")
            
        if "match_type" not in existing_columns:
            cursor.execute("ALTER TABLE merged_table ADD COLUMN match_type TEXT;")
            logger.info("Added column 'match_type' to merged_table.")

        # 점진적 매칭을 위한 새 컬럼 추가
        if "matching_attempt" not in existing_columns:
            cursor.execute("ALTER TABLE merged_table ADD COLUMN matching_attempt INTEGER DEFAULT 0;")
            logger.info("Added column 'matching_attempt' to merged_table.")
            
        if "matching_policy" not in existing_columns:
            cursor.execute("ALTER TABLE merged_table ADD COLUMN matching_policy TEXT;")
            logger.info("Added column 'matching_policy' to merged_table.")
            
        # KSIC 컬럼 추가
        if "ksic" not in existing_columns:
            cursor.execute("ALTER TABLE merged_table ADD COLUMN ksic TEXT;")
            logger.info("Added column 'ksic' to merged_table.")
            
        if "ksic9" not in existing_columns:
            cursor.execute("ALTER TABLE merged_table ADD COLUMN ksic9 TEXT;")
            logger.info("Added column 'ksic9' to merged_table.")            

        return conn, cursor
    except Exception as e:
        logger.error(f"Error ensuring matched columns exist: {e}")
        raise

# ===== 데이터 전처리 함수 =====

def preprocess_merged_table(conn, cursor, force=False):
    """
    merged_table 데이터 전처리 (pandas 벡터화 연산으로 최적화)
    - 중복 처리 방지 기능 추가
    - 진행 상황 표시 개선
    """
    try:
        # 전처리 필요 여부 확인
        if not is_processing_needed(cursor, "전처리", force):
            logger.info("이미 전처리된 데이터입니다. 전처리 단계 건너뜀 (--force 옵션으로 재처리 가능)")
            return
        
        # 필요한 전처리 컬럼 추가 (기존 코드 유지)
        columns_to_check = [
            ("processed_co_nm", "TEXT"),
            ("region_m", "TEXT"),
            ("address_m", "TEXT"),
            ("processed_pst_detail", "TEXT")
        ]
        
        # 컬럼 존재 여부 확인 및 추가
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'merged_table'
        """)
        existing_columns = {row[0] for row in cursor.fetchall()}
        
        for column_name, column_type in columns_to_check:
            if column_name not in existing_columns:
                cursor.execute(f"ALTER TABLE merged_table ADD COLUMN {column_name} {column_type};")
                logger.info(f"Added column '{column_name}' to merged_table.")
        
        # 총 레코드 수 확인
        cursor.execute("SELECT COUNT(*) FROM merged_table WHERE matched_company_name IS NULL")
        total_rows = cursor.fetchone()[0]
        logger.info(f"전처리 시작: 총 {total_rows}개 레코드")
        
        # 최적의 배치 크기 설정 (메모리와 성능 균형)
        batch_size = 50000  # 5만 건씩 처리 (이전보다 더 큰 배치)
        
        # 개선된 진행 상황 표시 생성
        progress_bar = create_progress_bar(total_rows, "데이터 전처리")
        
        # 배치 처리
        offset = 0
        processed_count = 0
        
        while offset < total_rows:
            # 현재 배치 크기 계산
            current_batch_size = min(batch_size, total_rows - offset)
            
            # 배치 데이터 조회
            query = f"""
                SELECT 
                    pst_id, co_nm, loc, corp_addr, pst_detail, pst_title, source
                FROM merged_table
                WHERE matched_company_name IS NULL
                ORDER BY pst_id
                LIMIT {current_batch_size} OFFSET {offset}
            """
            
            # pandas로 직접 조회하여 데이터프레임 생성 (I/O 최적화)
            batch_df = pd.read_sql(query, conn)
            
            if batch_df.empty:
                break  # 더 이상 처리할 데이터가 없으면 종료
            
            # 배치 크기 재확인
            batch_count = len(batch_df)
            
            # pandas 벡터화 연산으로 전처리 수행
            
            # 회사명 정규화 (null 처리 포함)
            batch_df['processed_co_nm'] = batch_df['co_nm'].apply(
                lambda x: normalize_company_name(x) if pd.notna(x) else ""
            )
            
            # 지역 정규화
            batch_df['region_m'] = batch_df['loc'].apply(
                lambda x: normalize_region(x) if pd.notna(x) else ""
            )
            
            # 주소 정규화
            batch_df['address_m'] = batch_df['corp_addr'].apply(
                lambda x: extract_address_for_matching(x) if pd.notna(x) else ""
            )
            
            # pst_detail 전처리 (pst_title과 결합)
            def combine_and_preprocess(row):
                combined_text = ""
                if pd.notna(row['pst_title']):
                    combined_text += str(row['pst_title']) + " "
                if pd.notna(row['pst_detail']):
                    combined_text += str(row['pst_detail'])
                return preprocess_text(combined_text) if combined_text else ""
            
            # 텍스트 전처리 적용
            batch_df['processed_pst_detail'] = batch_df.apply(combine_and_preprocess, axis=1)
            
            # 업데이트할 데이터만 추출
            update_data = batch_df[['processed_co_nm', 'region_m', 'address_m', 
                                  'processed_pst_detail', 'pst_id']].values.tolist()
            
            # 효율적인 일괄 업데이트
            update_query = """
                UPDATE merged_table 
                SET 
                    processed_co_nm = %s,
                    region_m = %s,
                    address_m = %s,
                    processed_pst_detail = %s
                WHERE pst_id = %s;
            """
            
            # executemany로 일괄 업데이트
            cursor.executemany(update_query, update_data)
            conn.commit()
            
            # 진행 상황 업데이트
            processed_count += batch_count
            progress_bar.update(batch_count)
            
            # 진행 상황 로깅 (10만건 간격으로 통계 표시)
            current_progress = min(offset + batch_count, total_rows)
            if current_progress % 100000 == 0 or current_progress == total_rows:
                logger.info(f"전처리 진행: {current_progress}/{total_rows} 완료 ({(current_progress/total_rows*100):.1f}%)")
            
            # 다음 배치로 이동
            offset += batch_count
            
            # 명시적인 메모리 해제 (불필요한 배치 데이터)
            del batch_df
            del update_data
        
        # 진행 바 닫기
        progress_bar.close()
        
        # 처리 상태 업데이트
        update_processing_status(cursor, conn, "전처리", total_rows)
        
        logger.info(f"전처리 완료: {total_rows}개 레코드 처리됨")
        
    except Exception as e:
        logger.error(f"데이터 전처리 오류: {e}")
        raise

def prepare_elasticsearch_data(es, force=False):
    """
    Elasticsearch 인덱스 준비 및 필요시 데이터 전처리
    - 중복 처리 방지 기능 추가
    - 진행 상황 표시 개선
    
    Args:
        es: Elasticsearch 클라이언트 객체
        force (bool): True인 경우, 모든 필드가 이미 존재하더라도 데이터 전처리를 강제 실행
    
    Returns:
        bool: 성공 여부
    """
    try:
        # ES 중복 처리 확인 (간단한 방법)
        if not force:
            # 샘플 문서로 처리 여부 확인
            sample_query = {
                "query": {"match_all": {}},
                "size": 1
            }
            
            sample_response = es.search(index="company_info", body=sample_query)
            
            if sample_response['hits']['hits']:
                sample_doc = sample_response['hits']['hits'][0]['_source']
                
                # 필요한 필드들이 이미 있는지 확인
                has_processed_title = 'processed_title' in sample_doc
                has_region_m = 'region_m' in sample_doc
                has_address_m = 'address_m' in sample_doc
                
                # 모든 필드가 있으면 처리 불필요
                if has_processed_title and has_region_m and has_address_m:
                    logger.info("이미 인덱싱된 Elasticsearch 데이터입니다. 인덱싱 단계 건너뜀 (--force 옵션으로 재처리 가능)")
                    return True
        
        # 인덱스 존재 확인
        if not es.indices.exists(index="company_info"):
            logger.error("company_info index does not exist in Elasticsearch")
            return False
        
        # 인덱스 매핑 업데이트 - 새 필드 추가
        mapping_update = {
            "properties": {
                "processed_title": {"type": "text"},
                "region_m": {"type": "keyword"},
                "address_m": {"type": "text"}
            }
        }
        
        # 매핑 업데이트 시도 (이미 존재해도 오류 안 남)
        try:
            es.indices.put_mapping(index="company_info", body=mapping_update)
            logger.info("Updated mapping for company_info index")
        except Exception as e:
            logger.warning(f"Failed to update mapping: {e}. This might be OK if fields already exist.")
        
        # 모든 문서를 가져와서 처리한 후 다시 색인화
        query = {
            "query": {
                "match_all": {}
            },
            "size": 1000  # 적절한 크기로 조정
        }
        
        response = es.search(index="company_info", body=query)
        total_hits = response['hits']['total']['value']
        logger.info(f"Found {total_hits} documents in company_info index")
        
        # 배치 처리를 위한 준비
        batch_size = 1000
        processed_count = 0
        
        # 개선된 진행 상황 표시 생성
        progress_bar = create_progress_bar(total_hits, "Elasticsearch 인덱싱")
        
        # 스크롤 API를 사용하여 모든 문서 처리
        scroll_id = None
        scroll_time = "5m"
        
        try:
            # 첫 스크롤 검색
            response = es.search(
                index="company_info",
                body=query,
                scroll=scroll_time
            )
            
            scroll_id = response["_scroll_id"]
            batch_num = 1
            
            while True:
                batch_hits = response["hits"]["hits"]
                if not batch_hits:
                    break
                    
                # 진행 상황 로깅 (50개 배치마다)
                if batch_num % 50 == 0 or batch_num == 1:
                    logger.info(f"Processing Elasticsearch batch {batch_num} with {len(batch_hits)} documents")
                
                # 벌크 업데이트 요청 준비
                bulk_actions = []
                
                for hit in batch_hits:
                    doc_id = hit["_id"]
                    source = hit["_source"]
                    
                    # 필드 전처리
                    title = source.get("title", "")
                    address = source.get("address", "")
                    
                    processed_title = normalize_company_name(title)
                    region_m = normalize_region(address)
                    address_m = extract_address_for_matching(address)
                    
                    # 업데이트 작업 추가
                    update_action = {
                        "update": {
                            "_index": "company_info",
                            "_id": doc_id
                        }
                    }
                    
                    update_doc = {
                        "doc": {
                            "processed_title": processed_title,
                            "region_m": region_m,
                            "address_m": address_m
                        }
                    }
                    
                    bulk_actions.append(update_action)
                    bulk_actions.append(update_doc)
                
                # 벌크 요청 실행
                if bulk_actions:
                    es.bulk(body=bulk_actions, refresh=True)
                    processed_count += len(batch_hits)
                    progress_bar.update(len(batch_hits))
                    
                    # 진행 상황 로깅 (50만건마다)
                    if processed_count % 500000 == 0 or processed_count >= total_hits:
                        logger.info(f"Updated {processed_count}/{total_hits} documents ({(processed_count/total_hits*100):.1f}%)")
                
                # 다음 배치 가져오기
                response = es.scroll(scroll_id=scroll_id, scroll=scroll_time)
                batch_num += 1
                
                # 마지막 배치인지 확인
                if len(batch_hits) < batch_size:
                    break
        
        finally:
            # 스크롤 정리
            if scroll_id:
                es.clear_scroll(scroll_id=scroll_id)
            
            # 진행 바 닫기
            progress_bar.close()
        
        logger.info(f"Processed {processed_count} documents in Elasticsearch")
        return True
        
    except Exception as e:
        logger.error(f"Error preparing Elasticsearch data: {e}")
        return False

def fetch_preprocessed_companies(matching_attempt=None) -> pd.DataFrame:
    """전처리된 회사 정보 조회 (미매칭 데이터만)"""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        
        # 기본 쿼리
        query = """
            SELECT 
                pst_id, co_nm, processed_co_nm, 
                loc, region_m, 
                corp_addr, address_m,
                pst_detail, processed_pst_detail, 
                pst_title, source, matching_attempt
            FROM merged_table
            WHERE matched_company_name IS NULL
        """
        
        # 매칭 시도 차수에 따른 필터링
        if matching_attempt is not None:
            query += f" AND matching_attempt = {matching_attempt}"
            
        query += " ORDER BY pst_id;"
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        # 타입별 건수 로깅
        with_co_nm = len(df[df['co_nm'].notna() & (df['co_nm'] != '')])
        without_co_nm = len(df) - with_co_nm
        
        logger.info(f"Fetched {len(df)} preprocessed unmatched companies from PostgreSQL")
        logger.info(f"Distribution: with co_nm={with_co_nm}, without co_nm={without_co_nm}")
        
        return df
    except Exception as e:
        logger.error(f"Error fetching preprocessed company data: {e}")
        raise

# ===== 회사명 매칭 함수 =====

def match_company(es: Elasticsearch, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    회사명 유무에 따른 매칭 로직 적용 
    - policy_name 오류 수정
    """
    logger = logging.getLogger(__name__)
    pst_id = row.get("pst_id", "")
    
    try:
        # 매칭 정책 결정
        matching_attempt = row.get("matching_attempt", 0)
        
        # policy_name 변수 명시적 선언
        policy_name = "strict"
        if matching_attempt == 1:
            policy_name = "moderate"
        elif matching_attempt >= 2:
            policy_name = "relaxed"
            
        # 정책이 정의되어 있지 않으면 기본값 사용
        if not 'MATCHING_POLICIES' in globals() or policy_name not in MATCHING_POLICIES:
            # 기본 정책 정의
            policy = {
                "short_name_length": 3,
                "very_short_name_length": 4,
                "similarity_threshold_short": 0.8,
                "similarity_threshold_normal": 0.7,
                "score_threshold_short": 0.85,
                "score_threshold_normal": 0.75,
                "fuzziness": "1",
                "prefix_length": 2,
                "name_weight": 0.6,
                "region_weight": 0.3,
                "address_weight": 0.2,
                # pst_detail 관련 설정 추가
                "min_threshold_short": 0.65,
                "min_threshold_medium": 0.55,
                "min_threshold_normal": 0.35
            }
        else:
            policy = MATCHING_POLICIES[policy_name]
        
        # policy 객체에 정책 이름 추가 (중요: policy_name 참조 오류 해결)
        policy['name'] = policy_name
        
        # 기본 정보
        co_nm = row.get("co_nm", "")
        processed_co_nm = row.get("processed_co_nm", "")
        pst_detail = row.get("pst_detail", "")
        processed_pst_detail = row.get("processed_pst_detail", "")
        
        # 상세 로깅은 디버그 레벨로 설정
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"pst_id={pst_id} 매칭 시작 (정책: {policy_name}, 시도: {matching_attempt})")
        
        # co_nm 존재 여부에 따라 다른 매칭 전략 적용
        if co_nm and processed_co_nm:
            # co_nm이 있으면 회사명 기반 매칭
            result = match_by_company_name(es, row, policy)
            if result:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"pst_id={pst_id} 회사명 기반 매칭 성공: {result['matched_company_name']}")
                # 매칭 정책 정보 추가
                result["matching_policy"] = policy_name
                return result
            else:
                # 회사명 매칭 실패 시 상세 내용 기반 매칭도 시도
                if pst_detail or processed_pst_detail:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"pst_id={pst_id} 회사명 매칭 실패, pst_detail 매칭 시도")
                    result = match_by_pst_detail(es, row, policy)
                    if result:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"pst_id={pst_id} pst_detail 매칭 성공: {result['matched_company_name']}")
                        # 매칭 정책 정보 추가
                        result["matching_policy"] = policy_name
                    return result
                return None
        elif pst_detail or processed_pst_detail:
            # co_nm이 없지만 pst_detail이 있으면 상세 내용 기반 매칭
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"pst_id={pst_id} co_nm 없음, pst_detail 매칭 시도")
            result = match_by_pst_detail(es, row, policy)
            if result:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"pst_id={pst_id} pst_detail 매칭 성공: {result['matched_company_name']}")
                # 매칭 정책 정보 추가
                result["matching_policy"] = policy_name
            return result
        else:
            # 둘 다 없으면 매칭 불가
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"pst_id={pst_id} 매칭 데이터 부족")
            return None
        
    except Exception as e:
        logger.error(f"pst_id={pst_id} 매칭 오류: {e}")
        return None

def calculate_name_similarity(name1, name2):
    """
    두 회사명 간의 유사도를 계산
    - 정확 일치, 접두사 일치, 자카드 유사도 등 다양한 방법 적용
    - 최종 유사도는 0~1 사이 값으로 반환
    """
    if not name1 or not name2:
        return 0.0
    
    # 두 이름 모두 소문자로 변환
    name1 = name1.lower()
    name2 = name2.lower()
    
    # 1. 정확히 일치하는 경우
    if name1 == name2:
        return 1.0
    
    # 2. 한 이름이 다른 이름의 정확한 접두사인 경우 (예: "삼성" vs "삼성전자")
    # 짧은 이름이 긴 이름의 접두사인 경우에만 높은 점수
    if len(name1) < len(name2) and name2.startswith(name1):
        prefix_ratio = len(name1) / len(name2)
        # 접두사가 짧으면 유사도를 낮게 평가
        if len(name1) < 3:
            return 0.5 * prefix_ratio
        return 0.8 * prefix_ratio
    elif len(name2) < len(name1) and name1.startswith(name2):
        prefix_ratio = len(name2) / len(name1)
        if len(name2) < 3:
            return 0.5 * prefix_ratio
        return 0.8 * prefix_ratio
    
    # 3. 자카드 유사도 계산 (문자 기반)
    set1 = set(name1)
    set2 = set(name2)
    
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    
    char_similarity = intersection / union if union > 0 else 0
    
    # 4. 자카드 유사도 계산 (2글자 n-gram 기반)
    ngrams1 = set()
    ngrams2 = set()
    
    for i in range(len(name1) - 1):
        ngrams1.add(name1[i:i+2])
    
    for i in range(len(name2) - 1):
        ngrams2.add(name2[i:i+2])
    
    ngram_intersection = len(ngrams1.intersection(ngrams2))
    ngram_union = len(ngrams1.union(ngrams2))
    
    ngram_similarity = ngram_intersection / ngram_union if ngram_union > 0 else 0
    
    # 5. 길이 차이에 따른 페널티
    length_ratio = min(len(name1), len(name2)) / max(len(name1), len(name2))
    
    # 최종 유사도 계산 (가중 조합)
    # n-gram 유사도에 더 높은 가중치 부여
    weighted_similarity = (char_similarity * 0.3) + (ngram_similarity * 0.5) + (length_ratio * 0.2)
    
    return weighted_similarity

def search_by_company_name(es, processed_name, policy=None):
    """회사명 기반 후보 검색 (오매칭 방지 강화)"""
    if not processed_name:
        return []
    
    # 정책이 지정되지 않은 경우 기본값 사용
    if policy is None:
        policy = MATCHING_POLICIES["strict"]
    
    # 짧은 회사명은 더 엄격한 매칭 필요
    is_short_name = len(processed_name) <= policy["short_name_length"]
    
    # 여러 매칭 전략 시도
    strategies = [
        # 1. 정확한 회사명 매칭 (가장 높은 신뢰도)
        {
            "query": {
                "match_phrase": {
                    "processed_title": processed_name
                }
            },
            "_source": ["title", "processed_title", "region_m", "address_m", "bzno", "ksic", "ksic9"],
            "size": 3
        }
    ]
    
    # 짧은 이름이 아닐 때만 추가 전략 적용
    if not is_short_name:
        # 2. 정확한 접두사 매칭 (앞부분이 정확히 일치)
        prefix_query = {
            "query": {
                "prefix": {
                    "processed_title": processed_name
                }
            },
            "size": 5
        }
        
        # 3. 퍼지 매칭 (짧은 이름에는 적용하지 않음)
        fuzzy_query = {
            "query": {
                "match": {
                    "processed_title": {
                        "query": processed_name,
                        "fuzziness": policy["fuzziness"],
                        "prefix_length": policy["prefix_length"]
                    }
                }
            },
            "size": 5
        }
        
        strategies.append(prefix_query)
        strategies.append(fuzzy_query)
    
    all_candidates = []
    
    # 각 전략 시도
    for query_body in strategies:
        try:
            response = es.search(index="company_info", body=query_body)
            hits = response["hits"]["hits"]
            
            if hits:
                # 중복 제거를 위해 ID 기준으로 추적
                existing_ids = {c["_id"] for c in all_candidates}
                
                # 새로운 후보 추가
                for hit in hits:
                    if hit["_id"] not in existing_ids:
                        all_candidates.append(hit)
                        existing_ids.add(hit["_id"])
        except Exception as e:
            logger.error(f"Error in search strategy: {e}")
    
    # 후보가 있으면 유사도 기반 필터링
    filtered_candidates = []
    
    for candidate in all_candidates:
        candidate_title = candidate["_source"].get("processed_title", "")
        similarity = calculate_name_similarity(processed_name, candidate_title)
        
        # 유사도 임계값 (짧은 이름은 더 높은 임계값 적용)
        threshold = policy["similarity_threshold_short"] if is_short_name else policy["similarity_threshold_normal"]
        
        if similarity >= threshold:
            # 유사도 점수를 후보에 추가
            candidate["name_similarity"] = similarity
            filtered_candidates.append(candidate)
    
    # 유사도 점수 기준으로 정렬
    filtered_candidates.sort(key=lambda x: x.get("name_similarity", 0), reverse=True)
    
    return filtered_candidates[:5]  # 최대 5개 후보 반환

def match_by_company_name(es: Elasticsearch, row: Dict[str, Any], policy=None) -> Optional[Dict[str, Any]]:
    """
    회사명 기반 매칭 로직 (개선된 버전)
    - strict가 아닌 정책에서는 지역/주소 일치를 보너스로 처리
    """
    # 정책이 지정되지 않은 경우 기본값 사용
    if policy is None:
        policy = MATCHING_POLICIES["strict"]
    
    # 정책 이름 확인
    policy_name = policy.get('name', 'strict')
    
    # 회사명 정보
    co_nm = row.get("co_nm", "")
    processed_co_nm = row.get("processed_co_nm", "")
    
    # 지역 정보
    region_m = row.get("region_m", "")
    
    # 주소 정보
    address_m = row.get("address_m", "")
    
    # co_nm이 없으면 매칭하지 않음
    if not co_nm or not processed_co_nm:
        return None
    
    # 회사명 유사도 기반 후보 검색
    name_candidates = search_by_company_name(es, processed_co_nm, policy)
    if not name_candidates:
        return None
    
    # 각 후보에 대해 점수 계산 (지역/주소 일치 여부 고려)
    scored_candidates = []
    
    for candidate in name_candidates:
        source = candidate["_source"]
        
        # 이름 유사도 점수
        name_similarity = candidate.get("name_similarity", 0.0)
        
        # 지역 일치 여부
        candidate_region = source.get("region_m", "")
        region_match = False
        
        if region_m and candidate_region and region_m == candidate_region:
            region_match = True
        
        # 주소 일치 여부
        candidate_address = source.get("address_m", "")
        address_match = False
        
        if address_m and candidate_address and address_m == candidate_address:
            address_match = True
        
        # 매우 짧은 이름 처리 (정책에 따라 다름)
        is_very_short_name = len(processed_co_nm) <= policy["very_short_name_length"]
        
        # strict 정책에서만 지역/주소 일치를 필수로 함 (매우 짧은 이름의 경우)
        if policy_name == "strict" and is_very_short_name and not (region_match or address_match):
            continue  # strict 정책에서는 매우 짧은 이름에 추가 검증 필요
        
        # 최종 점수 계산 (가중 조합)
        # 기본 점수는 이름 유사도에 기반
        total_score = name_similarity * policy["name_weight"]
        
        # 지역/주소 일치는 보너스 점수로 추가
        if region_match:
            total_score += policy["region_weight"]
        if address_match:
            total_score += policy["address_weight"]
        
        # 짧은 이름 여부
        is_short_name = len(processed_co_nm) <= policy["short_name_length"]
        
        # 점수 임계값 - strict 정책에서는 더 높게, 다른 정책에서는 낮게 설정
        if policy_name == "strict":
            min_threshold = policy["score_threshold_short"] if is_short_name else policy["score_threshold_normal"]
        else:
            # 완화된 정책에서는 임계값을 더 낮게 설정 (기존 정책값에서 추가 할인)
            threshold_discount = 0.1  # 임계값 할인 (moderate, relaxed, very_relaxed에 적용)
            min_threshold = (policy["score_threshold_short"] if is_short_name else policy["score_threshold_normal"]) - threshold_discount
        
        # 임계값 이상이면 후보로 추가
        if total_score >= min_threshold:
            scored_candidates.append({
                "candidate": candidate,
                "total_score": total_score,
                "name_similarity": name_similarity,
                "region_match": region_match,
                "address_match": address_match
            })
    
    # 점수가 가장 높은 후보 선택
    if scored_candidates:
        best_match = max(scored_candidates, key=lambda x: x["total_score"])
        
        # 매칭 형태 결정
        if best_match["region_match"] and best_match["address_match"]:
            match_type = "name_region_address_match"
            confidence = best_match["total_score"]
        elif best_match["region_match"]:
            match_type = "name_region_match"
            confidence = best_match["total_score"]
        elif best_match["address_match"]:
            match_type = "name_address_match"
            confidence = best_match["total_score"]
        else:
            match_type = "name_only_match"
            confidence = best_match["total_score"]
        
        # 로깅 - 매칭 품질 확인용
        logger.debug(
            f"Match: '{co_nm}' -> '{best_match['candidate']['_source']['title']}' "
            f"(Score: {confidence:.2f}, Type: {match_type}, Policy: {policy_name}, "
            f"Name Sim: {best_match['name_similarity']:.2f})"
        )
        
        # 최종 매칭 결과 생성
        return create_match_result(
            best_match["candidate"], 
            row, 
            match_type, 
            confidence
        )
    
    return None

def extract_potential_company_names(text):
    """
    pst_detail에서 잠재적인 회사명을 추출하는 함수
    - 첫 문장의 첫 단어
    - 법인 표시((주), ㈜, 주식회사 등) 근처 단어
    """
    if not text:
        return []
    
    potential_names = []
    
    # 1. 첫 문장 추출
    first_sentence_match = re.search(r'^([^.!?]+)[.!?]', text)
    if first_sentence_match:
        first_sentence = first_sentence_match.group(1).strip()
        # 첫 문장의 첫 단어 (괄호가 있을 경우 포함)
        words = first_sentence.split()
        if words:
            first_word = words[0].strip()
            if len(first_word) >= 2 and first_word.lower() not in COMPANY_NAME_BLOCKLIST:
                potential_names.append(first_word)
            
            # 첫 문장의 첫 2~3개 단어 조합도 고려
            if len(words) >= 2:
                two_words = ' '.join(words[:2]).strip()
                if len(two_words) >= 3:
                    potential_names.append(two_words)
            
            if len(words) >= 3:
                three_words = ' '.join(words[:3]).strip()
                if len(three_words) >= 4:
                    potential_names.append(three_words)
    
    # 2. 법인 표시 근처 단어 추출
    company_patterns = [
        r'(\S+)\s*\(주\)',              # 회사명(주)
        r'(\S+)\s*㈜',                  # 회사명㈜
        r'(\S+)\s*주식회사',            # 회사명주식회사
        r'주식회사\s*(\S+)',            # 주식회사회사명
        r'\(주\)\s*(\S+)',              # (주)회사명
        r'㈜\s*(\S+)',                  # ㈜회사명
        r'(\S+)\s*\(유\)',              # 회사명(유)
        r'\(유\)\s*(\S+)',              # (유)회사명
        r'(\S+)\s*\(합\)',              # 회사명(합)
        r'\(합\)\s*(\S+)',              # (합)회사명
        r'(\S+)\s*\(재\)',              # 회사명(재)
        r'\(재\)\s*(\S+)',              # (재)회사명
        r'재단법인\s*(\S+)',            # 재단법인회사명
        r'(\S+)\s*재단법인',            # 회사명재단법인
        r'사단법인\s*(\S+)',            # 사단법인회사명
        r'(\S+)\s*사단법인'             # 회사명사단법인
    ]
    
    for pattern in company_patterns:
        matches = re.finditer(pattern, text)
        for match in matches:
            company_name = match.group(1).strip()
            if len(company_name) >= 2 and company_name.lower() not in COMPANY_NAME_BLOCKLIST:
                # 법인명 패턴 주변의 단어는 신뢰도가 높으므로 원본과 주변 컨텍스트도 저장
                context_start = max(0, match.start() - 10)
                context_end = min(len(text), match.end() + 10)
                context = text[context_start:context_end].strip()
                
                potential_names.append({
                    'name': company_name,
                    'pattern': pattern,
                    'context': context,
                    'confidence': 0.9  # 법인 표시 근처는 신뢰도 높음
                })
    
    return potential_names

def search_companies_in_pst_detail(es, text_to_use, policy=None):
    """채용공고 상세 내용에서 회사명 검색 (개선된 버전)"""
    if not text_to_use:
        return []
    
    try:
        # 기본 정책 설정
        if policy is None:
            policy = MATCHING_POLICIES["strict"]["pst_detail"]
        
        # 1단계: 전처리된 텍스트로 회사명 후보 검색 (Elasticsearch 활용)
        candidate_query = {
            "query": {
                "bool": {
                    "should": [
                        # 1. 정확한 회사명 매칭
                        {
                            "match_phrase": {
                                "title": {
                                    "query": text_to_use,
                                    "slop": 0  # 정확한 매칭
                                }
                            }
                        },
                        # 2. 전처리된 회사명 매칭
                        {
                            "match_phrase": {
                                "processed_title": {
                                    "query": text_to_use,
                                    "slop": 0  # 정확한 매칭
                                }
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            },
            "size": 100,  # 충분한 후보 검색
            "_source": ["title", "processed_title", "region_m", "address_m", "bzno"]
        }
        
        response = es.search(index="company_info", body=candidate_query)
        candidates = response['hits']['hits']
        
        # 2단계: 후보 회사명이 원본 텍스트에 포함되어 있는지 확인 (정확한 매칭 검증)
        verified_candidates = []
        
        # 앞부분 텍스트 - 200자로 조정
        prefix_text = text_to_use[:200] if len(text_to_use) > 200 else text_to_use
        
        for candidate in candidates:
            source = candidate["_source"]
            company_title = source.get("title", "")
            processed_title = source.get("processed_title", "")
            original_score = candidate["_score"]
            
            # 회사명 블록리스트 체크 (중요 - 길이와 무관하게 항상 적용)
            if not company_title or not processed_title:
                continue
                
            # 블록리스트 회사명 제외 (길이와 무관)
            if processed_title.lower() in COMPANY_NAME_BLOCKLIST:
                continue
                
            # 정확히 일치하는 요청된 제외 회사 목록 검사
            excluded_companies = [
                "준수(주)", "주", "주식회사의", "포지션", "회사", "부동산", 
                "25퍼센트", "이미지", "바리스타", "인천", "구내식당"
            ]
            if company_title in excluded_companies:
                continue
            
            # 회사명 패턴 검사 (가중치 계산용)
            has_company_pattern = bool(re.search(r'(\([주유]\)|주식회사|유한회사|\(주\)|\(유\)|[㈜㈔])', company_title))
            
            # 지역/주소 일치 여부 미리 검사 (짧은 이름 필터링용)
            candidate_region = source.get("region_m", "")  # 이 줄 추가/수정
            candidate_address = source.get("address_m", "")  # 이 줄 추가/수정
            
            region_match = False
            address_match = False
            
            # 후보 회사의 지역 정보가 pst_detail의 지역과 일치하는지
            if candidate_region and region_m and candidate_region == region_m:
                region_match = True
                
            # 후보 회사의 주소 정보가 pst_detail의 주소와 일치하는지
            if candidate_address and address_m and candidate_address == address_m:
                address_match = True
            
            # 짧은 회사명(2글자 이하) 처리
            if processed_title and len(processed_title) <= 2:
                # 특수 짧은 회사명 리스트
                special_short_names = {"sk", "lg", "cj", "kt", "gs", "ds", "hp", "nc", "db", "kb", "sc", "sj", "gm"}
                
                # 특수 리스트에 없고 지역/주소 일치하지 않으면 제외
                if processed_title.lower() not in special_short_names and not (region_match or address_match):
                    continue
            
            # 매칭 점수 초기화
            score = 0
            match_method = "none"
            
            # 1. 원본 회사명 매칭 확인
            if company_title:
                # 앞부분에 있는지 확인 (높은 가중치)
                if company_title in prefix_text:
                    score = 10.0
                    match_method = "exact_title_prefix"
                # 전체 텍스트에 있는지 확인
                elif company_title in text_to_use:
                    score = 8.0
                    match_method = "exact_title"
            
            # 2. 전처리된 회사명 매칭 확인
            if score == 0 and processed_title:
                # 앞부분에 있는지 확인
                if processed_title in prefix_text:
                    score = 7.0
                    match_method = "processed_title_prefix"
                # 전체 텍스트에 있는지 확인
                elif processed_title in text_to_use:
                    score = 5.0
                    match_method = "processed_title"
            
            # 3. 부분 매칭 확인 (3글자 이상 단어로 수정)
            if score == 0 and len(processed_title) >= 3:
                words = processed_title.split()
                valid_words = [word for word in words if len(word) >= 3 and word.lower() not in COMPANY_NAME_BLOCKLIST]
                
                if valid_words:
                    # 앞부분에 단어가 있는지 확인
                    if any(word in prefix_text for word in valid_words):
                        score = 4.0  # 향상된 점수 (3.0 -> 4.0)
                        match_method = "partial_match_prefix"
                    # 전체 텍스트에 단어가 있는지 확인
                    elif any(word in text_to_use for word in valid_words):
                        score = 3.0  # 향상된 점수 (2.0 -> 3.0)
                        match_method = "partial_match"
            
            # 회사 패턴이 있으면 추가 가중치
            if has_company_pattern and score > 0:
                score *= 1.2
                
            # Elasticsearch 점수 가중치 (원래 검색에서의 관련성 반영)
            score += min(original_score / 20.0, 1.0)
            
            # 유효한 점수가 있으면 후보에 추가
            if score > 0:
                verified_candidate = {
                    "_id": candidate["_id"],
                    "_score": score,
                    "_source": source,
                    "match_method": match_method,
                    "region_match": region_match,
                    "address_match": address_match
                }
                verified_candidates.append(verified_candidate)
        
        # 4단계: 점수 기준으로 정렬하고 상위 후보 반환
        verified_candidates.sort(key=lambda x: x["_score"], reverse=True)
        
        return verified_candidates[:10]
        
    except Exception as e:
        logger.error(f"Error in search_companies_in_pst_detail: {e}")
        return []

def match_by_pst_detail(es: Elasticsearch, row: Dict[str, Any], policy=None) -> Optional[Dict[str, Any]]:
    """
    채용공고 상세 내용 기반 매칭 로직 (개선된 버전)
    - 패턴 기반 회사명 추출 활용
    """
    # 기본 정책 설정
    if policy is None:
        policy = MATCHING_POLICIES["strict"]
    
    # pst_detail 특정 정책이 있으면 사용, 없으면 루트 정책 사용
    pst_policy = policy.get("pst_detail", policy)
    
    # 정책 이름 처리
    policy_name = policy.get('name', 'unknown')
    
    # 데이터 준비
    pst_detail = row.get("pst_detail", "")
    processed_pst_detail = row.get("processed_pst_detail", "")
    pst_title = row.get("pst_title", "")
    region_m = row.get("region_m", "")
    address_m = row.get("address_m", "")
    
    # 사용할 텍스트 결정 (전처리된 것 우선)
    text_to_use = processed_pst_detail
    
    # 전처리된 텍스트가 없거나 너무 짧으면 pst_title과 pst_detail을 즉석에서 처리
    if not text_to_use or len(text_to_use) < 20:
        combined_text = ""
        if pst_title:
            combined_text += pst_title + " "
        if pst_detail:
            combined_text += pst_detail
            
        text_to_use = preprocess_text(combined_text)
    
    # 텍스트가 없으면 매칭할 수 없음
    if not text_to_use:
        return None
    
    # 1. 패턴 기반으로 잠재적 회사명 추출
    potential_companies = extract_potential_company_names(text_to_use)
    
    # 2. 제목에서 회사명 패턴 찾기
    if pst_title:
        title_companies = extract_potential_company_names(pst_title)
        # 제목에서 찾은 회사명은 신뢰도가 더 높음
        for company in title_companies:
            if isinstance(company, dict):
                company['confidence'] = 0.95  # 제목에서 찾은 것은 신뢰도 더 높게
            potential_companies.append(company)
    
    # 3. 추출된 잠재적 회사명으로 Elasticsearch 검색
    all_candidates = []
    
    for company_info in potential_companies:
        company_name = company_info if isinstance(company_info, str) else company_info['name']
        base_confidence = company_info['confidence'] if isinstance(company_info, dict) else 0.7
        
        # 짧은 이름(2글자 이하) 필터링
        if len(company_name) <= 2:
            common_short_names = {"sk", "lg", "cj", "kt", "gs", "db", "sc"}
            if company_name.lower() not in common_short_names:
                continue
        
        # Elasticsearch로 회사명 검색
        query = {
            "query": {
                "bool": {
                    "should": [
                        {"match_phrase": {"title": company_name}},
                        {"match_phrase": {"processed_title": company_name}}
                    ],
                    "minimum_should_match": 1
                }
            },
            "size": 5
        }
        
        try:
            response = es.search(index="company_info", body=query)
            hits = response["hits"]["hits"]
            
            if hits:
                for hit in hits:
                    source = hit["_source"]
                    
                    # 이미 추가된 후보인지 확인
                    existing_ids = {c["_id"] for c in all_candidates}
                    if hit["_id"] in existing_ids:
                        continue
                    
                    # 지역/주소 일치 여부 확인
                    region_match = False
                    address_match = False
                    
                    candidate_region = source.get("region_m", "")
                    if region_m and candidate_region and region_m == candidate_region:
                        region_match = True
                    
                    candidate_address = source.get("address_m", "")
                    if address_m and candidate_address and address_m == candidate_address:
                        address_match = True
                    
                    # 기본 점수 계산
                    score = base_confidence * hit["_score"] / 10.0  # 검색 점수 정규화
                    
                    # 지역/주소 일치 시 추가 점수
                    if region_match:
                        score += pst_policy.get("region_weight", 0.3)
                    if address_match:
                        score += pst_policy.get("address_weight", 0.2)
                    
                    hit["adjusted_score"] = score
                    hit["region_match"] = region_match
                    hit["address_match"] = address_match
                    hit["extracted_from"] = "pattern"
                    
                    all_candidates.append(hit)
        
        except Exception as e:
            logger.error(f"패턴 기반 회사명 검색 중 오류: {e}")
    
    # 4. 기존 방식으로 전체 텍스트 기반 검색도 병행 (백업)
    backup_candidates = search_companies_in_pst_detail(es, text_to_use)
    
    # 기존 방식으로 찾은 후보 중 패턴 기반으로 찾지 못한 것 추가
    existing_ids = {c["_id"] for c in all_candidates}
    for candidate in backup_candidates:
        if candidate["_id"] not in existing_ids:
            # 점수 조정 (패턴 기반보다 신뢰도 낮음)
            candidate["adjusted_score"] = candidate["_score"] / 15.0
            candidate["extracted_from"] = "full_text"
            
            # 지역/주소 일치 여부 확인
            source = candidate["_source"]
            region_match = False
            address_match = False
            
            candidate_region = source.get("region_m", "")
            if region_m and candidate_region and region_m == candidate_region:
                region_match = True
                candidate["adjusted_score"] += pst_policy.get("region_weight", 0.3)
            
            candidate_address = source.get("address_m", "")
            if address_m and candidate_address and address_m == candidate_address:
                address_match = True
                candidate["adjusted_score"] += pst_policy.get("address_weight", 0.2)
            
            candidate["region_match"] = region_match
            candidate["address_match"] = address_match
            
            all_candidates.append(candidate)
    
    # 5. 조정된 점수로 후보 정렬
    all_candidates.sort(key=lambda x: x.get("adjusted_score", 0), reverse=True)
    
    # 6. 최적의 후보 선택
    if all_candidates:
        best_candidate = all_candidates[0]
        
        # 임계값 설정 (짧은 이름은 더 높은 임계값)
        min_threshold = pst_policy.get("min_threshold_normal", 0.35)
        best_score = best_candidate.get("adjusted_score", 0)
        
        if best_score >= min_threshold:
            # 매칭 타입 결정
            source = best_candidate["_source"]
            region_match = best_candidate.get("region_match", False)
            address_match = best_candidate.get("address_match", False)
            
            if region_match and address_match:
                match_type = "detail_region_address_match"
            elif region_match:
                match_type = "detail_region_match"
            elif address_match:
                match_type = "detail_address_match"
            else:
                match_type = "detail_only_match"
            
            # 로깅
            logger.debug(
                f"PST Detail Match: Score={best_score:.2f}, "
                f"Company={source.get('title', '')}, "
                f"Type={match_type}, "
                f"Method={best_candidate.get('extracted_from', 'unknown')}"
            )
            
            # 매칭 결과 생성
            return {
                "pst_id": row["pst_id"],
                "matched_company_name": source.get("title", ""),
                "bzno": source.get("bzno", ""),
                "score": best_candidate.get("_score", 0),
                "confidence": best_score,
                "match_type": match_type
            }
    
    return None

def create_match_result(hit, row, match_type, confidence):
    """
    매칭 결과 생성 헬퍼 함수
    - 매칭된 회사 정보와 신뢰도 점수 포함
    """
    return {
        "pst_id": row["pst_id"],
        "matched_company_name": hit["_source"]["title"],
        "bzno": hit["_source"].get("bzno", ""),
        "ksic": hit["_source"].get("ksic", ""),
        "ksic9": hit["_source"].get("ksic9", ""),
        "score": hit["_score"],
        "confidence": confidence,
        "match_type": match_type
    }

def save_matches_to_postgres(matches: List[Dict[str, Any]]) -> None:
    """매칭 결과를 PostgreSQL에 저장하고 ksic 정보 연결"""
    try:
        if not matches:
            logger.info("No matches to save")
            return

        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # 1단계: 기본 매칭 정보 저장
        update_query = """
            UPDATE merged_table 
            SET 
                matched_company_name = %s,
                bzno = %s,
                match_score = %s,
                match_type = %s,
                matching_policy = %s
            WHERE pst_id = %s;
        """

        for match in matches:
            cursor.execute(
                update_query,
                (
                    match['matched_company_name'],
                    match['bzno'],
                    match.get('confidence', 0),
                    match.get('match_type', ''),
                    match.get('matching_policy', 'unknown'),
                    match['pst_id']
                )
            )

        # 2단계: ksic 정보 업데이트 (join 사용)
        ksic_update_query = """
            UPDATE merged_table mt
            SET 
                ksic = cr.ksic,
                ksic9 = cr.ksic9
            FROM company_reference cr
            WHERE mt.bzno = cr.bzno AND mt.pst_id = ANY(%s);
        """
        
        # 매칭된 pst_id 목록
        pst_ids = [match['pst_id'] for match in matches]
        cursor.execute(ksic_update_query, (pst_ids,))

        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Updated {len(matches)} matches in PostgreSQL with ksic information")

    except Exception as e:
        logger.error(f"Error saving matches to PostgreSQL: {e}")
        raise

def update_matching_attempt(conn, cursor, pst_ids=None):
    """매칭 시도 차수 업데이트 (배치 처리 지원)"""
    try:
        if pst_ids is None:
            # 모든 매칭되지 않은 레코드 업데이트
            update_query = """
                UPDATE merged_table 
                SET matching_attempt = matching_attempt + 1
                WHERE matched_company_name IS NULL;
            """
            cursor.execute(update_query)
        else:
            # 대용량 처리를 위한 배치 업데이트
            if len(pst_ids) > 5000:
                # 대량의 ID를 배치로 나눠서 처리
                batch_size = 5000
                for i in range(0, len(pst_ids), batch_size):
                    batch_ids = pst_ids[i:i+batch_size]
                    cursor.execute("""
                        UPDATE merged_table 
                        SET matching_attempt = matching_attempt + 1
                        WHERE pst_id = ANY(%s);
                    """, (batch_ids,))
                    conn.commit()
            else:
                # 일반적인 크기의 배치는 한 번에 처리
                cursor.execute("""
                    UPDATE merged_table 
                    SET matching_attempt = matching_attempt + 1
                    WHERE pst_id = ANY(%s);
                """, (pst_ids,))
            
        conn.commit()
        total_updated = len(pst_ids) if pst_ids else "모든"
        logger.info(f"매칭 시도 차수 증가: {total_updated}건 업데이트됨")
        
    except Exception as e:
        logger.error(f"매칭 시도 차수 업데이트 오류: {e}")
        conn.rollback()
        raise

# ===== 메인 처리 함수 =====

def reset_matching_attempts(conn, cursor, only_unmatched=True):
    """
    matching_attempt 값 리셋 함수
    
    Args:
        conn: PostgreSQL 연결 객체
        cursor: 커서 객체
        only_unmatched: True면 matched_company_name이 NULL인 항목만 리셋
    """
    try:
        if only_unmatched:
            update_query = """
                UPDATE merged_table 
                SET matching_attempt = 0
                WHERE matched_company_name IS NULL;
            """
        else:
            update_query = """
                UPDATE merged_table 
                SET matching_attempt = 0;
            """
        
        cursor.execute(update_query)
        conn.commit()
        
        # 영향 받은 행 수 확인
        affected_rows = cursor.rowcount
        logger.info(f"매칭 시도 차수 리셋: {affected_rows}건 업데이트됨")
        
        return affected_rows
        
    except Exception as e:
        logger.error(f"매칭 시도 차수 리셋 오류: {e}")
        conn.rollback()
        raise

def process_matching_file(force_preprocess=False, force_indexing=False, reset_attempts=True, max_attempts=3) -> str:
    """
    회사 매칭 프로세스 실행
    
    Args:
        force_preprocess: 전처리 단계를 강제로 실행할지 여부
        force_indexing: Elasticsearch 인덱싱을 강제로 실행할지 여부
        reset_attempts: 매칭 시도 차수를 리셋할지 여부
        max_attempts: 최대 매칭 시도 차수 (기본값 3, 0부터 시작하므로 총 4회)
    """
    try:
        logger = setup_enhanced_logging()
        logger.info("===== 회사 매칭 프로세스 시작 =====")

        # 필요한 컬럼이 없으면 추가
        logger.info("데이터베이스 컬럼 검사 및 준비 중...")
        conn, cursor = ensure_matched_columns_exist()
        
        # 처리 상태 테이블 확인 및 생성
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'processing_status'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            cursor.execute("""
                CREATE TABLE processing_status (
                    type_name TEXT PRIMARY KEY,
                    last_processed TIMESTAMP,
                    count INTEGER
                )
            """)
            logger.info("처리 상태 추적 테이블 생성됨")
        
        # 매칭 시도 차수 리셋 옵션이 활성화된 경우
        if reset_attempts:
            logger.info("매칭 시도 차수 리셋 중...")
            reset_count = reset_matching_attempts(conn, cursor)
            logger.info(f"매칭 시도 차수 리셋 완료: {reset_count}건")
        
        # 데이터 전처리 (필요한 경우에만)
        logger.info("데이터 전처리 단계...")
        preprocess_merged_table(conn, cursor, force=force_preprocess)
        
        # Elasticsearch 연결
        logger.info("Elasticsearch 연결 중...")
        es = get_elasticsearch_client()
        
        # Elasticsearch 데이터 전처리 (필요한 경우에만)
        logger.info("Elasticsearch 데이터 준비 단계...")
        prepare_elasticsearch_data(es, force=force_indexing)
        
        # 정책 이름 매핑
        policy_names = ['strict', 'moderate', 'relaxed', 'very_relaxed']
        
        # 모든 매칭 시도 차수를 순차적으로 처리 (최대 차수 설정 가능)
        for attempt in range(max_attempts + 1):  # 0 ~ max_attempts
            # 정책 이름 설정 (범위 초과 시 마지막 정책 사용)
            policy_name = policy_names[min(attempt, len(policy_names) - 1)]
            
            # 현재 차수에 해당하는 미매칭 레코드 수 확인
            cursor.execute("""
                SELECT COUNT(*) FROM merged_table 
                WHERE matched_company_name IS NULL AND matching_attempt = %s
            """, (attempt,))
            attempt_count = cursor.fetchone()[0]
            
            if attempt_count == 0:
                logger.info(f"매칭 시도 차수 {attempt}에 해당하는 미매칭 레코드가 없습니다. 다음 차수로 진행.")
                continue
            
            logger.info(f"매칭 시도 차수 {attempt} 처리 시작 (정책: {policy_name})")
            logger.info(f"매칭 대상: {attempt_count}건")
            
            # 매칭 진행 추적기 초기화
            tracker = MatchingProgressTracker(total_items=attempt_count, log_interval=1000, match_detail_interval=100)
            
            # 배치 크기 설정
            batch_size = 5000
            
            # 배치 처리로 매칭 수행
            offset = 0
            all_matches = []
            
            while offset < attempt_count:
                # 현재 배치 크기 계산
                current_batch_size = min(batch_size, attempt_count - offset)
                
                # 배치 데이터 조회
                cursor.execute("""
                    SELECT 
                        pst_id, co_nm, processed_co_nm, 
                        loc, region_m, 
                        corp_addr, address_m,
                        pst_detail, processed_pst_detail, 
                        pst_title, source, matching_attempt
                    FROM merged_table
                    WHERE matched_company_name IS NULL AND matching_attempt = %s
                    ORDER BY pst_id
                    LIMIT %s OFFSET %s
                """, (attempt, current_batch_size, offset))
                
                rows = cursor.fetchall()
                if not rows:
                    break  # 더 이상 처리할 데이터가 없으면 종료
                
                # 컬럼명
                columns = [desc[0] for desc in cursor.description]
                
                # DataFrame 생성
                batch_df = pd.DataFrame(rows, columns=columns)
                
                # 배치 내에서 병렬 처리
                batch_matches = []
                with ThreadPoolExecutor(max_workers=8) as executor:
                    # 병렬 처리로 매칭 수행
                    future_to_row = {
                        executor.submit(match_company, es, row.to_dict()): row 
                        for _, row in batch_df.iterrows()
                    }
                    
                    for future in as_completed(future_to_row):
                        try:
                            row = future_to_row[future]
                            result = future.result()
                            
                            # 진행 상황 업데이트
                            tracker.update(result)
                            
                            if result:
                                batch_matches.append(result)
                                # 매칭 결과 상세 정보 로깅
                                tracker.log_match_detail(row.to_dict(), result)
                                
                        except Exception as e:
                            logger.error(f"매칭 처리 중 오류 발생: {e}")
                            tracker.update(None)
                            continue
                
                # 매칭 결과 저장
                if batch_matches:
                    logger.info(f"배치 매칭 결과 저장 중: {len(batch_matches)}건...")
                    save_matches_to_postgres(batch_matches)
                    
                    # 전체 매칭 결과 누적 (나중에 통계용)
                    all_matches.extend(batch_matches)
                
                # 매칭되지 않은 레코드의 matching_attempt 증가 (마지막 차수 제외)
                unmatch_pst_ids = batch_df[~batch_df['pst_id'].isin([m['pst_id'] for m in batch_matches])]['pst_id'].tolist()
                if unmatch_pst_ids and attempt < max_attempts:  # 마지막 차수가 아닐 경우에만
                    update_matching_attempt(conn, cursor, unmatch_pst_ids)
                    logger.info(f"매칭 시도 차수 {attempt}에서 {attempt+1}로 증가: {len(unmatch_pst_ids)}건")
                
                # 다음 배치로 이동
                offset += len(rows)
                
                # 메모리 관리
                del batch_df
                del batch_matches
                del future_to_row
                gc.collect()  # 명시적 가비지 컬렉션
            
            # 현재 차수 처리 완료
            tracker.finish()
            logger.info(f"매칭 시도 차수 {attempt} 처리 완료")
            
            # 매칭된 결과 집계
            matched_in_attempt = len(all_matches)
            logger.info(f"매칭 시도 차수 {attempt}에서 매칭된 결과: {matched_in_attempt}건")
            
            # 메모리 관리
            del all_matches
            gc.collect()
        
        # 전체 매칭 결과 확인
        cursor.execute("SELECT COUNT(*) FROM merged_table WHERE matched_company_name IS NOT NULL")
        total_matched = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM merged_table")
        total_records = cursor.fetchone()[0]
        
        match_percentage = (total_matched / total_records) * 100 if total_records > 0 else 0
        logger.info(f"전체 매칭 결과: 총 {total_records}건 중 {total_matched}건 매칭 성공 ({match_percentage:.1f}%)")
        
        cursor.close()
        conn.close()
        logger.info("===== 회사 매칭 프로세스 완료 =====")
        return "Company matching completed successfully"

    except Exception as e:
        logger.error(f"회사 매칭 프로세스 오류: {e}")
        raise

# ===== 테스트 함수 =====

def main_test():
    """
    전처리 기능 테스트 함수
    """
    # 테스트 텍스트
    test_text = """
    <p>글로벌 기업 (주)테스트컴퍼니에서 경력 개발자를 채용합니다.</p>
    <br>
    <strong>채용제목을 입력해주세요</strong>
    <p>포지션: 백엔드 개발자</p>
    <p>담당업무: API 개발 및 유지보수</p>
    <p>지원자격: 경력 3년 이상</p>
    """
    
    # 전처리 결과 확인
    processed = preprocess_text(test_text)
    print("\n--- 전처리 결과 ---")
    print(processed)
    
    # 확인: "포지션:" 이후 텍스트가 절삭되었는지
    position_in_text = "포지션:" in processed
    
    print("\n--- 검증 결과 ---")
    print(f"포지션: 텍스트가 전처리 결과에 포함됨: {position_in_text}")
    
    # 채용제목 텍스트가 제거되었는지 확인
    title_text = "채용제목을 입력해주세요"
    title_in_text = title_text in processed
    
    print(f"채용제목 텍스트가 전처리 결과에 포함됨: {title_in_text}")
    
    # 회사명 추출 확인
    company_name = "(주)테스트컴퍼니"
    company_in_text = company_name in processed
    
    print(f"회사명이 전처리 결과에 포함됨: {company_in_text}")
    
    return {
        "processed": processed,
        "position_in_text": position_in_text,
        "title_in_text": title_in_text,
        "company_in_text": company_in_text
    }

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='회사 매칭 프로세스 실행')
    parser.add_argument('--force-preprocess', action='store_true', help='전처리 단계 강제 실행')
    parser.add_argument('--force-indexing', action='store_true', help='Elasticsearch 인덱싱 강제 실행')
    parser.add_argument('--disable-tqdm', action='store_true', help='진행 바 출력 비활성화')
    
    args = parser.parse_args()
    
    # 환경변수 설정
    if args.disable_tqdm:
        os.environ['DISABLE_TQDM'] = '1'
    
    # 프로세스 실행
    process_matching_file(
        force_preprocess=args.force_preprocess,
        force_indexing=args.force_indexing
    )