# OJPs Matching Pipeline (Skill Matching & Job Classification System)

본 리포지토리는 대규모 온라인 구인공고 데이터(OJPs)를 처리하여  
직무 분류, 회사명 매칭, 스킬 추출 및 태깅, 중복 제거 등의 과정을  
자동화된 파이프라인으로 구현한 분석 프로젝트의 구조를 공유합니다.

> ⚠️ 데이터 및 모델 파일은 포함되어 있지 않으며, 구조 설명 목적의 코드입니다.

---

## 🔧 프로젝트 개요

- **데이터 소스 예시** (비공개):
  - 온라인 구인공고 원자료 (JobKorea, 사람인 등)
  - 기업명/주소 정보 (사업체 데이터)
  - 스킬 분류 사전 (Skill Taxonomy 기반)

- **주요 기능**:
  1. CSV 데이터 PostgreSQL 적재
  2. 텍스트 유사도 기반 중복 공고 제거
  3. KoBERT 기반 직업 분류
  4. Elasticsearch 기반 회사명/주소 매칭
  5. 정규표현식 + 유사도 기반 숙련 태깅
  6. 파이프라인 자동화 (Airflow + Docker)

---

## 🧱 기술 스택

| 범주 | 사용 기술 |
|------|-----------|
| 워크플로우 | Apache Airflow, Docker Compose |
| 데이터베이스 | PostgreSQL |
| 검색·매칭 | Elasticsearch (Nori tokenizer), RapidFuzz |
| NLP/모델 | KoBERT, Transformers |
| API 서버 | Flask, Gunicorn |
| 언어/라이브러리 | Python, pandas, numpy, PyTorch |

---

## 🗂 디렉토리 구조 예시

```
📦 ojp-matching-pipeline
├── dags/
│   └── ojp_processing.py            # 전체 워크플로우 정의
├── scripts/
│   ├── csv_ingestion.py             # CSV → PostgreSQL
│   ├── drop_duplicate.py           # 중복 제거
│   ├── kobert_classification.py     # 직무 분류 (KoBERT)
│   ├── company_matching.py          # 회사명 매칭
│   ├── match_skills.py              # 스킬 태깅
│   └── process_skill_data.py        # 결과 export
├── model_server/
│   └── app.py                       # API 서버
├── docker-compose.yml
└── Dockerfile
```

---

## ⚠️ 사용 시 유의사항

- 데이터베이스 접속 정보는 `.env` 파일로 분리해야 하며, 실제 환경에서는 동작하지 않습니다.
- 이 리포지토리는 코드 구조 및 분석 흐름을 설명하기 위한 포트폴리오 목적으로 작성되었습니다.

---

## 📄 라이선스 / 저작권 안내

```text
Copyright (c) 2025 Ahn Seongjun  
이 리포지토리는 개인 학습 및 포트폴리오 공유 목적에 한정하여 공개되었습니다.  
재배포, 상업적 사용 등은 원저작자의 사전 동의 없이 허용되지 않습니다.
```

---

## 🙋🏻 프로젝트 목적

- 실무에서 수행한 공공 프로젝트 구조를 바탕으로 분석·모델링·자동화 설계 경험을 공유
- KoBERT 기반 직무 분류, 텍스트 유사도 기반 중복 제거, Elastic 기반 매칭 등 적용 예시 제공