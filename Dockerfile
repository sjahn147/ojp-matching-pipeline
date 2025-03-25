FROM apache/airflow:2.10.5
USER root
# 필수 시스템 패키지 설치
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    curl \
    git \
    unzip \
    gcc \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*
# 최신 pip로 업그레이드
RUN python -m pip install --upgrade pip
# 필수 데이터 사이언스 및 유틸리티 패키지 설치
USER airflow
RUN pip install --no-cache-dir \
    numpy \
    pandas \
    scipy \
    scikit-learn \
    "elasticsearch==8.12.0" \
    "urllib3>=1.26.0,<2.0.0" \
    certifi \
    openpyxl \
    tqdm \
    rapidfuzz \
    loguru \
    "dask[complete]" \
    distributed