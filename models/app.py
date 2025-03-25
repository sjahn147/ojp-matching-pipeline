import torch
import pandas as pd
import logging
from tqdm import tqdm
from kobert.utils import get_tokenizer
from kobert.pytorch_kobert import get_pytorch_kobert_model
import gluonnlp as nlp
import numpy as np
import torch.backends.mkldnn
import os
from flask import Flask, request, jsonify
import io
import time

# CUDA 설정
torch.backends.mkldnn.enabled = False  # CPU SIMD 최적화 비활성화

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 전역 변수 선언
MODEL_PATH = "./models/model.pth"
device = None
model = None
tok = None

class BERTClassifier(torch.nn.Module):
    def __init__(self, bert, hidden_size=768, num_classes=447, dr_rate=0.5):
        super(BERTClassifier, self).__init__()
        self.bert = bert
        self.dr_rate = dr_rate
        self.classifier = torch.nn.Linear(hidden_size, num_classes)
        self.dropout = torch.nn.Dropout(p=dr_rate)

    def gen_attention_mask(self, token_ids, valid_length):
        attention_mask = torch.zeros_like(token_ids)
        for i, v in enumerate(valid_length):
            attention_mask[i, :v] = 1
        return attention_mask.float()

    def forward(self, token_ids, valid_length, segment_ids):
        attention_mask = self.gen_attention_mask(token_ids, valid_length)
        _, pooler = self.bert(
            input_ids=token_ids, 
            token_type_ids=segment_ids.long(),
            attention_mask=attention_mask.float().to(token_ids.device)
        )
        out = self.dropout(pooler)
        return self.classifier(out)

def analyze_model_structure(model_path):
    checkpoint = torch.load(model_path, map_location='cpu')
    if 'config' in checkpoint:
        logger.info(f"Model config found: {checkpoint['config']}")
    
    state_dict = checkpoint['model_state_dict'] if 'model_state_dict' in checkpoint else checkpoint
    structure = {key: state_dict[key].shape for key in state_dict.keys()}
    logger.info(f"Model structure: {structure}")
    return structure

def load_checkpoint(model, checkpoint_path, device):
    logger.info("Loading checkpoint...")
    
    original_structure = analyze_model_structure(checkpoint_path)
    
    checkpoint = torch.load(checkpoint_path, map_location=device)
    if 'model_state_dict' in checkpoint:
        state_dict = checkpoint['model_state_dict']
    else:
        state_dict = checkpoint
        
    current_state = model.state_dict()
    
    new_state_dict = {}
    for key in state_dict:
        if key in current_state:
            if state_dict[key].shape == current_state[key].shape:
                new_state_dict[key] = state_dict[key]
            else:
                logger.warning(f"Shape mismatch for {key}: "
                             f"checkpoint: {state_dict[key].shape}, "
                             f"model: {current_state[key].shape}")
                
    missing_keys = set(current_state.keys()) - set(new_state_dict.keys())
    if missing_keys:
        logger.warning(f"Missing keys: {missing_keys}")
        for key in missing_keys:
            if 'position_ids' in key:
                new_state_dict[key] = model.state_dict()[key]
    
    try:
        model.load_state_dict(new_state_dict, strict=True)
        logger.info("Model loaded successfully with strict=True")
    except Exception as e:
        logger.warning(f"Strict loading failed: {e}")
        model.load_state_dict(new_state_dict, strict=False)
        logger.info("Model loaded with strict=False")
    
    return model

def preprocess_text(text):
    if not isinstance(text, str):
        text = str(text)
    
    max_length = 256
    if len(text) > max_length:
        text = text[:max_length]
    
    return text

class BERTDataset:
    def __init__(self, dataset, sent_idx, bert_tokenizer, max_len, pad, pair):
        transform = nlp.data.BERTSentenceTransform(
            bert_tokenizer, max_seq_length=max_len, pad=pad, pair=pair)
        
        self.sentences = [transform((item,)) for item in dataset]
        self.sent_idx = sent_idx
        
    def __getitem__(self, i):
        token_ids, valid_length, segment_ids = self.sentences[i]
        return (
            token_ids, 
            np.array(valid_length, dtype=np.int64),
            segment_ids
        )

    def __len__(self):
        return len(self.sentences)

def process_batch(texts, batch_size=32, max_len=256):
    """모델을 사용하여 텍스트 처리"""
    global model, tok, device
    
    if model is None or tok is None:
        logger.error("Model or tokenizer not initialized")
        return None
    
    texts = [preprocess_text(text) for text in texts]
    all_predictions = []
    
    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i:i+batch_size]
        
        # 데이터셋 생성
        batch_dataset = BERTDataset(batch_texts, 0, tok, max_len, True, False)
        
        # 배치 처리
        batch_inputs = []
        for j in range(len(batch_dataset)):
            token_ids, valid_length, segment_ids = batch_dataset[j]
            batch_inputs.append((token_ids, valid_length, segment_ids))

        # numpy array로 변환 후 텐서화
        token_ids = np.array([item[0] for item in batch_inputs])
        valid_length = np.array([item[1] for item in batch_inputs])
        segment_ids = np.array([item[2] for item in batch_inputs])

        # 텐서 변환
        token_ids = torch.tensor(token_ids).to(device)
        valid_length = torch.tensor(valid_length).to(device)
        segment_ids = torch.tensor(segment_ids).to(device)

        with torch.no_grad():
            outputs = model(token_ids, valid_length, segment_ids)
            batch_predictions = torch.argmax(outputs, dim=1)
            all_predictions.extend(batch_predictions.cpu().tolist())
    
    return all_predictions

def load_model():
    """모델 로드 함수"""
    global model, tok, device
    
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info(f"Using device: {device}")
    
    try:
        # BERT 모델 로드
        logger.info("Loading KoBERT model...")
        bertmodel, vocab = get_pytorch_kobert_model()
        tokenizer = get_tokenizer()
        tok = nlp.data.BERTSPTokenizer(tokenizer, vocab, lower=False)

        # 모델 초기화
        model = BERTClassifier(bertmodel).to(device)
        
        # 체크포인트 로드
        logger.info(f"Loading checkpoint from: {MODEL_PATH}")
        model = load_checkpoint(model, MODEL_PATH, device)
        model.eval()
        
        logger.info("Model loaded successfully")
        return True
    except Exception as e:
        logger.error(f"Error loading model: {e}")
        logger.exception("Detailed error trace:")
        return False

# Flask 앱 생성
app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    """API 서버 상태 확인"""
    if model is not None and tok is not None:
        return jsonify({"status": "ok", "model_loaded": True})
    else:
        return jsonify({"status": "initializing", "model_loaded": False})

@app.route('/predict', methods=['POST'])
def predict():
    """텍스트 분류 엔드포인트"""
    start_time = time.time()
    
    if not request.json or 'data' not in request.json:
        return jsonify({"error": "Invalid request format. Expected JSON with 'data' field."}), 400
    
    data = request.json['data']
    
    if not isinstance(data, list):
        return jsonify({"error": "Data must be a list of objects with 'pst_id' and 'job_desc' fields."}), 400
    
    if not data:
        return jsonify({"error": "Empty data list provided."}), 400
    
    # 입력 데이터 유효성 검사
    for item in data:
        if not isinstance(item, dict) or 'pst_id' not in item or 'job_desc' not in item:
            return jsonify({"error": "Each data item must have 'pst_id' and 'job_desc' fields."}), 400
    
    # 모델 상태 확인
    if model is None or tok is None:
        load_model()
        if model is None or tok is None:
            return jsonify({"error": "Model initialization failed."}), 500
    
    # 배치 예측 수행
    try:
        texts = [item['job_desc'] for item in data]
        pst_ids = [item['pst_id'] for item in data]
        
        logger.info(f"Processing {len(texts)} texts")
        predictions = process_batch(texts)
        
        if predictions is None:
            return jsonify({"error": "Prediction failed."}), 500
        
        # 결과 생성
        results = [
            {"pst_id": pst_id, "predicted_occupation": pred}
            for pst_id, pred in zip(pst_ids, predictions)
        ]
        
        processing_time = time.time() - start_time
        
        return jsonify({
            "results": results,
            "count": len(results),
            "processing_time_seconds": processing_time
        })
    
    except Exception as e:
        logger.error(f"Error processing prediction: {e}")
        logger.exception("Detailed error trace:")
        return jsonify({"error": str(e)}), 500

@app.route('/predict_csv', methods=['POST'])
def predict_csv():
    """CSV 파일 업로드 처리 엔드포인트"""
    start_time = time.time()
    
    if 'file' not in request.files:
        return jsonify({"error": "No file provided."}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({"error": "Empty filename provided."}), 400
    
    try:
        # CSV 파일 읽기
        df = pd.read_csv(file)
        
        # 필요한 컬럼 확인
        if 'pst_id' not in df.columns or 'job_desc' not in df.columns:
            return jsonify({"error": "CSV must contain 'pst_id' and 'job_desc' columns."}), 400
        
        # 모델 상태 확인
        if model is None or tok is None:
            load_model()
            if model is None or tok is None:
                return jsonify({"error": "Model initialization failed."}), 500
        
        # 배치 예측 수행
        texts = df['job_desc'].tolist()
        pst_ids = df['pst_id'].tolist()
        
        logger.info(f"Processing {len(texts)} texts from CSV file")
        predictions = process_batch(texts)
        
        if predictions is None:
            return jsonify({"error": "Prediction failed."}), 500
        
        # 결과 생성
        results_df = pd.DataFrame({
            "pst_id": pst_ids[:len(predictions)],
            "predicted_occupation": predictions
        })
        
        # CSV 스트링으로 변환
        csv_buffer = io.StringIO()
        results_df.to_csv(csv_buffer, index=False)
        csv_string = csv_buffer.getvalue()
        
        processing_time = time.time() - start_time
        
        return jsonify({
            "csv_result": csv_string,
            "count": len(results_df),
            "processing_time_seconds": processing_time
        })
    
    except Exception as e:
        logger.error(f"Error processing CSV: {e}")
        logger.exception("Detailed error trace:")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # 서버 시작 시 모델 로드
    load_model()
    
    # 서버 실행
    app.run(host='0.0.0.0', port=5000)
