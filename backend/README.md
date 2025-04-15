# 🧠 MCP_Voice_Transfer - Backend System

## 📦 구조 개요

```
backend/
├── auth/              # 인증 모듈 (지문/음성 모킹 등)
├── data/              # 계좌, 로그 등 JSON 저장
├── event/             # Redis Pub/Sub 처리
├── fds/               # 이상거래 탐지
├── gateway/           # FastAPI Gateway + router
├── llm/               # Intent 분석 + gRPC client
├── proto/             # gRPC 메시지 정의 및 컴파일
├── shared/            # 공통 유틸, 로거 등
├── transfer/          # 송금 처리 로직
├── requirements.txt   # 의존성 명세
└── docker-compose.yml # 전체 서비스 실행
```
---

### 백엔드 아키텍처

<br>

#### 1. 논의 사항

<br>

- 모듈간 동기성 연결로써 gRPC(Google Remote Procedure Call) 채택하는 것이 효율적인가?
  - **프로젝트는 완전한 모놀리식으로 구성할 것이라면 오버엔지니어링이므로 불필요**
  - 즉 하나의 서버 내에서 작동하므로 모듈 간 함수 호출이면 충분함
  - 그러나 MSA 구조를 일부라도 채택할 경우 gRPC를 적용하는 것이 좋음. 각 모듈을 독립 서비스로 컨테이너화하기에 유리하기 때문.
  
- LLM → FDS 사이 비동기 처리는 Redis Pub/Sub 구조를 채택하는 것이 효율적인가?
  - **Redis Pub/Sub 구조를 채택해도 좋음.**
  - 아키텍처에 크게 구애 받지 않고 후처리, 병렬 처리, 결합도 분리에 유리하기 때문.
   
- MCP를 사용하는 것이 효율적인가?
  - **MCP는 오버엔지니어링이므로 불필요**
  - 프로젝트의 입력 채널이 음성 앱 하나뿐이므로 MCP의 강점인 ‘멀티 채널인 경우 동일한 시스템으로 데이터 처리’한다는 강점을 이용할 수 없기 때문입니다.
  - 또한 백엔드 아키텍처를 모놀리식으로 채택할 것이라면 입력 구조가 간단하므로 MCP를 적용할 필요 없음
  - 그리고 이미 클라이언트에서 NLU/DM 처리를 하고 있으므로 MCP를 적용할 경우 구조가 중복되어 비효율적일 가능성이 있음.
 
 <br>

#### 2. 폴더 구조 및 아키텍처

<br>

- 모놀리식 + EDA 하이브리드
  -  현재 깃허브 폴더 구조를 보다 효율적으로 수정하고, Redis Pub/Sub 구조를 채택한 것임.
  -  아래 기능들만 eda 구조로 전환하면 좋을 것 같음
      -  송금 완료 이벤트 → 리스크 탐지, 트랜잭션 기록, 보안 알림 전송
      -  로그인 성공 이벤트 → 2차 인증 요청, 사용자 접속 분석
      -  질문 요청 이벤트 → RAG 호출, 응답 비동기 반환
  -  수정 사항
      - 뒤섞여 있는 backend/ 모듈을 분리
      - 추상적 폴더 이름을 실제 기능 단위(nlu, dialog_manager, auth, rag)로 명확히 구분
      - Frontend 내 lib/ 내 폴더를 아키텍처 단위로 분리해서 backend와 구조를 대응.

```
MCP_Voice_Transfer/
├── backend/
│   ├── api/                      # REST/gRPC 엔드포인트
│   │   ├── endpoints/           # 기능별 라우터 (transfer, auth, nlu 등)
│   │   └── main.py              # FastAPI 앱 진입점
│   ├── core/                    # 인증, 설정 등 핵심 로직
│   │   ├── auth/                # 1차/2차 인증 처리
│   │   └── security.py          # 인증 관련 보안 유틸
│   ├── modules/                 # 각 도메인 기능 모듈
│   │   ├── nlu/                 # Intent/Slot 분석
│   │   ├── dialog_manager/      # 대화 흐름 관리
│   │   ├── transfer/            # 송금 처리 로직
│   │   ├── rag/                 # 보안 질문 응답용 RAG 모듈
│   │   ├── fds/                 # 이상 거래 탐지 로직
│   │   ├── stt/                 # 서버 STT 처리 모듈
│   │   └── logger/              # 로그 저장소, DB 연동
│   ├── events/                  # 이벤트 프로듀서 / 컨슈머
│   │   ├── producer.py          # 송금 완료 시 이벤트 발행
│   │   ├── consumer_rag.py      # 질문 응답 요청 이벤트 구독
│   │   ├── consumer_logger.py   # 트랜잭션 기록 이벤트 구독
│   │   └── consumer_fds.py      # 리스크 탐지 이벤트 구독
│   ├── message_broker/          # Kafka/Redis stream 설정
│   │   └── kafka_client.py      # Kafka 프로듀서/컨슈머 공통 로직
│   ├── proto/                   # 메시지 정의 (향후 확장 대비)
│   ├── logs/                    # 로그 저장소 (단순 파일 or DB 연동)
│   ├── requirements.txt         # Python 의존성 명세
│   └── docker-compose.yml       # App 전체 실행 구성 (DB 포함)

├── frontend/
│   ├── lib/
│   │   ├── stt/                 # Whisper / Android STT 연동
│   │   ├── post_processing/     # STT 후처리 (정규화 등)
│   │   ├── nlu/                 # 클라이언트 Intent/Slot 분석
│   │   ├── dialog/              # 대화 흐름 UI 및 상태 관리
│   │   ├── auth/                # 1차 인증 처리
│   │   └── tts/                 # 음성 합성 (TTS) 출력 처리
│   ├── android/
│   ├── ios/
│   └── pubspec.yaml             # Flutter 메타 정보 및 의존성

├── README.md                    # 프로젝트 설명서
```

<br>

- MSA
  - 대규모 서비스를 제공하는 실무 환경에 가까운 구조.    
  - 그러나 해당 구조로 아키텍처 구현 시, 배포할 때 컨테이너 개수를 조절하기 힘들 수 있음
  - 컨테이너 개수가 지나치게 늘어날 경우 테스트 환경에서 운영 구조를 생각하기 어려울 수 있고 서버 비용이 많이 부과될 위험이 있음
```
MCP_Voice_Transfer/
├── services/
│   ├── dialog-service/              # 대화 흐름 제어 (NLU 포함 가능)
│   │   ├── app/
│   │   │   └── main.py              # 서비스 진입점
│   │   ├── modules/
│   │   │   └── dialog_manager/      # 대화 흐름 로직
│   │   ├── requirements.txt         # 서비스별 Python 의존성
│   │   └── Dockerfile               # 컨테이너 정의

│   ├── transfer-service/            # 송금 처리 (핵심 도메인)
│   │   ├── app/
│   │   ├── modules/transfer/        # 송금 로직
│   │   └── Dockerfile

│   ├── auth-service/                # 1차/2차 인증 처리
│   │   ├── app/
│   │   ├── modules/auth/            # 인증 관련 로직
│   │   └── Dockerfile

│   ├── fds-service/                 # 이상 거래 탐지 (FDS)
│   │   ├── app/
│   │   ├── consumer_fds.py          # FDS 이벤트 수신 로직
│   │   └── Dockerfile

│   ├── rag-service/                 # 보안 질문 응답 처리
│   │   ├── app/
│   │   ├── consumer_rag.py          # 질문 이벤트 수신 및 RAG 호출
│   │   └── Dockerfile

│   ├── logger-service/              # 이벤트 로깅/트랜잭션 기록
│   │   ├── app/
│   │   ├── consumer_logger.py       # 로그 이벤트 수신 및 저장
│   │   └── Dockerfile

│   └── api-gateway/                 # 클라이언트 진입점 (FastAPI/NGINX)
│       ├── main.py
│       └── routes/
│           ├── dialog.py
│           ├── transfer.py
│           └── auth.py

├── shared/                          # 공통 코드 저장소
│   ├── proto/                       # gRPC 메시지 및 서비스 정의
│   ├── utils/                       # 유틸리티 함수
│   └── constants.py                 # 공통 상수 정의

├── docker-compose.yml              # 전체 마이크로서비스 오케스트레이션 구성
└── README.md                       # 프로젝트 설명서

```

---

## ✅ 실행법 (로컬 개발 기준)

```bash
# 의존성 설치
pip install -r requirements.txt

# 각 서비스별 실행 (예: gateway)
cd backend/gateway
uvicorn main:app --reload --port 8000

# 또는 전체 실행
docker-compose up --build
```

## 🌐 주요 API

| 경로                | 설명                  |
|---------------------|-----------------------|
| `POST /api/intent`  | 의도 분석 + 이벤트 발행 |
| `POST /api/transfer`| 송금 수행 (더미)        |
| `POST /api/auth`    | 사용자 인증 모킹        |
| `POST /api/log`     | 로그 저장 (JSON 파일)   |
| `GET /api/healthcheck` | 시스템 상태 확인     |






## 2. 모델 성능 검증

###  1. STT 모듈 (음성 → 텍스트 변환)

- **검증 목표**: 음성 입력에 대해 정확한 텍스트 변환 수행 여부 확인
- **평가 지표**: `WER (Word Error Rate)`
- **WER 계산 방식**: WER = (Substitutions + Insertions + Deletions) / Total Words

- - **예시**:
  - 
| 실제 문장              | STT 결과                         | WER |
|------------------------|----------------------------------|-----|
| 엄마한테 오만원 보내줘 | 엄마 한테 5만 원 보내 줘         | 0% (거의 정확) |


### 2. NLU (Intent / Slot 분석)

- **검증 목표**: 사용자의 의도를 정확히 분류하고, 슬롯을 추출하는 능력 평가
- **평가 지표**:
- Intent: Accuracy
- Slot: Precision / Recall / F1-score

**예시 데이터**:

```json
{
  "text": "아빠한테 만원 보내줘",
  "intent": "transfer",
  "slots": {
    "recipient": "아빠",
    "amount": "10000"
  }
}
```

### 3. 음성 로그 데이터 처리

- **검증 목표**: 사용자-시스템 간 대화 흐름과 송금 요청/응답을 정확히 기록하고 관리

#### 검증 항목
- 로그 정합성 (누락된 필드 없음)
- 대화 흐름 추적 가능 여부
- 검색 정확도 (특정 조건 거래 필터링 등)

#### 예시 로그 구조 (JSON)
```json
{
  "timestamp": "2025-04-06T10:00:00Z",
  "text": "엄마한테 3만원 보내줘",
  "intent": "transfer",
  "slots": {
    "recipient": "엄마",
    "amount": 30000
  },
  "authenticated": true,
  "transfer_status": "success"
}
```

### 4. 이상거래 탐지 시스템

- **검증 목표**: 비정상/의심 거래 탐지 정확도 측정
- **사용 데이터**: [IEEE-CIS Fraud Detection Dataset (Kaggle)](https://www.kaggle.com/competitions/ieee-fraud-detection)

#### 평가 지표

- **Precision**: 탐지된 거래 중 실제 이상거래일 확률  
  → `Precision = TP / (TP + FP)`
  
- **Recall**: 전체 이상거래 중 모델이 탐지한 비율  
  → `Recall = TP / (TP + FN)`
  
- **F1-score**: Precision과 Recall의 조화 평균  
  → `F1 = 2 * (Precision * Recall) / (Precision + Recall)`
  
- **AUC (Area Under Curve)**: ROC 곡선 아래 면적, 전체 분류 성능 지표  
  → 1에 가까울수록 성능 우수



