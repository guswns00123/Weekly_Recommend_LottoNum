  <h3 align="center">Weekly Lotto Number Recommendation System</h3>

  <!-- ABOUT THE PROJECT -->
## About The Project
이 프로젝트는 매주 자동으로 로또 당첨 데이터를 수집하여 추천 번호를 생성하고 이를 사용자에게 알리는 자동화된 파이프라인을 구축하는 시스템입니다. 

Apache Airflow를 사용하여 주간 데이터 수집, 로또 번호 예측, 알림 시스템을 자동화하고, Prometheus와 Grafana를 활용하여 시스템의 성능 모니터링을 구축하였습니다.

## 기간
2023.09 : 과거 데이터 수집 및 시스템 구축


2024.10~2024.11 : 보안 기능 강화 및 시스템 안정성 향상

## 기술 스택
<img src="https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white"/> : 데이터 처리 및 로직 구현

<img src="https://img.shields.io/badge/Apache Ariflow-017CEE?style=flat&logo=apacheairflow&logoColor=white"/> : 워크플로우 자동화

<img src="https://img.shields.io/badge/Postgresql-4169E1?style=flat&logo=postgresql&logoColor=white"/> : 데이터 저장

<img src="https://img.shields.io/badge/AWS S3-569A31?style=flat&logo=amazons3&logoColor=white"/> : 백업 및 데이터 공유

![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=Prometheus&logoColor=white) : 시스템 모니터링

![Grafana](https://img.shields.io/badge/grafana-%23F46800.svg?style=for-the-badge&logo=grafana&logoColor=white) : 메트릭 시각화

![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white) : 컨테이너화

## Main Features
![image](https://github.com/guswns00123/Weekly_Recommend_LottoNum/assets/65805176/acd13bd4-0643-467c-90fb-7b3ead55373a)

## 파이프라인 설명
**1. 데이터 수집 및 저장**

DAG 이름: dags_lotto_data

매주 로또 데이터를 URL에서 자동으로 가져와 CSV 파일로 저장한 뒤, PostgreSQL DB에 로드합니다. 또한, S3에 백업을 수행합니다.

![image](https://github.com/user-attachments/assets/d7b31c24-4925-4c68-b3a6-9dcf8fefabad)


**2. 로또 번호 예측 및 알림**

DAG 이름: lotto_number_prediction

PostgreSQL에서 로또 당첨 데이터를 불러와 Scikit-learn을 활용하여 예측 모델을 학습하고, 예측된 번호를 이메일 및 카카오톡으로 전송합니다.

![image (1)](https://github.com/user-attachments/assets/caa852fc-f47c-4353-8a67-371532783c79)

**3. 시스템 모니터링**

Prometheus: Airflow의 각 DAG 실행 시 메트릭을 수집하여 CPU 및 메모리 사용량 등을 추적

Grafana: Prometheus에서 수집된 메트릭을 기반으로 실시간 대시보드를 시각화하여 Airflow 시스템의 성능을 모니터링

![image (2)](https://github.com/user-attachments/assets/7a767644-70e7-46dc-bf74-ce8a25b04d07)

![image (3)](https://github.com/user-attachments/assets/ea234956-469d-4b1c-9abb-c84a9157dd3c)


## 로또 추천 번호 카카오톡 알림 예시

![Untitled](https://github.com/user-attachments/assets/4ad53eb1-3a4f-41e5-bcd9-60bed009f3b9)


  
    
    
