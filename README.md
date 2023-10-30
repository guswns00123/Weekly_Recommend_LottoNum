  <h3 align="center">Weekly Lotto Number Recommendation System</h3>

  <!-- ABOUT THE PROJECT -->
## About The Project
이번 프로젝트의 목표는 데이터파이프라인을 직접 구축하고 관리해보는 것이 목표이다.

그래서 데이터 파이프라인을 주기적으로 관리해보기 위해 Apache Airflow라는 플랫폼을 사용하였다.

또한 Airflow에서 워크플로우(DAG)를 작성하여 스케쥴링, 모너터링을 쉽게 해주는 장점을 적용해볼 수 있는 시스템을 생각 해 본 결과, 매 주 로또번호를 추천해주는 시스템을 만들어 보기로 하였다. 

프로젝트의 가장 큰 목표

1. 먼저 주기적인 데이터를 지속적으로 업데이트하여 매 주 새로운 정보 받아보기

2. 각 task들을 지속적으로 모니터링해보기

3. 다양한 외부 툴(DB, kakao 알림)들을 사용해보기
4. 
## Main Features
![image](https://github.com/guswns00123/Weekly_Recommend_LottoNum/assets/65805176/acd13bd4-0643-467c-90fb-7b3ead55373a)



### Built With
 <img src="https://img.shields.io/badge/Apache Ariflow-017CEE?style=flat&logo=apacheairflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/Postgresql-4169E1?style=flat&logo=postgresql&logoColor=white"/>
    <img src="https://img.shields.io/badge/AWS S3-569A31?style=flat&logo=amazons3&logoColor=white"/>
    <img src="https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white"/>
