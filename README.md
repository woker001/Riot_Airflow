# Building Data Pipelines from Riot APIs with Airflow

[![pylint](https://img.shields.io/badge/pylint-9.81-brightgreen)]()
[![airflow](https://img.shields.io/badge/airflow-2.5.3-blue)]()
[![pandas](https://img.shields.io/badge/pandas-1.2.0-blue)]()
[![python](https://img.shields.io/badge/python-3.9-blue)]()
[![postgres](https://img.shields.io/badge/postgres-5.4.0-blue)]()
[![redis](https://img.shields.io/badge/redis-4.5.4-red)]()
[![Maintainability](https://api.codeclimate.com/v1/badges/164d89eefe620e5b9945/maintainability)](https://codeclimate.com/github/woker001/Riot_Airflow/maintainability)  
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
  
### 개요
[Riot API](https://developer.riotgames.com/apis) 로부터 데이터를 수집하여 대시보드로 통계화. 데이터 수집은 Airflow를 사용하여 24시간, 1시간 주기로 데이터를 수집.  
[LOL hepler](https://www.lolhepler.com/) 에 접속하여 시각화 확인 가능.

### 웹 기능
- 검색 기능 
- 사용자별 상세조회 기능 [SEARCH](https://www.lolhepler.com/user/viper3) 
- 랭킹 차트 기능 [RANK](https://www.lolhepler.com/rank)
- 사용자 접속 정보 수집 Google Analytics
- 사용자 이벤트 정보 수집 Amplitude (게임 이용자 조회 기록)

## 데이터 수집
**challengerleagues_queue.py**  
![](image/puuid.png)

**challengerleagues_match.py**  
![](image/match.png)


## 데이터 전처리
- 상위 라인 챔피언
- 상위 이벤트
- 유저별 평균 비교

## 데이터 시각화
![](image/web.png)
