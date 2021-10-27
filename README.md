# 네이버 실시간 검색어 순위 크롤링
- 지정한 키워드들의 네이버 view 검색 결과(키워드들의 수)를 매일 밤 11시 40분에 출력 및 DB에 저장합니다. 
- 저장한 데이터를 주 별로 다시 저장을 합니다. 이유는 년 간 API를 출력시 좀 더 연산을 줄이기 위해서 입니다. 

### 사용방법
1. https://chromedriver.chromium.org/downloads 사이트에서 chromedriver를 chrome버전에 맞게 설치한다.
2. 4개의 pip 패키지를 설치한다.
  - pip install schedule
  - pip install bs4
  - pip install selenium
  - pip install pandas
3. naver_crawling.py를 열어 path를 chromedriver의 위치로 설정한다.
4. 파일을 실행시킨다.

키워드는 아무거나 상관없습니다. 

특이사항은 DB를 pandas를 통해 접속을 하기때문에 pandas를 설치를 해줘야합니다.

DB는 mysql을 사용했습니다. 

url주소 변경시 다른 홈페이지 크롤링도 가능합니다.


