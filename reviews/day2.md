# 7월 3일(수) Day 2

## ETL 프로세스 구현하기 과제 진행

### 리뷰

ETL 프로세스를 구현 해보는 과제를 진행했다.
과제는 위키피디아 있는 IMF가 제공한 나라 별 GDP를 ETL 프로세스에 따라 구현 하는 과제였다.
구현에는 큰 어려움은 없었지만 코드 가독성과 ETL 프로세스를 나누는 과정이 고민을 많이했다.
먼저 코드 가독성 측면에서 절차지향적인 스크립트를 어떻게 가독성이 좋은 코드로 만들 수 있을지 고민이 됐다.
ETL 프로세스에서 Load 단계의 구분은 명확했는데, Extract 단계와 Transform 단계의 구분이 어려웠다.
인터넷에서 검색해보니 추출 단계에서 중요한 것은 추출의 소스가 여러개 일 수 있으므로 인터페이스를 구축하는것이라는 글을 봤다.
그래서 소스가 다르더라고 최소한의 인터페이스는 맞출수 있도록 추출한 데이터를 data frame 타입으로 변환하는 것이 추출 단계의 끝이라고 생각했다.
변환 단계에서는 data frame 타입의 데이터를 데이터 웨어하우스(json파일, sqlite)에 맞게 변환하고 필터링하는것으로 생각하고 진행했다

### KEEP

### PROBLEM

ETL 프로세스에서 Load 단계의 구분은 명확했는데, Extract 단계와 Transform 단계의 구분이 어려웠다.
과제는 거의 다 끝냈기 때문에 내일은 인터넷 검색이 아니라 제대로 된 책을 추천받아서 ETL에 대해 공부해서 코드를 개선할 예정이다.

### TRY

코드의 가독성을 높이기 위해서 적용 할 수 있는 방법을 찾아보고 시도해 봐야겠다.