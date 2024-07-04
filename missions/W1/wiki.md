# Week1

## pandas

### [pandas cheatsheet](https://pandas.pydata.org/Pandas_Cheat_Sheet.pdf)

## SQL

### 윈도우 함수 (WINDOW FUNCTION)

1. 그룹 내 순위(RANK) 관련 함수: RANK, DENSE_RANK, ROW_NUMBER
2. 그룹 내 집계(AGGREGATE) 관련 함수 : SUM, MAX, MIN, AVG, COUNT
3. 그룹 내 행 순서 관련 함수 : FIRST_VALUE, LAST_VALUE, LAG, LEAD

* GROUP BY : GROUP BY 절은 데이터를 그룹화하는 데 사용되는 절입니다. GROUP BY 절은 특정 열을 기준으로 데이터를 그룹화하고,
  그룹 내에서 집계 함수를 적용하여 결과를 도출합니다. GROUP BY 절은 집계 함수와 함께 사용되며, 집계 함수를 적용할 그룹의 기준을 제공합니다.

* HAVING 절: HAVING 절은 그룹화된 데이터에 조건을 적용하여 원하는 결과를 필터링하는 데 사용됩니다. HAVING 절은 WHERE 절과 유사하지만,
  WHERE 절은 개별 행에 대한 조건을 검사하는 반면, HAVING 절은 그룹화된 데이터에 조건을 적용합니다.
  HAVING 절은 집계 함수의 결과에 대한 조건을 설정하여 그룹화된 데이터를 필터링할 수 있습니다.

## ETL

### ETL 개념

### ETL 과제 토의 사항

> wikipeida 페이지가 아닌, IMF 홈페이지에서 직접 데이터를 가져오는 방법은 없을까요? 어떻게 하면 될까요?

1. IMF 홈페이지에서 지원하는 API를 사용한다.
2. API가 없다면, IMF 홈페이지에서 크롤링한다.

> 만약 데이터가 갱신되면 과거의 데이터는 어떻게 되어야 할까요? 과거의 데이터를 조회하는 게 필요하다면 ETL 프로세스를 어떻게 변경해야 할까요?

별도의 조건이 없다면 과거의 데이터는 삭제하고 새로운 데이터를 받는 것이 멱등성을 보장하기 좋을 것 같다.
과거의 데이터를 조회하는 것이 필요하다면 데이터가 생성된 시점을 추가하여 테이블을 구성하고, 데이터를 매번 추가하는 방식으로 만든다면
과거의 데이터를 조회하는 것이 가능하다. 