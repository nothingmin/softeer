# Week2

## Wordcloud

Wordcloud 동작 방식

1. Wordcloud를 만들 텍스트에서 단어별로 가중치를 만든다 - 예시에서는 단어별로 가중치를 만든다.
    ```
    cv = CountVectorizer(min_df=0, charset_error="ignore", stop_words="english", max_features=200)
    counts = cv.fit_transform([text]).toarray().ravel()                                                  
    words = np.array(cv.get_feature_names())
   
    # normalize
    counts = counts / float(counts.max())
   ```
2. 단어를 가중치가 큰 것부터 이미지에 그려나간다.
   단어들이 서로 겹치면 안되므로 단어를 그리기 전에 이미지에 공간이 비었는지 확인해야 한다.
   이미지의 빈 공간을 검은색으로 만들면 검은색의 픽셀 값이 0이므로 이미지의 특정 구역의 픽셀 값이 총합이 0인 곳이 빈 공간이다.
   따라서, 단어가 들어갈 박스를 만들고 단어 박스 크기만큼 이미지를 잘라서 확인해서 해당 구역의 픽셀값이 총합이 0인 곳을 찾는다.

   위 과정에서 이미지의 특정 구역의 픽셀의 총합을 구하는 것을 빈 공간을 찾을 때까지 반복한다.
   저자는 [적분 영상(Integral Image, Summed-area table)](https://en.wikipedia.org/wiki/Summed-area_table)
   을 이용해 특정 구역의 픽셀 값의 합을 구하는 계산을 최적화 했다.
   적분 영상을 이용하면 미리 계산된 2차원 테이블을 사용하여 이미지의 임의의 구역에 대한 픽셀 값의 총합을 효율적으로 구할 수있다.

전체 코드 : https://github.com/nothingmin/softeer/tree/main/missions/W2/wordcloud.ipynb
원문 출처 : https://peekaboo-vision.blogspot.com/2012/11/a-wordcloud-in-python.html

## Sentiment Analysis 추가 학습

추가 학습거리에서 주어진 논문은 트위터로부터 선거 결과를 예측하는 머신 러닝 알고리즘을 소개하고 있다. 예측의 정확도는 90%가 넘고 기존 방식(출구조사)보다
높은 정확도를 보인다고 주장한다. 논문 요약을 읽고나서 어떻게 트위터의 데이터(트윗)가 전체 여론을 대변할 수 있었는지가 궁금했다.
그 이유는 여론 조사를 할 때 트위터와 같은 온라인 매체는 젊은 사람들의 여론만 반영하기 때문에 전화 조사와 같은 전통적인 매체도 섞어서 사용하는 것이
우리나라에서는 일반적이기 떄문이다.

## Docker

Docker를 사용하는 이유가 뭘까요?

어떤 점은 더 불편한가요?

이번 미션에서는 하나의 EC2에 하나의 Docker container를 배포했습니다. 만약에 여러대의 EC2에 여러 개의 컨테이너를 배포해야 한다면 어떻게 해야 할까요?

# AWS

IAM
EC2
ECR
ECS