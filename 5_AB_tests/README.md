# A|B Testing

## Scenario:
The ML-team made a new recommendation algorithm to make posts more interesting for users. So, we have to measure if they really did. To check this we will measure CTR.

(groups 2, 3 - test; groups 0, 1 - control).



## 1) Does the splitting system work correctly? AA-test.

We'd like to ensure that our metric doesn't differ between the groups. 
We have to repeatedly extract sub-samples with repetitions from our data, then conduct t-test. And at the end we will see in what percentage of cases we rejected the H0.

a) Simulate 10000 AA-tests.  
b) Form 500 users sub-samples with repetitions from 2 and 3 experimental group.  
c) Compare these groups with statistical ***t-test***(and ***mann-whitney test***).

*aa_test.ipynb* - AA-test.

**Answer:**  
The key metric doesn't statistically significant differ between the groups, so our groups are identical and we can measure a new feature effect.


## 2) AB-test.

Our hypothesis is that the new algorithm in group 2 will increase the CTR.
We have to analyze AB-test results.

a) Select analysis method (***t-test, mann-whitney test, bootstrap, smoothed ctr t-test, bucketing***).  
b) Analyze the results.  
c) Should we implement the new algorithm?

*ab_test.ipynb* - AB-test.

**Answer:**  
We can reject the hypothesis that a new algorithm in group 2 will increase the CTR.

Based on the tests performed, we can conclude that the CTR drop is caused by the decrease (stat. significant) of likes. This means that users became less happy with the posts offered by the new algorithm.

I don’t recommend to implement the new algorithm.


(Результаты всех тестов показали, что с 2022-04-04 по 2022-04-10 средний CTR в тестовой группе (2) не выше среднего CTR в контрольной группе (1), а даже наоборот. То же самое подтвердилось и визуально.  
Соответственно, можем отвергнуть гипотезу о том, что новый алгоритм во 2-й группе приведет к увеличению CTR.  
Почему так произошло?  
Самый очевидный вариант - что-то не так с алгоритмом, новый алгоритм стал выдавать менее интересные посты пользователям. Нужно обращаться ML-щикам.  
Возможно, в экспериментальной 2 группе существенно выросло количество просмотров или количество показываемых постов.  
Исходя из проведенных тестов, мы видим, что разница в CTR в тестовой и контрольной группах обусловлено только изменением количества лайков пользователями. Значит, пользователям стали меньше нравится посты, предлагаемые новым алгоритмом.  
Таким образом, гипотеза о том, что новый алгоритм приведет к увеличению CTR была опровергнута.  
Падение CTR вызвано уменьшением (стат. значимым) количества лайков. Т.е. новый алгоритм стал рекомендовать пользователям "не те" посты.  
Не рекомендую раскатывать новый алгоритм на всех новых пользователей.)


## 3) AB-test with linearized likes metric.

a) Analyze the test between groups 0 and 3 with linearized likes metric. Is there a difference? Has p-value become smaller?  
b) Analyze the test between groups 1 and 2 with linearized likes metric. Is there a difference? Has p-value become smaller?

*lnrzd_likes_metric_test.ipynb* - Linearized likes metric AB-test.

***The linearized likes metric*** is much more sensitive than CTR. It even shows statistically significant difference between groups 1 and 2.
