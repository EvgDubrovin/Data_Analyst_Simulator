#!/usr/bin/env python
# coding: utf-8

# # Построение ETL-пайплайна

# Ожидается, что на выходе будет DAG в airflow, который будет считаться каждый день за вчера. 
# 
# 1. Параллельно будем обрабатывать две таблицы. В feed_actions для каждого юзера посчитаем число просмотров и лайков контента. В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. Каждая выгрузка должна быть в отдельном таске.
# 2. Далее объединяем две таблицы в одну.
# 3. Для этой таблицы считаем все эти метрики в разрезе по полу, возрасту и ос. Делаем три разных таска на каждый срез.
# 4. И финальную данные со всеми метриками записываем в отдельную таблицу в ClickHouse.
# 
# Структура финальной таблицы должна быть такая:
# * Дата - event_date
# * Пол - gender
# * Возраст - age
# * Операционная система - os
# * Число просмотров - views
# * Числой лайков - likes
# * Число полученных сообщений - messages_received
# * Число отправленных сообщений - messages_sent
# * От скольких пользователей получили сообщения - users_received
# * Скольким пользователям отправили сообщение - users_sent
# 
# Вашу таблицу необходимо загрузить в схему test, ответ на это задание - название Вашей таблицы в схеме test

# ## 1. Запросы к таблицам feed_actions и message_actions

# In[1]:


# Импортируем необходимые библиотеки
import pandas as pd
import pandahouse
from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import datetime, timedelta
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# In[2]:


# Задаем параметры в DAG
default_args = {
    'owner': 'evg.dubrovin',             # Владелец операции
    'depends_on_past': False,            # Зависимость от прошлых запусков
    'retries': 2,                        # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=5), # Промежуток между перезапусками
    'start_date': datetime(2022, 5, 17)  # Дата начала выполнения DAG
}
# Интервал запуска DAG
schedule_interval = '0 10 * * *'


# In[3]:


# Подключаемся к БД
def ch_get_df(query):
    connection_1 = {
                   'host': 'https://clickhouse.lab.karpov.courses',
                   'password': 'dpo_python_2020',
                   'user': 'student',
                   'database': 'simulator_20220420'
                    }
    result = pandahouse.read_clickhouse(query, connection=connection_1)
    return result


# In[4]:


# DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_dubrovin():
    
    @task
    # В feed_actions для каждого юзера посчитаем число просмотров и лайков контента
    def extract_feed():
        query = '''
            SELECT
                user_id,
                toDate(time) as event_date,
                MIN(gender) as gender,
                MIN(age) as age,
                MIN(os) as os, 
                countIf(action='view') as views, 
                countIf(action='like') as likes
            FROM
                simulator_20220420.feed_actions
            WHERE
                toDate(time) = yesterday()
            GROUP BY
                user_id, event_date
            '''
        feed_df = ch_get_df(query=query)
        return feed_df
    
    @task
    # В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему
    def extract_messenger():
        # 
        query_1 = '''
            SELECT 
                CASE
                    WHEN reciever_id = 0 THEN user_id
                    WHEN user_id = 0 THEN reciever_id
                    ELSE user_id
                END as user_id,
                yesterday() as event_date,
                messages_received,
                messages_sent,
                users_received,
                users_sent
            FROM
                -- Для каждого юзера посчитаем, сколько он отсылает сообщений и скольким людям он пишет
                (
                SELECT
                    user_id,
                    toDate(time) as event_date,
                    COUNT(reciever_id) as messages_sent,
                    uniqExact(reciever_id) as users_sent
                FROM
                    simulator_20220420.message_actions
                WHERE
                    toDate(time) = yesterday()
                GROUP BY
                    user_id,
                    event_date
                ) sent

                FULL JOIN

                -- Для каждого юзера посчитаем, сколько он получает сообщений и сколько людей пишут ему
                (
                SELECT
                    reciever_id,
                    toDate(time) as event_date,
                    COUNT(user_id) as messages_received,
                    uniqExact(user_id) as users_received
                FROM
                    simulator_20220420.message_actions
                WHERE
                    toDate(time) = yesterday()
                GROUP BY
                    reciever_id,
                    event_date
                ) received

                ON sent.user_id = received.reciever_id
            '''
        messenger_df_0 = ch_get_df(query=query_1)
        
        # Пол, возраст и ОС пользователей мессенджера
        query_2 = '''
            SELECT
                id as user_id,
                MIN(gender) as gender, 
                MIN(age) as age, 
                MIN(os) as os
            FROM
                (SELECT
                    user_id as id,
                    MIN(gender) as gender,
                    MIN(age) as age,
                    MIN(os) as os
                FROM
                    simulator_20220420.message_actions
                WHERE
                    toDate(time) = yesterday()
                GROUP BY
                    id

                UNION ALL

                SELECT
                    reciever_id as id,
                    MIN(gender) as gender,
                    MIN(age) as age,
                    MIN(os) as os
                FROM
                    simulator_20220420.message_actions
                WHERE
                    toDate(time) = yesterday()
                GROUP BY
                    id)
            GROUP BY
                user_id
            '''
        messenger_attributes_df = ch_get_df(query=query_2)
        
        # Объединение таблиц messenger
        messenger_df = pd.merge(messenger_df_0, messenger_attributes_df, on='user_id', how='inner')
        
        return messenger_df
    
    
    
    @task
    # Объединяем feed и messenger
    def merge_tables(feed_df, messenger_df):
        full_df = pd.merge(feed_df, messenger_df, on=['user_id', 'event_date', 'gender', 'age', 'os'], how='outer').fillna(0)
        
        # Предобработка full_df
        # Заменим float значения на int
        float_cols = ['views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']
        full_df[float_cols] = full_df[float_cols].astype(int)

        # Заменим значения 0 и 1 в столбце gender на более понятные male и female
        full_df.loc[full_df['gender']==0, 'gender'] = 'male'
        full_df.loc[full_df['gender']==1, 'gender'] = 'female'

        # Разобъем значения age на bins для удобства 
        age_min = full_df['age'].min()
        age_max = full_df['age'].max()

        age_groups = [f'{age_min}-18', '19-25', '26-45', '46-65', '66+']
        age_bins = [age_min, 18, 25, 45, 65, age_max]
        full_df['age_bins'] = pd.cut(full_df['age'], bins=age_bins, labels=age_groups)
        
        return full_df

    
    
    # Для общей таблицы считаем все эти метрики в разрезе по полу, возрасту и ос. Делаем три разных таска на каждый срез
    @task
    # Считаем метрики по полу
    def metrics_by_gender(full_df):
        metrics = ['gender','event_date','views','likes','messages_received','messages_sent','users_received','users_sent']
        metrics_by_gender = full_df[metrics].groupby(['gender','event_date'], as_index=[True, False]).sum()
        return metrics_by_gender
    
    @task
    # Считаем метрики по возрасту
    def metrics_by_age(full_df):
        metrics = ['age_bins','event_date','views','likes','messages_received','messages_sent','users_received','users_sent']
        metrics_by_age = full_df[metrics].groupby(['age_bins','event_date'], as_index=[True, False]).sum()
        return metrics_by_age
    
    @task
    # Считаем метрики по ОС
    def metrics_by_os(full_df):
        metrics = ['os','event_date','views','likes','messages_received','messages_sent','users_received','users_sent']
        metrics_by_os = full_df[metrics].groupby(['os','event_date'], as_index=[True, False]).sum()
        return metrics_by_os
    
    
    # Финальные данные со всеми метриками записываем в отдельную таблицу в ClickHouse
    @task
    # Получаем необходимую таблицу
    def get_fin_table(full_df):
        fin_df = full_df[['event_date', 'gender', 'age_bins', 'os', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        fin_df = fin_df.groupby(['event_date', 'gender', 'age_bins', 'os'], as_index=False).sum().rename({'age_bins':'age'}, axis=1)
        return fin_df
    
    @task
    # Загружаем финальную таблицу в схему test
    def load_table(fin_df):
        # Подключаемся к БД
        connection_2 = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': '656e2b0c9c',
            'user': 'student-rw',
            'database': 'test'
        }
        # Создаем таблицу
        q = '''
            CREATE TABLE IF NOT EXISTS test.evg_dubrovin 
                (
                event_date datetime,
                gender TEXT,
                age TEXT, 
                os TEXT,
                views INTEGER,
                likes INTEGER,
                messages_received INTEGER,
                messages_sent INTEGER,
                users_received INTEGER,
                users_sent INTEGER
                )
                ENGINE = MergeTree 
                ORDER BY (event_date);
            '''
        # Отправляем таблицу в базу данных
        pandahouse.execute(connection=connection_2, query=q)
        pandahouse.to_clickhouse(df=fin_df, table='evgdubrovin_test', index=False, connection=connection_2)
        
    
    
    # Выполняем таски 
    # В feed_actions для каждого юзера посчитаем число просмотров и лайков контента. 
    feed_df = extract_feed()
    # В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему
    messenger_df = extract_messenger()
    
    # Далее объединяем две таблицы в одну
    full_df = merge_tables(feed_df, messenger_df)
    
    # Метрики в разрезе по полу
    metrics_by_gender = metrics_by_gender(full_df)
    # Метрики в разрезе по возрасту
    metrics_by_age = metrics_by_age(full_df)
    # Метрики в разрезе по ОС
    metrics_by_os = metrics_by_os(full_df)
    
    # Финальная таблица со всеми данными
    fin_df = get_fin_table(full_df)
    
    # Загружаем таблицу в базу данных
    load_table(fin_df)
    
dag_dubrovin = dag_dubrovin()


# In[ ]:




