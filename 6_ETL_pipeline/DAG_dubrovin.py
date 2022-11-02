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
                toDate(time) AS event_date, 
                user_id AS user, 
                gender, 
                age, 
                os, 
                countIf(action = 'like') AS likes, 
                countIf(action = 'views') AS views
            FROM
                simulator_20220420.feed_actions
            WHERE
                toDate(time) = yesterday()
            GROUP BY
                event_date,  
                user,
                gender, 
                age, 
                os
            ORDER BY
                user
            '''
        
        feed_df = ch_get_df(query=query)
        return feed_df
    
    @task
    # В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему
    def extract_messenger():
        # 
        query_1 = '''
            SELECT
                event_date, 
                user, 
                gender, 
                age, 
                os, 
                messages_sent, 
                messages_received, 
                users_sent, 
                users_received
            FROM
            -- Для каждого юзера посчитаем, сколько он отсылает сообщений и скольким людям он пишет
                (
                SELECT
                    toDate(time) AS event_date, 
                    user_id AS user, 
                    gender, 
                    age, 
                    os, 
                    COUNT(reciever_id) as messages_sent,
                    uniqExact(reciever_id) as users_sent
                FROM
                    simulator_20220420.message_actions
                WHERE
                    toDate(time) = yesterday()
                GROUP BY
                    event_date,
                    user, 
                    gender, 
                    age, 
                    os
                ) AS sent

            FULL JOIN
            -- Для каждого юзера посчитаем, сколько он получает сообщений и сколько людей пишут ему
                (
                SELECT
                    toDate(time) AS event_date, 
                    reciever_id AS user, 
                    gender, 
                    age, 
                    os, 
                    COUNT(user_id) as messages_received,
                    uniqExact(user_id) as users_received
                FROM
                    simulator_20220420.message_actions
                WHERE
                    toDate(time) = yesterday()
                GROUP BY
                    event_date,
                    user, 
                    gender, 
                    age, 
                    os
                ) AS received

            ON sent.user = received.user
            '''
        messenger_df = ch_get_df(query=query_1)
        
        return messenger_df
    
    @task
    # Объединяем feed и messenger
    def merge_tables(feed_df, messenger_df):
        full_df = feed_df.merge(messenger_df, on=['user', 'event_date', 'gender', 'age', 'os'], how='outer').dropna()
        return full_df    
        
    # Для общей таблицы считаем все эти метрики в разрезе по полу, возрасту и ос. Делаем три разных таска на каждый срез
    @task
    # Считаем метрики по полу
    def metrics_by_gender(full_df):
        metrics = ['event_date','views','likes','messages_received','messages_sent','users_received','users_sent', 'gender']
        metrics_by_gender = full_df[metrics].groupby(['event_date', 'gender']).sum().reset_index()
        return metrics_by_gender
    
    @task
    # Считаем метрики по возрасту
    def metrics_by_age(full_df):
        metrics = ['event_date','views','likes','messages_received','messages_sent','users_received','users_sent', 'age']
        metrics_by_age = full_df[metrics].groupby(['event_date', 'age']).sum().reset_index()
        return metrics_by_age
    
    @task
    # Считаем метрики по ОС
    def metrics_by_os(full_df):
        metrics = ['event_date','views','likes','messages_received','messages_sent','users_received','users_sent', 'os']
        metrics_by_os = full_df[metrics].groupby(['event_date', 'os']).sum().reset_index()
        return metrics_by_os
    
    
    # Финальные данные со всеми метриками записываем в отдельную таблицу в ClickHouse
    @task
    # Получаем необходимую таблицу
    def get_fin_table(full_df):
        metrics = ['event_date','views','likes','messages_received','messages_sent','users_received','users_sent', 'gender', 'age', 'os']
        fin_df = full_df[metrics].groupby(['event_date', 'gender', 'age', 'os']).sum().reset_index()
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
            CREATE TABLE IF NOT EXISTS test.evg_dubrovin_test 
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
        pandahouse.to_clickhouse(df=fin_df, table='evg_dubrovin_test', index=False, connection=connection_2)
        
    
    
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


# Fin.
