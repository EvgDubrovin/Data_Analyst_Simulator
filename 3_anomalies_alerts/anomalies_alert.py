#!/usr/bin/env python
# coding: utf-8

# # Система алертов для приложения

# Система должна с периодичность каждые 15 минут проверять ключевые метрики, такие как: 
# * **активные пользователи в ленте / мессенджере**, 
# * **просмотры**, 
# * **лайки**, 
# * **CTR**, 
# * **количество отправленных сообщений**. 
# 
# Изучите поведение метрик и подберите наиболее подходящий метод для детектирования аномалий. На практике как правило применяются статистические методы. 
# В самом простом случае можно, например, проверять отклонение значения метрики в текущую 15-минутку от значения в такую же 15-минутку день назад. 
# 
# В случае обнаружения аномального значения, в чат должен отправиться алерт - сообщение со следующей информацией: метрика, ее значение, величина отклонения.  
# В сообщение можно добавить дополнительную информацию, которая поможет при исследовании причин возникновения аномалии, это может быть, например,  график, ссылки на дашборд/чарт в BI системе. 

# In[ ]:





# In[1]:


# Импортируем необходимые библиотеки

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse
from datetime import date
import io # библиотека для управления потоками ввода/вывода данных (чтобы  пересылать графики из буфера обмена и не сохранять их лишний раз)
import sys
import os


# In[2]:


# # Создаем бота с помощью нашего токена
# # (Токен получили в "BotFather" в Телеграме)
# # bot_token = os.environ.get("tg_bot_token")
# # bot = telegram.Bot(token=os.environ.get("tg_bot_token"))

# bot = telegram.Bot(token='********************************************')


# In[3]:


# # Сохраним id, куда бот будет отправлять отчеты (наш id)
# chat_id = *********


# In[ ]:


def check_anomaly(df, metric, a=4, n=5):
    """
    функция check_anomaly предлагает алгоритм проверки значения на аномальность посредством
    сравнения текущего значения метрики со средненедельным показателем в 15 минутном интервале.

    Параметры:
    df - датафрейм
    metric - метрика для проверки на аномалии
    a - коэффициент перед межквартильным размахом (a*IQR)
    n - количество временных промежутков.

    Функция возвращает: 
    is_alert - оповещение, есть отклонение (1) или нет (0)
    df 
    current_val - 
    last_val_diff - отклонение от предыдущего значения
    """
    # Реализуем метод межквартильного размаха
    # Добавим столбцы с 25-м и 75-м квартилями и IQR
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25) # сдвигаем на одну 15-минутку назад, чтобы избежать влияния неполной 15-минутки на расчет
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    # Добавим столбцы со значениями верхней и нижней границ
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    # Сгладим значения верхней и нижней границ
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    # Проверяемое значение метрики (в последней 15-минутке)
    current_val = df[metric].iloc[-1]
    # Отклонение от предыдущего значения (предыдущей 15-минутки)
    last_val_diff = abs(1 - (current_val/df[metric].iloc[-2]))
    # Проверяем на отклонение
    if current_val < df['low'].iloc[-1] or current_val > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df, current_val, last_val_diff


# In[5]:


def run_alerts(chat=None):
    """
    Функция run_alerts запускает подключение к базе данных, выполняет запрос на проверку 
    метрик на аномалии, а также формирует информацию для отчета в telegram
    
    Параметры:
    chat - chat_id telegram для отправки сообщения
    sigma - значение сигмы
    """
    # Подключаем tg-бота и канал, куда будем отправлять сообщения
    chat_id = chat or 244991955
    bot = telegram.Bot(token='5227151149:AAGkhDIQK5PZo1i88XKZ22INjOnBXq_06Z4')
    
    # Загружаем датасет
    # Подключаемся к БД
    connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': 'dpo_python_2020',
        'user': 'student',
        'database': 'simulator_20220420'
    }
    # Запрос к БД
    # Получим обобщенную таблицу с пользователями, просмотрами, лайками, CTR и сообщениями за сегодня
    q = '''
        SELECT * FROM
        (SELECT
            toStartOfFifteenMinutes(time) as ts, 
            toDate(time) as day, 
            formatDateTime(ts, '%R') as hm, 
            uniqExact(user_id) as users_feed,
            countIf(user_id, action='view') as views, 
            countIf(user_id, action='like') as likes,
            100 * countIf(user_id, action='like') / countIf(user_id, action='view') as CTR
        FROM 
            simulator.feed_actions 
        WHERE 
            ts >= yesterday() and ts < toStartOfFifteenMinutes(now()) 
        GROUP BY 
            ts, day, hm) as f 
        FULL JOIN
        (SELECT
            toStartOfFifteenMinutes(time) as ts, 
            toDate(time) as day, 
            formatDateTime(ts, '%R') as hm, 
            uniqExact(user_id) as users_messenger,
            COUNT(reciever_id) AS messages
        FROM
            simulator.message_actions
        GROUP BY
            ts, day, hm) as m 
        USING 
            ts, day, hm
        ORDER BY 
            ts
    '''
    # Сохраним результат запроса в pandas df
    data = pandahouse.read_clickhouse(q, connection=connection)
    
    # Список метрик, которые будем проверять на аномалии
    metrics = ['users_feed', 'views', 'likes', 'CTR', 'users_messenger', 'messages']
    # Проверим каждую метрику на наличие аномалий
    for metric in metrics:
        # print(metric.capitalize())
        df = data[['ts', 'day', 'hm', metric]].copy()
        is_alert, df, current_val, last_val_diff = check_anomaly(df, metric)
        
        # Если обнаружили аномалию, отправляем отчет в чат 
        if is_alert == 1 or True:
            # Сообщение об аномалии
            message = (f'''Метрика {metric.capitalize()}:\nтекущее значение: {current_val:.2f} \nотклонение от предыдущего значения: {last_val_diff:.2%}\n ссылка на график: https://superset.lab.karpov.courses/superset/dashboard/759/''')

            # Зададим параметры графиков
            sns.set(rc={"figure.figsize":(10, 5)}) 
            sns.set_palette("colorblind")
            plt.tight_layout() # чтобы все подписи к графика были в окошке графика

            # Строим график
            ax = sns.lineplot(x=df['hm'], y=df[metric], data=df, label='metric') # метрика
            ax = sns.lineplot(x=df['hm'], y=df['up'], data=df, label='upper bound') # линия верхней границы
            ax = sns.lineplot(x=df['hm'], y=df['low'], data=df, label='lower bound') # линия нижней границы
            # plt.xticks(df['hm'][::10]) 

            # Сделаем подписи по оси Х чуть реже
            for ind, label in enumerate(ax.get_xticklabels()): 
                if ind % 3 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)

            ax.set(xlabel='time') # задаем имя оси Х
            ax.set(ylabel=metric.capitalize()) # задаем имя оси У
            ax.set_title('{}'.format(metric.capitalize())) # задаем заголовок графика
            ax.set(ylim=(0, None)) # задаем лимит для оси У

            # формируем файловый объект
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            # отправляем алерт
            bot.sendMessage(chat_id=chat_id, text=message)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)


# In[6]:


try:
    run_alerts()
except Exception as e:
    print(e)
