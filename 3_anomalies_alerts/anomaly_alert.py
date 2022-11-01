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

# Создаем бота с помощью нашего токена
# (Токен получили в "BotFather" в Телеграме)
bot_token = os.environ.get("tg_bot_token")
bot = telegram.Bot(token=bot_token)

# Сохраним id, куда бот будет отправлять отчеты (id чата АЛЕРТЫ)
chat_id = -*********


# Поиск аномалий

def check_anomaly(df, metric, n=6, a=4):
    '''
    Функция сравнивает текущее значение метрики с ближайшими n значениями
    
    Параметры:
    df - датафрейм; 
    metric - метрика для проверки на аномалии; 
    n - количество временных промежутков; 
    a - коэффициент перед межквартильным размахом (a*IQR) и сигмами (a*sigma)
    
    Функция возвращает: 
    is_alert - оповещение, есть отклонение (1) или нет (0); 
    current_val - текущее значение метрики; 
    last_val_diff - отклонение от предыдущего значения
    '''
    # 1. Метод межквартильного размаха
    # Добавим столбцы с 25-м и 75-м квартилями и IQR
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25) # сдвигаем на одну 15-минутку назад, чтобы избежать влияния неполной 15-минутки на расчет
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    # Добавим столбцы со значениями верхней и нижней границ
    df['low_q'] = df['q25'] - a*df['iqr']
    df['up_q'] = df['q75'] + a*df['iqr']
    # Сгладим значения верхней и нижней границ
    df['low_q'] = df['low_q'].rolling(n, center=True, min_periods=1).mean()
    df['up_q'] = df['up_q'].rolling(n, center=True, min_periods=1).mean()
    
    # 2. Метод сигм
    # Добавим в таблицу среднее и стандартное отклонение
    df['mean'] = df[metric].shift(1).rolling(n).mean()
    df['std'] = df[metric].shift(1).rolling(n).std()
    # Верхняя и нижняя границы
    df['low_s'] = df['mean'] - a*df['std']
    df['up_s'] = df['mean'] + a*df['std']
    # Сгладим значения верхней и нижней границ
    df['low_s'] = df['low_s'].rolling(n, center=True, min_periods=1).mean()
    df['up_s'] = df['up_s'].rolling(n, center=True, min_periods=1).mean()
    
    # Проверяемое значение метрики (в последней 15-минутке)
    current_val = df[metric].iloc[-1]
    
    # Отклонение от предыдщего значения (предыдущей 15-минутки)
    last_val_diff = abs(1 - (current_val/df[metric].iloc[-2]))
    
    # Время аномалии
    hm_anomaly = df[df[metric]==current_val]['hm']
    
    # Проверяем на отклонение
    if current_val < (df['low_q'].iloc[-1] or df['low_s'].iloc[-1]) or current_val > (df['up_q'].iloc[-1] or df['up_s'].iloc[-1]):
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, current_val, last_val_diff, hm_anomaly


def run_alerts():
    """
    Функция выполняет запрос на проверку метрик на аномалии, а также формирует информацию для отчета в telegram

    """
    # Загружаем датасет
    # Подключаемся к БД
    connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': 'dpo_python_2020',
        'user': 'student',
        'database': 'simulator_20220420'
    }
    # Запрос к БД
    # Данные о количестве пользователей, просмотров, лайков и сообщений в приложении за вчера и сегодня
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
                simulator_20220420.feed_actions 
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
                simulator_20220420.message_actions
            WHERE 
                ts >= yesterday() and ts < toStartOfFifteenMinutes(now())
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
    
    for metric in metrics:
        df = data[['ts', 'day', 'hm', metric]].copy()
        is_alert, current_val, last_val_diff, hm_anomaly = check_anomaly(df, metric)
        
        # Если обнаружили аномалию, отправляем отчет в чат
        if is_alert == 1:
            # Сообщение об аномалии
            message = (f'''Алерт!\nМетрика {metric}:\nТекущее значение: {current_val:.2f} \nОтклонение от предыдущего значения: {last_val_diff:.2%}\nСсылка на дашборд: https://superset.lab.karpov.courses/superset/dashboard/759/''')
            # print(message)
            
            # Зададим параметры графиков
            sns.set(rc={"figure.figsize":(16, 8)}) 
            sns.set_palette("colorblind")
            plt.tight_layout() # чтобы все подписи к графика были в окошке графика
            
            # Строим график
            ax = sns.lineplot(x=df['ts'], y=df[metric], data=df, label='metric', linewidth=3) # метрика
            ax = sns.lineplot(x=df['ts'], y=df['up_q'], data=df, label='IQR upper bound') # линия верхней границы IQR
            ax = sns.lineplot(x=df['ts'], y=df['low_q'], data=df, label='IQR lower bound') # линия нижней границы IQR
            ax = sns.lineplot(x=df['ts'], y=df['up_s'], data=df, label='sigma upper bound') # линия верхней границы 3-х сигм
            ax = sns.lineplot(x=df['ts'], y=df['low_s'], data=df, label='sigma lower bound') # линия нижней границы 3-х сигм
            # plt.xticks(df['hm'][::10]) 

            ## Сделаем подписи по оси Х чуть реже
            #for ind, label in enumerate(ax.get_xticklabels()): 
            #    if ind % 2 == 0:
            #        label.set_visible(True)
            #    else:
            #        label.set_visible(False)

            # Параметры графика
            ax.set(xlabel='time') # задаем имя оси Х
            ax.set(ylabel=metric) # задаем имя оси У
            ax.set_title('{}'.format(metric)) # задаем заголовок графика
            ax.set(ylim=(0, None)) # задаем лимит для оси У
            # plt.axvline("hm_anomaly", color='r', linestyle='--')
            
            # формируем файловый объект
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            # отправляем алерт
            bot.sendMessage(chat_id=chat_id, text=message)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)


try:
    run_alerts()
except Exception as e:
    print(e)
