#!/usr/bin/env python
# coding: utf-8

# # Отчет по ленте новостей

# Напишите скрипт для сборки отчета по ленте новостей. Отчет должен состоять из двух частей:
# 
# * текст с информацией о значениях ключевых метрик за предыдущий день
# * график с значениями метрик за предыдущие 7 дней
# 
# Отобразите в отчете следующие ключевые метрики: 
# * DAU 
# * Просмотры
# * Лайки
# * CTR

# Создайте свой репозиторий в GitLab для автоматизации отчетности, сохраните в него код сборки отчета. Автоматизируйте отправку отчета с помощью GitLab CI/CD.
# 
# Используйте следующий докер образ:
# 
# **image:** *cr.yandex/crp742p3qacifd2hcon2/practice-da:latest*
# 
# Отчет должен приходить ежедневно в 11:00 в чат. 

# In[ ]:


# Импортируем необходимые библиотеки

import telegram # библиотека для работы с Telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io # библиотека для управления потоками ввода/вывода данных (чтобы  пересылать графики из буфера обмена и не сохранять их лишний раз)
import logging
import pandas as pd
import pandahouse
import os


# In[3]:


# Создаем бота с помощью нашего токена
# (Токен получили в "BotFather" в Телеграме)
# bot_token = os.environ.get("tg_bot_token")
# bot = telegram.Bot(token=os.environ.get("tg_bot_token"))

bot = telegram.Bot(token='5227151149:AAGkhDIQK5PZo1i88XKZ22INjOnBXq_06Z4')


# In[76]:


# Сохраним id, куда бот будет отправлять отчеты (наш id)
chat_id = 244991955


# In[ ]:


# Приветствие
initial_message = "Доброе утро!\nОтчет по Ленте Новостей"
bot.sendMessage(chat_id=chat_id, text=initial_message)


# In[4]:


# Подключаемся к БД
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20220420'
}


# In[7]:


# Запрос к базе данных
# Выберем DAU, views, likes и CTR за предыдущие 7 дней
q = '''
SELECT 
    toDate(time) as day,
    COUNT(DISTINCT user_id) as DAU, 
    countIf(user_id, action='view') as views, 
    countIf(user_id, action='like') as likes, 
    (countIf(user_id, action='like') / countIf(user_id, action='view'))*100 as CTR
FROM
    simulator_20220420.feed_actions
WHERE
    toDate(time) BETWEEN (today() - 7) AND yesterday()
GROUP BY
    day
ORDER BY 
    day
    '''


# In[8]:


# Сохраним результат запроса в pandas df
df = pandahouse.read_clickhouse(q, connection=connection)
# df


# In[25]:


df.info()


# ## Ключевые метрики за вчерашний день - текст

# In[39]:


# DAU
DAU = df[df['day'] == df['day'].max()]['DAU'].item()
DAU


# In[40]:


# views
views = df[df['day'] == df['day'].max()]['views'].item()
views


# In[41]:


# likes
likes = df[df['day'] == df['day'].max()]['likes'].item()
likes


# In[42]:


# CTR
CTR = df[df['day'] == df['day'].max()]['CTR'].item()
CTR


# In[59]:


yesterday = df['day'].max().strftime("%d.%m.%Y")
yesterday


# In[74]:


# Текст с информацией о значениях ключевых метрик за предыдущий день
message = "Ключевые Метрики по Ленте Новостей за {}: \nDAU - {:,} \nПросмотры - {:,} \nЛайки - {:,} \nCTR - {}%".format(
                                            yesterday,       DAU,              views,         likes,  round(CTR, 2)).replace(',', ' ')


# In[75]:


# print(message)


# In[95]:


# Отправим сообщение от бота по необходимому id
bot.sendMessage(chat_id=chat_id, text=message)


# ## График со значениями метрик за предыдущие 7 дней

# In[78]:


sns.set()


# In[86]:


df['date_month'] = df['day'].dt.strftime('%d-%m')


# In[144]:


# Зададим размеры графика
sns.set(rc={"figure.figsize":(8, 4)}) 


# In[145]:


# Отправляем график DAU за 7 дней

sns.set_palette("colorblind")

sns.lineplot(x='date_month', y='DAU', data=df, linewidth=3, marker='o', color='r')
plt.title('DAU за последнюю неделю', fontweight='bold')
plt.xlabel('Дата')
plt.ylabel('Количество')
# plt.show()

plot_object = io.BytesIO() # создаем файловый объект - картинку
plt.savefig(plot_object) # сохраняем в него наш график
plot_object.name = 'DAU_week_plot.png' # задаем имя нашего файлового объекта
plot_object.seek(0) # переносим курсор из конца файлового объекта в начало, чтобы прочитать весь файл (чтобы избежать отправки боту пустого файла)

plt.close()

# Отправляем изображение
bot.sendPhoto(chat_id=chat_id, photo=plot_object)


# In[146]:


# Отправляем график просмотров за 7 дней

sns.lineplot(x='date_month', y='views', data=df, linewidth=3, marker='o', color='b')
plt.title('Просмотры за последнюю неделю', fontweight='bold')
plt.xlabel('Дата')
plt.ylabel('Количество')

plot_object = io.BytesIO() # создаем файловый объект - картинку
plt.savefig(plot_object) # сохраняем в него наш график
plot_object.name = 'views_week_plot.png' # задаем имя нашего файлового объекта
plot_object.seek(0) # переносим курсор из конца файлового объекта в начало, чтобы прочитать весь файл (чтобы избежать отправки боту пустого файла)

plt.close()

# Отправляем изображение
bot.sendPhoto(chat_id=chat_id, photo=plot_object)


# In[148]:


# Отправляем график лайков за 7 дней

sns.lineplot(x='date_month', y='likes', data=df, linewidth=3, marker='o', color='g')
plt.title('Лайки за последнюю неделю', fontweight='bold')
plt.xlabel('Дата')
plt.ylabel('Количество')

plot_object = io.BytesIO() # создаем файловый объект - картинку
plt.savefig(plot_object) # сохраняем в него наш график
plot_object.name = 'likes_week_plot.png' # задаем имя нашего файлового объекта
plot_object.seek(0) # переносим курсор из конца файлового объекта в начало, чтобы прочитать весь файл (чтобы избежать отправки боту пустого файла)

plt.close()

# Отправляем изображение
bot.sendPhoto(chat_id=chat_id, photo=plot_object)


# In[152]:


# Отправляем график CTR за 7 дней

sns.lineplot(x='date_month', y='CTR', data=df, linewidth=3, marker='o', color='purple')
plt.title('CTR за последнюю неделю', fontweight='bold')
plt.xlabel('Дата')
plt.ylabel('Лайки / Просмотры, %')

plot_object = io.BytesIO() # создаем файловый объект - картинку
plt.savefig(plot_object) # сохраняем в него наш график
plot_object.name = 'CTR_week_plot.png' # задаем имя нашего файлового объекта
plot_object.seek(0) # переносим курсор из конца файлового объекта в начало, чтобы прочитать весь файл (чтобы избежать отправки боту пустого файла)

plt.close()

# Отправляем изображение
bot.sendPhoto(chat_id=chat_id, photo=plot_object)
