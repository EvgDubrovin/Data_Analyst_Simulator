#!/usr/bin/env python
# coding: utf-8

# # Единый отчет по всему приложению (лента новостей и мессенджер)

# In[1]:


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


# In[2]:


# Создаем бота с помощью нашего токена
# (Токен получили в "BotFather" в Телеграме)
# bot_token = os.environ.get("tg_bot_token")
# bot = telegram.Bot(token=os.environ.get("tg_bot_token"))

bot = telegram.Bot(token='**********************************************')



# In[3]:


# Сохраним id, куда бот будет отправлять отчеты (наш id)
chat_id = *********


# In[4]:


# Приветствие
initial_message = "Доброе утро!\nОбщий Отчет по Приложению"
bot.sendMessage(chat_id=chat_id, text=initial_message)


# In[5]:


# Подключаемся к БД
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20220420'
}


# In[6]:


# Запрос к базе данных
# Получим обобщенную таблицу с пользователями, просмотрами, лайками и сообщениями, сгруппированными по дням  
q = '''
SELECT *
FROM
(
SELECT 
    user_id, 
    toDate(time) as day,
    countIf(post_id, action='view') as views,
    countIf(post_id, action='like') as likes
FROM 
    simulator_20220420.feed_actions
GROUP BY 
    user_id, day) as f
FULL JOIN
(
SELECT 
    user_id,
    toDate(time) as day,
    COUNT(reciever_id) AS messages
FROM 
    simulator_20220420.message_actions
GROUP BY 
    user_id, day) as m 
USING user_id, day
'''


# In[7]:


# Сохраним результат запроса в pandas df
df = pandahouse.read_clickhouse(q, connection=connection)
df.head()


# In[8]:


df.info()


# ## Ключевые метрики за вчерашний день

# In[9]:


# Приветствие - 2
message = "Ключевые метрики за вчерашний день:"
bot.sendMessage(chat_id=chat_id, text=message)


# ### DAU - текст

# In[10]:


# Количество пользователей вчера

# Всего пользователей вчера
# Общее DAU за все время
DAU_total_daily = df.groupby('day', as_index=False)['user_id'].nunique()
# Общее DAU за вчера
DAU_total_yesterday = DAU_total_daily[DAU_total_daily['day'] == DAU_total_daily['day'].iloc[-2]]['user_id'].item()

# Пользователей только ленты новостей вчера
# DAU только ленты за все время
DAU_feed_only_daily = df.query('messages == 0').groupby('day', as_index=False)['user_id'].nunique() 
# DAU только ленты вчера
DAU_feed_only_yesterday = DAU_feed_only_daily[DAU_feed_only_daily['day'] == DAU_feed_only_daily['day'].iloc[-2]]['user_id'].item()

# Пользователей только мессенджера вчера
# DAU только мессенджера за все время
DAU_messenger_only_daily = df.query('messages>0 & (views+likes)==0').groupby('day', as_index=False)['user_id'].nunique() 
# DAU только мессенджера вчера
DAU_messenger_only_yesterday = DAU_messenger_only_daily[DAU_messenger_only_daily['day'] == DAU_messenger_only_daily['day'].iloc[-2]]['user_id'].item()

# Пользователей и ленты новостей и мессенджера вчера
# DAU только ленты+мессенджера за все время
DAU_feed_messenger_only_daily = df.query('messages>0 & (views+likes)>0').groupby('day', as_index=False)['user_id'].nunique() 
# DAU только ленты+мессенджера за вчера
DAU_feed_messenger_only_yesterday = DAU_feed_messenger_only_daily[DAU_feed_messenger_only_daily['day'] == DAU_feed_messenger_only_daily['day'].iloc[-2]]['user_id'].item()


# In[11]:


# Найдем и сохраним вчерашнюю дату в удобном формате
yesterday = df['day'].drop_duplicates().nlargest(2).iloc[-1]
yesterday = yesterday.strftime('%d.%m.%Y')


# In[12]:


# Текст с информацией о DAU за предыдущий день
message_1 = "DAU за вчерашний день ({}): \nВсего - {:,} \nТолько в ленте - {:,} \nТолько в мессенджере - {:,} \nИ лента и мессенджер - {}".format(
                            yesterday,                  DAU_total_yesterday,      DAU_feed_only_yesterday,       DAU_messenger_only_yesterday,  DAU_feed_messenger_only_yesterday).replace(',', ' ')


# In[13]:


print(message_1)


# In[14]:


# Отправим сообщение от бота по необходимому id
bot.sendMessage(chat_id=chat_id, text=message_1)


# ### DAU за вчера - графики

# In[15]:


# Просмотры, лайки, сообщения и  в течение вчерашнего дня   
q = '''
SELECT *
FROM
(
SELECT 
    user_id, 
    toStartOfFifteenMinutes(time) as mins_15,
    countIf(post_id, action='view') as views,
    countIf(post_id, action='like') as likes
FROM 
    simulator_20220420.feed_actions
WHERE
    toDate(time) = yesterday()
GROUP BY 
    user_id, mins_15) as f
FULL JOIN
(
SELECT 
    user_id,
    toStartOfFifteenMinutes(time) as mins_15,
    COUNT(reciever_id) AS messages
FROM 
    simulator_20220420.message_actions
WHERE
    toDate(time) = yesterday()
GROUP BY 
    user_id, mins_15) as m 
USING user_id, mins_15
'''


# In[16]:


# Таблица за вчера
# Сохраним результат запроса в pandas df
df_yesterday = pandahouse.read_clickhouse(q, connection=connection)
df_yesterday.sort_values(by='mins_15')


# In[17]:


# Добавим столбец status для разбивки пользователей по группам: только мессенджер, только лента, лента+мессенджер
df_yesterday['status'] = ' '

# Только мессенджер
df_yesterday.loc[(df_yesterday['messages']>0) & (df_yesterday['views'] + df_yesterday['likes'] == 0), 'status'] = 'messenger_only'

# Только лента новостей
df_yesterday.loc[df_yesterday['messages']==0, 'status'] = 'feed_only'

# Лента новостей + мессенджер
df_yesterday.loc[(df_yesterday['messages']>0) & (df_yesterday['views'] + df_yesterday['likes'] > 0), 'status'] = 'feed+messenger'


# In[18]:


# DAU в течение вчерашнего дня
DAU_yesterday = df_yesterday.groupby(['mins_15', 'status'], as_index=False)['user_id'].nunique()


# In[19]:


DAU_yesterday.tail()


# In[20]:


# Время за вчера в удобном формате
hours_mins = np.array(DAU_yesterday['mins_15'].dt.strftime('%H-%M').drop_duplicates())
# print(hour_mins)
hours = np.array(DAU_yesterday['mins_15'].dt.strftime('%H').drop_duplicates())
# print(hours)


# In[21]:


# Дополним таблицу
DAU_yesterday['hours_mins'] = DAU_yesterday['mins_15'].dt.strftime('%H-%M')


# In[22]:


# Зададим параметры графиков
sns.set(rc={"figure.figsize":(9, 5)}) 
sns.set_palette("colorblind")


# In[23]:


# График DAU за вчера
sns.lineplot(x='hours_mins', y='user_id', data=DAU_yesterday, hue='status', linewidth=3)

plt.title('DAU за вчерашний день - {}'.format(yesterday), fontweight='bold')
plt.xlabel('Время')
plt.ylabel('Количество')
plt.xticks(DAU_yesterday['hours_mins'][::5],  rotation='90')
plt.tight_layout() # чтобы все подписи к графика были в окошке графика
# plt.show()

# Отправляем DAU за вчера
plot_object_1 = io.BytesIO() # создаем файловый объект - картинку
plt.savefig(plot_object_1) # сохраняем в него наш график
plot_object_1.name = 'DAU_yesterday.png' # задаем имя нашего файлового объекта
plot_object_1.seek(0) # переносим курсор из конца файлового объекта в начало, чтобы прочитать весь файл (чтобы избежать отправки боту пустого файла)

plt.close()

# Отправляем изображение
bot.sendPhoto(chat_id=chat_id, photo=plot_object_1)


# ### Просмотры, лайки, сообщения за вчера - текст

# In[24]:


# Всего просмотров вчера
views_yesterday = df_yesterday['views'].sum()
# Всего лайков вчера
likes_yesterday = df_yesterday['likes'].sum()
# Всего сообщений вчера
messages_yesterday = df_yesterday['messages'].sum()


# In[25]:


# Текст с информацией о просмотрах, лайках и сообщениях за вчера
message_2 = "Всего событий за вчера ({}): \nПросмотры - {:,} \nЛайки - {:,} \nСообщения - {:,}".format(
                                                      yesterday, views_yesterday, likes_yesterday, messages_yesterday).replace(',', ' ')


# In[26]:


print(message_2)


# In[27]:


# Отправим сообщение от бота по необходимому id
bot.sendMessage(chat_id=chat_id, text=message_2)


# ### Просмотры, лайки, сообщения за вчера - график

# In[28]:


# Дополним таблицу
df_yesterday['hours_mins'] = df_yesterday['mins_15'].dt.strftime('%H-%M')


# In[29]:


# Просмотры, лайки, сообщения в течение вчерашенго дня
vlm_yesterday = df_yesterday.groupby('hours_mins', as_index=False).sum(['views', 'likes', 'messages'])


# In[30]:


# График просмотров, лайков, сообщений в течение вчерашенго дня
sns.lineplot(x='hours_mins', y='views', data=vlm_yesterday, linewidth=3)
sns.lineplot(x='hours_mins', y='likes', data=vlm_yesterday, linewidth=3)
sns.lineplot(x='hours_mins', y='messages', data=vlm_yesterday, linewidth=3)

plt.title('Просмотры, лайки, сообщения за вчерашний день - {}'.format(yesterday), fontweight='bold')
plt.xlabel('Время')
plt.ylabel('Количество')
plt.xticks(vlm_yesterday['hours_mins'][::5],  rotation='90')
plt.legend(labels=["views","likes", "messages"])
plt.tight_layout() # чтобы все подписи к графика были в окошке графика
# plt.show()

# Отправляем DAU за вчера
plot_object_2 = io.BytesIO() # создаем файловый объект - картинку
plt.savefig(plot_object_2) # сохраняем в него наш график
plot_object_2.name = 'views_likes_messages_yesterday.png' # задаем имя нашего файлового объекта
plot_object_2.seek(0) # переносим курсор из конца файлового объекта в начало, чтобы прочитать весь файл (чтобы избежать отправки боту пустого файла)

plt.close()

# Отправляем изображение
bot.sendPhoto(chat_id=chat_id, photo=plot_object_2)


# ### Среднее число просмотров, лайков и сообщений на пользователя за вчера - текст

# In[31]:


# Среднее количество просмотров на пользователя за вчера
views_avg_yesterday = df_yesterday.query("views>0").groupby('user_id', as_index=False).agg({'views':'sum'})['views'].mean()
# Среднее количество лайков на пользователя за вчера
likes_avg_yesterday = df_yesterday.query("likes>0").groupby('user_id', as_index=False).agg({'likes':'sum'})['likes'].mean()
# Среднее количество сообщений на пользователя за вчера
messages_avg_yesterday = df_yesterday.query("messages>0").groupby('user_id', as_index=False).agg({'messages':'sum'})['messages'].mean()


# In[32]:


# Текст с информацией о просмотрах, лайках и сообщениях за вчера
message_3 = "Среднее число событий на пользователя вчера ({}): \nПросмотры (сред.) - {:,} \nЛайки (сред.) - {:,} \nСообщения (сред.) - {:,}".format(
                                        yesterday, round(views_avg_yesterday,2), round(likes_avg_yesterday,2), round(messages_avg_yesterday,2)).replace(',', ' ')
print(message_3)


# In[33]:


# Отправим сообщение от бота по необходимому id
bot.sendMessage(chat_id=chat_id, text=message_3)


# ### Новых пользователей за вчера

# In[34]:


# Запрос - новые пользователи за вчера   
q = '''
SELECT DISTINCT user_id
FROM
(
SELECT 
    DISTINCT user_id
FROM 
    simulator_20220420.feed_actions
WHERE
    toDate(time) = yesterday()
) as f
FULL JOIN
(
SELECT 
    DISTINCT user_id
FROM 
    simulator_20220420.message_actions
WHERE
    toDate(time) = yesterday()
) as m 
USING user_id
WHERE
    user_id NOT IN
        (
        SELECT * FROM
            (SELECT DISTINCT user_id FROM simulator_20220420.feed_actions 
             WHERE toDate(time) BETWEEN (SELECT MIN(toDate(time)) FROM simulator_20220420.feed_actions) AND (yesterday() - 1)) f
            FULL JOIN
            (SELECT DISTINCT user_id FROM simulator_20220420.message_actions 
             WHERE toDate(time) BETWEEN (SELECT MIN(toDate(time)) FROM simulator_20220420.message_actions) AND (yesterday() - 1)) m
            USING user_id
        )
'''
# Новые пользователи за вчера
# Сохраним результат запроса в pandas df
df_new_users_yesterday = pandahouse.read_clickhouse(q, connection=connection)
# df_new_users_yesterday


# In[35]:


new_users_yesterday = df_new_users_yesterday.user_id.nunique()


# In[36]:


# Текст с информацией о новых пользователях за вчера
message_4 = "Новых пользователей вчера ({}) - {:,}".format(
                                 yesterday, new_users_yesterday).replace(',', ' ')
print(message_4)


# In[37]:


# Отправим сообщение от бота по необходимому id
bot.sendMessage(chat_id=chat_id, text=message_4)


# ## Ключевые метрики за все время

# ### DAU за все время - график

# In[38]:


# Добавим столбец status для разбивки пользователей по группам: только мессенджер, только лента, лента+мессенджер
df['status'] = ' '
# Только мессенджер
df.loc[(df['messages']>0) & (df['views'] + df['likes'] == 0), 'status'] = 'messenger_only'
# Только лента новостей
df.loc[df['messages']==0, 'status'] = 'feed_only'
# Лента новостей + мессенджер
df.loc[(df['messages']>0) & (df['views'] + df['likes'] > 0), 'status'] = 'feed+messenger'


# In[39]:


# DAU за все время в разрезе ленты (только ленты новостей) и мессенджера
DAU_all_time = df.groupby(['day', 'status'], as_index=False)['user_id'].nunique()


# In[40]:


DAU_all_time.head()


# In[41]:


# Вспомогательные переменные (для оформления отчета)
start_date = DAU_all_time['day'].min().strftime('%d.%m')
end_date = DAU_all_time['day'].max().strftime('%d.%m')
year = DAU_all_time['day'].max().strftime('%Y')


# In[42]:


# Приветствие - 3
message = f"Ключевые метрики за период {start_date}-{end_date} {year}:"
bot.sendMessage(chat_id=chat_id, text=message)


# In[43]:


# График DAU за все время
sns.set(rc={"figure.figsize":(9, 6)})

sns.lineplot(x='day', y='user_id', data=DAU_all_time, hue='status', linewidth=3)

plt.title(f'DAU за период {start_date}-{end_date} {year}', fontweight='bold')
plt.xlabel('Время')
plt.ylabel('Количество')
plt.xticks(DAU_all_time['day'][::10],  rotation='45')
plt.tight_layout() # чтобы все подписи к графика были в окошке графика
# plt.show()

# Отправляем DAU за все время
plot_object_3 = io.BytesIO() # создаем файловый объект - картинку
plt.savefig(plot_object_3) # сохраняем в него наш график
plot_object_3.name = 'DAU_all_time.png' # задаем имя нашего файлового объекта
plot_object_3.seek(0) # переносим курсор из конца файлового объекта в начало, чтобы прочитать весь файл (чтобы избежать отправки боту пустого файла)

plt.close()

# Отправляем изображение
bot.sendPhoto(chat_id=chat_id, photo=plot_object_3)


# ### Просмотры, лайки, сообщения за все время - график

# In[44]:


# Просмотры, лайки, сообщения за все время
vlm_all_time = df[['day','views','likes','messages']].groupby('day', as_index=False).sum(['views','likes','messages'])


# In[45]:


# График просмотров, лайков, сообщений за все время
sns.set(rc={"figure.figsize":(9, 6)})

sns.lineplot(x='day', y='views', data=vlm_all_time, linewidth=3)
sns.lineplot(x='day', y='likes', data=vlm_all_time, linewidth=3)
sns.lineplot(x='day', y='messages', data=vlm_all_time, linewidth=3)

plt.title(f'Просмотры, лайки, сообщения за период {start_date}-{end_date} {year}', fontweight='bold')
plt.xlabel('Дата')
plt.ylabel('Количество')
plt.xticks(vlm_all_time['day'][::10])
plt.legend(labels=["views","likes", "messages"])
plt.tight_layout() # чтобы все подписи к графика были в окошке графика
# plt.show()

# Отправляем просмотры, лайки, сообщения за весь период
plot_object_4 = io.BytesIO() # создаем файловый объект - картинку
plt.savefig(plot_object_4) # сохраняем в него наш график
plot_object_4.name = 'views_likes_messages_all_time.png' # задаем имя нашего файлового объекта
plot_object_4.seek(0) # переносим курсор из конца файлового объекта в начало, чтобы прочитать весь файл (чтобы избежать отправки боту пустого файла)

plt.close()

# Отправляем изображение
bot.sendPhoto(chat_id=chat_id, photo=plot_object_4)


# ### Среднее число просмотров, лайков, сообщений на пользователя за все время - график

# In[46]:


# Среднее количество просмотров, лайков, сообщений за весь период
vlm_avg_all_time = pd.DataFrame() # пустой датафрейм
# Среднее просмотров
vlm_avg_all_time[['day','views_avg']] = df[['day','views']].query('views>0').groupby('day', as_index=False).mean('views')
# Среднее лайков
vlm_avg_all_time['likes_avg'] = df[['day','likes']].query('likes>0').groupby('day', as_index=False).mean('likes')['likes']
# Среднее сообщений
vlm_avg_all_time['messages_avg'] = df[['day','messages']].query('messages>0').groupby('day', as_index=False).mean('messages')['messages']


# In[47]:


# График среднего количества просмотров, лайков, сообщений на пользователя за все время
sns.set(rc={"figure.figsize":(9, 6)})

sns.lineplot(x='day', y='views_avg', data=vlm_avg_all_time, linewidth=3)
sns.lineplot(x='day', y='likes_avg', data=vlm_avg_all_time, linewidth=3)
sns.lineplot(x='day', y='messages_avg', data=vlm_avg_all_time, linewidth=3)

plt.title(f'Среднее число событий на пользователя за период {start_date}-{end_date} {year}', fontweight='bold')
plt.xlabel('Дата')
plt.ylabel('Количество')
plt.xticks(vlm_avg_all_time['day'][::10])
plt.legend(labels=["avg_views","avg_likes", "avg_messages"])
plt.tight_layout() # чтобы все подписи к графика были в окошке графика
# plt.show()

# Отправляем просмотры, лайки, сообщения за весь период
plot_object_5 = io.BytesIO() # создаем файловый объект - картинку
plt.savefig(plot_object_5) # сохраняем в него наш график
plot_object_5.name = 'avg_views_likes_messages_all_time.png' # задаем имя нашего файлового объекта
plot_object_5.seek(0) # переносим курсор из конца файлового объекта в начало, чтобы прочитать весь файл (чтобы избежать отправки боту пустого файла)

plt.close()

# Отправляем изображение
bot.sendPhoto(chat_id=chat_id, photo=plot_object_5)


# ### Новые пользователи за все время - график

# In[48]:


# Новые пользователи по дням
new_users_daily = (df.sort_values(by='day').groupby('user_id', as_index=False).first().rename(columns={'day':'birthday'})
                  .groupby(['birthday', 'status'], as_index=False).agg({'user_id':'count'}))


# In[49]:


new_users_daily.birthday.min()


# In[50]:


# График новых пользователей приложения за все время по дням
sns.set(rc={"figure.figsize":(10, 6)})

sns.lineplot(x='birthday', y='user_id', data=new_users_daily, linewidth=3)

plt.title(f'Новые пользователи приложения по дням за период {start_date}-{end_date} {year}', fontweight='bold')
plt.xlabel('Время')
plt.ylabel('Количество')
plt.xlim([new_users_daily.birthday.min(), new_users_daily.birthday.max()])
plt.xticks(new_users_daily['birthday'][::6],  rotation='45')
plt.tight_layout() # чтобы все подписи к графика были в окошке графика
# plt.show()

# Отправляем DAU за все время
plot_object_6 = io.BytesIO() # создаем файловый объект - картинку
plt.savefig(plot_object_6) # сохраняем в него наш график
plot_object_6.name = 'new_users_all_time.png' # задаем имя нашего файлового объекта
plot_object_6.seek(0) # переносим курсор из конца файлового объекта в начало, чтобы прочитать весь файл (чтобы избежать отправки боту пустого файла)

plt.close()

# Отправляем изображение
bot.sendPhoto(chat_id=chat_id, photo=plot_object_6)


# Fin.
