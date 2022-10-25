import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse
from datetime import date
import io
from read_db.CH import Getch, Insertch
import sys
import os

sns.set()


def get_alert_info():
    metrics_list = {
        'users_feed': {
            'alias': 'users_feed',
            'formula': 'uniqExact(user_id)',
            'metric_name': 'Users Feed',
            'table_name': 'simulator.feed_actions'
        },
        'likes': {
            'alias': 'likes',
            'formula': "countIf(user_id, action='like')",
            'metric_name': 'Likes',
            'table_name': 'simulator.feed_actions'
        },
        'views': {
            'alias': 'views',
            'formula': "countIf(user_id, action='view')",
            'metric_name': 'Views',
            'table_name': 'simulator.feed_actions'
        },
        'ctr': {
            'alias': 'ctr',
            'formula': "countIf(user_id, action='like') / countIf(user_id, action='view')",
            'metric_name': 'CTR',
            'table_name': 'simulator.feed_actions'
        },
        'lpu': {
            'alias': 'lpu',
            'formula': "countIf(user_id, action='like') / uniqExact(user_id)",
            'metric_name': 'Likes per user',
            'table_name': 'simulator.feed_actions'
        },
        'vpu': {
            'alias': 'vpu',
            'formula': "countIf(user_id, action='view') / uniqExact(user_id)",
            'metric_name': 'Views per user',
            'table_name': 'simulator.feed_actions'
        },
        'users_msg': {
            'alias': 'users_msg',
            'formula': 'uniqExact(user_id)',
            'metric_name': 'Users Messenger',
            'table_name': 'simulator.message_actions'
        },
        'messages': {
            'alias': 'messages',
            'formula': 'count(user_id)',
            'metric_name': 'Messages',
            'table_name': 'simulator.message_actions'
        },
        'mpu': {
            'alias': 'mpu',
            'formula': 'count(user_id) / uniqExact(user_id)',
            'metric_name': 'Messages per user',
            'table_name': 'simulator.message_actions'
        },
    }
    group_by_slices_list = {
        'os': {
            'alias': 'os',
            'group_levels': ['iOS', 'Android'],
            'formula': 'os',
        }
    }
    where_expression_template = ''' 
         toDate(time) in (today(), today() - 1, today() - 7) and formatDateTime(time, '%R') >= '01:00' '''

    responsible_users = {
        "metrics" : {"users_msg": "@mary_som"},
        "groups" : {"os": "@mary_som"}
    }
    alert_plots = {
        'total': {
            'users_feed': 'http://superset.lab.karpov.courses/r/27',
            'likes': 'http://superset.lab.karpov.courses/r/30',
            'views': 'http://superset.lab.karpov.courses/r/33',
            'ctr': 'http://superset.lab.karpov.courses/r/36',
            'vpu': 'http://superset.lab.karpov.courses/r/39',
            'lpu': 'http://superset.lab.karpov.courses/r/42',
            'users_msg': 'http://superset.lab.karpov.courses/r/45',
            'messages': 'http://superset.lab.karpov.courses/r/50',
            'mpu': 'http://superset.lab.karpov.courses/r/55'
        },
        'os': {
            'users_feed': 'http://superset.lab.karpov.courses/r/28',
            'likes': 'http://superset.lab.karpov.courses/r/32',
            'views': 'http://superset.lab.karpov.courses/r/35',
            'ctr': 'http://superset.lab.karpov.courses/r/38',
            'vpu': 'http://superset.lab.karpov.courses/r/41',
            'lpu': 'http://superset.lab.karpov.courses/r/44',
            'users_msg': 'http://superset.lab.karpov.courses/r/46',
            'messages': 'http://superset.lab.karpov.courses/r/52',
            'mpu': 'http://superset.lab.karpov.courses/r/54'
        }
    }
    return metrics_list, group_by_slices_list, where_expression_template, alert_plots, responsible_users


def get_select_vars(mertics, metrics_list, group_by_slices_list, slice, all_data=False):
    date_select = "toDate(time) as date, "

    time_select = '''toStartOfFifteenMinutes(time) as time, 
                     formatDateTime(toStartOfFifteenMinutes(time), '%R') as time_,'''

    select_expression = []
    for metric in mertics:
        formula = metrics_list[metric]['formula']
        alias = metrics_list[metric]['alias']
        expression = '{formula} AS {alias}'.format(formula=formula, alias=alias)
        select_expression.append(expression)

    if slice != 'total':
        formula = group_by_slices_list[slice]['formula']
        alias = group_by_slices_list[slice]['alias']
        expression = '{formula} AS {alias}'.format(formula=formula, alias=alias)
        select_expression.append(expression)

    select_expression = ','.join(select_expression)

    if not all_data:
        select_expression = date_select + select_expression
    else:
        select_expression = date_select + time_select + select_expression
    return select_expression


def get_from_table_name(mertics, metrics_list):
    table_name_list = []
    for metric in mertics:
        table_name = metrics_list[metric]['table_name']
        table_name_list.append(table_name)

    table_name_list = list(set(table_name_list))[0]
    table_name = str(table_name_list)
    return table_name


def get_where_expression(where_expression_template, group_by_slices_list, time, slice, all_data=False):
    where_expression = ''' {where_expression_template} and {alias} in ({group_levels}) '''

    day_0_min_time = time
    day_0_max_time = time + pd.Timedelta(minutes=15)
    day_1_min_time = time - pd.Timedelta(minutes=15) - pd.Timedelta(days=1)
    day_1_max_time = time + pd.Timedelta(minutes=30) - pd.Timedelta(days=1)
    day_7_min_time = time - pd.Timedelta(minutes=15) - pd.Timedelta(days=7)
    day_7_max_time = time + pd.Timedelta(minutes=30) - pd.Timedelta(days=7)

    time_expression = ''' and ((time >= '{day_0_min_time}' and time  < '{day_0_max_time}') 
                            or (time >= '{day_1_min_time}' and time < '{day_1_max_time}')
                            or (time >= '{day_7_min_time}' and time < '{day_7_max_time}'))'''.format(day_0_min_time=day_0_min_time,
                                                                                                  day_0_max_time=day_0_max_time,
                                                                                                  day_1_min_time=day_1_min_time,
                                                                                                  day_1_max_time=day_1_max_time,
                                                                                                  day_7_min_time=day_7_min_time,
                                                                                                  day_7_max_time=day_7_max_time)
    if not all_data:
        where_expression_template = where_expression_template + time_expression
    else:
        where_expression_template = where_expression_template

    if slice == 'total':
        where_expression = where_expression_template
    else:
        group_levels = group_by_slices_list[slice]['group_levels']
        alias = group_by_slices_list[slice]['alias']
        group_levels = "'" + "', '".join(group_levels) + "'"

        where_expression = where_expression.format(where_expression_template=where_expression_template,
                                                   alias=alias,
                                                   group_levels=group_levels)
    return where_expression


def get_group_by_expression(slice, group_by_slices_list, all_data=False):
    if not all_data:
        group_by_expression = ' date'
    else:
        group_by_expression = ' date, toStartOfFifteenMinutes(time) as time, time_'
    if slice != 'total':
        alias = group_by_slices_list[slice]['alias']
        group_by_expression = group_by_expression + ',' + alias
    return group_by_expression


def send_alert(plot_df, group, metric_name, metric_alias, slice_name, alert_plot, responsible_users, chat):

    bot = telegram.Bot(token=os.environ.get('REPORT_BOT_TOKEN'))

    dahsboard_link = 'https://superset.lab.karpov.courses/superset/dashboard/52/'

    day_0 = date.today().strftime("%Y-%m-%d")
    day_1 = (date.today() - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
    day_7 = (date.today() - pd.DateOffset(days=7)).strftime("%Y-%m-%d")

    sns.set(rc={'figure.figsize': (16, 10)})
    sns.set(font_scale=2)
    sns.set_style("white")
    plt.tight_layout()

    ax = sns.lineplot(data=plot_df[plot_df['date'].isin([day_0, day_1, day_7])].sort_values(['date', 'time_']),
                      x='time_', y=metric_alias,
                      hue='date',
                      hue_order=[day_7, day_1, day_0],
                      style='date',
                      style_order=[day_0, day_1, day_7],
                      size='date',
                      sizes=[4, 3, 3],
                      size_order=[day_0, day_1, day_7])
    ax.legend([day_7, day_1, day_0])

    for ind, label in enumerate(ax.get_xticklabels()):
        if ind % 15 == 0:
            label.set_visible(True)
        else:
            label.set_visible(False)
    ax.set(xlabel='time')
    ax.set(ylabel=metric_name)
    ax.set_title('{}\n{}'.format(group, metric_name))

    ax.set(ylim=(0, None))

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = '{}.png'.format(metric_name)
    plot_object.seek(0)
    plt.close()

    if slice_name == 'total':
        text = '''Проблема с метрикой {} - сильное отклонение от вчера/неделю назад!\nГрафик алерта {}\nДашборд {}'''.format(metric_name, alert_plot, dahsboard_link)
    else:
        text = '''Проблема с метрикой {} - сильное отклонение от вчера/неделю назад\nв срезе {} - {}\nГрафик алерта {}\nДашборд {}'''.format(metric_name, slice_name, group, alert_plot, dahsboard_link)

    responsible_users_metric = responsible_users['metrics'].get(metric_alias)
    if responsible_users_metric:
        text = text + '''\n\n{} посмотри, пожалуйста, все ли ок?'''.format(responsible_users_metric)

    responsible_users_group = responsible_users['groups'].get(slice_name)
    if responsible_users_group:
        text = text + '''\n\n{} посмотри, пожалуйста, все ли ок?'''.format(responsible_users_group)

    bot.sendMessage(chat_id=chat, text=text)
    bot.sendPhoto(chat_id=chat, photo=plot_object)
    return


def get_alert(alert_data, time, chat):
    chat_id = chat

    metrics_list, group_by_slices_list, where_expression_template, alert_plots, responsible_users = get_alert_info()

    mertics = alert_data['mertics']
    slice = alert_data['slice']

    query = '''SELECT {select_vars} 
               FROM {from_table_name} 
               WHERE {where_expression} 
               GROUP BY {group_by_expression}'''
    select_vars = get_select_vars(mertics, metrics_list, group_by_slices_list, slice)
    from_table_name = get_from_table_name(mertics, metrics_list)
    where_expression = get_where_expression(where_expression_template, group_by_slices_list, time, slice)
    group_by_expression = get_group_by_expression(slice, group_by_slices_list)

    alert_query = query.format(select_vars=select_vars,
                               from_table_name=from_table_name,
                               where_expression=where_expression,
                               group_by_expression=group_by_expression)

    all_data = Getch(alert_query).df

    data_to_alerts = {}

    if slice != 'total':
        group_levels = group_by_slices_list[slice]['group_levels']
        alias = group_by_slices_list[slice]['alias']

        for group_level in group_levels:
            alert_df = all_data[all_data[alias] == group_level].copy()
            data_to_alerts[group_level] = {'df': alert_df, 'slice': slice, 'group_level': group_level}

    else:
        data_to_alerts['total'] = {'df': all_data, 'slice': 'total', 'group_level': 'total'}

    for group in data_to_alerts:
        alert_df = data_to_alerts[group]['df']
        slice_name = data_to_alerts[group]['slice']

        for metric in mertics:
            metric_alias = metrics_list[metric]['alias']
            metric_name = metrics_list[metric]['metric_name']
            alert_data = get_metric_alert(alert_df, metric, metric_alias, slice, group, day_threshhold=0.5)

            alert_data['metric'] = metric
            alert_data['metric_name'] = metric_name
            alert_data['slice'] = slice
            alert_data['group'] = group
            alert_data['group_level'] = group
            alert_data['time'] = time

            alerts_log_df = pd.DataFrame(alert_data, index=[0]).fillna(0)
            alerts_log_df = alerts_log_df[['time', 'day_0_value', 'day_1_value', 'day_7_value',
                                           'day_1_diff', 'day_7_diff', 'slice', 'group_level',
                                           'metric', 'metric_name', 'is_alert']]

            Insertch(alerts_log_df, 'alerts_log').insert

            if not alert_data['is_alert']:
                continue

            select_vars = get_select_vars(mertics, metrics_list, group_by_slices_list, slice, True)
            from_table_name = get_from_table_name(mertics, metrics_list)
            where_expression = get_where_expression(where_expression_template, group_by_slices_list, time, slice, True)
            group_by_expression = get_group_by_expression(slice, group_by_slices_list, True)

            plot_df_query = query.format(select_vars=select_vars,
                                         from_table_name=from_table_name,
                                         where_expression=where_expression,
                                         group_by_expression=group_by_expression)

            plot_df = Getch(plot_df_query).df

            if slice != 'total':
                plot_df = plot_df.query("{slice} == '{group}'".format(group=group, slice=slice))

            plot_df['time'] = pd.to_datetime(plot_df['time'])
            plot_df = plot_df[plot_df['time'] <= time]
            alert_plot = alert_plots[slice][metric]

            send_alert(plot_df, group, metric_name, metric_alias, slice_name, alert_plot, responsible_users, chat=chat_id)


def get_metric_alert(alert_df, metric, metric_alias, slice, group, day_threshhold=0.3):
    alert_data = {}
    is_alert = 0

    day_0 = date.today().strftime("%Y-%m-%d")
    day_1 = (date.today() - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
    day_7 = (date.today() - pd.DateOffset(days=7)).strftime("%Y-%m-%d")
    day_0_value = alert_df[alert_df['date'] == day_0][metric].iloc[0]
    day_1_value = alert_df[alert_df['date'] == day_1][metric].iloc[0]
    day_7_value = alert_df[alert_df['date'] == day_7][metric].iloc[0]

    if metric in ('users_feed', 'users_msg', 'likes', 'views', 'messages'):
        day_1_value = day_1_value/3
        day_7_value = day_7_value/3

    if day_0_value <= day_7_value:
        day_7_diff = abs(day_0_value / day_7_value - 1)
    else:
        day_7_diff = abs(day_7_value / day_0_value - 1)

    if day_0_value <= day_1_value:
        day_1_diff = abs(day_0_value / day_1_value - 1)
    else:
        day_1_diff = abs(day_1_value / day_0_value - 1)

    alert_data['day_0_value'] = day_0_value
    alert_data['day_1_value'] = day_1_value
    alert_data['day_7_value'] = day_7_value
    alert_data['day_1_diff'] = day_1_diff
    alert_data['day_7_diff'] = day_7_diff
    alert_data['is_alert'] = is_alert

    q = '''SELECT max(time) as last_time FROM simulator.alerts_log 
           WHERE is_alert = 1 and slice = '{}' and metric = '{}' and group_level = '{}' '''.format(slice,
                                                                                                   metric_alias,
                                                                                                   group)
    last_time = Getch(q).df
    last_time = last_time['last_time'].iloc[0]

    if last_time != '1970-01-01 03:00:00' and pd.Timedelta(pd.Timestamp('now') - pd.to_datetime(last_time)).seconds / 3600.0 < 3:
        return alert_data

    if (day_1_value <= day_0_value <= day_7_value) or (day_7_value <= day_0_value <= day_1_value):
        return alert_data
    if day_7_diff >= day_threshhold and day_1_diff >= day_threshhold:
        is_alert = 1
        alert_data['is_alert'] = is_alert
        return alert_data
    else:
        return alert_data


def global_monitoring(chat=None):
    chat_id = chat or 454623234
    alerts = [
        {"mertics": ["users_feed", "likes", "views", "ctr"], "slice": "total"},
        {"mertics": ["users_feed", "likes", "views", "ctr"], "slice": "os"},
        {"mertics": ["users_msg", "messages", "mpu"], "slice": "total"},
    ]

    time = Getch('''SELECT toStartOfFifteenMinutes(max(time))  - interval 15 minute AS mas_time
              FROM simulator.feed_actions WHERE toDate(time) >= today() - 1''').df
    time = time['mas_time'].iloc[0]

    for alert_data in alerts:
        get_alert(alert_data, time, chat=chat_id)


global_monitoring(chat=None)

