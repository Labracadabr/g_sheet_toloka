import pygsheets
import toloka.client as toloka
import time
from datetime import datetime
from acc_secret_info import accounts

# google api
service_file = 'token.json'
gc = pygsheets.authorize(service_file=service_file)
sheet_url = 'https://docs.google.com/spreadsheets/d/1F2489hUxzXtMJMJuXPE_GKl8eR203HVpNc3bR-sNf7M/edit#gid=0'
spreadsheet = gc.open_by_url(sheet_url)


def pools():
    # с какого времени проверять
    yesterday = int(time.time()) - 60 * 60 * 24

    # дата сейчас
    date_str = str(datetime.fromtimestamp(time.time()).date())

    print('\nСтарт')
    # Перебор аккаунтов
    for account in accounts:
        if 'id' in account.lower() or 'yandex' in account.lower():
            print('skip', account)
            continue  # тут не все акки нужны

        print('Аккаунт:', account)
        token = accounts[account]['token']
        toloka_client = toloka.TolokaClient(token, 'PRODUCTION')
        balance = int(toloka_client.get_requester().balance)
        today_pools = []

        # Перебор проектов активных
        for project in toloka_client.get_projects(status='ACTIVE', batch_size=100):
            prj_id = project.id
            # print(prj_id, project.public_name, project.created.date())

            # Перебор пулов за сегодня
            for pool in toloka_client.get_pools(project_id=prj_id,
                                                created_gt=datetime.fromtimestamp(yesterday)):
                pool_name = pool.private_name
                # print(pool.id, pool.status, pool.private_name, pool.created)
                x = pool_name.split()  # убрать дату из начала названия пула
                if '202' in x[0]:
                    pool_name = ' '.join(x[1:])
                today_pools.append(pool_name)

        if today_pools:
            today_pools = '\n'.join(today_pools)
        else:
            today_pools = '-'

        # аппенд в таблицу
        data: list = [date_str, today_pools, balance, '?']
        print(data)
        spreadsheet.worksheet_by_title(account).append_table(values=data)
        print('Строка добавлена в', account)
        print()

    print('✅ Внесено за', date_str)
