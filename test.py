import pygsheets
import toloka.client as toloka
import schedule
import time
import datetime
# pip install --upgrade toloka-kit, pygsheets, oauth2client, --upgrade google-api-python-client, schedule

gc = pygsheets.authorize(service_file='token.json')
sheet_url = 'https://docs.google.com/spreadsheets/d/1F2489hUxzXtMJMJuXPE_GKl8eR203HVpNc3bR-sNf7M/edit#gid=0'
spreadsheet = gc.open_by_url(sheet_url)

# last_check = 1690911654  # 1 авг для примера
last_check = 1692046633  # 13 авг для примера
# last_check = int(time.time())  # сейчас

# Словарь акков {Toloka User ID: (Имя листа в g.sheet, toloka api token)}
toloka_acc = {"av1toteam1": ("Avito", "AQAAAAAsuDIIAACtpdJX3MlmSkiRkJrieApB-yU")}


def sheet_update():
    global last_check

    print('Старт')
    for account in toloka_acc.values():
        print('Аккаунт:', account[0])
        toloka_client = toloka.TolokaClient(account[1], 'PRODUCTION')
        balance = int(toloka_client.get_requester().balance)
        today_pools = []

        for project in toloka_client.get_projects(status='ACTIVE'):
            prj_id = project.id
            # print(prj_id, project.public_name, project.created.date())
            for pool in toloka_client.get_pools(project_id=prj_id, created_gt=datetime.datetime.fromtimestamp(last_check)):
                pool_name = pool.private_name
                # print(pool.id, pool.status, pool.private_name, pool.created)
                x = pool_name.split()  # убрать дату из начала названия пула
                if '202' in x[0]:
                    pool_name = ' '.join(x[1:])
                today_pools.append(pool_name)

        if today_pools:
            today_pools = max('\n'.join(today_pools), '-')
        else:
            today_pools = '-'

        # аппенд в таблицу
        data: list[str | int] = [time.strftime("%d %b"), today_pools, balance, '?']
        spreadsheet.worksheet_by_title(account[0]).append_table(values=data)
        print('Строка добавлена в', account[0])

    last_check = int(time.time())
    print('Внесено за', time.strftime("%d %b"))


# Задать время обновления
target_hour, target_minute = 23, 16

print(f'Скрипт заработает в {target_hour}:{target_minute}')
print('Сейчас', ':'.join(time.strftime("%H %M %S").split()))
schedule.every().day.at(f"{target_hour:02d}:{target_minute:02d}").do(sheet_update)

# sheet_update()
while 1:
    schedule.run_pending()
    time.sleep(10)
