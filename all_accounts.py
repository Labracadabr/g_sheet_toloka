import gspread
from google.auth import exceptions
import toloka.client as toloka
import schedule
import time
import datetime
import requests
import json
from decimal import Decimal
from acc_info import *
from datetime import datetime
from pprint import pprint


# google api
service_file='token.json'
gc = gspread.service_account(filename=service_file)
sheet_url = 'https://docs.google.com/spreadsheets/d/1_O2Ran9qpu_eXlQ0MuqnGYa5PS2IMQVT1qWgLobNaDE/edit#gid=0'
spreadsheet = gc.open_by_url(sheet_url)


def month_and_year():
    current_date = datetime.now()
    month = current_date.strftime('%B')
    year = str(datetime.now().year)
    result = f"{month} {year[2:]}"
    return result  # > например "November 23"


def g_upd_start(page):
    try:  # проверить есть ли такая страница
        sheet = spreadsheet.worksheet(page)
    except gspread.exceptions.WorksheetNotFound:  # создать страницу если ее нет
        create_page(newname=page)
        sheet = spreadsheet.worksheet(page)

    sheet.update(range_name='A2', values='Обновление в процессе'.upper())

    # удалить все строки кроме первых двух
    rows = sheet.row_count
    if rows > 2:
        sheet.delete_rows(start_index=3, end_index=rows)


def create_page(newname):
    print('Создание', newname)
    # создать новый лист по шаблону
    template = spreadsheet.worksheet('Шаблон')
    new_worksheet = spreadsheet.add_worksheet(title=newname, rows=2, cols=8)

    # внести данные из шаблона
    data = template.get_all_values()
    new_worksheet.update(range_name='A1', values=data)

    # отправить запрос
    body = {
        "requests": [
            {
                "copyPaste": {
                    "source": {"sheetId": template.id, "startRowIndex": 0, "startColumnIndex": 0},
                    "destination": {"sheetId": new_worksheet.id, "startRowIndex": 0, "startColumnIndex": 0},
                    "pasteType": "PASTE_FORMAT",
                }
            }
        ]
    }
    spreadsheet.batch_update(body)
    print('Создан лист', newname)

def g_upd_end(page):
    upd_text = f'Обновлено {datetime.now().strftime("%d/%m, %H:%M:%S")}'
    print(upd_text)
    spreadsheet.worksheet(page).update(range_name='A2', values=upd_text)

def google_append(page, data):
    print(f'Внесено в лист {page}:')
    print(data)
    spreadsheet.worksheet(page).append_row(values=data)

# кол-во сообщений
def count_unread_msgs(client):
    count = 0
    for message_thread in client.get_message_threads(folder=['INBOX', 'UNREAD'], batch_size=300):
        count += 1
    return count


# заморожено и проч
def count_funds(acc: str) -> dict:
    # токен
    if 'td.pro' in acc:
        # для get запроса у дочерних акков нужен не собственный токен, а токен родительского акка
        token = accounts['td.pro']['token']
    else:
        token = accounts[acc]['token']

    HEADERS = {"Authorization": "OAuth %s" % token, "Content-Type": "application/JSON"}
    start_date = '2023-11-01'  # включительно
    finish_date = '2023-11-30'  # не включительно
    url = f'https://platform.toloka.ai/api/billing/company/expense-log?from={start_date}&to={finish_date}&timezone=%2B03%3A00&requesterId=company'
    resp = requests.get(url, headers=HEADERS)
    full_money_data = json.loads(resp.content)
    # pprint(full_money_data)

    output_data = ('projects', 'pools', 'total_spent', 'total_block')
    projects_dict = {}
    for i in output_data:
        projects_dict.setdefault(i, 0)
    projects, pools = [], []
    total_spent = total_block = 0
    for date_bill in full_money_data:
        # bonus_data += date_bill['bonuses']
        if not isinstance(date_bill, dict):
            continue

        for assignment_bill in date_bill['assignments']:
            requester_id = assignment_bill['requesterId']

            # если проект относится не к самому аккаунту, а к родственному
            if requester_id != accounts[acc]['id']:
                continue

            # данные проекта
            project_name = assignment_bill['project']['name']
            project_id = assignment_bill['project']['id']
            pool_id = assignment_bill['pool']['id']
            projects_dict.setdefault(project_id, {})
            project_link = f'https://platform.toloka.ai/requester/project/{project_id}'

            # потрачено и заморожено
            spent = float(Decimal(assignment_bill['spent'] + assignment_bill['fee']).quantize(Decimal("1.00")))
            block = float(
                Decimal(assignment_bill['blockedSpent'] + assignment_bill['blockedFee']).quantize(Decimal("1.00")))

            # плюсануть в словарь проекта
            projects_dict[project_id]['spent'] = projects_dict.get(spent, 0) + spent
            projects_dict[project_id]['block'] = projects_dict.get(block, 0) + block

            # плюсануть в общие значения аккаунта
            total_spent += spent
            total_block += block
            projects.append(project_id)
            pools.append(pool_id)

    # посчитать кол-во уникальных
    projects = len(set(projects))
    pools = len(set(pools))

    # сохранить инфу в словарь аккаунта
    for i in output_data:
        projects_dict[i] = vars()[i]
    # accounts[acc].setdefault('projects', projects_dict)

    return projects_dict


def read_account(account):
    # данные аккаунта
    token = accounts[account]['token']
    toloka_client = toloka.TolokaClient(token, 'PRODUCTION')
    req_name = toloka_client.get_requester().public_name.values()
    print('Аккаунт:', *req_name)
    balance = int(toloka_client.get_requester().balance)
    msgs = count_unread_msgs(client=toloka_client)
    acc_dict = count_funds(acc=account)

    # финансы аккаунта
    spent = acc_dict.get('total_spent')
    block = acc_dict.get('total_block')

    # кол-во проектов и пулов
    projects = acc_dict.get('projects')
    pools = acc_dict.get('pools')

    # внести строку в главный лист таблицы
    acc_data = [account, msgs, balance, spent, block, projects, pools]
    google_append('Main', data=acc_data)

    for project_id in acc_dict:
        # тут читаем только словари
        if not isinstance(acc_dict[project_id], dict):
            continue
        # pprint(acc_dict[project_id])
        # данные проекта
        project = toloka_client.get_project(project_id=project_id)
        proj_url = f'https://platform.toloka.ai/requester/project/{project_id}'

        proj_name = project.public_name

        # данные из private_comment. если в поле есть '#', то
        comment = project.private_comment
        client = manager = ''
        if '#' in comment:
            comment = comment.split('#')
            client = comment[1]
            manager = comment[-1]
            comment = comment[0]

        # финансы проекта
        spent = acc_dict[project_id].get('spent')
        block = acc_dict[project_id].get('block')

        # внести в таблицу
        project_data = [proj_name, account, spent, block, proj_url, client, manager, comment]
        google_append(page=month_and_year(), data=project_data)


def main():
    # текущий месяц
    month_page = month_and_year()

    # очистить таблицу
    g_upd_start(page='Main')
    g_upd_start(page=month_page)
    print('start')

    # собрать и внести данные каждого акка
    for account in accounts:
        read_account(account)

    # вписать время последнего обновления
    g_upd_end(page=month_page)
    g_upd_end(page='Main')
    print('end')


if __name__ == '__main__':
    # main()
    # input()
    try:
        main()
    except Exception as e:
        # если что-то пошло не так, записать это в таблице
        print(e)
        spreadsheet.worksheet('Main').update(range_name='B2', values=f'ОШИБКА:\n{e}')
        # spreadsheet.worksheet('Main').update_cell(row=2, col=1, value=f'ОШИБКА:\n{e}')

# Задать время обновления
upd_hour, upd_minute = 23, 59

# print(f'Скрипт заработает в {upd_hour}:{upd_minute}')
# print('Сейчас', ':'.join(time.strftime("%H %M %S").split()))
# schedule.every().day.at(f"{upd_hour:02d}:{upd_minute:02d}").do(sheet_update)

# sheet_update()
# while 1:
#     schedule.run_pending()
#     time.sleep(10)
