import time
import traceback
import gspread
import pytz
import toloka.client as toloka
import datetime
import requests
import json
from gspread import Cell

from common import *
from acc_secret_info import accounts
from datetime import datetime
from pprint import pprint

# текущий месяц и год
month_map = {
    '1': 'Январь',
    '2': 'Февраль',
    '3': 'Март',
    '4': 'Апрель',
    '5': 'Май',
    '6': 'Июнь',
    '7': 'Июль',
    '8': 'Август',
    '9': 'Сентябрь',
    '10': 'Октябрь',
    '11': 'Ноябрь',
    '12': 'Декабрь'
}


def month_year_now() -> str:  # > например "Ноябрь 2024"
    current_date = datetime.now()
    string = f'{month_map[str(current_date.month)]} {current_date.year}'
    return string


month_page = month_year_now()  # страница текущего месяца
main_page = 'Main'  # главная страница

# GOOGLE API

# данные для подключения к таблице
sheet_url = 'https://docs.google.com/spreadsheets/d/1_O2Ran9qpu_eXlQ0MuqnGYa5PS2IMQVT1qWgLobNaDE/edit#gid=0'
spreadsheet = gc.open_by_url(sheet_url)


# стереть строки в таблице
@api_decorator
def clear_rows(page: str):
    try:  # проверить есть ли такая страница
        sheet = spreadsheet.worksheet(page)
    except gspread.exceptions.WorksheetNotFound:  # создать страницу если ее нет
        clone_page(new_name=page)
        sheet = spreadsheet.worksheet(page)

    # удалить все строки кроме первых двух
    rows = sheet.row_count
    if rows > 2:
        sheet.delete_rows(start_index=3, end_index=rows)


# клонировать лист по шаблону - втч текст, размер таблицы и цвета ячеек
@api_decorator
def clone_page(new_name: str):
    print('Создание лист', new_name)
    # создать новый лист по шаблону
    template = spreadsheet.worksheet('Шаблон')
    new_worksheet = spreadsheet.add_worksheet(title=new_name, rows=template.row_count, cols=template.col_count)

    # внести данные из шаблона
    data = template.get_all_values()
    new_worksheet.update(range_name='A1', values=data)

    # отправить copyPaste запрос
    body = {"requests": [{"copyPaste": {
        "source": {"sheetId": template.id, "startRowIndex": 0, "startColumnIndex": 0},
        "destination": {"sheetId": new_worksheet.id, "startRowIndex": 0, "startColumnIndex": 0},
        "pasteType": "PASTE_FORMAT", }}]}
    spreadsheet.batch_update(body)
    print('Создан лист', new_name)


# что делать каждое 1ое число в листе main
@api_decorator
def new_month_action():
    insert_empty_rows()  # вставить новые строки
    merge_cells()  # объединить 3 ячейки для назв месяца
    spreadsheet.worksheet(main_page).update(range_name='D2', values=month_page)  # вписать в них новый месяц


@api_decorator
def insert_empty_rows():
    start = 1
    end = len(accounts) + 4  # = 8
    # отправить insertDimension запрос
    body = {"requests": [{"insertDimension": {"range": {
        "sheetId": 0,
        "dimension": "ROWS",
        "startIndex": start,
        "endIndex": end}}}, ], }
    spreadsheet.batch_update(body)
    print('Вставлены строки с', start, 'по', end)


@api_decorator
def update_alert(page):
    sheet = spreadsheet.worksheet(page)
    sheet.update(range_name='A2', values='Обновление в процессе'.upper())


@api_decorator
def merge_cells():
    # отправить mergeCells запрос
    body = {"requests": [{"mergeCells": {"range": {
        "sheetId": 0,
        "startRowIndex": 1,
        "endRowIndex": 2,
        "startColumnIndex": 3,
        "endColumnIndex": 7, }, "mergeType": "MERGE_ALL"}}, ], }
    spreadsheet.batch_update(body)
    print('mergeCells')


# указать время обновления по gmt+3
@api_decorator
def put_upd_time(page: str):
    upd_text = f'Обновлено {datetime.now(tz).strftime("%d/%m, %H:%M")} по мск'
    print(upd_text)
    spreadsheet.worksheet(page).update(range_name='A2', values=upd_text)


# добавить строку в конец таблицы
@api_decorator
def google_append(page: str, data: list):
    print(f'Внесено в лист {page}:')
    print(data)
    spreadsheet.worksheet(page).append_row(values=data)


# TOLOKA API

# собрать все данные с аккаунта
def read_account(row_num, account: str, page: str, token):
    print('read_account', account)
    # у толоки и яндекса разный домен
    if 'yandex' in account.lower():
        base_url = 'https://tasks.yandex.ru'
    else:
        base_url = 'https://platform.toloka.ai'
    headers = {"Authorization": "OAuth %s" % accounts[account]['token'], "Content-Type": "application/JSON"}

    # данные аккаунта
    if 'id' in account.lower() or 'td.pro5' in account.lower():
        toloka_client = toloka.TolokaClient(accounts[account]['token'], 'PRODUCTION')
        balance = int(toloka_client.get_requester().balance)
        msgs = 0
        for message_thread in toloka_client.get_message_threads(folder=['INBOX', 'UNREAD'], batch_size=300):
            msgs += 1
    else:
        r = requests.get(url=f'{base_url}/api/users/current/requester', headers=headers)
        requester: dict = r.json()
        balance = int(requester.get('balance'))
        msgs = count_unread_msgs(account=account, base_url=base_url)

    # финансы аккаунта
    acc_dict = count_funds(acc=account, base_url=base_url, token=token)
    spent = acc_dict.get('total_spent')
    block = acc_dict.get('total_block')

    # кол-во проектов и пулов
    projects = acc_dict.get('projects')
    pools = acc_dict.get('pools')

    # внести строку в главный лист таблицы
    acc_data = [account, msgs, balance, spent, block, projects, pools]
    # приготовить список с объектами ячеек
    cell_list = []
    for i, val in enumerate(acc_data, start=1):
        cell_list.append(Cell(row=row_num, col=i, value=val))

    # обновить все разом
    while True:
        try:
            spreadsheet.worksheet(page).update_cells(cell_list)
            break
        except gspread.exceptions.APIError as e:
            print('\ngoogle error', e)
            time.sleep(30)
    print(f'обновлено {len(acc_data)} ячеек:', acc_data)

    # просмотреть каждый активный проект в аккаунте
    for project_id in acc_dict:
        # тут читаем только словари
        if not isinstance(acc_dict[project_id], dict):
            continue
        read_project(project_id, account, acc_dict, base_url, token)


# кол-во сообщений в аккаунте
def count_unread_msgs(account: str, base_url: str) -> int:
    token = accounts[account]['token']
    url = f'{base_url}/api/message/status'
    headers = {"Authorization": "OAuth %s" % token, "Content-Type": "application/JSON"}
    r = requests.get(url, headers=headers)
    unread = r.json().get('unread')

    # # старый способ
    # unread = 0
    # for message_thread in client.get_message_threads(folder=['INBOX', 'UNREAD'], batch_size=300):
    #     # message_thread
    #     unread += 1
    return unread


# заморожено, потрачено, кол-во пулов и проектов
def count_funds(acc: str, base_url: str, token: str) -> dict:
    # период трат
    from_date = datetime.today().replace(day=1).strftime('%Y-%m-%d')  # включительно. тут 1ое число текущего месяца
    till_date = '3023-11-30'  # не включительно. если указать дату из будущего, то поиск будет до сейчас, поэтому 3023

    # запрос
    headers = {"Authorization": "OAuth %s" % token, "Content-Type": "application/JSON"}
    url = f'{base_url}/api/new/requester/finance/expense-log?from={from_date}&to={till_date}'
    if acc == 'td.pro5':
        url = f'{base_url}/api/billing/company/expense-log?from={from_date}&to={till_date}'
    print('url', url)

    # ответ
    response = requests.get(url, headers=headers)
    full_money_data = json.loads(response.content)

    # создать ключи в словаре и переменные для подсчета
    output_keys = ('projects', 'pools', 'total_spent', 'total_block')
    projects_dict = {}
    for i in output_keys:
        projects_dict.setdefault(i, 0)
    projects, pools = [], []
    total_spent = total_block = 0

    # перебор каждой даты
    for date_bill in full_money_data:
        # bonus_data += date_bill['bonuses']
        if not isinstance(date_bill, dict):
            continue

        # каждый assignment за эту дату
        for assignment_bill in date_bill['assignments']:
            if acc == 'td.pro5':
                requester_id = assignment_bill['requesterId']
            else:
                requester_id = assignment_bill['requester']['id']

            # если проект относится не к самому аккаунту, а к родственному, то не учитывать
            if requester_id != accounts[acc]['id']:
                continue

            # данные проекта
            project_id = assignment_bill['project']['id']
            pool_id = assignment_bill['pool']['id']
            projects_dict.setdefault(project_id, {})

            # потрачено и заморожено
            if acc == 'td.pro5':
                spent = float(assignment_bill['spent'] + assignment_bill['fee'])
                block = float(assignment_bill['blockedSpent'] + assignment_bill['blockedFee'])
            else:
                spent = float(assignment_bill['totalIncome'] + assignment_bill['tolokaFee'])
                block = float(assignment_bill['blockedIncome'] + assignment_bill['blockedTolokaFee'])
            # spent = float(Decimal(assignment_bill['spent'] + assignment_bill['fee']).quantize(Decimal("1.00")))
            # block = float(
            #     Decimal(assignment_bill['blockedSpent'] + assignment_bill['blockedFee']).quantize(Decimal("1.00")))

            # плюсануть в словарь проекта
            projects_dict[project_id]['spent'] = projects_dict[project_id].get('spent', 0) + spent
            projects_dict[project_id]['block'] = projects_dict[project_id].get('block', 0) + block

            # плюсануть в общие значения аккаунта
            total_spent += spent
            total_block += block
            projects.append(project_id)
            pools.append(pool_id)

    # посчитать кол-во уникальных
    projects = len(set(projects))
    pools = len(set(pools))

    # сохранить инфу в словарь аккаунта
    for i in output_keys:
        projects_dict[i] = vars()[i]
    return projects_dict


# данные из private_comment
def read_comment(comment: str) -> tuple:
    try:  # разделить коммент на 3 части по символу # и убрать пробелы по краям
        comment, client, manager = map(lambda x: x.strip(), comment.split('#'))
    except ValueError:  # если разделителей не два, то client и manager останутся пустые, а comment не изменится
        client = manager = ''
    return comment, client, manager


# данные с проекта
def read_project(project_id, account, acc_dict, base_url: str, token: str):
    headers = {"Authorization": "OAuth %s" % token, "Content-Type": "application/JSON"}
    r = requests.get(url=f'{base_url}/api/v1/projects/{project_id}', headers=headers)

    # данные проекта
    # project = toloka_client.get_project(project_id=project_id)
    project = r.json()
    proj_url = f'{base_url}/requester/project/{project_id}'
    create_date = str(project['created'].split('T')[0])
    proj_name = project['public_name']

    # данные из private_comment. если в поле нет разделителя, то client и manager будут пустые
    comment, client, manager = read_comment(project['private_comment'])

    # финансы проекта
    spent = acc_dict[project_id].get('spent')
    block = acc_dict[project_id].get('block')

    # внести в таблицу
    project_data = [proj_name, create_date, account, spent, block, proj_url, client, manager, comment]
    google_append(page=month_page, data=project_data)


# запустить всё
def accounts_update():
    while True:
        try:
            print('\nstart')
            # какой месяц написан в таблице
            saved_month = spreadsheet.worksheet(main_page).acell('D2').value

            # если уже другой мес
            if saved_month != month_page:
                new_month_action()

            # очистить таблицу
            clear_rows(page=month_page)

            update_alert(page=month_page)
            update_alert(page=main_page)

            # собрать и внести данные каждого аккаунта в гугл таблицы
            for row_num, account in enumerate(accounts, start=3):
                # токен
                token = accounts[account]['token']
                if 'td.pro5' in account:
                    # для запросов у дочерних акков нужен не собственный токен, а токен родительского акка
                    token = accounts['td.pro']['token']

                read_account(row_num, account, page=main_page, token=token)

            # вписать время последнего обновления
            put_upd_time(page=main_page)
            put_upd_time(page=month_page)
            print('ok')

        # если что-то еще пошло не так, записать это в таблице
        except Exception:
            error = traceback.format_exc()
            print(error)
            t = datetime.now().strftime("%d/%m, %H:%M:%S")
            spreadsheet.worksheet(main_page).update(range_name='B2', values=f'ОШИБКА {t}\n{error}')
            time.sleep(30)
            break
        break


if __name__ == '__main__':
    accounts_update()

