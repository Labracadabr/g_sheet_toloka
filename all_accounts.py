import gspread
import toloka.client as toloka
import datetime
import requests
import json
from acc_secret_info import accounts
from datetime import datetime
from pprint import pprint

# GOOGLE API

# данные для подключения к таблице
service_file = 'token.json'
gc = gspread.service_account(filename=service_file)
sheet_url = 'https://docs.google.com/spreadsheets/d/1_O2Ran9qpu_eXlQ0MuqnGYa5PS2IMQVT1qWgLobNaDE/edit#gid=0'
spreadsheet = gc.open_by_url(sheet_url)


# стереть строки в таблице
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
def new_month_action():
    # вставить новые строки
    insert_rows()
    # объединить 3 ячейки
    merge_cells()
    # вписать в них новый месяц
    spreadsheet.worksheet(main_page).update(range_name='D2', values=month_page)


def insert_rows():
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


def update_alert(page):
    sheet = spreadsheet.worksheet(page)
    sheet.update(range_name='A2', values='Обновление в процессе'.upper())


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


# указать время обновления
def put_upd_time(page: str):
    upd_text = f'Обновлено {datetime.now().strftime("%d/%m, %H:%M:%S")}'
    print(upd_text)
    spreadsheet.worksheet(page).update(range_name='A2', values=upd_text)


# добавить строку в конец таблицы
def google_append(page: str, data: list):
    print(f'Внесено в лист {page}:')
    print(data)
    spreadsheet.worksheet(page).append_row(values=data)


# TOLOKA API

# собрать все данные с аккаунта
def read_account(row_num, account: str, page: str):
    # данные аккаунта
    token = accounts[account]['token']
    toloka_client = toloka.TolokaClient(token, 'PRODUCTION')
    req_name = toloka_client.get_requester().public_name.values()
    print('\nАккаунт:', *req_name)
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
    for i, val in enumerate(acc_data, start=1):
        print(f'row: {row_num}, col: {i}, val: {val}')
        spreadsheet.worksheet(page).update_cell(row=row_num, col=i, value=val)

    # просмотреть каждый активный проект в аккаунте
    for project_id in acc_dict:
        # тут читаем только словари
        if not isinstance(acc_dict[project_id], dict):
            continue
        read_project(project_id, account, acc_dict, toloka_client)


# кол-во сообщений в аккаунте толоки
def count_unread_msgs(client: toloka.TolokaClient) -> int:
    count = 0
    for message_thread in client.get_message_threads(folder=['INBOX', 'UNREAD'], batch_size=300):
        message_thread
        count += 1
    return count


# заморожено, потрачено, кол-во пулов и проектов
def count_funds(acc: str) -> dict:
    # токен
    if 'td.pro' in acc:
        # для get запроса у дочерних акков нужен не собственный токен, а токен родительского акка
        token = accounts['td.pro']['token']
    else:
        token = accounts[acc]['token']

    # период трат
    first_day_this_month = datetime.today().replace(day=1).strftime('%Y-%m-%d')
    from_date = first_day_this_month  # включительно. тут мне нужно 1ое число текущего месяца
    till_date = '3023-11-30'  # не включительно. если указать дату из будущего, то поиск будет до сейчас, поэтому 3023

    # запрос
    headers = {"Authorization": "OAuth %s" % token, "Content-Type": "application/JSON"}
    url = f'https://platform.toloka.ai/api/billing/company/expense-log?from={from_date}&to={till_date}&timezone=%2B03%3A00&requesterId=company'

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
            requester_id = assignment_bill['requesterId']

            # если проект относится не к самому аккаунту, а к родственному, то не учитывать
            if requester_id != accounts[acc]['id']:
                continue

            # данные проекта
            project_id = assignment_bill['project']['id']
            pool_id = assignment_bill['pool']['id']
            projects_dict.setdefault(project_id, {})

            # потрачено и заморожено
            spent = float(assignment_bill['spent'] + assignment_bill['fee'])
            block = float(assignment_bill['blockedSpent'] + assignment_bill['blockedFee'])
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
    # accounts[acc].setdefault('projects', projects_dict)

    return projects_dict


# данные из private_comment
def read_comment(comment: str) -> tuple:
    try:  # разделить коммент на 3 части по символу # и убрать пробелы по краям
        comment, client, manager = map(lambda x: x.strip(), comment.split('#'))

    except ValueError:  # если разделителей не два, то client и manager останутся пустые, а comment не изменится
        client = manager = ''

    return comment, client, manager


# данные с проекта
def read_project(project_id, account, acc_dict, toloka_client):
    # данные проекта
    project = toloka_client.get_project(project_id=project_id)
    proj_url = f'https://platform.toloka.ai/requester/project/{project_id}'
    create_date = str(project.created.date())
    proj_name = project.public_name

    # данные из private_comment. если в поле нет разделителя, то client и manager будут пустые
    comment, client, manager = read_comment(project.private_comment)

    # финансы проекта
    spent = acc_dict[project_id].get('spent')
    block = acc_dict[project_id].get('block')

    # внести в таблицу
    project_data = [proj_name, create_date, account, spent, block, proj_url, client, manager, comment]
    google_append(page=month_page, data=project_data)


# текущий месяц и год
def month_now() -> str:  # > например "November 23"
    current_date = datetime.now()
    month = current_date.strftime('%B')
    return month


month_page = month_now()  # страница текущего месяца
main_page = 'Main'  # главная страница


# запустить всё
def accounts_update():
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
            read_account(row_num, account, page=main_page)

        # вписать время последнего обновления
        put_upd_time(page=main_page)
        put_upd_time(page=month_page)
        print('ok')

    # если что-то пошло не так, записать это в таблице
    except Exception as e:
        print(e)
        t = datetime.now().strftime("%d/%m, %H:%M:%S")
        spreadsheet.worksheet(main_page).update(range_name='B2', values=f'ОШИБКА {t}\n{e}')
