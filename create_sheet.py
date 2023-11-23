import httplib2
import apiclient
from oauth2client.service_account import ServiceAccountCredentials

mail = 'd_minokin@trainingdata.pro'  # Вставь свою почту
CREDENTIALS_FILE = 'token.json'  # Имя файла с токеном
# Ниже можно менять только значения в словаре в spreadsheet, остальное не трогать

# Читаем ключи из файла
credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE,
                                                               ['https://www.googleapis.com/auth/spreadsheets',
                                                                'https://www.googleapis.com/auth/drive'])

httpAuth = credentials.authorize(httplib2.Http())  # Авторизуемся в системе
service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)  # Выбираем работу с таблицами и 4 версию API

spreadsheet = service.spreadsheets().create(body={
    'properties': {'title': 'Табличка', 'locale': 'ru_RU'},
    'sheets': [{'properties': {'sheetType': 'GRID',
                               'sheetId': 0,
                               'title': 'Лист номер один',
                               'gridProperties': {'rowCount': 100, 'columnCount': 15}}}]
}).execute()
spreadsheet_id = spreadsheet['spreadsheetId']  # сохраняем идентификатор файла

# Выдача доступа
driveService = apiclient.discovery.build('drive', 'v3', http=httpAuth)  # Выбираем работу с Google Drive и 3 версию API

# Открываем доступ на редактирование
access = driveService.permissions().create(
    fileId=spreadsheet_id,
    body={'type': 'user', 'role': 'writer', 'emailAddress': mail},
    fields='id').execute()

print('Готово, сохрани эту ссылку. Она продублирована тебе на', mail)
print('https://docs.google.com/spreadsheets/d/' + spreadsheet_id)
