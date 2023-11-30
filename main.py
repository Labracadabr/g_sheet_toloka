import schedule
import time
from datetime import datetime

from new_pools import pools
from all_accounts import accounts_update


#  эта функция запускается раз в сутки
def main():
    accounts_update()   # большой скрипт
    pools()             # скрипт поменьше


# время запуска
upd_hour, upd_minute = 23, 59
schedule.every().day.at(f"{upd_hour:02d}:{upd_minute:02d}").do(main)
s = start_time = 0

# поллинг каждые n секунд
n = 30
while 1:
    schedule.run_pending()
    if s < 1:
        start_time = int(datetime.now().replace(hour=upd_hour, minute=upd_minute, second=0).timestamp())
    s = start_time - int(time.time())
    print('\rСтарт через', s, 'сек', end='')
    time.sleep(n)
