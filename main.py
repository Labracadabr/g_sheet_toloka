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

# поллинг каждые n секунд
n = 30
while 1:
    # проверить, не настало ли время
    schedule.run_pending()
    time.sleep(n)
