import schedule
import time
from datetime import datetime

from new_pools import pools
from all_accounts import accounts_update


# большой скрипт - запускать неск раз в день
update_hours = [10, 18]
for hour in update_hours:
    schedule.every().day.at(f"{hour:02d}:00").do(accounts_update)

# скрипт поменьше - запускать каждые 24 ч
schedule.every().day.at("02:50").do(pools)

print('Сейчас', datetime.now().strftime("%H:%M:%S"))

# поллинг каждые n секунд
n = 20
while 1:
    # проверить, не настало ли время
    schedule.run_pending()
    time.sleep(n)
