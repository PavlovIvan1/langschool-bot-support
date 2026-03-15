import asyncio
import logging
import json
import gspread_asyncio
from google.oauth2.service_account import Credentials
import time
import datetime
import gc
from fastapi import FastAPI, Request
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import threading
from typing import Dict, Any
import aiofiles
import traceback
from handlers.start import start_router

from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.redis import RedisStorage

from redis.asyncio import Redis

import config
from database import MySQL
import keyboard

db = MySQL()
redis = Redis(host='localhost')

bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(link_preview_is_disabled=True))


def get_creds():
    creds = Credentials.from_service_account_file("credentials.json")
    scoped = creds.with_scopes([
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    return scoped

agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)

async def sort_update_data(data):
    return_data = {}

    for i in data:
        return_data[i['id']] = json.loads(i['data'])

    return return_data

def is_int(string):
    try:
        int(string)
        return True
    except ValueError:
        return False

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/add_data")
async def handle_alice_request(request: Request):
    email = str(request.url).split("email=")[-1].split("&")[0].replace("%40", "@")
    flow = str(request.url).split("flow=")[-1].split("&")[0]

    is_email_in_users_access = db.is_email_in_users_access(email)
    is_email_in_added_api_users = db.is_email_in_added_api_users(email)

    if is_email_in_users_access or is_email_in_added_api_users:
        return
    
    db.add_email_to_added_api_users(email)

    try:
        agc = await agcm.authorize()
        ss_2 = await agc.open_by_url(config.SPREADSHEET_URL_USERS)
        table = await ss_2.get_worksheet_by_id(0)
        await table.append_row([email, -1002572458943, flow], value_input_option="USER_ENTERED")
    except Exception as e:
        try:
            await bot.send_message(config.LOG_CHAT_ID, f'@infinityqqqq Не могу добавить {email} (API GETCOURSE): {e}')
        except:
            pass

def start_fast_api():
    uvicorn.run("bot:app", host="0.0.0.0", port=443, ssl_keyfile="/etc/letsencrypt/live/rb.infinitydev.tw1.su/privkey.pem", ssl_certfile="/etc/letsencrypt/live/rb.infinitydev.tw1.su/fullchain.pem")

def clean_string(string) -> tuple[str, bool]:
    cleaned_string = ' '.join(string.split())
    return cleaned_string

async def check_info():
    time_to_clear = time.time()
    time_to_update_trackers = 0
    trackers_data = {}

    # Авторизовываемся в гугл-таблице
    agc = await agcm.authorize()
    ss = await agc.open_by_url(config.SPREADSHEET_URL)
    ss_2 = await agc.open_by_url(config.SPREADSHEET_URL_USERS)
    
    for key, value in config.SHEET_IDS.items():
        table = await ss.get_worksheet_by_id(value)
        table_data = await table.get_all_values()

        for row in table_data[1:]:
            row_dict = {}

            n = 0
            for cell in row:
                row_dict[config.SHEETS_COLUMNS[key][n]] = cell
                n += 1

            config.SHEETS_DATA[key].append(row_dict)
    
    async with aiofiles.open('config.json', 'w') as f:
        await f.write(json.dumps(config.SHEETS_DATA))

    """table_2 = await ss_2.get_worksheet_by_id(0)
    table_2_data = await table_2.get_all_values()

    for row in table_2_data[1:]:
        if row[0].lower() not in config.USERS_ACCESS:
            config.USERS_ACCESS[row[0].lower()] = row[1]"""
    
    table_3 = await ss_2.get_worksheet_by_id(423528932)
    table_3_data = await table_3.get_all_values()

    for row in table_3_data[1:]:
        trackers_data[row[1]] = row[0]

    """print(f'config.USERS_ACCESS (first time run): {config.USERS_ACCESS}')"""
    config.BOT_IS_READY = True
    threading.Thread(target=start_fast_api).start()
    #return

    while True:
        print('Новый цикл')
        try:
            table_2 = await ss_2.get_worksheet_by_id(0)
            table_2_data = await table_2.get_all_values()
        except Exception as e:
            print(f"Ошибка при обновлении: {e}")
            await asyncio.sleep(2)
            continue
        
        try:
            # Обновление юзеров
            db_data = db.get_all_user_access_data()
            added_emails = []
            deleted_by_time = [] # Удаленные почты по дате удаления

            for row in table_2_data[1:]:
                if row[0] is None or row[1] is None or row[2] is None or len(row[0]) == 0 or len(row[1]) == 0 or not is_int(row[1]) or len(row[2]) == 0:
                    continue

                row_data = {'mail': clean_string(row[0].lower().strip()), 'chat_id': int(row[1]), 'flow': row[2]}

                if len(row[3]) != 0: # Есть ли дата удаления
                    try:
                        delete_time = int(datetime.datetime.strptime(row[3], "%d.%m.%Y").timestamp())

                        if delete_time < time.time():
                            users_list = db.get_user_by_email(row_data['mail'])

                            for user_2 in users_list:
                                db.delete_homework_by_tg_id(user_2['tg_id'])

                            db.delete_email(row_data['mail'])
                            db.delete_user_by_email(row_data['mail'])
                            deleted_by_time.append(row_data['mail'])
                            await table_2.delete_rows(table_2_data.index(row) + 2 - len(deleted_by_time))
                    except Exception as e:
                        print(f"Ошибка при проверке даты: {e}")
                else:
                    try:
                        await table_2.batch_update([{'range': f'D{table_2_data.index(row) + 1 - len(deleted_by_time)}:D{table_2_data.index(row) + 1 - len(deleted_by_time)}', 'values': [[f"=ARRAYFORMULA(ПРОСМОТРX(C{table_2_data.index(row) + 1 - len(deleted_by_time)}; 'Даты удаления потоков'!A:A; 'Даты удаления потоков'!B:B; ""))"]]}], value_input_option='USER_ENTERED')
                    except Exception as e:
                        print(f"Ошибка при обновлении: {e}")
                        print(traceback.format_exc())

                if row_data not in db_data and row_data['mail'] not in added_emails:
                    is_user_in_db = db.is_email_in_users_access(row_data['mail'])

                    if is_user_in_db:
                        db.delete_email(row_data['mail'])

                    db.insert_email(row_data['mail'], int(row_data['chat_id']), row_data['flow'])
                    print(f'Добавлено: {row_data}', row_data['mail'] not in added_emails)

                    if clean_string(row[0].lower().strip()) in added_emails:
                        try:
                            await bot.send_message(config.LOG_CHAT_ID, f'@infinityqqqq Обнаружен дубль почты: {clean_string(row[0].lower().strip())}')
                        except:
                            pass

                    added_emails.append(clean_string(row[0].lower().strip()))
            
            if len(deleted_by_time) != 0:
                try:
                    await bot.send_message(config.LOG_CHAT_ID, f'Удалены пользователи по дате удаления: {", ".join(deleted_by_time)}')
                except Exception as e:
                    print(f'Ошибка при отправке сообщения: {e}')
                    pass

            deleted_emails = []
            emails_list = [clean_string(row[0].lower().strip()) for row in table_2_data[1:]]

            for user in db_data:
                if user['mail'].lower() not in emails_list:
                    users_list = db.get_user_by_email(user['mail'])

                    for user_2 in users_list:
                        db.delete_homework_by_tg_id(user_2['tg_id'])
                        db.delete_all_user_homework_text(user_2['tg_id'])

                    db.delete_email(user['mail'])
                    db.delete_user_by_email(user['mail'])
                    deleted_emails.append(user['mail'])
            
            if len(added_emails) != 0:
                print(f'Добавлены новые пользователи: {", ".join(added_emails)}')
                try:
                    await bot.send_message(config.LOG_CHAT_ID, f'Добавлены новые пользователи: {", ".join(added_emails)}')
                except:
                    pass

            if len(deleted_emails) != 0:
                print(f'Удалены пользователи: {", ".join(deleted_emails)}')
                try:
                    await bot.send_message(config.LOG_CHAT_ID, f'Удалены пользователи: {", ".join(deleted_emails)}')
                except:
                    pass

            await asyncio.sleep(2)

            # Обновление времени потоков (modules_access)
            table_2 = await ss_2.get_worksheet_by_id(632094276)
            table_2_data = await table_2.get_all_values()
            table_2_data_cleaned = []

            modules_access_data = db.get_modules_access()

            for row in table_2_data[1:]:
                if row[0] is None or row[1] is None or row[2] is None or row[3] is None or len(row[0]) == 0 or len(row[1]) == 0 or len(row[3]) == 0 or not is_int(row[1]) or len(row[2]) == 0 or len(row[2].split('.')) != 3 or not is_int(row[3]):
                    continue

                table_2_data_cleaned.append(row[:4])
                
                try:
                    row_data = {'flow': row[0], 'module_id': int(row[1]), 'time': int(datetime.datetime.strptime(row[2], "%d.%m.%Y").timestamp()), 'num': int(row[3])}
                except Exception as e:
                    print(f"Ошибка при проверке даты: {e}")
                    continue

                if row_data not in modules_access_data:
                    db.insert_modules_access(row_data['flow'], row_data['module_id'], row_data['time'], row_data['num'])
                    print(f'Добавлено: {row_data}')

            for module in modules_access_data:
                module_dict = [module['flow'], str(module['module_id']), datetime.datetime.fromtimestamp(module['time']).strftime("%d.%m.%Y"), str(module['num'])]
                if module_dict not in table_2_data_cleaned:
                    db.delete_modules_access(module['flow'], module['module_id'], module['time'], module['num'])
                    print(f'Удалено: {module}')

            await asyncio.sleep(2)

            # Обновление заданий
            for key, value in config.SHEET_IDS.items():
                await asyncio.sleep(1)
                try:
                    table = await ss.get_worksheet_by_id(value)
                    table_data = await table.get_all_values()
                except Exception as e:
                    print(f"Ошибка при обновлении: {e}")
                    await asyncio.sleep(2)
                    continue

                # Проверяем есть ли ячейка в переменной
                for row in table_data[1:]:
                    row_dict = {}

                    n = 0
                    for cell in row:
                        #print(f'cell: {cell} key: {config.SHEETS_COLUMNS[key][n]}')
                        row_dict[config.SHEETS_COLUMNS[key][n]] = cell
                        n += 1

                    if row_dict not in config.SHEETS_DATA[key]:
                        config.SHEETS_DATA[key].append(row_dict)
                        print(f'Добавлено: {row_dict}')

                rows_to_delete = []

                # Проверяем есть ли ячейка в гугл-таблице
                for row in config.SHEETS_DATA[key]:
                    if list(row.values()) not in table_data[1:]:
                        rows_to_delete.append(row)
                        print(f'Удалено: {list(row.values())}//{table_data[1:]}')
                
                for row in rows_to_delete:
                    config.SHEETS_DATA[key].remove(row)
                    print(f'Удалено: {row}')
            
            #print(f'config.SHEETS_DATA (clear cache): {config.SHEETS_DATA}')
            
            if time.time() > time_to_clear:
                print("Обновляю список ДЗ")
                msg = await bot.send_message(config.LOG_CHAT_ID, f'⚠️ Обновляю список ДЗ. До завершения обновления не будет работать обновление пользователей в гугл таблице')

                time_to_clear = time.time() + 1200
                homework_dict = {}

                try:
                    homework_data = db.get_all_homeworks()
                    homework_data.reverse()
                    print(f'Кол-во заданий: {len(homework_data)}')

                    for homework in homework_data:
                        homework_time_name = ".".join(reversed(homework['update_time'].split()[0].split('-')[:2]))

                        if homework_time_name not in homework_dict:
                            homework_dict[homework_time_name] = []

                        homework_dict[homework_time_name].append([homework['homework_id'], homework['user_data'], homework['lesson_id'], homework['status'], homework['comment'], homework['update_time'], homework['check_time'], trackers_data[str(homework['chat_id'])], homework['message_link']])

                    homework_worksheets = await ss.worksheets()
                    worksheet_names = [worksheet.title for worksheet in homework_worksheets]
                    worksheet_ids = {worksheet.title: worksheet.id for worksheet in homework_worksheets}

                    for homework_time in homework_dict:
                        if homework_time in worksheet_names:
                            ws_3 = await ss.get_worksheet_by_id(worksheet_ids[homework_time])
                        else:
                            ws_3 = await ss.add_worksheet(homework_time, 1, 1)

                        await ws_3.clear()
                        await ws_3.append_row(["ID", "Данные пользователя", "ID урока", "Статус", "Ответ", "Время обновления", "Время проверки", "Трекер", "Ссылка на сообщение"])
                        rs = await ws_3.append_rows(homework_dict[homework_time])
                        print(f'Результат: {rs}')

                    try:
                        await msg.delete()
                    except:
                        pass

                    print("Обновляю список вопросов психолога")

                    items_list = []
                    ws_4 = await ss.get_worksheet_by_id(config.PSYCHOLOGY_SHEET_ID)
                    await ws_4.clear()

                    psychologist_questions = db.get_psychologist_questions()
                    psychologist_questions.reverse()
                    print(f'Кол-во вопросов: {len(psychologist_questions)}')

                    for item in psychologist_questions:
                        items_list.append([item['user_data'], item['email'], item['question'], item['message_link'], item['time']])

                    rs = await ws_4.append_rows(items_list)
                    print(f'Результат: {rs}')
                except Exception as e:
                    print(f"Ошибка при обновлении: {e}")
                    print(traceback.format_exc())
                    await asyncio.sleep(2)
                    continue
            
            if time.time() > time_to_update_trackers:
                time_to_update_trackers = time.time() + 2200

                try:
                    table_3 = await ss_2.get_worksheet_by_id(423528932)
                    table_3_data = await table_3.get_all_values()

                    for row in table_3_data[1:]:
                        if row[1] not in trackers_data:
                            trackers_data[row[1]] = row[0]
                        
                        if row[1] in trackers_data and row[0] != trackers_data[row[1]]:
                            trackers_data[row[1]] = row[0]
                except Exception as e:
                    print(f"Ошибка при обновлении: {e}")
                    await asyncio.sleep(2)
                    continue

            await asyncio.sleep(2)
        except Exception as e:
            try:
                await bot.send_message(config.LOG_CHAT_ID, f'@infinityqqqq Произошла непридвиденная ошибка при обновлении таблиц, приостанавливаю обновление на 3 минуты: {e}')
            except:
                pass

            await asyncio.sleep(180)

async def on_startup():
    asyncio.create_task(check_info())

async def set_default_commands(bot):
    await bot.set_my_commands([
        types.BotCommand(command="start", description="Перезапустить бота"),
    ])

async def main() -> None:
    # Dispatcher is a root router
    storage = RedisStorage(redis=redis)
    dp = Dispatcher(storage=storage)
    # Register all the routers from handlers package
    dp.include_routers(
        start_router,
    )
    

    await set_default_commands(bot)
    
    await on_startup()

    # And the run events dispatching
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
