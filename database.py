import mysql.connector
import config
import json
import time

class MySQL:

    def __init__(self):
        self.database = mysql.connector.connect(user=config.DATABASE_USER, password=config.DATABASE_PASSWORD, host=config.DATABASE_IP, database=config.DATABASE_NAME, autocommit=True)
        self.cursor = self.database.cursor(dictionary=True)
        self.cursor.execute("SET SESSION wait_timeout=31536000")
        self.database.commit()


    def get_user(self, tg_id):
        self.cursor.execute("SELECT * FROM users WHERE tg_id = %s", (tg_id,))
        return self.cursor.fetchall()
    
    def get_user_by_email(self, email):
        self.cursor.execute("SELECT * FROM users WHERE email = %s", (email,))
        return self.cursor.fetchall()
    
    def delete_homework_by_tg_id(self, tg_id):
        self.cursor.execute("DELETE FROM homework WHERE tg_id = %s", (tg_id,))
        self.database.commit()
    
    def delete_user_by_email(self, email):
        self.cursor.execute("DELETE FROM users WHERE email = %s", (email,))
        self.database.commit()
    
    def add_user(self, tg_id, email):
        self.cursor.execute("INSERT INTO users (tg_id, email) VALUES (%s,%s)", (tg_id, email))
        self.database.commit()

    """def get_modules(self):
        self.cursor.execute("SELECT * FROM modules")
        return self.cursor.fetchall()
    
    def get_module(self, id):
        self.cursor.execute("SELECT * FROM modules WHERE id = %s", (id,))
        return self.cursor.fetchone()
    
    def get_lessons(self, module_id):
        self.cursor.execute("SELECT * FROM lessons WHERE module_id = %s", (module_id,))
        return self.cursor.fetchall()
    
    def get_lesson(self, lesson_id):
        self.cursor.execute("SELECT * FROM lessons WHERE lesson_id = %s", (lesson_id,))
        return self.cursor.fetchone()"""
    
    def get_lesson(self, lesson_id, flow):
        for i in config.SHEETS_DATA["lessons"]:
            if i["lesson_id"] == lesson_id and flow in i["flow"].split(","):
                return i
            
        return None
    
    def get_lessons(self, module_id, flow):
        lessons = []

        for i in config.SHEETS_DATA["lessons"]:
            if i["module_id"] == module_id and flow in i["flow"].split(","):
                lessons.append(i)
        
        return sorted(lessons, key=lambda x: int(x["lesson_id"]))
    
    def get_module_name(self, lesson_id, flow):
        for i in config.SHEETS_DATA["lessons"]:
            if i["lesson_id"] == lesson_id and flow in i["flow"].split(","):
                return [self.get_module(i["module_id"]), i]
    
    def get_module(self, module_id, flow):
        for i in config.SHEETS_DATA["modules"]:
            if i["id"] == module_id and flow in i["flow"].split(","):
                return i
            
    def get_required_homework_ids(self, flow):
        homework_ids = []

        for i in config.SHEETS_DATA["required_tasks"]:
            if i["flow"] == flow:
                homework_ids = i["lesson_ids"].split(",")

        homework_ids_2 = {}

        for i in homework_ids:
            if len(i.split("_")) >= 2:
                for z in i.split("_"):
                    homework_ids_2[(int(z))] = {"analog": [int(s) for s in i.split("_")]}
            else:
                homework_ids_2[(int(i))] = {}

        return homework_ids_2
            
    def get_done_homework(self, tg_id):
        self.cursor.execute("SELECT * FROM homework WHERE tg_id = %s AND status = '✅' ORDER BY homework_id", (tg_id,))
        return self.cursor.fetchall()
    
    def get_done_homework_ids(self, tg_id):
        self.cursor.execute("SELECT lesson_id FROM homework WHERE tg_id = %s AND status = '✅'", (tg_id,))
        return [int(i["lesson_id"]) for i in self.cursor.fetchall()]
    
    def get_homework(self, homework_id):
        self.cursor.execute("SELECT * FROM homework WHERE homework_id = %s", (homework_id,))
        return self.cursor.fetchone()
    
    def get_all_homeworks(self):
        self.cursor.execute("SELECT * FROM homework ORDER BY homework_id DESC LIMIT 8000")
        return self.cursor.fetchall()
    
    def get_all_homeworks_2(self):
        self.cursor.execute("SELECT * FROM homework ORDER BY homework_id DESC LIMIT 200")
        return self.cursor.fetchall()
    
    def get_homework_by_lesson_id(self, tg_id, lesson_id):
        self.cursor.execute("SELECT * FROM homework WHERE tg_id = %s AND lesson_id = %s", (tg_id, lesson_id))
        return self.cursor.fetchall()
    
    def get_all_user_homeworks(self, tg_id):
        self.cursor.execute("SELECT * FROM homework WHERE tg_id = %s", (tg_id,))
        return self.cursor.fetchall()
    
    def add_homework(self, user_data, lesson_id, status, comment, update_time, message_link, check_time, message_id_1, message_id_2, tg_id, module_id, send_message_id, chat_id):
        self.cursor.execute("INSERT INTO homework (user_data, lesson_id, status, comment, update_time, message_link, check_time, message_id_1, message_id_2, tg_id, module_id, send_message_id, chat_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (user_data, lesson_id, status, comment, update_time, message_link, check_time, message_id_1, message_id_2, tg_id, module_id, send_message_id, chat_id))
        self.database.commit()

    def get_done_lessons_ids(self, tg_id, module_id):
        self.cursor.execute("SELECT lesson_id FROM homework WHERE tg_id = %s AND module_id = %s AND status = '✅'", (tg_id, module_id))
        return [i["lesson_id"] for i in self.cursor.fetchall()]

    def change_homework_send_message_id(self, homework_id, send_message_id):
        self.cursor.execute("UPDATE homework SET send_message_id = %s WHERE homework_id = %s", (send_message_id, homework_id))
        self.database.commit()
    
    def get_check_lessons_ids(self, tg_id, module_id):
        self.cursor.execute("SELECT lesson_id FROM homework WHERE tg_id = %s AND module_id = %s AND status = 'На проверке'", (tg_id, module_id))
        return [i["lesson_id"] for i in self.cursor.fetchall()]
    
    def get_rework_lessons_ids(self, tg_id, module_id):
        self.cursor.execute("SELECT lesson_id FROM homework WHERE tg_id = %s AND module_id = %s AND status = '❌'", (tg_id, module_id))
        return [i["lesson_id"] for i in self.cursor.fetchall()]
    
    def get_sent_lessons_ids(self, tg_id, module_id):
        self.cursor.execute("SELECT lesson_id FROM homework WHERE tg_id = %s AND module_id = %s AND status = '⏳'", (tg_id, module_id))
        return [i["lesson_id"] for i in self.cursor.fetchall()]

    def edit_homework(self, homework_id, user_data=None, lesson_id=None, status=None, comment=None, update_time=None, message_link=None, check_time=None, message_id_1=None, message_id_2=None, tg_id=None, send_message_id=None):
        print(f'edit_homework: {homework_id}, {user_data}, {lesson_id}, {status}, {comment}, {update_time}, {message_link}, {check_time}, {message_id_1}, {message_id_2}, {tg_id}, {send_message_id}')
        query = "UPDATE homework SET"
        data = []

        if user_data is not None:
            query += " user_data = %s,"
            data.append(user_data)
        if lesson_id is not None:
            query += " lesson_id = %s,"
            data.append(lesson_id)
        if status is not None:
            query += " status = %s,"
            data.append(status)
        if comment is not None:
            query += " comment = %s,"
            data.append(comment)
        if update_time is not None:
            query += " update_time = %s,"
            data.append(update_time)
        if message_link is not None:
            query += " message_link = %s,"
            data.append(message_link)
        if check_time is not None:
            query += " check_time = %s,"
            data.append(check_time)
        if message_id_1 is not None:
            query += " message_id_1 = %s,"
            data.append(message_id_1)
        if message_id_2 is not None:
            query += " message_id_2 = %s,"
            data.append(message_id_2)
        if tg_id is not None:
            query += " tg_id = %s,"
            data.append(tg_id)
        if send_message_id is not None:
            query += " send_message_id = %s,"
            data.append(send_message_id)

        query = query.rstrip(',')
        query += " WHERE homework_id = %s"
        data.append(homework_id)

        self.cursor.execute(query, data)
        self.database.commit()

    def get_homework_by_message_ids(self, message_id, chat_id):
        self.cursor.execute("SELECT * FROM homework WHERE (message_id_1 = %s OR message_id_2 = %s) AND chat_id = %s", (message_id, message_id, chat_id))
        return self.cursor.fetchall()
    
    def get_modules(self, flow):
        return sorted([i for i in config.SHEETS_DATA["modules"] if flow in i["flow"].split(",")], key=lambda x: int(x["id"]))
    
    def add_update_data(self, data):
        self.cursor.execute("INSERT INTO update_data (data) VALUES (%s)", (json.dumps(data),))
        self.database.commit()

    def get_update_data(self):
        self.cursor.execute("SELECT * FROM update_data")
        return self.cursor.fetchall()

    def delete_update_data(self, id):
        self.cursor.execute("DELETE FROM update_data WHERE id = %s", (id,))
        self.database.commit()
    
    def get_chat_id(self, mail):
        self.cursor.execute("SELECT chat_id FROM users_access WHERE mail = %s", (mail,))
        return self.cursor.fetchall()[0]["chat_id"]
    
    def is_email_in_users_access(self, email):
        self.cursor.execute("SELECT * FROM users_access WHERE mail = %s", (email,))
        result = self.cursor.fetchall()

        return True if len(result) > 0 else False
    
    def get_flow_by_email(self, email):
        self.cursor.execute("SELECT flow FROM users_access WHERE mail = %s", (email,))
        return self.cursor.fetchall()[0]["flow"]
    
    def get_chat_ids(self):
        self.cursor.execute("SELECT DISTINCT chat_id FROM users_access")
        return [i["chat_id"] for i in self.cursor.fetchall()]
    
    def get_all_user_access_data(self):
        self.cursor.execute("SELECT * FROM users_access")
        return self.cursor.fetchall()
    
    def delete_email(self, email):
        self.cursor.execute("DELETE FROM users_access WHERE mail = %s", (email,))
        self.database.commit()

    def insert_email(self, email, chat_id, flow):
        self.cursor.execute("INSERT INTO users_access (mail, chat_id, flow) VALUES (%s, %s, %s)", (email, chat_id, flow))
        self.database.commit()

    def delete_homework_by_homework_id(self, homework_id):
        self.cursor.execute("DELETE FROM homework WHERE homework_id = %s", (homework_id,))
        self.database.commit()

    def get_module_access(self, flow, module_id, num):
        self.cursor.execute("SELECT time FROM modules_access WHERE flow = %s AND module_id = %s AND num = %s", (flow, module_id, num))
        return self.cursor.fetchall()
    
    def get_module_access_2(self, flow, module_id):
        self.cursor.execute("SELECT time FROM modules_access WHERE flow = %s AND module_id = %s", (flow, module_id))
        return self.cursor.fetchall()
    
    def get_module_access_3(self, flow, time): # TODO Time временно убран
        self.cursor.execute("SELECT * FROM modules_access WHERE flow = %s", (flow,))
        return self.cursor.fetchall()
    
    def get_modules_access(self):
        self.cursor.execute("SELECT * FROM modules_access")
        return self.cursor.fetchall()
    
    def insert_modules_access(self, flow, module_id, time, num):
        self.cursor.execute("INSERT INTO modules_access (flow, module_id, time, num) VALUES (%s, %s, %s, %s)", (flow, module_id, time, num))
        self.database.commit()

    def delete_modules_access(self, flow, module_id, time, num):
        self.cursor.execute("DELETE FROM modules_access WHERE flow = %s AND module_id = %s AND time = %s AND num = %s", (flow, module_id, time, num))
        self.database.commit()

    def get_psychologist_questions(self):
        self.cursor.execute("SELECT * FROM psychologist_questions")
        return self.cursor.fetchall()
    
    def insert_psychologist_question(self, user_data, email, question, message_link, time):
        self.cursor.execute("INSERT INTO psychologist_questions (user_data, email, question, message_link, time) VALUES (%s, %s, %s, %s, %s)", (user_data, email, question, message_link, time))
        self.database.commit()

    def add_homework_text(self, tg_id, lesson_id, time, text):
        self.cursor.execute("INSERT INTO homework_text (tg_id, lesson_id, time, text) VALUES (%s, %s, %s, %s)", (tg_id, lesson_id, time, text))
        self.database.commit()

    def get_homework_text_data(self, tg_id, lesson_id):
        self.cursor.execute("SELECT * FROM homework_text WHERE tg_id = %s AND lesson_id = %s ORDER BY time DESC", (tg_id, lesson_id))
        return self.cursor.fetchall()
    
    def get_homework_text_data_2(self, tg_id, lesson_id, time):
        self.cursor.execute("SELECT * FROM homework_text WHERE tg_id = %s AND lesson_id = %s AND time = %s", (tg_id, lesson_id, time))
        return self.cursor.fetchall()

    def delete_all_user_homework_text(self, tg_id):
        self.cursor.execute("DELETE FROM homework_text WHERE tg_id = %s", (tg_id,))
        self.database.commit()
    
    def get_homework_by_msg_id_2_and_chat_id(self, message_id_2, chat_id):
        self.cursor.execute("SELECT * FROM homework WHERE message_id_2 = %s AND chat_id = %s", (message_id_2, chat_id))
        return self.cursor.fetchall()
    
    def add_email_to_added_api_users(self, email):
        self.cursor.execute("INSERT INTO added_api_users (email) VALUES (%s)", (email,))
        self.database.commit()

    def is_email_in_added_api_users(self, email):
        self.cursor.execute("SELECT * FROM added_api_users WHERE email = %s", (email,))
        result = self.cursor.fetchall()

        return True if len(result) > 0 else False