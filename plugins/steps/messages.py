# plugins/steps/messages.py
import os
from dotenv import load_dotenv
from airflow.providers.telegram.hooks.telegram import TelegramHook

load_dotenv() # подгружаем .env

telegram_token = os.environ.get('TG_TOKEN')
tg_chat_id = os.environ.get('TG_CHAT_ID')

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    hook = TelegramHook(telegram_conn_id='test',
                        token=telegram_token,
                        chat_id=tg_chat_id)
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '4592993771',
        'text': message
    }) # отправление сообщения 

def send_telegram_failure_message(context):
	# ваш код здесь #
    run_id = context['run_id']
    ti = context['task_instance_key_str']
    hook = TelegramHook(telegram_conn_id='test',
                        token=telegram_token,
                        chat_id=tg_chat_id)
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло неудачно!' # определение текста сообщения
    hook.send_message({
        'chat_id': tg_chat_id,
        'text': message
    }) # отправление сообщения    