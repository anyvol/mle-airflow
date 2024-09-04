# plugins/steps/messages.py
from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    hook = TelegramHook(telegram_conn_id='test',
                        token='7185610310:AAGYs_Z66PavbY5wni6C_Kp1W7E5v72eaOE',
                        chat_id='4592993771')
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
                        token='7185610310:AAGYs_Z66PavbY5wni6C_Kp1W7E5v72eaOE',
                        chat_id='4592993771')
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло неудачно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '4592993771',
        'text': message
    }) # отправление сообщения     