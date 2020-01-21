import os
from os.path import dirname
from dotenv import load_dotenv

load_dotenv()
BASE_DIR = dirname(dirname(dirname(os.path.abspath(__file__))))

DB_SETTING = {'DB_TYPE': os.getenv('DB_TYPE')}

if DB_SETTING['DB_TYPE'] == 'mysql':
    DB_SETTING.update({
        'NAME': 'data',
        'USER': os.getenv('DB_USER'),
        'PASSWORD': os.getenv('DB_PASS'),
        'HOST': os.getenv('DB_HOST'),
        'PORT': os.getenv('DB_PORT')
    })
else:
    DB_SETTING.update({
        'NAME': os.path.join(BASE_DIR, 'data.sqlite3'),
    })
