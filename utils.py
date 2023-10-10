import os

import psycopg2 as psc
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import dotenv


def connect_or_create_db(db_name=os.getenv('DATABASE')):
    """Connects to database if it exists, otherwise creates and connects to it"""
    dotenv.load_dotenv()
    print('connecting to database in progress...')
    try:
        conn = psc.connect(host=os.getenv('HOST'),
                           user=os.getenv('USER'),
                           password=os.getenv('PASSWORD'))
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute(f'CREATE DATABASE {db_name};')
        conn.commit()
        cur.close()
        conn.close()
        print(f'created {db_name} database')
    except psc.errors.DuplicateDatabase:
        pass
    finally:
        conn = psc.connect(host=os.getenv('HOST'),
                           user=os.getenv('USER'),
                           password=os.getenv('PASSWORD'),
                           database=db_name)
        cur = conn.cursor()
        print(f'connected to {db_name} database')
        return conn, cur


def create_tables(conn, cur):
    """Creates empty 'vacancies' and 'employers' tables"""
    cur.execute('DROP TABLE IF EXISTS vacancies;')
    cur.execute('DROP TABLE IF EXISTS employers;')
    cur.execute('CREATE TABLE employers('
                'employer_id int primary key,'
                'employer_name varchar(100),'
                'hh_url varchar(100),'
                'address_city varchar(100),'
                'address_street varchar(100),'
                'address_building varchar(100)'
                ');')
    cur.execute('CREATE TABLE vacancies ('
                'vacancy_id int PRIMARY KEY,'
                'job_title varchar(100),'
                'url varchar(100),'
                'area varchar(100),'
                'salary_from int,'
                'salary_to int,'
                'currency varchar(10),'
                'requirements text,'
                'published date,'
                'employment varchar(30),'
                'experience varchar(50),'
                'employer_id int  references employers(employer_id) on delete cascade'
                ');')
    conn.commit()


def load_db_tables(cur, vacancies):
    """Inserts information into database tables"""
    for vac in vacancies:
        cur.execute(f'INSERT INTO employers values(%s,%s,%s,%s,%s,%s) on conflict do nothing;',
                    list(vac.__dict__.values())[11:])

        cur.execute('INSERT INTO vacancies '
                    'values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);',
                    list(vac.__dict__.values())[:12])