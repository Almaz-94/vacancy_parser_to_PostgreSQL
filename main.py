from classes import Collector, DBManager
from utils import connect_or_create_db, create_tables, load_db_tables


company_names = ['LC Group', 'IBS', 'Rover', 'R-Style', 'Rosco', 'Марвел', 'Verysell', 'Yandex', 'Газпром', "Тинькофф"]


if __name__ == '__main__':
    conn, cur = connect_or_create_db('hh_vacancies')
    create_tables(conn, cur)

    collector = Collector()
    for company in company_names:
        collector.add_hh_vacancies(company)

    load_db_tables(cur, collector.vacancies)
    conn.commit()

    db_manager = DBManager(conn, cur)
    # print(db_manager.get_companies_and_vacancies_count())
    # print(db_manager.get_all_vacancies())
    # print(db_manager.get_avg_salary())
    # print(db_manager.get_vacancies_with_higher_salary())
    print(db_manager.get_vacancies_with_keyword(['инженер', 'редактор']))
    cur.close()
    conn.close()
