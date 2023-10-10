import requests
import json


class HeadHunterAPI:
    """Class for working with HeadHunter API"""
    def get_vacancies(self, company):
        params = {
            'text': f'COMPANY_NAME:{company}',
            'per_page': 50,
            'only_with_salary': True,
            'area': 113
        }
        req = requests.get('https://api.hh.ru/vacancies', params)
        jsObj = json.loads(req.content.decode())
        req.close()
        return jsObj


class Vacancy:
    """Class for standardizing vacancies"""
    def __init__(self, dictionary):
        for key, value in dictionary.items():
            setattr(self, key, value)


class Collector:
    """Class for storing instances of Vacancy class"""
    def __init__(self):
        self.vacancies = []

    def add_hh_vacancies(self, search_query: str):
        vacancies_hh = HeadHunterAPI().get_vacancies(search_query)
        for vacancy_hh in vacancies_hh['items']:
            try:
                dic = {
                    'vacancy_id': int(vacancy_hh['id']),
                    'name': vacancy_hh['name'],
                    'vacancy_url': vacancy_hh['alternate_url'],
                    'area': vacancy_hh['area']['name'],
                    'salary_from': vacancy_hh['salary']['from'] if vacancy_hh['salary']['from'] else None,
                    'salary_to': vacancy_hh['salary']['to'] if vacancy_hh['salary']['to'] else None,
                    'currency': vacancy_hh['salary']['currency'],
                    'requirements': vacancy_hh['snippet']['requirement'],
                    'published': vacancy_hh['published_at'],
                    'employment_type': vacancy_hh['schedule']['name'],
                    'experience': vacancy_hh['experience']['name'],
                    'employer_id': int(vacancy_hh['employer']['id']),
                    'employer': vacancy_hh['employer']['name'],
                    'employer_url': vacancy_hh['employer']['alternate_url'],
                    'employer_address_city': vacancy_hh['address']['city'] if vacancy_hh['address'] else None,
                    'employer_address_street': vacancy_hh['address']['street'] if vacancy_hh['address'] else None,
                    'employer_address_building': vacancy_hh['address']['building'] if vacancy_hh['address'] else None

                }
            except KeyError:
                continue
            vacancy = Vacancy(dic)
            self.vacancies.append(vacancy)

    def delete_vacancy(self, vacancy):
        try:
            self.vacancies.remove(vacancy)
        except ValueError:
            print('Vacancy does not exist')


class DBManager:
    """Class for working with a database using SQL queries"""
    def __init__(self, conn, cur):
        self.conn = conn
        self.cur = cur

    def get_companies_and_vacancies_count(self):
        """Returns a list of companies and their respective vacancy count"""
        self.cur.execute('''select employer_name, count(*)
                            from vacancies v
                            left join employers e using(employer_id)
                            group by employer_name
                            order by count(*) desc
                            ''')
        return self.cur.fetchall()

    def get_all_vacancies(self):
        """Returns a list of all vacancies"""
        self.cur.execute('''select employer_name, job_title, salary_from, salary_to, currency, url
                            from vacancies v
                            left join employers e using(employer_id)
                            order by employer_name   
                            ''')
        return self.cur.fetchall()

    def get_avg_salary(self):
        """Returns an integer - average salary among all vacancies"""
        self.cur.execute('''select round(avg(coalesce (salary_from,salary_to,average)))
                            from vacancies v
                            left join
                            (select vacancy_id,(salary_from + salary_to)/2 as average
                            from vacancies) a using(vacancy_id)
                            ''')
        print(f'Average salary is {int(*self.cur.fetchall()[0])} RUR among all vacancies')
        return int(*self.cur.fetchall()[0])

    def get_vacancies_with_higher_salary(self):
        """Returns a list of vacancies with above average salaries"""
        self.cur.execute('''select *
                            from vacancies 
                            where vacancy_id in
                            (
                                select v.vacancy_id
                                from vacancies v
                                left join
                                (select vacancy_id,(salary_from + salary_to)/2 as average
                                from vacancies) a using(vacancy_id)
                                where coalesce (salary_from,salary_to,average) > 
                                                        (
                                                        select avg(coalesce (salary_from,salary_to,average))
                                                        from vacancies v
                                                        left join
                                                        (select vacancy_id,(salary_from + salary_to)/2 as average
                                                        from vacancies) a using(vacancy_id)
                                                        )
                            )
                            ''')
        return self.cur.fetchall()

    def get_vacancies_with_keyword(self, words: list):
        """Returns a list of vacancies that contain at least one keyword in 'words' argument"""
        words = [f'%{word}%' for word in words]
        self.cur.execute(f'''select *
                            from vacancies v
                            join employers e using(employer_id)
                            where 
                            v::text ilike any (array{words}) or
                            e::text ilike any (array{words});
                            ''')
        return self.cur.fetchall()
