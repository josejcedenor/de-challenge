"""
etl_job.py
Created by: Jose J. Cedeño R.
Mail: josejcedenor@gmail.com 
Location: Santiago, Chile
Date: 2020-01-06
"""

import pandas as pd
import os
from datetime import datetime
from pandasql import sqldf
import logging

def extract(datalake_folder, date_folder, job_time):
    logging.debug('Starting extraction task')
    print(f'[{datetime.now()}] Starting extraction task')
    task_id = f'extraction_{job_time}'
    task_path = os.path.join(datalake_folder, 'RAW', date_folder, task_id)
    games_filename = f'games_{job_time}.csv'
    consoles_filename = f'consoles_{job_time}.csv'
    games_outpath = os.path.join(task_path, games_filename)
    consoles_outpath = os.path.join(task_path, consoles_filename)
    data_folder = 'data'
    games_filepath = os.path.join(data_folder, 'result.csv')
    consoles_filepath = os.path.join(data_folder, 'consoles.csv')
    try:     
        games_df = pd.read_csv(games_filepath)
        consoles_df = pd.read_csv(consoles_filepath)
        os.makedirs(task_path, exist_ok = True)
        games_df.to_csv(games_outpath, index=False)
        consoles_df.to_csv(consoles_outpath, index=False)
        status = 200
        logging.debug('Extraction task completed')
        print(f'[{datetime.now()}] Extraction task completed')
    except:
        logging.debug('Extraction task error')
        print(f'[{datetime.now()}] Extraction task error')
        status = 400
    finally:
        return {'status': status, 'games_outpath': games_outpath, 'consoles_outpath': consoles_outpath}

def isFloat(x):
    try:
        float(x)
        return x
    except:
        return '-1'

def transform(games_filepath, consoles_filepath): 
    logging.debug('Starting transform task')
    print(f'[{datetime.now()}] Starting transform task')
    try:
        games_df = pd.read_csv(games_filepath)
        games_df = games_df.dropna(how='all')
        games_df['console'] = games_df['console'].apply(lambda x: x.strip())
        games_df = games_df.drop_duplicates()
        games_df['userscore'] = games_df['userscore'].apply(isFloat)

        consoles_df = pd.read_csv(consoles_filepath)
        consoles_df = consoles_df.dropna(how='all')
        consoles_df['console'] = consoles_df['console'].apply(lambda x: x.strip())
        consoles_df = consoles_df.drop_duplicates()
        
        query = """
            select 
                 row_number() over (order by metascore desc, userscore desc, name asc, g.console asc, company asc) as rank_general
                ,row_number() over (partition by c.company order by metascore desc, userscore desc, name asc) as rank_company
                ,row_number() over (partition by g.console order by metascore desc, userscore desc, name asc) as rank_console
                ,g.* 
                ,company
            from games_df g
            left join consoles_df c
            on lower(trim(g.console)) = lower(trim(c.console))
            order by rank_general asc, userscore desc, name asc, rank_console asc, rank_company asc, g.console asc, company asc
        """
        transform_df = sqldf(query, locals())
        transform_df['userscore'] = transform_df['userscore'].replace('-1', 'tbd')
        status = 200
        logging.debug('Transform task completed')
        print(f'[{datetime.now()}] Transform task completed')
    except:
        logging.debug('Transform task error')
        print(f'[{datetime.now()}] Transform task error')
        status = 400
        transform_df= ''
    finally:
        return {'status': status, 'transform_df': transform_df}

def load(datalake_folder, date_folder, job_time, transform_df):
    logging.debug('Starting load task')
    print(f'[{datetime.now()}] Starting load task')
    task_id = f'transform_{job_time}'
    task_path = os.path.join(datalake_folder, 'Transformed', date_folder, task_id)
    transform_filename = f'games_ranking_{job_time}.csv'
    transform_outpath = os.path.join(task_path, transform_filename)
    try:     
        os.makedirs(task_path, exist_ok = True)
        transform_df.to_csv(transform_outpath, index=False)
        status = 200
        logging.debug('Load task completed')
        print(f'[{datetime.now()}] Load task completed')
    except:
        logging.debug('Load task error')
        print(f'[{datetime.now()}] Load task error')
        status = 400
    finally:
        return {'status': status, 'transform_df': transform_df}

def write_html(title, file, table):
    with open (file, 'a') as f:
        f.write(f'<h1>{title}</h1>')
        f.write(table)

def report(transform_df):
    logging.debug('Starting report task')
    print(f'[{datetime.now()}] Starting report task')
    try:
        report_folder = 'report'
        os.makedirs(report_folder, exist_ok = True)
        report_file = 'ranking_report.html'
        report_filepath = os.path.join(report_folder, report_file)
        if os.path.exists(report_filepath):
            os.remove(report_filepath)
        general_df = transform_df[['rank_general', 'metascore', 'userscore', 'name', 'console', 'company', 'date']]
        top_best_general_df = general_df.sort_values(by='rank_general', ascending=True)[:10]
        title = 'Top 10 Best Rank General'
        print(title)
        print(top_best_general_df)
        write_html(title, report_filepath, top_best_general_df.to_html(index=None))
        top_worst_general_df = general_df.sort_values(by='rank_general', ascending=False)[:10]
        title = 'Top 10 Worst Rank General'
        print(title)
        print(top_worst_general_df)
        write_html(title, report_filepath, top_worst_general_df.to_html(index=None))
        for company in transform_df['company'].unique():
            company_df = transform_df[['rank_company', 'metascore', 'userscore', 'name', 'console', 'company', 'date']]
            company_df = company_df[company_df['company'] == company]
            top_best_company_df = company_df.sort_values(by='rank_company', ascending=True)[:10]
            title = f'Top 10 Best Rank Company: {company}'
            print(title)
            print(top_best_company_df)
            write_html(title, report_filepath, top_best_company_df.to_html(index=None))
            top_worst_company_df = company_df.sort_values(by='rank_company', ascending=False)[:10]
            title = f'Top 10 Worst Rank Company: {company}'
            print(title)
            print(top_worst_company_df)
            write_html(title, report_filepath, top_worst_company_df.to_html(index=None))
        for console in transform_df['console'].unique():
            console_df = transform_df[['rank_console', 'metascore', 'userscore', 'name', 'console', 'company', 'date']]
            console_df = console_df[console_df['console'] == console]
            top_best_console_df = console_df.sort_values(by='rank_console', ascending=True)[:10]
            title = f'Top 10 Best Rank Console: {console}'
            print(title)
            print(top_best_console_df)
            write_html(title, report_filepath, top_best_console_df.to_html(index=None))
            top_worst_console_df = console_df.sort_values(by='rank_console', ascending=False)[:10]
            title = f'Top 10 Worst Rank Console: {console}'
            print(title)
            print(top_worst_console_df)
            write_html(title, report_filepath, top_worst_console_df.to_html(index=None))
        status = 200
        logging.debug('Report task completed')
        print(f'[{datetime.now()}] Report task completed')
    except:
        logging.debug('Report task error')
        print(f'[{datetime.now()}] Report task error')
        status = 400
    return {'status': status}

def main():
    datalake_folder = 'datalake'
    execution_time = datetime.now()
    date_folder = execution_time.strftime('%Y-%m-%d')
    job_time = execution_time.strftime('%y%m%d%H%M%S')
    logs_path = os.path.join('logs', date_folder)
    os.makedirs(logs_path, exist_ok = True)
    logs_filename = f'job_{job_time}.log'
    logs_outpath = os.path.join(logs_path, logs_filename)
    logging.basicConfig(filename=logs_outpath, format='[%(asctime)s][%(levelname)s] %(message)s', datefmt='%Y/%m/%d %H:%M:%S', level=logging.DEBUG)
    logging.debug('Job initialized')
    print(f'[{datetime.now()}] Job initialized')
    status_dic = extract(datalake_folder, date_folder, job_time)
    if status_dic.get('status') == 200: 
        status_dic = transform(status_dic.get('games_outpath'), status_dic.get('consoles_outpath') )
    if status_dic.get('status') == 200: 
        status_dic = load(datalake_folder, date_folder, job_time, status_dic.get('transform_df'))
    if status_dic.get('status') == 200: 
        status_dic = report(status_dic.get('transform_df'))
    if status_dic.get('status') == 200: 
        logging.debug('Job executed correctly')
        print(f'[{datetime.now()}] Job executed correctly')
    else:
        logging.debug('Job executed with errors')
        print(f'[{datetime.now()}] Job executed with errors')

if __name__ == "__main__":
    main()