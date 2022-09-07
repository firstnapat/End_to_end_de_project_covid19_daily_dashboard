# import library
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import pandas as pd
import requests

# path to api
base_url = "https://covid19.ddc.moph.go.th/api/Cases/"

# path ที่จะใช้
caseall_output_path = "/home/airflow/gcs/data/caseall_cleaned.csv"
line_list_output_path = "/home/airflow/gcs/data/line_list_cleaned.csv"
by_province_output_path = "/home/airflow/gcs/data/by_province_cleaned.csv"

def get_caseall(caseall_path):
    caseall_url = "timeline-cases-all"
    response_cases_all = requests.get(base_url+caseall_url)
    covid_cases_all = response_cases_all.json()

    # แปลงเป็น dataframe
    covid_cases_all = pd.DataFrame(covid_cases_all)

    # ใช้ `pd.to_datetime()` convert columns 'txn_date' and 'update_date' to datetime.
    covid_cases_all[['txn_date','update_date']] = covid_cases_all[['txn_date','update_date']].apply(pd.to_datetime, format='%Y/%m/%d')

    # เซฟไฟล์ CSV
    covid_cases_all.to_csv(caseall_path, index=False)
    print(f"Output to {caseall_path}")


def get_line_list(line_list_path):
    line_list_url = "round-3-line-lists?page="
    response_line_list = requests.get(base_url+line_list_url)
    
    covid_line_list = response_line_list.json()
    last_page = covid_line_list['meta']['last_page']

    response_line_list_all = requests.get(base_url + line_list_url + str(last_page))
    covid_line_list = response_line_list_all.json()

    # แปลงเป็น dataframe
    covid_line_list = covid_line_list['data']
    covid_line_list = pd.DataFrame(covid_line_list)

    # ใช้ `pd.to_datetime()` convert columns 'txn_date' and 'update_date' to datetime.
    covid_line_list[['txn_date','update_date']] = covid_line_list[['txn_date','update_date']].apply(pd.to_datetime, format='%Y/%m/%d')

    # เติมช่องว่าง null ด้วยคำว่า `ไม่ระบุ`
    covid_line_list["age_number"].fillna("ไม่ระบุ", inplace = True)
    covid_line_list["nationality"].fillna("ไม่ระบุ", inplace = True)

    # drop job column
    covid_line_list = covid_line_list.drop('job',axis=1)

    # เซฟไฟล์ CSV
    covid_line_list.to_csv(line_list_path, index=False)
    print(f"Output to {line_list_path}")


def get_by_province(by_province_path):
    case_by_provinces = "timeline-cases-by-provinces"
    response_by_provinces = requests.get(base_url + case_by_provinces)
    covid_by_provinces = response_by_provinces.json()
    # แปลงเป็น dataframe
    covid_by_provinces = pd.DataFrame(covid_by_provinces)

    # ใช้ `pd.to_datetime()` convert columns 'txn_date' and 'update_date' to datetime.
    covid_by_provinces[['txn_date','update_date']] = covid_by_provinces[['txn_date','update_date']].apply(pd.to_datetime, format='%Y/%m/%d')

    # เซฟไฟล์ CSV
    covid_by_provinces.to_csv(by_province_path, index=False)
    print(f"Output to {by_province_path}")


with DAG(
    "covid19_daily_dag",
    start_date=days_ago(1),
    schedule_interval='0 9 * * *',
    tags=["firstdeproject"]
) as dag:

    dag.doc_md = """
    Covid19 daily 9.00am 
    """
    
    t1 = PythonOperator(
        task_id="get_caseall",
        python_callable=get_caseall,
        op_kwargs={
            "caseall_path": caseall_output_path,
        },
    )

    t2 = PythonOperator(
        task_id="get_line_list",
        python_callable=get_line_list,
        op_kwargs={
            "line_list_path": line_list_output_path,
        },
    )

    t3 = PythonOperator(
        task_id="get_by_province",
        python_callable=get_by_province,
        op_kwargs={
            "by_province_path" : by_province_output_path,
        },
    )


    t4 = BashOperator(
        task_id="load_to_bq_file1",
        bash_command="bq load --source_format=CSV --autodetect report.covid19_caseall gs://asia-southeast1-airflowcovi-b358f964-bucket/data/caseall_cleaned.csv"
    )

    t5 = BashOperator(
        task_id="load_to_bq_file2",
        bash_command="bq load --source_format=CSV --autodetect report.covid19_linelist gs://asia-southeast1-airflowcovi-b358f964-bucket/data/line_list_cleaned.csv"
    )

    t6 = BashOperator(
        task_id="load_to_bq_file3",
        bash_command="bq load --source_format=CSV --autodetect report.covid19_by_province gs://asia-southeast1-airflowcovi-b358f964-bucket/data/by_province_cleaned.csv"
    )

    [t1, t2, t3] >> t4 >> t5 >> t6
