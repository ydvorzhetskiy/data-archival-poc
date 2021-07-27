from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator

project_id = 'sabre-cdw-dev-sandbox'
temp_ds = 'data_archival'
all_settings = [{
    'application': 'Ticketing',
    'dataset_name': 'ticket_as',
    'table_name': 'ticket_airline',
    'bq_retention_days': 1110,
    'gsc_retention_days': 690,
    'schedule': None,
    'partition_column': 'eda_load_datetime',
    'gsc_archival_path': 'gs://archiveddata_ticketing/core/ticketing/ticket_as/processeddata/',
    'standard': 0,
    'archival': 690,
    'near_line': 0,
    'to_be_archived': 'Y'
}, {
    'application': 'Beam Example',
    'dataset_name': 'beam_basics',
    'table_name': 'archival_test',
    'bq_retention_days': 2,
    'gsc_retention_days': 690,
    'schedule': None,
    'partition_column': 'load_timetsamp',
    'gsc_archival_path': 'gs://bean_basics_data_archival/',
    'standard': 0,
    'archival': 690,
    'near_line': 0,
    'to_be_archived': 'Y'
}, {
    'application': 'Ticketing',
    'dataset_name': 'ticket_as',
    'table_name': 'ticket_airline_error',
    'bq_retention_days': 7,
    'gsc_retention_days': None,
    'schedule': None,
    'partition_column': 'eda_load_datetime',
    'gsc_archival_path': None,
    'standard': None,
    'archival': None,
    'near_line': None,
    'to_be_archived': 'N'
}]


def create_dag(start_date, settings) -> DAG:
    dag_id = 'archive_table__' + settings['table_name']
    temp_table = f"{settings['dataset_name']}__{settings['table_name']}"
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }

    dag = DAG(
        dag_id=dag_id,
        start_date=start_date,
        default_args=default_args,
        description='data archival dag',
        schedule_interval=None,
        dagrun_timeout=timedelta(minutes=20))

    with dag:
        start = BashOperator(
            task_id='echo',
            bash_command=f"echo {settings['table_name']}"
        )

        delete_temp_table = BigQueryDeleteTableOperator(
            task_id="delete_temp_table",
            deletion_dataset_table=f'{project_id}.{temp_ds}.{temp_table}',
            ignore_if_missing=True
        )

        def get_schema_and_create_new_table():
            hook = BigQueryHook(bigquery_conn_id='bigquery_default')
            schema = hook.get_schema(
                project_id=project_id,
                dataset_id=settings['dataset_name'],
                table_id=settings['table_name'])
            hook.create_empty_table(
                project_id=project_id,
                dataset_id=temp_ds,
                table_id=temp_table,
                schema_fields=schema['fields'])

        create_temp_table = PythonOperator(
            task_id='get_archived_table_schema',
            python_callable=get_schema_and_create_new_table
        )

        def data_migration_dag():
            sub_dag = DAG(
                dag_id=f'{dag_id}.migration_sub_dag',
                default_args=default_args,
                schedule_interval=None,
                dagrun_timeout=timedelta(minutes=20),
            )

            sub_dag_start = DummyOperator(task_id='start_sub_dag')

            return sub_dag

        migration_sub_dag = SubDagOperator(
            task_id='migration_sub_dag',
            subdag=data_migration_dag(),
            default_args=default_args
        )

        start >> delete_temp_table >> create_temp_table >> migration_sub_dag
    return dag


for archival_settings in all_settings:
    # TODO: remove start date
    if archival_settings['to_be_archived'] == 'Y':
        created_dag = create_dag(datetime(2021, 7, 25), archival_settings)
        globals()[created_dag.dag_id] = created_dag

# create_temporary_
#
# read_from_bq = PythonOperator(
#     task_id='run_for_airshopping',
#     python_callable=archive_data,
#     dag=dag,
#     start_date=datetime.today())

# ..................


# logClient = google.cloud.logging.Client()
# log_handler = logClient.get_default_handler()
# logClient.setup_logging()
# logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
#                     level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
# logger = logging.getLogger('CDM_BQ_Dataset_Update_Checker')
# logger_logexp = logClient.logger('CDM_BQ_Dataset_Update_Checker')
# logger.setLevel(logging.INFO)
# logger.addHandler(log_handler)
#
# credentials = GoogleCredentials.get_application_default()
# bigquery = googleapiclient.discovery.build('bigquery', 'v2', credentials=credentials)
# project_id = 'sabre-cdw-dev-sandbox'
# gce_zone = 'us-central1'
# LOCAL_TZ = pendulum.timezone("America/Chicago")
# LOCATION = 'us-central1'
# dag_start_date = (datetime.today() - timedelta(1)
#                   ).astimezone(LOCAL_TZ).replace(hour=0, minute=0, second=0)
# DATASET_NAME = 'beam_example'
# BIGQUERY_DATASET_UPDATE_THRESHOLD = 5
#
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email': ['artem.bazarnyi@dxc.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     "dataset_default_options": {
#         "project": project_id,
#         "zone": gce_zone,
#     },
# }
#
# dag = DAG(dag_id='dataset_update_check_operator_dag',
#           default_args=default_args,
#           description='CDM dataset update checker DAG',
#           schedule_interval='*/5 * * * *',
#           start_date=dag_start_date,
#           )
#
# def dataset_update_checker():
#     Datasets = bigquery.datasets().list(projectId=project_id).execute()
#
#     bq_dataset = None
#     for dataset in Datasets['datasets']:
#         dataset_reference = dataset['datasetReference']
#         logger.info(f"Dataset name: {dataset_reference['datasetId']}")
#         if dataset_reference['datasetId'] == DATASET_NAME:
#             bq_dataset = dataset
#             logger.info(f"Dataset : {bq_dataset}")
#             bq_dataset_full = bigquery.datasets().get(projectId=project_id, datasetId=DATASET_NAME).execute()
#             logger.info(bq_dataset_full)
#             time_diff = (datetime.today() - datetime.fromtimestamp(int(bq_dataset_full['lastModifiedTime']) / 1000.0)).total_seconds() / 60.0
#             logger.info(f"Dataset Id {bq_dataset_full['id']} || Time since dataset's last update: {time_diff} minutes")
#             if time_diff > BIGQUERY_DATASET_UPDATE_THRESHOLD:
#                 #logger.error(f"Dataset Id {bq_dataset_full['id']} || Dataset was not updated for more than {BIGQUERY_DATASET_UPDATE_THRESHOLD} minutes")
#                 logger_logexp.log_struct({"message": f"Dataset was not updated for more than {BIGQUERY_DATASET_UPDATE_THRESHOLD} minutes", "dataset_id": DATASET_NAME, "project_id": project_id}, severity='ERROR')
#             break
#
#     if bq_dataset is None:
#         #logger.info(f"Dataset Id {DATASET_NAME} || Dataset was not found in the project {project_id}")
#         logger_logexp.log_struct({"message": f"Dataset was not found in the project {project_id}", "dataset_id": DATASET_NAME, "project_id": project_id}, severity='INFO')
#
# bq_update_check_task = PythonOperator(
#     task_id='run_for_airshopping',
#     python_callable=dataset_update_checker,
#     dag=dag
# )
#
