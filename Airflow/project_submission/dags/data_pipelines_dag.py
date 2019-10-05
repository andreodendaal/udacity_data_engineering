from datetime import datetime, timedelta 
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from helpers import sql_statements
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.base_hook import BaseHook

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_retry': False,    
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}
    

dag = DAG('data_pipelines_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False          
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
 
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    #python_callable=list_keys,
    dag=dag,
    redshift_conn_id='redshift',
    s3_conn_id='aws_credentials',
    table='staging_events',
    region='us-west-2',
    s3_path='log_data',
    s3_bucket='udacity-dend',   
    )
 
 
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    s3_conn_id='aws_credentials', 
    table='staging_songs',
    region='us-west-2',
    s3_path='song_data',
    s3_bucket='udacity-dend',    
   )
 

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table', 
    dag=dag,
    redshift_conn_id='redshift',  
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    dimension_name = 'user_table',    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    dimension_name = 'song_table', 
    
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    dimension_name = 'artist_table', 
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    dimension_name = 'time_table', 
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables = ['staging_songs', 'staging_events', 'songplays', 'users', 'songs', 'artists', 'time'],
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Task ordering for the DAG tasks 
#
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table>> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
