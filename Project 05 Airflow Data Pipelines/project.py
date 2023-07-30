from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from Operators.stage_redshift import StageToRedshiftOperator
from Operators.load_fact import LoadFactOperator
from Operators.create_tables import CreateTablesOperator
from Operators.load_dimensions import LoadDimensionOperator
from Operators.data_quality import DataQualityOperator
from common.final_project_quiries import SqlQueries


s3_bucket = "onasr"
events_s3_key = "log-data"
songs_s3_key = "song_data/A/A/A"

default_args = {
    'owner': 'omar',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    create_tables_in_redshift = CreateTablesOperator(
        task_id = 'create_tables',
        redshift_conn_id = "redshift"
    )
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        aws_credentials_id = "aws_credentials",
        redshift_conn_id = "redshift",
        table = "staging_events",
        s3_bucket = s3_bucket,
        s3_key = events_s3_key
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        aws_credentials_id = "aws_credentials",
        redshift_conn_id = "redshift",
        table = "staging_songs",
        s3_bucket = s3_bucket,
        s3_key = songs_s3_key        
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id = "redshift",
        sql = SqlQueries.songplay_table_insert
        
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id = "redshift",
        sql = SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id = "redshift",
        sql = SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id = "redshift",
        sql = SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id = "redshift",
        sql = SqlQueries.time_table_insert        
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = "redshift",
        tables = ["songplays", "users", "songs", "artists", "time"]
    )

    start_operator >> create_tables_in_redshift >> [stage_events_to_redshift, stage_songs_to_redshift] >> \
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

final_project_dag = final_project()