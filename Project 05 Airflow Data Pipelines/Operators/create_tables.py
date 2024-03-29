from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):

    ui_color = '#358140'
    #sql_statement_file='common/create_tables.sql'
    sql_quiries = [
                    """
            CREATE TABLE IF NOT EXISTS artists (
                    artistid varchar(256) NOT NULL,
                name varchar(256),
                location varchar(256),
                lattitude numeric(18,0),
                longitude numeric(18,0)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS songplays (
                playid varchar(32) NOT NULL,
                start_time timestamp NOT NULL,
                userid int4 NOT NULL,
                "level" varchar(256),
                songid varchar(256),
                artistid varchar(256),
                sessionid int4,
                location varchar(256),
                user_agent varchar(256),
                CONSTRAINT songplays_pkey PRIMARY KEY (playid)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS songs (
                songid varchar(256) NOT NULL,
                title varchar(256),
                artistid varchar(256),
                "year" int4,
                duration numeric(18,0),
                CONSTRAINT songs_pkey PRIMARY KEY (songid)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS staging_events (
                artist varchar(256),
                auth varchar(256),
                firstname varchar(256),
                gender varchar(256),
                iteminsession int4,
                lastname varchar(256),
                length numeric(18,0),
                "level" varchar(256),
                location varchar(256),
                "method" varchar(256),
                page varchar(256),
                registration numeric(18,0),
                sessionid int4,
                song varchar(256),
                status int4,
                ts int8,
                useragent varchar(256),
                userid int4
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS staging_songs (
                num_songs int4,
                artist_id varchar(256),
                artist_name varchar(256),
                artist_latitude numeric(18,0),
                artist_longitude numeric(18,0),
                artist_location varchar(256),
                song_id varchar(256),
                title varchar(256),
                duration numeric(18,0),
                "year" int4
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "time" (
                start_time timestamp NOT NULL,
                "hour" int4,
                "day" int4,
                week int4,
                "month" varchar(256),
                "year" int4,
                weekday varchar(256),
                CONSTRAINT time_pkey PRIMARY KEY (start_time)
            ) ;
            """,
            """
            CREATE TABLE IF NOT EXISTS users (
                userid int4 NOT NULL,
                first_name varchar(256),
                last_name varchar(256),
                gender varchar(256),
                "level" varchar(256),
                CONSTRAINT users_pkey PRIMARY KEY (userid)
            );
            """
    ]

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Creating Redshift tables ")

        for command in  CreateTablesOperator.sql_quiries:
            if command.rstrip() != '':
                redshift.run(command)