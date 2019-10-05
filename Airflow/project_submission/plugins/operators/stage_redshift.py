from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import sql_statements
from airflow.hooks.base_hook import BaseHook

class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'

    @apply_defaults
    
    def __init__(self, redshift_conn_id, s3_conn_id, table, s3_path, region, s3_bucket, *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.s3_conn_id = s3_conn_id
        self.table = table
        self.s3_path = s3_path
        self.region = region
        self.s3_bucket = s3_bucket     


    def execute(self, context): 
        
        # Get S3 Login
        self.s3_connection_object = BaseHook.get_connection(self.s3_conn_id)       
        if self.s3_connection_object.login:
            self.s3_access_key_id = self.s3_connection_object.login
            self.s3_secret_access_key = self.s3_connection_object.password
        else:
            self.s3_access_key_id = 'S3 connection failure'
            self.s3_secret_access_key = 'S3 connection failure'
        
        # Connect to S3  
        self.s3_connection_object = BaseHook.get_connection(self.s3_conn_id)
        #self.log.info("Connecting to S3 with... " + self.s3_conn_id)        
        #self.log.info("KEY = " + self.s3_access_key_id)
        #self.log.info("SECRET = " + self.s3_secret_access_key)
        
        #Connect to Redshift
        self.log.info(self.redshift_conn_id) 
        #self.log.info('Connecting to Redshift with....' + self.redshift_conn_id) 
        self.log.info("Create Staging Table in redshift = " + self.table)

# Create Staging tables
        sql_stmt = sql_statements.staging_songs_table_create        
        redshift_hook = PostgresHook(self.redshift_conn_id)    
 
        if self.table == 'staging_events':
            
            sql_stmt = sql_statements.staging_events_table_create
            self.log.info("Copy SQL statement = " + sql_stmt)
            
            redshift_hook.run(sql_stmt)
            sql_stmt = sql_statements.COPY_SQL_EVENTS.format(
            self.table, 
            self.s3_bucket, 
            self.s3_path,
            self.s3_access_key_id,
            self.s3_secret_access_key,
            self.region
            )
            self.log.info("Copy SQL statement = " + sql_stmt)
            #Uncomment for full run!
            redshift_hook.run(sql_stmt)
            
        elif self.table == 'staging_songs':

            sql_stmt = sql_statements.staging_songs_table_create
            self.log.info("Copy SQL statement = " + sql_stmt)
            redshift_hook.run(sql_stmt)
            sql_stmt = sql_statements.COPY_SQL_SONGS.format(
            self.table, 
            self.s3_bucket, 
            self.s3_path,
            self.s3_access_key_id,
            self.s3_secret_access_key,
            self.region
            )
            self.log.info("Copy SQL statement = " + sql_stmt)
            #Uncomment for full run!
            redshift_hook.run(sql_stmt)
        
        return True
