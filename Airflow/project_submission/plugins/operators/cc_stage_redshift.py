from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import sql_statements

class cc_StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):
    #def __init__(self, task_id, redshift_conn_id, table, s3_path, s3_access_key_id, s3_secret_access_key, region, s3_bucket, *args, **kwargs):
    #def __init__(self, redshift_conn_id, table, s3_path, s3_access_key_id, s3_secret_access_key, region, s3_bucket, sql_statement, *args, **kwargs):
    #def __init__(self, redshift_conn_id, table, s3_path, s3_access_key_id, s3_secret_access_key, region, s3_bucket, *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        #self.redshift_conn_id = redshift_conn_id
        #self.table = table
        #self.s3_path = s3_path
        #self.s3_access_key_id = s3_access_key_id
        #self.s3_secret_access_key = s3_secret_access_key
        #self.region = region
        #self.s3_bucket = s3_bucket
        #self.sql_statement = sql_statement       

    def execute(self, context):
        self.log.info('StageToRedshiftOperator connecting!') 
        
        """
        self.log.info(self.redshift_conn_id) 
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        conn = self.hook.get_conn() 
        cursor = conn.cursor()
        self.log.info("Connected with " + self.redshift_conn_id)
        # Create 
        
        COPY_SQL = """
        #COPY {0}
        #FROM 's3://{1}/{2}'
        #ACCESS_KEY_ID '{3}'
        #SECRET_ACCESS_KEY '{4}'
        #JSON 'auto'
        #REGION '{5}'
        #COMPUPDATE OFF;
        """.format(self.table, 
                   self.s3_bucket, 
                   self.s3_path,
                   self.s3_access_key_id,
                   self.s3_secret_access_key,
                   self.region)
        
        self.log.info("Copy SQL statement = " + COPY_SQL)
        self.log.info("Create Staging Table in redshift = " + self.sql_statement)
        #redshift_hook.run(sql_statements.{}.format(self.sql_statement))
        self.log.info("Running Staging Copy...")
        cursor.execute(COPY_SQL)
        cursor.close()
        conn.commit()
        """
        
        return True
