from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import sql_statements

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    
    def __init__(self, redshift_conn_id, *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):        
        
        self.log.info('LoadFactOperator running!')        
       
        # Create tables
        self.log.info("Creating Fact table - Songplay...")   
        redshift_hook = PostgresHook(self.redshift_conn_id)  
        sql_stmt = sql_statements.songplay_table_create  
        redshift_hook.run(sql_stmt)
        
        # Populate tables
        self.log.info("Loading Fact table - Songplay...")
        sql_stmt = sql_statements.songplay_table_insert  
        redshift_hook.run(sql_stmt)
        
        self.log.info('LoadFactOperator completed!')
        
        return True

        
        

