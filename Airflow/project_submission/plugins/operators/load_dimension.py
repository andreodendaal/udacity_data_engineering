from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import sql_statements


class LoadDimensionOperator(BaseOperator):
  
    ui_color = '#80BD9E'

    @apply_defaults
    
    def __init__(self, redshift_conn_id, dimension_name, *args, **kwargs):
    
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dimension_name = dimension_name   
      

    def execute(self, context):
        
        self.log.info('LoadDimensionOperator running!')

        redshift_hook = PostgresHook(self.redshift_conn_id)  
        
        if self.dimension_name  == 'song_table':
            sql_stmt = sql_statements.song_table_create
            redshift_hook.run(sql_stmt)
            sql_stmt = sql_statements.song_table_insert    
            redshift_hook.run(sql_stmt)
            
        elif self.dimension_name  == 'time_table':
            sql_stmt = sql_statements.time_table_create
            redshift_hook.run(sql_stmt)
            sql_stmt = sql_statements.time_table_insert    
            redshift_hook.run(sql_stmt)
            
        elif self.dimension_name  == 'artist_table':
            sql_stmt = sql_statements.artist_table_create
            redshift_hook.run(sql_stmt)
            sql_stmt = sql_statements.artist_table_insert    
            redshift_hook.run(sql_stmt)
            
        elif self.dimension_name  == 'user_table':
            sql_stmt = sql_statements.user_table_create
            redshift_hook.run(sql_stmt)
            sql_stmt = sql_statements.user_table_insert    
            redshift_hook.run(sql_stmt)
        
        self.log.info('{} completed!'.format(self.dimension_name))
        
        return True 
       

