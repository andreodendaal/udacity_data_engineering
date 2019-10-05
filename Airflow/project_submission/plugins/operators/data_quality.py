from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, redshift_conn_id, tables, *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)        
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        
        self.log.info('DataQualityOperator Running!')
        
        def check_greater_than_zero(table, redshift_hook):          
                       
            self.log.info('Checking for empty tables on table..' + table)            
            
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records") 
            
            # Check for NULL values
            self.log.info('Checking for Null values on table..' + table)  
            col = ' '
            
            if table == 'songplays':
                col = 'artist_id'                
            elif table == 'users':
                col = 'user_id'                
            elif table == 'songs':
                col = 'title'     
            elif table == 'artists':
                col = 'name'                
            elif table == 'time':
                col = 'hour'
                
            if col != ' ':
                
                null_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL" )
                num_null_records = null_records[0][0]
            
                if num_null_records < 1:
                    self.log.info(f"Null Data quality on table {table} and column {col} check PASSED with {null_records[0][0]} records")                    
                else:                    
                    raise ValueError(f"Data quality check failed. {table}, column {col} contains {null_records[0][0]} - NULL values")
 
        #Run Quality checks
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            self.log.info('Checking Quality on Table... = ' + table)
            check_greater_than_zero(table, redshift_hook)
        
        self.log.info('DataQualityOperator completed!')
        