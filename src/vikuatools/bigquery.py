import pandas as pd
from google.cloud import bigquery

def bq_get_last_updated_object(bq_client, project_name, dataset_name, table_name, field_name):
  
  """
  Get max date/datetime in 'field_name' from 'table_name'. This is usefull to run the update routine
  
  bq_client: BigQuery Client
  project_name: str GCP project where the dataset_name.table_name lives
  dataset_name: str dataset where table_name lives
  table_name: str table to query
  field_name: str name of the date/datetime field to get max value
  
  return: max field_name in table_name
  """
  
  checkpoint_query = f"SELECT max({field_name}) as last_updated FROM `{project_name}.{dataset_name}.{table_name}` "
  checkpoints_df = bq_client.query(checkpoint_query, project=project_name).to_dataframe()
  
  last_updated_datetime = checkpoints_df['last_updated'][0]
  
  return last_updated_datetime

def drop_duplicates(bq_client, table_id, field_name, ids):
  
  """
  Drop ids from table_id that will be updated to avoid duplicates
  
  bq_client: BigQuery Client
  table_id: Table to affect
  field_name: name of te "primary_key" field
  ids: list of ids to drop
  
  return: Nothing, resulting call to bq
  """
  
  if not ids:
    return 'No ids to remove'
  
  quotation_ids = ['"' +s + '"' for s in ids]
  collapsed_ids = ', '.join(map(str, quotation_ids))
    
  query = f"""DELETE `{table_id}` WHERE {field_name} in ({collapsed_ids})"""
  query_job = bq_client.query(query)
    
  return query_job.result()

def load_table_from_dataframe_safely(bq_client, df: pd.DataFrame, table_id: str, table_schema = None, drop_id_field = None):
  
  """
  Load table to BQ avoiding error if df is empty. Could be drop ids to avoid duplicates if drop_id_field is set.
  Could set a table_schema or trust in automatic schema setting
  
  bq_client: BigQuery Client
  df: dataframe to upload to BigQuery
  table_id: id of table in BigQuery, it should consist of project.dataset.table
  table_schema: list of .SchemaField element for every column in the table
  drop_id_field: name of the field to drop in table_id to avoid duplicates
    
  return: nothing, it uploads the df to BQ in the table_id destination
  """
    
  if df.empty:
    return(print(f'Empty Table, there are not new records in {table_id}'))
  
  if drop_id_field:
    drop_ids = list(df[drop_id_field])
    drop_duplicates(bq_client, table_id = table_id, field_name =  drop_id_field, ids = drop_ids)
  
  if table_schema:
    job_config = bigquery.LoadJobConfig(schema=table_schema)
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
  
  else:
    job = bq_client.load_table_from_dataframe(df, table_id)
    
  return(job.result())
