import pandas as pd
import datetime as dt
import requests
import json

def timestamp_to_unix(x):
  
  """
  parse timestamp to unix numeric
  
  x: datetime
  
  return: numeric unix representation of x
  """
  
  x_unix = dt.datetime.timestamp(x)
  
  return x_unix.__round__()

def parse_properties(df, columns_to_integer=None, columns_to_datetime=None, columns_to_numeric=None, columns_to_boolean=None, columns_to_string = None, dt_unit = 'ms', boolean_dict = {'true': True, 'false': False, '': None}):
  
  """
  Parse string columns to other formats. This function is used in hubspot routine, its not yet scaled to other routines
  
  df: pd.DataFrame
  columns_to_: list with names of the columns to parse
  
  return: pd.DataFrame with parsed columns
  """
  
  if columns_to_integer:
    df[columns_to_integer] = df[columns_to_integer].apply(string_to_integer)
  
  if columns_to_datetime:
    df[columns_to_datetime] = df[columns_to_datetime].apply(pd.to_datetime, unit = dt_unit)
  
  if columns_to_numeric:
    df[columns_to_numeric] = df[columns_to_numeric].apply(pd.to_numeric, errors = 'coerce', downcast='float')
    
  if columns_to_boolean:
    df[columns_to_boolean] = df[columns_to_boolean].replace(boolean_dict).astype('boolean')

  if columns_to_string:
    df[columns_to_string] = df[columns_to_string].apply(int_to_string)
  
  return df

def remove_value_from_dict_key(dict_, values_to_rm):
  
  """
  Remove value include in 'values_to_rm' from every dictionary key
  
  dict_: dict
  values_to_rm: list
  
  return: dict_ without values include in values_to_rm
  """
  
  for dict_values in dict_.values():
    if dict_values is None:
      continue
    
    for value_to_rm in values_to_rm:
      if value_to_rm in dict_values:
        dict_values.remove(value_to_rm)

def one_to_many(df, one, many, convert_to_string = True):
  
  """
  Explode column-list dataframe to get df with relation one-to-many. Optional: convert "many" column to string
  
  one: column name of one in one-to-many association
  many: column name of many in one-to-many association
  
  return: df with two columns ['one', 'many']
  """
  
  if df.empty:
    print('No dataframe to convert')
    return pd.DataFrame()
  
  map_assoc = df[[one, many]].explode(many).dropna().reset_index(drop = True)
  
  if convert_to_string:
    map_assoc[many] = map_assoc[many].apply(int_to_string)
  
  return map_assoc

def extract_n_element(df, column_name, n = 0):
  
  """
  Extract nth element in every row of a column-list
  
  df: pd.DataFrame
  column_name: name of the column-list
  n: int position of element to extract
  
  return: df with unnested-list column
  """
  
  dfcopy = df
  dfcopy[column_name] = [val[n] if val else None for val in dfcopy[column_name]]
  
  return dfcopy

def int_to_string(x):
  
  """
  Parse integer to string safely, conserving null's
  
  x: int
  
  return: string
  """
  
  if not pd.isnull(x):
    r = "{:.0f}".format(x)
  else:
    r = x

  return r

def string_to_integer(x):
  
  """
  Parse string to integer safely, conserving null's
  
  x: str
  
  return: int
  """
  
  xi = pd.to_numeric(x, errors='coerce').astype('Int64')
  
  return xi

def unlist_column(df: pd.DataFrame(), list_column: str, new_column_names: list):
  
  """
  df: df containing the column to split
  list_column: str name of the column to split
  new_column_names: lst of names with new column names
  
  return: same df but with splitted column
  """
  
  df_copy = df.copy()
  
  try:
    df_copy[new_column_names] = df_copy[list_column].apply(pd.Series)
  except ValueError:
    df_copy
  
  return df_copy

def get_request(base_url, parameters = {}, header = {}):
  
  """
  Send Request to endpoint 
  
  base_url: str url to point to. Consist of endpoint_base and object to retreive
  endpoint_parameters: dict Parameters to include in the request
  header: dict Headers to include in the request
  
  return: list
  """
  
  req = requests.get(base_url, params = parameters, headers = header)
  respond = json.loads(req.content)
  
  return respond
