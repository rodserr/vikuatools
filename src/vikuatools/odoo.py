import xmlrpc.client
import pandas as pd
from vikuatools.utils import int_to_string, unlist_column

def get_odoo_model(odoo_model, db, uid, password, model_name, fields, checkpoint = '2000/01/01 00:00:00', extra_filters = None):
    
    """
    odoo_model: model object returned from xmlrpc call
    db, uid, password: str credentials
    model_name: str model name to query
    fields: fields to query
    checkpoint: str datetime to retreive records after. Format must be: %Y/%m/%d %H:%M:%S
    extra_filters: list difinning other filter to apply to the query
    
    return: pd.df with the corresponding odoo model data
    """
    
    call_filter = [['write_date', '>', checkpoint]]
    
    if extra_filters:
      call_filter.append(extra_filters)
    
    omodel = odoo_model.execute_kw(db, uid, password, model_name, 'search_read',
      [call_filter],
      {'fields': fields})
    
    model_df = pd.DataFrame(omodel)
    
    if not model_df.empty:
      model_df.loc[:, ('write_date')] = model_df.loc[:, ('write_date')].apply(pd.to_datetime)
    
    print(f'{model_name} new records: {len(model_df)}')
    
    return(model_df)

def clean_move(df):
  
  """
  Clean df, which contains data from move collection
  
  df: pd.df
  
  return: pd.df
  """
  
  if df.empty:
    return df
  
  df_copy = df.replace({False: None})
  
  df_copy['id'] = df_copy['id'].apply(int_to_string)
  df_copy['invoice_date'] = df_copy['invoice_date'].apply(pd.to_datetime)
  
  
  return df_copy

def clean_move_line(df):
    
    """
    Clean df, which contains data from move.line collection
  
    df: pd.df
  
    return: pd.df
    """
    
    if df.empty:
      return df
    
    move_line_df = df.copy()
    
    if not move_line_df.empty:
      # Split columns ids and descriptions
      move_line_df = unlist_column(move_line_df, 'account_id', ['account_id','account_description'])
      move_line_df = unlist_column(move_line_df, 'move_id', ['move_id','move_description'])
      move_line_df = unlist_column(move_line_df, 'company_id', ['company_id','company_description'])
      move_line_df = unlist_column(move_line_df, 'partner_id', ['partner_id','partner_description'])
      move_line_df = unlist_column(move_line_df, 'currency_id', ['currency_id','currency_description'])
      move_line_df = unlist_column(move_line_df, 'journal_id', ['journal_id','journal_description'])
      move_line_df = unlist_column(move_line_df, 'tax_fiscal_country_id', ['tax_fiscal_country_id','tax_fiscal_country_description'])
      move_line_df = unlist_column(move_line_df, 'analytic_account_id', ['analytic_account_id','analytic_account_description'])
      
      # Extract just first tag_id
      move_line_df['analytic_tag_ids'] = move_line_df['analytic_tag_ids'].str[0]
      
      # Drop columns
      move_line_df = move_line_df.drop(['account_description', 'tax_fiscal_country_id', 'partner_id', 'analytic_account_id'], axis = 'columns')
      
      # Convert Datetimes
      move_line_df['date'] = move_line_df['date'].apply(pd.to_datetime)
      
      # Convert Integer to string
      int_to_str_columns = ['id', 'move_id', 'account_id', 'company_id', 'currency_id', 'journal_id']
      move_line_df[int_to_str_columns] = move_line_df[int_to_str_columns].applymap(int_to_string)
      
      # Replace False with None
      move_line_df = move_line_df.replace({False: None})
      
    
    return move_line_df
  
def clean_account(df):
  
  """
  Clean df, which contains data from account.account collection
  
  df: pd.df
  
  return: pd.df
  """
  
  if df.empty:
    return df
  
  df = df.rename(columns={"name": "account_name", "code": "account_code"})
  
  try:
    df_split = split_column(df, column_to_split='account_name')
    df = df.join(df_split).drop('account_name', axis = 'columns')
  
  except KeyError:
    df
  
  df['id'] = df['id'].apply(int_to_string)
  df.replace({False: None}, inplace = True)
  
  return df

def clean_analytic_tag(df):
  
  """
  Clean df, which contains data from account.analytic.tag collection
  
  df: pd.df
  
  return: pd.df
  """
  
  if df.empty:
    return df
  
  df = df.rename(columns={"name": "analytic_tag_name"})
  
  df['id'] = df['id'].apply(int_to_string)
  df=df.replace({False: None})
  
  return df

def clean_analytic_account(df):
  
  """
  Clean df, which contains data from account.analytic.account collection
  
  df: pd.df
  
  return: pd.df
  """
  
  if df.empty:
    return df
  
  df = df.rename(columns={"name": "analytic_account_name"})
  
  df['id'] = df['id'].apply(int_to_string)
  df=df.replace({False: None})
  
  return df

def clean_currency_rate(df):
    
    """
    Clean df, which contains data from res.currency.rate collection
  
    df: pd.df
  
    return: pd.df
    """
    
    if df.empty:
      return df
    
    currency_df = df.copy()
    
    if not currency_df.empty:
      # Split columns ids and descriptions
      currency_df = unlist_column(currency_df, 'company_id', ['company_id','company_description'])
      currency_df = unlist_column(currency_df, 'currency_id', ['currency_id','currency_description'])
      
      # Convert Datetimes
      currency_df['date'] = currency_df['name'].apply(pd.to_datetime)
      
      # Drop columns
      currency_df = currency_df.drop(['name'], axis = 'columns')
      
      # Convert Integer to string
      int_to_str_columns = ['id', 'company_id', 'currency_id']
      currency_df[int_to_str_columns] = currency_df[int_to_str_columns].applymap(int_to_string)
      
      # Replace False with None
      currency_df = currency_df.replace({False: None})
      
    return currency_df


def split_column(df: pd.DataFrame, column_to_split: str, sep = ' - ', prefix = 'category_'):
    
    """
    df: account.account odoo model with account_name field
    column_to_split: str name of the column to split
    sep: str character to split to
    prefix: str character to append at the beggining of every new column
    
    return: pd.DataFrame with account_name splitted in n columns, where n is the maximum count of ' - ' + 1 found in account_name
    """
    
    if df.empty:
      return(df)
    
    # Split
    acc = df[column_to_split].str.split(sep, expand=True) 
    
    # Set column names
    n_categories = len(acc.columns)
    new_names = [prefix + str(i) for i in range(1,n_categories)]
    acc.columns = ['account_type']+new_names
    
    # Find None values in every column
    acc_none = acc.applymap(lambda x: x is None)
    
    # Loop to replace None with left-valid value
    for column in range(1, len(acc.columns)):
        current_column = acc.columns[column]
        prev_column = acc.columns[column-1]
        acc[current_column][acc_none[current_column]] = acc[prev_column][acc_none[current_column]]
    
    return acc
