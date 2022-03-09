from fulcrum import Fulcrum
import pandas as pd

def query_to_df(fulcrum_client, query):
  
  """
  Send query to fulcrum and convert response to pd.df
  
  fulcrum_client: client initialized via Fulcrum()
  query: str query to send via fulcrum

  return: pd.df
  """
  
  response_json = fulcrum_client.query(query)
  response_df = pd.DataFrame(response_json['rows'])

  return response_df
