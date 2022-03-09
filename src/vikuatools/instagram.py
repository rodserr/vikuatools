def ig_get(base_url, endpoint_parameters, to_df = True):
  
  """
  Send Request to Faceboook endpoint 
  
  base_url: str url to point to. Consist of endpoint_base and client_id/page_id depending on endpoint to request
  endpoint_parameters: dict Parameters to include in the request
  to_df: bool flag to transform response to pd.DataFrame
  
  return: list or df depending on 'to_df'
  """
  
  req = requests.get(base_url, endpoint_parameters)
  respond = json.loads(req.content)
  
  if to_df:
    respond = pd.DataFrame(respond['data'])
  
  return respond

def ig_media_insight(insight_list, endpoint_parameters, metrics = 'engagement,impressions,reach,saved'):
  
  """
  Loop over ig media posts to get deeper insight and convert to tidy df
  Source: https://towardsdatascience.com/discover-insights-from-your-instagram-business-account-with-facebook-graph-api-and-python-81d20ee2e751
  
  insight_list: list of media, response from '/media' endpoint
  endpoint_parameters: dict Parameters to include in the request
  metrics: str metric names comma-separated
  
  return: pd.df
  """
  
  media_insight = []
  # Loop Over 'Media ID'
  for imedia in insight_list['data']:
    # Define URL
    url = endpoint_parameters['endpoint_base'] + imedia['id'] + '/insights'
    # Define Endpoint Parameters
    parameters_media = dict() 
    parameters_media['metric'] = metrics
    parameters_media['access_token'] = endpoint_parameters['access_token'] 
    # Requests Data
    media_data = requests.get(url, parameters_media )
    json_media_data = json.loads(media_data.content)
    media_insight.append(list(json_media_data['data']))
  
  # Initialize Empty Container
  engagement_list = []
  impressions_list = []
  reach_list = []
  saved_list = []
  
  # Loop Over Insights to Fill Container
  for insight in media_insight:
    engagement_list.append(insight[0]['values'][0]['value'])
    impressions_list.append(insight[1]['values'][0]['value'])
    reach_list.append(insight[2]['values'][0]['value'])
    saved_list.append(insight[3]['values'][0]['value'])
  
  # Create DataFrame
  media_insight = list(zip(engagement_list, impressions_list, reach_list, saved_list))
  media_insight_df = pd.DataFrame(media_insight, columns =['engagement', 'impressions', 'reach', 'saved'])
  
  basic_insight_df = pd.DataFrame(insight_list['data'])
  insight_df = pd.concat([basic_insight_df, media_insight_df], axis=1)
  
  insight_df['timestamp'] = insight_df['timestamp'].apply(pd.to_datetime)
  
  return insight_df

def ig_audience_insight(endpoint_parameters):
  
  """
  Get Audience insight
  Source: https://towardsdatascience.com/discover-insights-from-your-instagram-business-account-with-facebook-graph-api-and-python-81d20ee2e751
  
  endpoint_parameters: dict Parameters to include in the request
  
  return: 3 pd.df
  """
  
  # Define URL
  url_account_insights = endpoint_parameters['endpoint_base'] + endpoint_parameters['instagram_account_id'] + '/insights'
  # Define Endpoint Parameters
  parameters_account_insights = dict()
  parameters_account_insights['metric'] = 'audience_city,audience_country,audience_gender_age'
  parameters_account_insights['period'] = 'lifetime'
  parameters_account_insights['access_token'] = endpoint_parameters['access_token']
  # Requests Data
  audience = ig_get(url_account_insights, parameters_account_insights, to_df=False)
  
  city = pd.Series(audience['data'][0]['values'][0]['value']).rename_axis('city').to_frame('follower_count').reset_index(level=0)
  country = pd.Series(audience['data'][1]['values'][0]['value']).rename_axis('country').to_frame('follower_count').reset_index(level=0)
  gender_age = pd.Series(audience['data'][2]['values'][0]['value']).rename_axis('gender_age').to_frame('follower_count').reset_index(level=0)
  
  return city, country, gender_age

def ig_metric_to_df(metric_list):
  
  """
  Helper to transform metric list into tidy dataframe
  
  metric_list: list of metric values
  
  return: pd.df
  """
  
  column_name = metric_list['name']
  df = pd.DataFrame(metric_list['values']).rename(columns={"value": column_name})
  
  return df

def ig_user_insight(endpoint_parameters, since = None, until = None):
  
  """
  Get User insights
  
  endpoint_parameters: dict Parameters to include in the request
  since, until: numeric unix timestamp 
  
  return: pd.df
  """
  
  # Define URL
  url_account_insights = endpoint_parameters['endpoint_base'] + endpoint_parameters['instagram_account_id'] + '/insights'
  # Define Endpoint Parameters
  parameters_account_insights = dict()
  parameters_account_insights['metric'] = 'email_contacts,follower_count,impressions,profile_views,reach,website_clicks'
  parameters_account_insights['period'] = 'day'
  parameters_account_insights['access_token'] = endpoint_parameters['access_token']
  
  if since and until:
    parameters_account_insights['since'] = since
    parameters_account_insights['until'] = until
  
  # Requests Data
  metrics = ig_get(url_account_insights, parameters_account_insights, to_df=False)
  # Tidy Metrics 
  metrics_list = list(map(ig_metric_to_df, metrics['data']))
  # Bind Dataframes
  metric_df = reduce(lambda left,right: pd.merge(left,right,on='end_time'), metrics_list)
  
  # Convert to date
  metric_df['end_time'] = metric_df['end_time'].apply(pd.to_datetime)
  metric_df['end_time'] = metric_df['end_time'].apply(lambda x: x.date())
  
  return metric_df
