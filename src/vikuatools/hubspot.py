import requests
import json
import urllib
import pandas as pd
from vikuatools.utils import int_to_string, remove_value_from_dict_key, parse_properties

def hs_get_recent_modified(url, parameters, max_results):
  
  """
  Get recent modified object from hubspot API legacy
  
  url: str endpoint to retreive. one of deals, companies or engagements
  parameters: dict with parameters to include in call e.g. api_key, count, since
  max_results: dbl max number of objects to retreive
  
  return: list with object from responses
  """
  
  object_list = []
  get_recent_url = url
  parameter_dict = parameters
  headers = {}
  
  # Paginate your request using offset
  has_more = True
  while has_more:
    params = urllib.parse.urlencode(parameter_dict)
    get_url = get_recent_url + params
    r = requests.get(url= get_url, headers = headers)
    response_dict = json.loads(r.text)
    
    try:
      has_more = response_dict['hasMore']
    except KeyError:
      has_more = response_dict['has-more']
    
    try:
      object_list.extend(response_dict['results'])
    except KeyError:
      object_list.extend(response_dict['contacts'])
    
    try:
      parameter_dict['offset'] = response_dict['offset']
    except KeyError:
      parameter_dict['vidOffset'] = response_dict['vid-offset']
    
    if len(object_list) >= max_results: # Exit pagination, based on whatever value you've set your max results variable to.
      print('maximum number of results exceeded')
      break
  
  print(f'Done!! Found {len(object_list)} object')
  
  return object_list

def hs_get_recent_modified_contacts(url, hapikey, count, max_results, contact_property):
  
  """
  Get recent modified contacts from hubspot API legacy. Contacts requires another function due to different name in hasMore attribute and 
  it need to ask for specific properties on the call
  
  url: str endpoint to retreive. one of deals, companies or engagements
  hapikey: str api_key
  count: dbl number of object to retreive in a single call
  max_results: dbl max number of objects to retreive
  contact_property: list Properties to query
  
  return: list with object from responses
  """
  
  object_list = []
  get_recent_url = url
  parameter_dict = {'hapikey': hapikey, 'count': count}
  headers = {}
  properties_w_header = ['property='+x for x in contact_property]
  properties_url = '&'+'&'.join(properties_w_header)
  
  # Paginate your request using offset
  has_more = True
  while has_more:
    parameters = urllib.parse.urlencode(parameter_dict)
    get_url = get_recent_url + parameters + properties_url
    
    r = requests.get(url= get_url, headers = headers)
    response_dict = json.loads(r.text)
    
    has_more = response_dict['has-more']
    object_list.extend(response_dict['contacts'])
    parameter_dict['vidOffset'] = response_dict['vid-offset']
    
    if len(object_list) >= max_results: # Exit pagination, based on whatever value you've set your max results variable to.
      print('maximum number of results exceeded')
      break
  
  print(f'Done!! Found {len(object_list)} object')
  
  return object_list

def hs_extract_value(new_objects, property_names):
  
  """
  Extract insterested properties from api call response. If response has association, it will extract company and vids
  
  new_objects: list with http response
  property_names: list with property names to keep
  
  return: pd.DataFrame with property_names fields
  """
  
  # Association Flag
  has_associations = 'associations' in new_objects[0].keys()
  
  # If exist, append association properties to element to keep
  if has_associations:
    property_names = property_names + ['associatedCompanyIds', 'associatedVids']
  
  # Start loop to extract values
  list_properties = []
  for obj in new_objects:
    
    # If association exist, extract association
    if has_associations:
      associatedCompanyIds = obj['associations']['associatedCompanyIds']
      associatedVids = obj['associations']['associatedVids']
    
    # Extract all property values
    props = obj['properties']
    saved_properties = {}
    for key, value in props.items():
      saved_properties[key] = value['value']
    
    # If exist, append associations properties
    if has_associations:
      saved_properties['associatedCompanyIds'] = associatedCompanyIds
      saved_properties['associatedVids'] = associatedVids
    
    # Save properties
    list_properties.append(saved_properties)
  
  # Properties to df
  df_properties = pd.DataFrame(list_properties)
  
  # Keep only properties of interest
  subset_columns = df_properties.columns.intersection(set(property_names))
  df_properties = df_properties[subset_columns]
    
  return df_properties

def hs_extract_engagements(engagement_list, *arg):
  
  """
  Extract properties and associations from engagements
  
  engagement_list: list with engagement and associations, response from engagement endpoint
  
  return: df with engagement fields and company associations
  """
  
  list_properties = []
  for obj in engagement_list:
    props_dict = obj['engagement']
    props = dict((k, props_dict[k]) for k in ['id', 'createdAt', 'lastUpdated', 'type'] if k in props_dict)
  
    companyIds = obj['associations']['companyIds']
    dealIds = obj['associations']['dealIds']
    contactIds = obj['associations']['contactIds']
    props['companyIds'] = companyIds
    props['dealIds'] = dealIds
    props['contactIds'] = contactIds
  
    list_properties.append(props)
  
  df_eng = pd.DataFrame(list_properties).rename(columns = {'id': 'hs_object_id'})
  df_eng['hs_object_id'] = df_eng['hs_object_id'].apply(int_to_string)
  
  return df_eng

def clean_hubspot_response(response_list, properties, parse_column, extraction_fun):
  
  """
  response_list: list with http response
  properties: list of properties names to query
  parse_column: dictionary with columns to parse with 'parse_properties'
  extraction_fun: funtion to extract properties and association one of hs_extract_value or hs_extract_engagements
  
  return: pd.DataFrame with necessary columns and correct types
  """
  
  if not response_list:
    print('Empty response')
    return pd.DataFrame()
  
  response_df = extraction_fun(response_list, properties)
  
  if properties:
    values_to_rm_ = [x for x in properties if x not in response_df.columns]
    remove_value_from_dict_key(parse_column, values_to_rm_)
  
  response_df = parse_properties(
    response_df,
    columns_to_integer = parse_column['to_integer'],
    columns_to_datetime = parse_column['to_datetime'],
    columns_to_numeric = parse_column['to_numeric'],
    columns_to_boolean = parse_column['to_boolean']
    )
  
  return response_df

def get_mkt_email_stats(parameters):
  
  """
  Get marketing email stats
  documentation at https://legacydocs.hubspot.com/docs/methods/cms_email/get-all-marketing-email-statistics
  
  parameters: dict with parameters to encode to endpoint
  
  return: pd.df with stats and basic metadata
  """
  
  # endp = f'https://api.hubapi.com/marketing-emails/v1/emails/with-statistics?hapikey={hapikey}&limit={limit}&campaign=3086d92e-e66a-4b14-99f5-2b03523eb8ab'
  parameters_parsed = urllib.parse.urlencode(parameters)
  endp = 'https://api.hubapi.com/marketing-emails/v1/emails/with-statistics?' + parameters_parsed
  r = requests.get(url=endp)
  response_dict = json.loads(r.text)
  objects = response_dict['objects']
  
  campaign_df=pd.DataFrame(objects).query('currentState != "DRAFT"').reset_index(drop=True).dropna(subset='stats')
  
  metadata = campaign_df[['id', 'name', 'created', 'publishDate', 'updated', 'campaignName']].reset_index(drop=True)
  stats = pd.DataFrame([x['counters'] for x in campaign_df['stats']])
  
  df = pd.concat([metadata, stats], axis=1)
  
  df = parse_properties(df, columns_to_datetime = ['created', 'publishDate', 'updated'], columns_to_string='id')
  
  return df

def get_stage_history(l):

  """
  Unlist Deal Stage History

  l: list
  return: pd.df
  """

  stage_history = pd.DataFrame(l["properties"]["dealstage"]["versions"])[['value', 'timestamp']]

  stage_history['hs_object_id'] = l['dealId']

  stage_history = parse_properties(stage_history, columns_to_datetime='timestamp').rename(columns={'value': 'stageId'})

  return stage_history
