import requests
import time
import boto3
import botocore
import pandas as pd
import re

from datetime import datetime
from typing import Optional, Dict, List
from furl import furl

class AppsFlyerAPI:
  api_version: str
  api_key: str
  app_id: Optional[str]

  def __init__(self, api_key: str, app_id: Optional[str]=None, api_version: str='v5'):
    self.api_key = api_key
    self.app_id = app_id
    self.api_version = api_version

  def _api_call(self, method, endpoint: str, parameters: Dict[str, any], verbose: bool=False, retry_limit: int=2):
    if self.app_id is not None:
      endpoint = endpoint.format(app_id=self.app_id)

    url = furl('https://hq.appsflyer.com')
    url.path = f'/{endpoint}/{self.api_version}'

    parameters['api_token'] = self.api_key
    url.args = parameters

    retry_attempt = 0
    while retry_attempt <= retry_limit:
      if retry_attempt > 0:
        print(f'Retrying {endpoint} after failure reason:\n\n{req.reason}\n\n{req.text}\n\n-- status code: {req.status_code} | attempt no. {retry_attempt}')
        time.sleep(90)

      req = method(url.url)

      if verbose:
        print(url)
        print(req.text)
      
      if req.status_code == 200:
        break
        
      retry_attempt += 1

    if req.status_code != 200:
      raise ValueError(req.text)

    return req.text

  def get(self, endpoint: str, parameters: Dict[str, any], verbose: bool=False):
    return self._api_call(
      method=requests.get,
      endpoint=endpoint,
      parameters=parameters,
      verbose=verbose,
    )

  def put(self, endpoint: str, parameters: Dict[str, any], verbose: bool=False):
    return self._api_call(
      method=requests.put,
      endpoint=endpoint,
      parameters=parameters,
      verbose=verbose,
    )

  def post(self, endpoint: str, parameters: Dict[str, any], verbose: bool=False):
    return self._api_call(
      method=requests.post,
      endpoint=endpoint,
      parameters=parameters,
      verbose=verbose,
    )

def s3_rate_limit(func):
  def wrapper(*args, **kargs):
    try:
      return func(*args, **kargs)
    except botocore.exceptions.ClientError as exception:
      print('s3_rate_limit caught exception and will retry')
      print(exception)
      time.sleep(90)
      return func(*args, **kargs)
  return wrapper

class AppsFlyerDataLockerAPI:
  s3: boto3.session.Session.resource
  hourly_data_path: str
  bucket_name: str

  def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, region: str, bucket_name: str, hourly_data_path: str):
    self.s3 = boto3.resource(
      service_name='s3',
      region_name=region,
      aws_access_key_id=aws_access_key_id, 
      aws_secret_access_key=aws_secret_access_key
    )
    self.bucket_name = bucket_name
    self.hourly_data_path = hourly_data_path
    self._date_format = '%Y-%m-%d'
  
  @s3_rate_limit
  def print_report_folders(self):
    result = self.s3.meta.client.list_objects(
      Bucket=self.bucket_name,
      Prefix=self.hourly_data_path,
      Delimiter='/'
    )
    for o in result.get('CommonPrefixes'):
        print(o.get('Prefix'))
    
  def prefix(self, event_name: str, date: datetime, hour_value: int) -> str:
    assert hour_value >= 0 and hour_value <= 24
    date_name = date.strftime(self._date_format)
    hour_name = 'late' if hour_value is 24 else hour_value
    return f'{self.hourly_data_path}t={event_name}/dt={date_name}/h={hour_name}/'

  @s3_rate_limit
  def get_contents(self, event_name: str, date: datetime, hour_value: int) -> List[Dict[str, any]]:
    result = self.s3.meta.client.list_objects(
      Bucket=self.bucket_name,
      Prefix=self.prefix(event_name=event_name, date=date, hour_value=hour_value),
      Delimiter='/'
    )
    return result.get('Contents')
  
  @s3_rate_limit
  def get_object(self, key: str) -> Dict[str, any]:
    result = self.s3.meta.client.get_object(Bucket=self.bucket_name, Key=key)
    return result