import pandas as pd
import re

from . import api
from datetime import datetime
from typing import Optional, List, Dict
from io import StringIO

class AppsFlyerReporter:
  api: api.AppsFlyerAPI
  verbose: bool = False
  timezone: str = 'GMT'

  def __init__(self, api: api.AppsFlyerAPI, verbose: bool=False, timezone: str='GMT'):
    self.api = api
    self.verbose = verbose
    self.timezone = timezone
    self._date_format = '%Y-%m-%d'

  def _convert_response_to_data_frame(self, response: str, columns: Optional[List[str]]) -> pd.DataFrame:
    stream = StringIO(response)
    df = pd.read_csv(stream, dtype='object')
        
    if columns is not None:
      df = df[columns]

    return df

  def get_installs_report(self, start_date: datetime, end_date: datetime, columns: Optional[List[str]]=None) -> pd.DataFrame:
    report = self.get_report(
      report_endpoint='installs_report', 
      start_date=start_date, 
      end_date=end_date, 
      columns=columns
    )
    return report

  def get_events_report(self, start_date: datetime, end_date: datetime, event_names: List[str], columns: Optional[List[str]]=None) -> pd.DataFrame:
    report = self.get_report(
      report_endpoint='in_app_events_report', 
      start_date=start_date, 
      end_date=end_date, 
      report_parameters={'event_name': ','.join(event_names)},
      columns=columns
    )
    return report

  def get_master_report(self, start_date: datetime, end_date: datetime, groupings: List[str], kpis: List[str], app_ids: Optional[List[str]]=None,  columns: Optional[List[str]]=None) -> pd.DataFrame:
    parameters = {
      'from': start_date.strftime(self._date_format),
      'to': end_date.strftime(self._date_format),
      'app_id': ','.join(app_ids) if app_ids is not None else self.api.app_id,
      'groupings': ','.join(groupings),
      'kpis': ','.join(kpis),
      'timezone': 'GMT',
    }

    endpoint = 'export/master_report'
    response = self.api.get(
      endpoint=endpoint, 
      parameters=parameters, 
      verbose=self.verbose
    )
    data_frame = self._convert_response_to_data_frame(response=response, columns=columns)
    return data_frame

  def get_report(self, report_endpoint: str, start_date: datetime, end_date: datetime, report_parameters: Dict[str, any]={}, columns: Optional[List[str]]=None) -> pd.DataFrame:
    parameters = {
      'from': start_date.strftime(self._date_format),
      'to': end_date.strftime(self._date_format),
      'timezone': self.timezone,
    }
    parameters.update(report_parameters)

    endpoint = 'export/{app_id}/' + report_endpoint
    response = self.api.get(
      endpoint=endpoint, 
      parameters=parameters, 
      verbose=self.verbose
    )
    data_frame = self._convert_response_to_data_frame(response=response, columns=columns)
    return data_frame

class AppsFlyerDataLockerReporter:
  api: api.AppsFlyerDataLockerAPI

  def __init__(self, api: api.AppsFlyerDataLockerAPI):
    self.api = api
  
  def get_hour_part_report(self, event_name: str, date: datetime, hour_value: int, part: int) -> pd.DataFrame:
    key = f'{self.api.prefix(event_name=event_name, date=date, hour_value=hour_value)}part-{part:05}.gz'
    part_object = self.api.get_object(key=key)
    df = pd.read_csv(part_object['Body'], compression='gzip', dtype='object')
    return df
  
  def get_hour_part_metadata(self, event_name: str, date: datetime, hour_value: int) -> int:
    bucket_contents = self.api.get_contents(event_name=event_name, date=date, hour_value=hour_value)
    if bucket_contents is None:
      return {'completed': False, 'part_count': 0}
    filtered_contents = [content for content in bucket_contents if re.match(r'.*/part-\d+.gz$', content['Key'])]
    completed = len([content for content in bucket_contents if re.match(r'.*/_SUCCESS$', content['Key'])]) > 0
    return {'completed': completed, 'part_count': len(filtered_contents)}