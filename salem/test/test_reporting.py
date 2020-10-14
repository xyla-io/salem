import unittest
import code

from ..api import AppsFlyerAPI
from ..reporting import AppsFlyerReporter
from datetime import datetime, timedelta

class Test_AppsFlyerReporter(unittest.TestCase):
    def setUp(self):
      api = AppsFlyerAPI(api_key='API_KEY', app_id='APP_ID')
      api.verbose = True
      self.reporter = AppsFlyerReporter(api=api)
      self.reporter.verbose = False

    def test_events_reporting(self):
      start = datetime.utcnow() - timedelta(days=3)
      end = datetime.utcnow() - timedelta(days=3)
      df = self.reporter.get_events_report(start_date=start, end_date=end, event_names=['af_purchase'])

      print(df)
      # code.interact(local=locals())
      self.assertIsNotNone(df)
    
    def test_custom_event_report(self):
      start = datetime.utcnow() - timedelta(days=3)
      end = datetime.utcnow() - timedelta(days=3)
      df = self.reporter.get_events_report(start_date=start, end_date=end, event_names=['tbadrevenue'])
      import pdb; pdb.set_trace()

      self.assertIsNotNone(df)

    def test_master_reporting(self):
      start = datetime.utcnow() - timedelta(days=30)
      end = datetime.utcnow() - timedelta(days=30)

      groupings = [
        'app_id',
      ]

      kpis = [
        'retention_day_0',
        'retention_day_1',
        'retention_day_2',
        'retention_day_3',
        'retention_day_4',
        'retention_day_5',
        'retention_day_6',
        'retention_day_7',
        'retention_day_8',
        'retention_day_21',
      ]

      df = self.reporter.get_master_report(start_date=start, end_date=end, groupings=groupings, kpis=kpis)

      print(df)
      # code.interact(local=locals())
      self.assertIsNotNone(df)

    def test_retention_event(self):
        start = datetime(2018, 6, 7)
        end = datetime(2018, 6, 7)

        self.reporter.timezone = 'America/Chicago'
        df = self.reporter.get_events_report(start_date=start, end_date=end, event_names=['d1_retention'])
        import pdb; pdb.set_trace()
        print(df)