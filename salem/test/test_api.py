import unittest

from ..api import AppsFlyerAPI

class Test_AppsFlyerAPI(unittest.TestCase):
    def setUp(self):
      self.api = AppsFlyerAPI(api_key='API_KEY', app_id='APP_ID')

    def test_api_connection(self):
      """
      Test that the AppsFlyer class connects to the Apple Search Ads API
      """
      parameters = {
        'from': '2018-04-29',
        'to': '2018-04-30',
      }
      response = self.api.get(endpoint='export/{app_id}/daily_report', parameters=parameters, verbose=True)

      self.assertIsNotNone(response)
