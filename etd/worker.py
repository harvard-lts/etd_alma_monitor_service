import os
import requests

"""

Since: 2023-06-07
Author: vcrema
"""


class Worker():
    version = None

    def __init__(self):
        self.version = os.getenv("APP_VERSION", "0.0.1")

    def get_version(self):
        return self.version

    # this is call to the DIMS healthcheck for integration testing
    def call_api(self):
        dims_healtheck_url = "https://dims-dev.lib.harvard.edu:10580/health"
        r = requests.get(dims_healtheck_url)
        return r.text
