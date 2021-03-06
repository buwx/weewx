"""
Upload data to wetter.com
  http://wetter.com

[StdRESTful]
    [[Wetter]]
        enable = true | false
        username = STATION ID
        password = STATION PASSWORD
"""

import Queue
import sys
import syslog
import time
import urllib
import urllib2

import weewx.restx
import weewx.units
from weeutil.weeutil import to_bool

API_VERSION = "5.0.2 - 2015/06/01"

if weewx.__version__ < "3":
    raise weewx.UnsupportedFeature("weewx 3 is required, found %s" %
                                   weewx.__version__)

def logmsg(level, msg):
    syslog.syslog(level, 'restx: Wetter: %s' % msg)

def logdbg(msg):
    logmsg(syslog.LOG_DEBUG, msg)

def loginf(msg):
    logmsg(syslog.LOG_INFO, msg)

def logerr(msg):
    logmsg(syslog.LOG_ERR, msg)

class Wetter(weewx.restx.StdRESTful):
    def __init__(self, engine, config_dict):
        super(Wetter, self).__init__(engine, config_dict)
        site_dict = weewx.restx.check_enable(config_dict, 'Wetter', 'username', 'password')
        if site_dict is None:
            return

        loginf("WeatherReport API %s" % API_VERSION)

        site_dict['manager_dict'] = weewx.manager.get_manager_dict_from_config(config_dict,
                                                                               'wx_binding')

        self.archive_queue = Queue.Queue()
        self.archive_thread = WetterThread(self.archive_queue, **site_dict)
        self.archive_thread.start()
        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
        loginf("Data will be uploaded for station id %s" % site_dict['username'])

    def new_archive_record(self, event):
        self.archive_queue.put(event.record)

class WetterThread(weewx.restx.RESTThread):

    _SERVER_URL = 'http://interface.wetterarchiv.de/weather'
    _DATA_MAP = {'hu':  ('outHumidity', '%.0f'), # percent
                 'te':  ('outTemp',     '%.1f'), # C
                 'dp':  ('dewpoint',    '%.1f'), # C
                 'pr':  ('barometer',   '%.1f'), # hPa
                 'wd':  ('windDir',     '%.0f'), # degrees
                 'ws':  ('windSpeed',   '%.1f'), # m/s
                 'wg':  ('windGust',    '%.1f'), # m/s
                 'pa':  ('hourRain',    '%.2f'), # mm
                 'rr':  ('rainRate',    '%.2f'), # mm/hr
                 'uv':  ('UV',          '%.0f'), # uv index
                 'sr':  ('radiation',   '%.2f'), # W/m^2
                 'hui': ('inHumidity',  '%.0f'), # percent
                 'tei': ('inTemp',      '%.1f'), # C
                 'huo': ('extraHumid1', '%.0f'), # percent
                 'teo': ('extraTemp1',  '%.1f'), # C
                 'tes': ('soilTemp1',   '%.1f')  # C
                 }

    def __init__(self, queue, username, password, manager_dict,
                 server_url=_SERVER_URL, skip_upload=False,
                 post_interval=None, max_backlog=sys.maxint, stale=None,
                 log_success=True, log_failure=True,
                 timeout=60, max_tries=3, retry_wait=5):
        super(WetterThread, self).__init__(queue,
                                           protocol_name='Wetter',
                                           manager_dict=manager_dict,
                                           post_interval=post_interval,
                                           max_backlog=max_backlog,
                                           stale=stale,
                                           log_success=log_success,
                                           log_failure=log_failure,
                                           max_tries=max_tries,
                                           timeout=timeout,
                                           retry_wait=retry_wait)
        self.username = username
        self.password = password
        self.server_url = server_url
        self.skip_upload = to_bool(skip_upload)

    def process_record(self, record, dbmanager):
        r = self.get_record(record, dbmanager)
        data = self.get_data(r)
        url = urllib.urlencode(data)
        if self.skip_upload:
            loginf("skipping upload")
            return
        req = urllib2.Request(self.server_url, url)
        req.add_header("User-Agent", "weewx/%s" % weewx.__version__)
        self.post_with_retries(req)

    def check_response(self, response):
        txt = response.read().lower()
        if txt.find('"errorcode":"100"') != -1 or \
           txt.find('"errorcode":"101"') != -1 or \
           txt.find('"errorcode":"102"') != -1:
            raise weewx.restx.BadLogin(txt)
        elif txt.find('"status":"error"') != -1:
            raise weewx.restx.FailedPost("Server returned '%s'" % txt)

    def get_data(self, in_record):
        # put everything into the right units
        record = weewx.units.to_METRICWX(in_record)

        # put data into expected scaling, structure, and format
        values = {}
        values['id'] = self.username
        values['pwd'] = self.password
        values['sid'] = 'weewx'
        values['ver'] = weewx.__version__
        values['dtutc'] = time.strftime('%Y%m%d%H%M', time.gmtime(record['dateTime']))
        for key in self._DATA_MAP:
            rkey = self._DATA_MAP[key][0]
            if record.has_key(rkey) and record[rkey] is not None:
                values[key] = self._DATA_MAP[key][1] % record[rkey]

        return values
