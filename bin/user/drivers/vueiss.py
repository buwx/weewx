##
##This program is free software; you can redistribute it and/or modify it under
##the terms of the GNU General Public License as published by the Free Software
##Foundation; either version 2 of the License, or (at your option) any later
##version.
##
##This program is distributed in the hope that it will be useful, but WITHOUT 
##ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
##FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
##details.
"""Driver for Vantage Vue @ DIY datalogger"""

from __future__ import with_statement
import time
import syslog
import MySQLdb
import Queue

from math import sin, cos, pi, acos, pow, exp, log
from numpy import array
from numpy.linalg import norm

import weecfg
import weedb
import weeutil.weeutil
import weewx.drivers
import weewx.manager

def logmsg(msg):
    syslog.syslog(syslog.LOG_INFO, 'vueiss: %s' % msg)

POLYNOMIAL = 0x1021
PRESET = 0

def _initial(c):
    crc = 0
    c = c << 8
    for _ in range(8):
        if (crc ^ c) & 0x8000:
            crc = (crc << 1) ^ POLYNOMIAL
        else:
            crc = crc << 1
        c = c << 1
    return crc

_tab = [ _initial(i) for i in range(256) ]

def _update_crc(crc, c):
    cc = 0xff & c

    tmp = (crc >> 8) ^ cc
    crc = (crc << 8) ^ _tab[tmp & 0xff]
    crc = crc & 0xffff

    return crc

# Calculates the crc
def crc(data):
    crc = PRESET
    try:
        for idx in range(2, 10):
            crc = _update_crc(crc, int(data[idx], 16))
    except:
        pass
    return crc

# Calculates the saturation vapour pressure (see dwd)
def calc_svp(temperature):
    if not temperature:
        return None
    
    C1   = 6.10780
    C2_P = 17.08085
    C2_N = 17.84362
    C3_P = 234.175
    C3_N = 245.425

    C2 = C2_P if temperature >= 0 else C2_N
    C3 = C3_P if temperature >= 0 else C3_N

    return C1 * exp(C2 * temperature / (C3 + temperature))

# Calculates the dewpoint (see dwd)
def calc_dewpoint(temperature, humidity):
    if not temperature or not humidity:
        return None

    C1   = 6.10780
    C2_P = 17.08085
    C2_N = 17.84362
    C3_P = 234.175
    C3_N = 245.425

    C2 = C2_P if temperature >= 0 else C2_N
    C3 = C3_P if temperature >= 0 else C3_N

    svp = calc_svp(temperature)

    tmp = log(0.01 * humidity * svp / C1)
    dt = C3 * tmp / (C2 - tmp)

    if temperature >= 0 and dt < 0.0:
        dt = C3_N * tmp / (C2_N - tmp)

    if dt > temperature:
        dt = temperature

    return round(dt, 1)

# wind data 1-min-average
class WindData(object):

    def __init__(self):
        self.windVektor = array([0.0, 0.0])
        self.windSpeed = 0
        self.windCount = 0

    def reset(self):
        self.windVektor = array([0.0, 0.0])
        self.windSpeed = 0
        self.windCount = 0

    def add(self, data):
        speed = int(data[3], 16) * 0.44704
        direction = (int(data[4], 16) << 2) | (int(data[6], 16) & 0x02)
        direction = 360 if direction > 1024 or direction <= 0 else int(round(direction * 360.0 / 1024.0))  
        rad = pi * (90.0 - direction) / 180.0

        self.windSpeed += speed
        self.windVektor += (speed * array([cos(rad), sin(rad)]))
        self.windCount += 1

    def get(self):
        if self.windCount == 0:
            return None

        direction = None
        length = norm(self.windVektor)
        speed = self.windSpeed / self.windCount
        if speed > 0 and length > 0.01:
            rad = acos(self.windVektor[0] / length)
            if self.windVektor[1] < 0:
                rad = 2*pi - rad

            direction = 90.0 - 180.0 * rad / pi
            if direction < 0:
                direction += 360.0

        return [speed, direction]

# wind data floating N-min-average
class WindDataN(object):

    def __init__(self, N):
        self.N = N
        self.pos = 0
        self.array = [WindData() for _ in range(self.N)]

    def reset(self):
        self.pos = (self.pos + 1) % self.N
        self.array[self.pos].reset()

    def add(self, data):
        for i in range(self.N):
            self.array[i].add(data)

    def get(self):
        return self.array[(self.pos + 1) % self.N].get()

# rain data
class RainData(object):

    def __init__(self):
        self.rainSum = 0
        self.rainTicks = None

    def reset(self):
        self.rainSum = 0

    def add(self, data):
        ticks = int(data[5], 16) & 0x7f
        if self.rainTicks != None and ticks > self.rainTicks:
            self.rainSum += (ticks - self.rainTicks) * 0.2001
            self.rainTicks = ticks
        elif self.rainTicks != None and ticks < self.rainTicks:
            self.rainSum += (128 + ticks - self.rainTicks) * 0.2001
            self.rainTicks = ticks
        self.rainTicks = ticks

    def get(self):
        return self.rainSum

# temperatur data 1-min-average
class TemperatureData(object):

    def __init__(self):
        self.temp = 0
        self.tempCount = 0

    def reset(self):
        self.temp = 0
        self.tempCount = 0

    def add(self, data):
        value = int(data[5], 16) * 256 + int(data[6], 16)
        value = value - 65536 if value > 32767 else value
        tempAkt = (value/160.0 - 32.0)*5.0/9.0
        self.temp += tempAkt
        self.tempCount += 1

    def get(self):
        return self.temp / self.tempCount if self.tempCount > 0 else None

# temperatur data floating N-min-average
class TemperatureDataN(object):

    def __init__(self, N):
        self.N = N
        self.pos = 0
        self.array = [TemperatureData() for _ in range(self.N)]

    def reset(self):
        self.pos = (self.pos + 1) % self.N
        self.array[self.pos].reset()

    def add(self, data):
        for i in range(self.N):
            self.array[i].add(data)

    def get(self):
        return self.array[(self.pos + 1) % self.N].get()

# humity data 1-min-average
class HumityData(object):

    def __init__(self):
        self.humidity = 0
        self.humidityCount = 0

    def reset(self):
        self.humidity = 0
        self.humidityCount = 0

    def add(self, data):
        value = ((int(data[6], 16) >> 4) << 8) + int(data[5], 16)
        humidityAkt = value * 1.01 / 10.0
        humidityAkt = 100 if humidityAkt > 100 else humidityAkt
        self.humidity += humidityAkt
        self.humidityCount += 1

    def get(self):
        return self.humidity / self.humidityCount if self.humidityCount > 0 else None

# humity data floating N-min-average
class HumityDataN(object):

    def __init__(self, N):
        self.N = N
        self.pos = 0
        self.array = [HumityData() for _ in range(self.N)]

    def reset(self):
        self.pos = (self.pos + 1) % self.N
        self.array[self.pos].reset()

    def add(self, data):
        for i in range(self.N):
            self.array[i].add(data)

    def get(self):
        return self.array[(self.pos + 1) % self.N].get()

# wind gust data
class WindGustData(object):

    def __init__(self):
        self.windGust = 0
        self.windGustCount = 0

    def reset(self):
        self.windGust = 0
        self.windGustCount = 0

    def add(self, data):
        _windGust = int(data[5], 16) * 0.44704
        if _windGust > self.windGust:
            self.windGust = _windGust
        self.windGustCount += 1

    def get(self):
        return self.windGust if self.windGustCount > 0 or self.windGust > 0 else None

# barometer data
class BarometerData(object):

    def __init__(self, height):
        self.height = height
        self.barometer = 0
        self.barometerCount = 0

    def reset(self):
        self.barometer = 0
        self.barometerCount = 0

    def add(self, data):
        if data[0] == 'A':
            self.barometer += float(data[4])/100.0
        else:
            self.barometer += pow(pow(float(data[4])/100.0, 0.1902614) + 8.417168e-05 * self.height, 5.255927)
        self.barometerCount += 1

    def get(self):
        return self.barometer / self.barometerCount if self.barometerCount > 0 else None

# barometer floating N-min-average
class BarometerDataN(object):

    def __init__(self, height, N):
        self.N = N
        self.pos = 0
        self.array = [BarometerData(height) for _ in range(self.N)]

    def reset(self):
        self.pos = (self.pos + 1) % self.N
        self.array[self.pos].reset()

    def add(self, data):
        for i in range(self.N):
            self.array[i].add(data)

    def get(self):
        return self.array[(self.pos + 1) % self.N].get()

class StationParser(object):

    def __init__(self):
        # initialize data objects
        self.barometer_data = BarometerDataN(310.8, 10)
        self.wind_data = WindDataN(10)
        self.rain_data = RainData()
        self.temp_data = TemperatureDataN(5)
        self.humidy_data = HumityDataN(5)
        self.gust_data = WindGustData()
        self.packet_time = None

    def parse(self, data, data_time):
        if not data or (data[0] != 'A' and data[0] != 'B' and data[0] != 'I'):
            return None

        if data[0] == 'A' or data[0] == 'B':
            self.barometer_data.add(data)
            new_packet_time = data_time - data_time % 60
            if self.packet_time != new_packet_time:
                self.packet_time = new_packet_time

                packet = {}
                packet['dateTime'] = self.packet_time

                _barometer = self.barometer_data.get()
                packet['barometer'] = round(_barometer, 1) if _barometer else None

                wind_info = self.wind_data.get()
                _windSpeed = wind_info[0] if wind_info else None
                packet['windSpeed'] = round(_windSpeed, 2) if _windSpeed else None
                _windSpeed = wind_info[1] if wind_info else None
                packet['windDir'] = round(_windSpeed, 0) if _windSpeed else None

                _windGust = self.gust_data.get()
                packet['windGust'] = round(_windGust, 2) if _windGust else None

                packet['rain'] = self.rain_data.get()

                _outTemp = self.temp_data.get()
                packet['outTemp'] = round(_outTemp, 1) if _outTemp else None

                _outHumidity = self.humidy_data.get()
                packet['outHumidity'] = round(_outHumidity, 0) if _outHumidity else None

                packet['dewpoint'] = calc_dewpoint(_outTemp, _outHumidity)

                # reset data
                self.barometer_data.reset()
                self.wind_data.reset()
                self.rain_data.reset()
                self.temp_data.reset()
                self.gust_data.reset()
                self.humidy_data.reset()

                return packet
            return None

        if crc(data) == 0:
            self.wind_data.add(data)

            sensor_id = StationParser.sensor(data)
            if sensor_id == 'N':
                self.rain_data.add(data)

            elif sensor_id == 'T' :
                self.temp_data.add(data)

            elif sensor_id == 'G' :
                self.gust_data.add(data)

            elif sensor_id == 'H':
                self.humidy_data.add(data)

        return None

    @staticmethod
    def sensor(data):
        # returns the sensor id
        if not data or len(data) < 10 or len(data[2]) < 1:
            return None

        ch = data[2][0]
        if ch == '2':
            return 'V'
        if ch == '5':
            return 'R'
        if ch == '7':
            return 'S'
        if ch == '8':
            return 'T'
        if ch == '9':
            return 'G'
        if ch == 'A':
            return 'H'
        if ch == 'E':
            return 'N'

        return 'I'

DRIVER_NAME = 'VueISS'
DRIVER_VERSION = "2.3"

def loader(config_dict, engine):

    station = VueISS(config_dict)    
    return station

class VueISS(weewx.drivers.AbstractDevice):
    """Vantage Vue @ Meteostick database"""

    def __init__(self, config_dict):
        """Initialize the station        
        """

        self.parser = StationParser()

        self.config_dict = config_dict

        self.the_time = 0
        self.old_time = 0

        self.packets = Queue.Queue()

        with weewx.manager.open_manager_with_config(self.config_dict, 'wx_binding') as dbmanager:
            with weedb.Transaction(dbmanager.connection) as cursor:
                cursor.execute("SELECT dateTime FROM last_sensor") 
                for row in cursor:
                   self.the_time = int(row[0])

                DELTA = 600000
                if self.the_time - DELTA >= 0:
                    cursor.execute("SELECT dateTime,data FROM sensor WHERE dateTime>=%d AND dateTime<=%d ORDER BY dateTime ASC LIMIT 500" % (self.the_time - DELTA, self.the_time))
                    for (data_time, strdata) in cursor:
                        data = strdata.split()
                        self.parser.parse(data, data_time/1000)

                logmsg("Starting with %d" % (self.the_time/1000))

    def genLoopPackets(self):

        while True:
            with weewx.manager.open_manager_with_config(self.config_dict, 'wx_binding') as dbmanager:
                with weedb.Transaction(dbmanager.connection) as cursor:
                    cursor.execute("SELECT dateTime,data FROM sensor WHERE dateTime>%d ORDER BY dateTime ASC LIMIT 5000" % (self.the_time))
                    for (self.the_time, strdata) in cursor:
                        data = strdata.split()
                        values = self.parser.parse(data, self.the_time/1000)
                        if values:
                            packet = {'usUnits' : weewx.METRICWX }
                            packet.update(values)
                            self.packets.put(packet)
                            logmsg("Yield packet (%d)" % (self.the_time/1000))
                            yield packet

                    if self.old_time != self.the_time:
                        self.old_time = self.the_time
                        logmsg("Remember last timestamp %d" % (self.old_time))
                        cursor.execute("UPDATE last_sensor SET dateTime=%d" % (self.old_time))

            time.sleep(15.0)

    def genArchiveRecords(self, lastgood_ts):
        while not self.packets.empty():
            packet = self.packets.get()
            if packet['dateTime'] > lastgood_ts:
                packet['interval'] = 1
                yield packet

    @property
    def hardware_name(self):
        return "Davis Vue ISS"

    # The driver archive interval is 60 seconds.
    @property
    def archive_interval(self):
        return 60

    def getTime(self):
        return self.the_time/1000

def confeditor_loader():
    return VueISSConfEditor()

class VueISSConfEditor(weewx.drivers.AbstractConfEditor):
    @property
    def default_stanza(self):
        return """
[VueISS]
    # This section is for the weewx vue iss weather station

    # The driver to use:
    driver = user.drivers.vueiss
"""

if __name__ == "__main__":
    station = VueISS()
    for packet in station.genLoopPackets():
        print weeutil.weeutil.timestamp_to_string(packet['dateTime']), packet
