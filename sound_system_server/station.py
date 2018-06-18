import asyncio
import json
import logging
import aiohttp

__all__ = ['Station', 'StationPool']

class Station:
    def __init__(self, server, host, http_port):
        self._server = server
        self._host = host
        self._http_port = int(http_port)
        # The base URL of the station HTTP server
        self._url = 'http://{self.host}:{self.http_port:d}'.format(self=self)
        self._station_data = None
        self._server.gst_process.add_client(host, http_port)

    @property
    def host(self):
        return self._host
    @property
    def http_port(self):
        return self._http_port

    @property
    def stream_data(self):
        return self._stream_data
    @stream_data.setter
    def stream_data(self, data):
        self._stream_data = data

    async def ping(self, absolute_url='/ping'):
        async with aiohttp.ClientSession() as session:
            url = self._url + absolute_url
            async with session.get(url) as resp:
                return resp

    async def post(self, absolute_url, json_payload):
        async with aiohttp.ClientSession() as session:
            url = self._url + absolute_url
            payload = json.dumps(json_payload)
            headers = {'Content-Type': 'application/json'}
            async with session.post(url, data=payload, headers=headers) as resp:
                return resp

    async def send_volume(self, volume):
        '''Change the volume of the station. Use range of 0-100.'''
        print('Updating volume', volume)
        if volume < 0 or volume > 100:
            logging.warning('Invalid volume value: %d', volume)
            volume = max(0, min(100, volume))
        try:
            resp = await self.post('/volume', {'value': volume})
            if not (resp.status < 300):
                logging.warning('Failed to update volume of station %s:%d',
                                self._host,
                                self._http_port)
        except Exception:
            pass

    async def send_stream_data(self):
        if self._stream_data is None:
            return
        try:
            resp = await self.post('/stream-data', self._stream_data)
            if resp.status < 300:
                logging.debug('Stream data sent to station')
        except Exception:
            logging.debug('Stream data failed to be sent')

    def __str__(self):
        return 'Station(host={}, http_port={})'.format(
            self.host,
            self.http_port)

class StationWatcher:
    def __init__(self, server, station, timeout_notify):
        self._server = server
        self._station = station
        self._notify = timeout_notify
        self._running = False

    def start(self):
        self._running = True
        asyncio.ensure_future(self._watch())

    def stop(self):
        self._running = False

    async def _watch(self):
        while self._running:
            try:
                resp = await self._station.ping()
                if resp is None:
                    pass
            except aiohttp.client_exceptions.ClientConnectorError:
                self._notify(self._station.host, self._station.http_port)
            await asyncio.sleep(1)

class StationPool:
    def __init__(self, server):
        self._server = server
        # Stations and watchers are indexed by host:httpport
        self._stations = {}
        self._watchers = {}

    def add_station(self, host, http_port):
        idx = StationPool._station_index(host, http_port)
        station = self._stations.get(idx)
        if station is not None:
            # Known station, just send the stream data
            asyncio.ensure_future(station.send_stream_data())
        else:
            station = Station(self._server, host, http_port)
            watcher = StationWatcher(self._server, station, self._station_timeout_notify)
            watcher.start()
            self._stations[idx] = station
            self._watchers[idx] = watcher
        return station

    def remove_station(self, host, http_port):
        station = self.find(host, http_port)
        if station is not None:
            idx = StationPool._station_index(host, http_port)
            self._watchers[idx].stop()
            del self._stations[idx]
            del self._watchers[idx]
            self._server.gst_process.del_client(host, http_port)

    def find(self, host, http_port):
        for station in self._stations.values():
            if host == station.host and int(http_port) == station.http_port:
                return station
    
    def get_all(self):
        return self._stations.values()

    @staticmethod
    def _station_index(host, http_port):
        return '{0}:{1:d}'.format(host, http_port)

    def _station_timeout_notify(self, host, http_port):
        logging.debug('Station %s:%d timeout', host, http_port)
        self.remove_station(host, http_port)
