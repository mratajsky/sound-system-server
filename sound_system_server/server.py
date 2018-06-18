import asyncio
import logging
import signal
from contextlib import suppress

from .gstreamer import GStreamerProcess
from .web import HTTPServer
from .station import StationPool

class Server:
    def __init__(self, config):
        self._config = config
        self._discovery = None
        self._running = False
        self._host = None
        self._http = HTTPServer(self)
        self._station_pool = StationPool(self)
        self._discovery = None
        self._gst_process = None

    def start(self):
        self._running = True
        loop = asyncio.get_event_loop()
        for signame in ('SIGINT', 'SIGTERM'):
            loop.add_signal_handler(getattr(signal, signame), self.stop)
        try:
            loop.create_task(self._get_valid_ip_address(self._start_server))
            loop.run_forever()
            pending = asyncio.Task.all_tasks()
            for task in pending:
                task.cancel()
                # Await task to execute its cancellation
                with suppress(asyncio.CancelledError):
                    loop.run_until_complete(task)
        finally:
            loop.close()

    def stop(self):
        self._http.stop()
        self._gst_process.stop()
        if self._discovery is not None:
            self._discovery.stop()

        self._running = False
        asyncio.get_event_loop().stop()

    @property
    def config(self):
        return self._config
    @property
    def gst_process(self):
        return self._gst_process
    @property
    def http(self):
        return self._http
    @property
    def station_pool(self):
        return self._station_pool

    def _start_http(self):
        host = self.config.get('http', 'host', fallback=None)
        port = self.config.getint('http', 'port')
        self._http.start(host, port)

    def _start_server(self, ip):
        self._host = ip
        ntp_host = self.config.get('stream', 'ntphost', fallback=None)
        ntp_server = self.config.getboolean('stream', 'useonserver', fallback=False)
        source = self.config.get('stream', 'source', fallback=None)
        self._gst_process = GStreamerProcess(self._host, ntp_host, ntp_server, source)
        self._gst_process.start()
        asyncio.ensure_future(self._gst_pipe_watch())
        self._start_http()
        dnssd = self.config.getboolean('dnssd', 'enable', fallback=False)
        if dnssd:
            from .discovery import DiscoveryServerService
            self._discovery = DiscoveryServerService(self._host, self._http.port)
            self._discovery.start()

    async def _get_valid_ip_address(self, notify):
        from .utils import get_ip_address
        while self._running:
            ip = get_ip_address()
            if ip is not None:
                notify(ip)
                break
            await asyncio.sleep(1)

    async def _gst_pipe_watch(self):
        pipe = self._gst_process.pipe
        while self._running:
            if pipe.poll():
                try:
                    item = pipe.recv()
                except EOFError:
                    # The pipe could be already closed
                    return
                if item.t == GStreamerProcess.MSG_T_STREAM_DATA:
                    station_data, stream_data = item.args
                    station = self.station_pool.find(station_data['host'], station_data['http_port'])
                    if station is None:
                        logging.error('Failed to find station %s:%d',
                                      station_data['host'],
                                      station_data['http_port'])
                        continue
                    logging.debug('Got stream data for station %s:%d',
                                  station_data['host'],
                                  station_data['http_port'])
                    station.stream_data = stream_data
                    await station.send_stream_data()
            await asyncio.sleep(1)
