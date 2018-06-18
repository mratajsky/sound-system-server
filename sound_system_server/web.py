import asyncio
import logging

from aiohttp import web

class HTTPServer:
    # List of GET routes and their handlers
    _ROUTES_GET = (
        {'url': '/station/all', 'handler': 'handle_station_all'},)
    # List of POST routes and their handlers
    _ROUTERS_POST = (
        {'url': '/registration', 'handler': 'handle_registration'},)

    def __init__(self, server):
        self._server = server
        self._web_app = web.Application()
        self._web_server = None
        self._get_handler = HTTPServerGETHandler(self._server)
        self._post_handler = HTTPServerPOSTHandler(self._server)
        self._port = None
        self._setup_routes()

    @property
    def port(self):
        return self._port

    def start(self, host, port, loop=None):
        host = host or '0.0.0.0'
        loop = loop or asyncio.get_event_loop()
        handler = self._web_app.make_handler(access_log=None)
        self._web_server = loop.create_server(handler, host, port)
        self._port = port
        logging.info('Starting HTTP server on %s:%d', host, port)
        loop.create_task(self._web_server)

    def stop(self):
        if self._web_server is not None:
            self._web_server.close()
            self._web_server = None
            logging.info('HTTP server stopped')

    def _setup_routes(self):
        router = self._web_app.router
        # Install GET routes
        for route in self._ROUTES_GET:
            router.add_get(route['url'],
                           getattr(self._get_handler, route['handler']))
        # Install POST routes
        for route in self._ROUTERS_POST:
            router.add_post(route['url'],
                            getattr(self._post_handler, route['handler']))

class HTTPServerGETHandler:
    def __init__(self, server):
        self._server = server

    async def handle_station_all(self, req):
        '''Returns data about all speakers connected.'''
        body = []
        for station in self._server.station_pool.get_all():
            body.append({'host': station.host, 'port': station.http_port})
        return web.json_response({'data': body})

class HTTPServerPOSTHandler:
    _REG_FIELDS = ('http_port',)

    def __init__(self, server):
        self._server = server

    async def handle_registration(self, req):
        '''Handle station registration.'''
        if req.has_body and req.content_type == 'application/json':
            data = await req.json()
            peername = req.transport.get_extra_info('peername')
            if peername is None:
                logging.warning('Could not identify peer of a registering station')
                return web.HTTPInternalServerError()
            if self._process_registration(peername[0], data):
                return web.HTTPNoContent()
            else:
                return web.HTTPBadRequest(text='Invalid registration data')
        else:
            return web.HTTPBadRequest(text='JSON location data required')

    def _process_registration(self, host, json):
        try:
            # Make sure the registration has all and only the required fields
            data = {key: json[key] for key in self._REG_FIELDS}
            data['host'] = host
            station = self._server.station_pool.add_station(**data)
            logging.info('Registered station %s:%d', station.host, station.http_port)
        except KeyError:
            logging.warning('Incomplete registration request form station %s', host)
            return False
        return True
