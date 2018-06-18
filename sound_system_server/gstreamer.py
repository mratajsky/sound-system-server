import asyncio
import collections
import gi
import logging
import multiprocessing
import re
from collections import defaultdict

gi.require_version('Gst', '1.0')
gi.require_version('GstNet', '1.0')
from gi.repository import GLib, Gst, GstNet

Gst.init(None)

Message = collections.namedtuple('Message', ['t', 'args'])
Message.__new__.__defaults__ = ([],)

class GStreamerProcess:
    # Message types
    MSG_T_ADD_CLIENT = 0            # Add a client to the stream
    MSG_T_DEL_CLIENT = 1            # Remove a client from the stream
    MSG_T_STOP_LOOP = 2             # Stop the main loop
    MSG_T_STREAM_DATA = 3           # Stream data for the client

    def __init__(self, host, ntp_host=None, ntp_server=False, source=None):
        parent_pipe, child_pipe = multiprocessing.Pipe()
        self._pipe = parent_pipe
        self._process = multiprocessing.Process(
            target=self._entry,
            args=(child_pipe, host, ntp_host, ntp_server, source))
        self._loop = None
        self._gstreamer = None

    @property
    def pipe(self):
        return self._pipe

    def start(self):
        if not self._process.is_alive():
            logging.debug('Starting gstreamer process')
            self._process.start()

    def stop(self):
        if self._process.is_alive():
            logging.debug('Stopping gstreamer process')
            message = Message(t=self.MSG_T_STOP_LOOP)
            self._pipe.send(message)
            self._process.join()

    def add_client(self, host, http_port):
        message = Message(t=self.MSG_T_ADD_CLIENT, args=(host, http_port))
        self._pipe.send(message)

    def del_client(self, host, http_port):
        message = Message(t=self.MSG_T_DEL_CLIENT, args=(host, http_port))
        self._pipe.send(message)

    def _ipc_watch(self):
        if self._pipe.poll():
            item = self._pipe.recv()
            if item.t == GStreamerProcess.MSG_T_ADD_CLIENT:
                self._gstreamer.add_client(*item.args)
            elif item.t == GStreamerProcess.MSG_T_DEL_CLIENT:
                self._gstreamer.del_client(*item.args)
            elif item.t == GStreamerProcess.MSG_T_STOP_LOOP:
                self._loop.quit()
        # Return True so that this function is run again
        return True

    def _entry(self, pipe, host, ntp_host=None, ntp_server=False, source=None):
        self._pipe = pipe
        # Create a GLib main loop to be used by the gstreamer pipeline
        self._loop = GLib.MainLoop()
        try:
            self._gstreamer = GStreamer(host, ntp_host, ntp_server, source)
            self._gstreamer.data_notify = self._gstreamer_data_notify
            self._gstreamer.start()
            # Check the IPC pipe every second
            GLib.timeout_add_seconds(1, self._ipc_watch)
            self._loop.run()
        except (KeyboardInterrupt, SystemExit):
            self._loop.quit()
        except RuntimeError:
            logging.exception('Failed to setup the gstreamer pipeline')

    def _gstreamer_data_notify(self, station_data, stream_data):
        message = Message(
            t=self.MSG_T_STREAM_DATA,
            args=(station_data, stream_data))
        self._pipe.send(message)

class GStreamer:
    def __init__(self, host, ntp_host=None, ntp_server=False, source=None):
        self._host = host
        self._ntp_host = ntp_host
        self._ntp_server = ntp_server
        self._rtcp_port = 9999
        self._station_port = 10000
        self._station_data = {}
        self._station_host_count = defaultdict(int)
        self._source = source or 'audiotestsrc'
        self._pipeline = None
        self._data_notify = None
        self._caps = None
        self._sink_rtp = None
        self._sink_rtcp = None

    def start(self):
        if not self.running:
            if not self._build_stream():
                return False
            logging.debug('Starting stream')
            self._pipeline.set_state(Gst.State.PLAYING)
        return True

    def stop(self):
        if self.running:
            logging.debug('Stopping stream')
            self._pipeline.set_state(Gst.State.NULL)
            self._pipeline = None

    @property
    def running(self):
        return self._pipeline and self._pipeline.target_state == Gst.State.PLAYING

    @property
    def data_notify(self):
        return self._data_notify
    @data_notify.setter
    def data_notify(self, notify):
        self._data_notify = notify

    @property
    def host(self):
        return self._host
    @property
    def source(self):
        return self._source

    def add_client(self, host, http_port):
        ident = self._station_ident(host, http_port)
        rtp_port = self._station_port + self._station_host_count[host]
        logging.debug('Adding client %s:%d RTP port %d', host, http_port, rtp_port)
        station_data = {
            'host': host,
            'http_port': http_port,
            'rtp_port': rtp_port,
            'caps': None
        }
        self._station_data[ident] = station_data
        self._station_host_count[host] += 1
        # Add station to both multiudpsinks
        self._sink_rtp.emit('add', host, rtp_port)
        self._sink_rtcp.emit('add', host, self._rtcp_port)

        # Inform the parent about the station and stream data
        self._data_notify(self._station_data[ident], self._stream_data(ident))

    def del_client(self, host, http_port):
        ident = self._station_ident(host, http_port)
        if ident not in self._station_data:
            logging.debug('Not removing invalid client %s:%d', host, http_port)
            return

        logging.debug('Removing client %s:%d', host, http_port)

        # Remove station from both multiudpsinks, the host counter is not
        # decreased here to avoid port clashing with new streams
        rtp_port = self._station_data[ident]['rtp_port']
        self._sink_rtp.emit('remove', host, rtp_port)
        self._sink_rtcp.emit('remove', host, self._rtcp_port)

        del self._station_data[ident]

    def _build_stream(self):
        self._pipeline = Gst.Pipeline.new('pipeline')
        if self._ntp_host and self._ntp_server:
            logging.debug('Pipeline NTP server: %s:%d', self._ntp_host, 123)
            clock = GstNet.NtpClock.new('mrs-cup-cake-clock', self._ntp_host, 123, 0)
            self._pipeline.use_clock(clock)
        rtpbin = Gst.ElementFactory.make('rtpbin', 'rtpbin')
        if rtpbin is None:
            logging.error('Failed to create element rtpbin')
            return False
        rtpbin.set_property('rtp-profile', 'savpf')
        rtpbin.connect('pad-added', self._on_pad_added)
        self._pipeline.add(rtpbin)

        # Parse the source description and add the element to the pipeline
        parts = self._source.split()
        if not parts:
            logging.error('Invalid stream source')
            return False
        source = Gst.ElementFactory.make(parts[0])
        if source is None:
            logging.error('Failed to create element {}'.format(parts[0]))
            return False
        for part in parts[1:]:
            key, value = part.split('=', 1)
            if value.isdigit():
                value = int(value)
            source.set_property(key, value)
        self._pipeline.add(source)
        last = source

        # Add the stable elements
        elements = ('queue', 'audioconvert', 'audioresample', 'opusenc', 'rtpopuspay')
        for name in elements:
            element = Gst.ElementFactory.make(name)
            if element is None:
                logging.error('Failed to create element {}'.format(name))
                return False
            self._pipeline.add(element)
            last.link(element)
            last = element

        # Connect the RTP input
        pad1 = last.get_static_pad('src')
        pad2 = rtpbin.get_request_pad('send_rtp_sink_0')
        Gst.Pad.link(pad1, pad2)

        pad1 = rtpbin.get_request_pad('send_rtcp_src_0')
        sink = Gst.ElementFactory.make('multiudpsink', 'udp_rtcpsink_0')
        if sink is None:
            logging.error('Failed to create element multiudpsink')
            return False
        sink.set_property('sync', False)
        sink.set_property('async', False)
        self._pipeline.add(sink)
        self._sink_rtcp = sink
        Gst.Pad.link(pad1, sink.get_static_pad('sink'))

        src = Gst.ElementFactory.make('udpsrc')
        if src is None:
            logging.error('Failed to create element udpsrc')
            return False
        # This is the local port where server receives RTCP messages, it
        # should be the same as the outgoing RTCP port
        src.set_property('port', self._rtcp_port)
        pad1 = src.get_static_pad('src')
        pad2 = rtpbin.get_request_pad('recv_rtcp_sink_0')
        self._pipeline.add(src)
        Gst.Pad.link(pad1, pad2)

        # Connect to the GST bus to receive messages about state changes
        # and errors
        bus = self._pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect('message::error', self._on_bus_error)
        bus.connect('message::state-changed', self._on_bus_state_changed)
        return True

    def _on_pad_added(self, _, pad):
        if pad.get_name() == 'send_rtp_src_0':
            # Create the RTP stream
            self._sink_rtp = Gst.ElementFactory.make('multiudpsink', None)
            self._pipeline.add(self._sink_rtp)
            Gst.Pad.link(pad, self._sink_rtp.get_static_pad('sink'))

    def _on_bus_error(self, _, message):
        err, _ = message.parse_error()
        logging.error('Stream error: %s', err)
        self.stop()

    def _on_bus_state_changed(self, _, message):
        # Only care about state changes of the sink
        if message.src != self._sink_rtp:
            return
        prev, new, _ = message.parse_state_changed()
        logging.debug('Pipeline state changed: %s -> %s (%s)',
                      Gst.Element.state_get_name(prev),
                      Gst.Element.state_get_name(new),
                      message.src.name)
        if not self._caps and new == Gst.State.PAUSED:
            #
            # Fill in the dynamic caps which normally become known when
            # the PAUSED state is reached and notify the station.
            #
            for pad in message.src.pads:
                caps = pad.get_current_caps()
                if caps is not None:
                    logging.debug('Read capabilities from pipeline sink: %s', caps.to_string())
                    self._caps = caps.to_string()
                    break

    def _station_ident(self, host, http_port):
        '''Create a string identifying station.'''
        return '{0:s}:{1:d}'.format(host, http_port)

    def _stream_data(self, ident):
        '''Create data structure with stream metadata.'''
        pipeline = ('rtpbin name=rtpbin buffer-mode=0 ntp-sync=yes ntp-time-source=3 '
                    'udpsrc caps="{caps}"  port={rtp_port} ! '
                    'rtpbin.recv_rtp_sink_0 rtpbin. ! '
                    'rtpopusdepay ! '
                    'opusdec plc=true ! '
                    'volume name=volume ! '
                    'autoaudiosink '
                    'udpsrc port={rtcp_recv_port} ! '
                    'rtpbin.recv_rtcp_sink_0 '
                    'rtpbin.send_rtcp_src_0 ! '
                    'udpsink host={host} port={rtcp_send_port} sync=false async=false')
        stream_data = {
            'pipeline': pipeline.format(
                caps=str(self._caps),
                host=str(self._host),
                rtp_port=str(self._station_data[ident]['rtp_port']),
                rtcp_recv_port=str(self._rtcp_port),
                rtcp_send_port=str(self._rtcp_port)
            ),
            'volume_name': 'volume'
        }
        if self._ntp_host:
            stream_data['clock'] = {'host': self._ntp_host}
        return stream_data
