# Sound System: server component

Server application capable of on-demand RTP streaming from a GStreamer source to
the connected station components.

## Dependencies

The application requires Python 3.5 or higher to be installed with the following
modules:

* glib
* gstreamer (Gst and GstNet libraries)
* zeroconf (when server discovery is enabled in the configuration file)
