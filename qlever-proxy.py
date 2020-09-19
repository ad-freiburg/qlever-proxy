"""
Copyright 2020, University of Freiburg,
Chair of Algorithms and Data Structures
Author: Hannah Bast <bast@cs.uni-freiburg.de>
"""

import sys
import logging
import http.server
import urllib
import re

""" Global log """
log = logging.getLogger("proxy logger")
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s  %(message)s", "%Y-%m-%d %H:%M:%S"))
log.addHandler(handler)
   

class RequestHandler(http.server.BaseHTTPRequestHandler):
    """
    Class for handling GET requests to the proxy. The requests come from the
    QLever UI have the form ...
    """

    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        path = str(self.path)
        headers = re.sub("\n", " | ", str(self.headers))
        log.info("GET request: %s" % path)
        log.info("Headers: %s" % headers)
        self._set_response()
        self.wfile.write("GET request for {}".format(self.path).encode('utf-8'))


def server_loop(hostname, port):
    """
    Create a HTTP server that listens and respond to queries for the given
    hostname under the given port, using the request handler above. Runs in an
    infinite loop.
    """

    server_address = (hostname, port)
    server = http.server.HTTPServer(server_address, RequestHandler)
    log.info("Listening to GET requests on %s:%d" % (hostname, port))
    server.serve_forever()


if __name__ == "__main__":
    # Parse command line arguments + usage info.
    if len(sys.argv) != 2:
        print("Usage: python3 qlever-proxy.py <port>")
        sys.exit(1)
    port = int(sys.argv[1])

    # Listen and respond to queries at that port, no matter to which hostname on
    # this machine the were directed.
    server_loop("0.0.0.0", port)
