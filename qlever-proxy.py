"""
Copyright 2020, University of Freiburg,
Chair of Algorithms and Data Structures
Author: Hannah Bast <bast@cs.uni-freiburg.de>
"""

import sys
import logging
import http.server
import urllib3
import re

from urllib3 import HTTPSConnectionPool, Retry, make_headers
from urllib3.util import Timeout
from urllib3.exceptions import MaxRetryError
import certifi


""" Global log """
log = logging.getLogger("proxy logger")
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s  %(message)s", "%Y-%m-%d %H:%M:%S"))
log.addHandler(handler)
   

class QLeverBackend:
    """
    Class for asking queries to one or several of the QLever backend.

    NOTE: Also the backend is now HTTPS, so we need to do some additional work
    with certificates here. We also had to do this for eval.py from the QLever
    evaluation, where a first version of this code has been copied from.
    """

    def __init__(self, host, port, base_path):
        """
        Create HTTP connection pool for asking request to
        http(s):<host>:<port>/<base_path>
        """

        self.host = host
        self.port = port
        self.base_path = base_path

        # Retry on 404 and 503 messages because these seem to happen sometimes,
        # but very rarely.
        # TODO: do we need this here? I copied it from eval.py from the Qlever
        # evaluation
        retry = Retry(total=0, status_forcelist=[404, 503], backoff_factor=0.1)
        # Set timeout object
        timeout_seconds = 5
        timeout = Timeout(connect=timeout_seconds, read=timeout_seconds)
        # What exactly is a connection pool? Why do we need a pool? The number 4
        # comes from eval.py
        max_pool_size = 4
        self.connection_pool = HTTPSConnectionPool(
            self.host, port=self.port, maxsize=max_pool_size,
            timeout=Timeout(connect=timeout_seconds, read=timeout_seconds),
            retries=Retry(total=0, status_forcelist=[404, 503], backoff_factor=0.1),
            cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
        log.info("Will be forwarding requests to %s:%d%s" % (host, port, base_path))

    def query(self, query_path):
        """ 
        Sent a GET request to the QLever backend, with the given path.

        NOTE: The path is the part after GET starting with /
        """

        # params = { "cmd": "clearcachecomplete" if clearPinned else "clearcache" }
    
        full_path = self.base_path + query_path
        log.info("Sending GET request to backend: %s" % full_path)

        headers = make_headers(keep_alive=False)  # , fields=params
        try:
            response = self.connection_pool.request('GET', full_path, headers=headers)
            assert(response.status == 200)
            log.debug("Response data : %s", response.data \
                if len(response.data) < 50 else str(response.data)[:47] + "...")
            log.debug("Content type  : %s", response.getheader("Content-Type"))
            log.debug("All headers   : %s", response.getheaders())
            return response
        except Exception as e:
            log.error("Error with request to %s" % self.host)
            log.error("Headers: %s.", response.headers)
            log.error("Data: %s.", response.data)
            return None


class RequestHandler(http.server.BaseHTTPRequestHandler):
    """
    Class for handling GET requests to the proxy. Currently all requests are
    simply forwarded to a QLever backend.
    """

    def __init__(self, request, client_address, server):
        """
        NOTE: BaseHTTPRequestHandler has three arguments, which need to be
        repeated here if we want to do additional stuff in the __init__. Also
        note that the call to super().__init__ must come at the end because
        (confusingly) do_GET is called during BaseHTTPRequestHandler.__init__
        """

        self.qlever = QLeverBackend("qlever.cs.uni-freiburg.de", 443, "/api/wikidata")
        super().__init__(request, client_address, server)

    def log_message(self, format_string, *args):
        """ We do our own logging. """

        log.debug(format_string % args)


    def do_GET(self):
        # Process request.
        path = str(self.path)
        headers = re.sub("\n", " | ", str(self.headers))
        log.info("")
        log.info("Received GET request: %s" % path)
        log.debug("Headers: %s" % headers)

        # Send request to QLever backend. Returns None if something went wrong,
        # otherwise tuple of data and content type.
        response = self.qlever.query(path)

        # CASE 1: No response, send 404.
        if response == None:
            self.send_response(404)
            self.end_headers()
            log.info("Proxy sent 404")
        # CASE 2: Proper response received, just forward it.
        else:
            self.send_response(200)
            # Forward selected headers.
            # NOTE: without the Access-Control-Allow-Origin, the QLever UI will
            # not accept the result.
            headers_preserved = ["Content-Type", "Access-Control-Allow-Origin"]
            for header_key in headers_preserved:
                header_value = response.getheader(header_key)
                if header_value != None:
                    self.send_header(header_key, header_value)
            self.end_headers()
            self.wfile.write(response.data)
            log.info("Forwarded result from backend to caller"
                    ", headers preserved: %s" % ", ".join(headers_preserved))


def server_loop(hostname, port):
    """
    Create a HTTP server that listens and respond to queries for the given
    hostname under the given port, using the request handler above. Runs in an
    infinite loop.
    """

    server_address = (hostname, port)
    request_handler = RequestHandler
    server = http.server.HTTPServer(server_address, request_handler)
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
