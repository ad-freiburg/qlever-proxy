"""
Copyright 2020, University of Freiburg,
Chair of Algorithms and Data Structures
Author: Hannah Bast <bast@cs.uni-freiburg.de>
"""

import sys
import logging
import http.server
import socket
import urllib3
import urllib.parse
import certifi
import re
import threading
import queue
import time
import yaml


""" Global log """
log = logging.getLogger("proxy logger")
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(asctime)s.%(msecs)03d %(levelname)s  %(message)s", "%Y-%m-%d %H:%M:%S"))
log.addHandler(handler)
   

class Backend:
    """
    Class for asking queries to one or several of the QLever backend.

    NOTE: Also the backend is now HTTPS, so we need to do some additional work
    with certificates here. We also had to do this for eval.py from the QLever
    evaluation, where a first version of this code has been copied from.
    """

    def __init__(self, host, port, base_path, timeout_seconds, backend_id):
        """
        Create HTTP connection pool for asking request to
        http(s):<host>:<port>/<base_path>
        """

        self.host = host
        self.port = port
        self.base_path = base_path
        self.timeout_seconds = timeout_seconds
        self.backend_id = backend_id

        # Values copied from eval.py from the QLever evaluation ... needed here?
        max_pool_size = 4
        self.connection_pool = urllib3.HTTPSConnectionPool(
            self.host, port=self.port, maxsize=max_pool_size,
            timeout=urllib3.util.Timeout(connect=self.timeout_seconds,
                                         read=self.timeout_seconds),
            retries=urllib3.Retry(total=0,
                                  status_forcelist=[404, 503],
                                  backoff_factor=0.1),
            cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

        # Log what we have created.
        log.info("Backend %d: %s:%d%s with timeout %.1fs",
             backend_id, host, port, base_path, timeout_seconds)

    def query(self, query_path, queue, timeout):
        """ 
        Sent a GET request to the QLever backend, with the given path (which
        should always start with a / even if it's a relative path).

        Append the result to the given queue along with the backend id (so that
        we know which result comes from with backend). Note that queue.put
        automatically locks the queue during appending so that appends from
        different threads are serialized and don't interfere with each other.
        """

        log_prefix = "Backend %d:" % self.backend_id
        full_path = self.base_path + query_path
        log.info("%s Sending GET request \"%s\"", log_prefix, full_path
            if len(full_path) < 50 else full_path[:47] + "...")

        headers = urllib3.make_headers(keep_alive=False)  # , fields=params
        try:
            response = self.connection_pool.request(
                           'GET', full_path, headers=headers, timeout=timeout)
            assert(response.status == 200)
            log.debug("%s Response data from backend %d: %s", log_prefix,
                response.data if len(response.data) < 50 \
                              else str(response.data)[:47] + "...")
            # log.debug("Content type  : %s", response.getheader("Content-Type"))
            # log.debug("All headers   : %s", response.getheaders())
            queue.put((response, self.backend_id))
        except socket.timeout as e:
            log.info("%s Timeout (socket.timeout) after %d seconds",
                      log_prefix, self.timeout_seconds)
            queue.put((None, self.backend_id))
        except urllib3.exceptions.ReadTimeoutError as e:
            log.info("%s Timeout (ReadTimeoutError) after %d seconds",
                      log_prefix, self.timeout_seconds)
            queue.put((None, self.backend_id))
        except urllib3.exceptions.MaxRetryError as e:
            log.info("%s Timeout (MaxRetryError) after %d seconds",
                      log_prefix, self.timeout_seconds)
            queue.put((None, self.backend_id))
        except Exception as e:
            log.error("%s Error with request to %s (%s)",
                      log_prefix, self.host, str(e))
            # log.error("%s Headers: %s", log_prefix, response.headers)
            # log.error("%s Data: %s", log_prefix, response.data)
            queue.put((None, self.backend_id))

class QueryProcessor:
    """
    Class for sending a query to one or both backends, each with a timeout.

    If we get a response from the first backend before the specified timeout, we
    take that. Otherwise we take the response from the second backend (as a
    fallback). In the worst case, both backends fail.
    """

    def __init__(self):
        """
        Create the two backends using the class above.
        """
        # The first backend ("primary") has a timeout of 2 seconds.
        self.timeout_single_query = 10.0
        self.timeout_backend_1 = 1.0
        self.timeout_backend_2 = 5.0
        self.backend_1 = Backend("qlever.cs.uni-freiburg.de", 443,
                                 "/api/wikidata",
                                 self.timeout_backend_1, 1)
        # The second backend ("fallback") has a timeout of 5 seconds.
        self.backend_2 = Backend("qlever.cs.uni-freiburg.de", 443,
                                 "/api/wikidata-vulcano",
                                 self.timeout_backend_2, 2)

    def query_first_backend_only(self, path):
        """
        Query only the first backend. Used for commands like clearing the cache
        (there is no point in also clearing the cache of the second backend).
        """
        result_queue = queue.Queue()
        self.backend_1.query(path, result_queue, self.timeout_single_query)
        response, backend_id = result_queue.get()
        return response

    def query_both_backends_in_parallel(self, path_1, path_2):
        """
        Query both backends in parallel with a preference for a result from the
        first backend, as explained above.
        """

        # Send two concurrent requests to the two backends. Each of them will
        # append their response object to the given queue.
        result_queue = queue.Queue()
        thread_1 = threading.Thread(target=self.backend_1.query,
                                    args=(path_1, result_queue, self.timeout_backend_1))
        thread_2 = threading.Thread(target=self.backend_2.query,
                                    args=(path_2, result_queue, self.timeout_backend_2))
        for thread in [thread_1, thread_2]:
            thread.daemon = True
            thread.start()
         
        # Wait until the first of the two backends has responded (note that
        # queue.get is blocking). Then there are three cases:
        # 1. Backend 1 first, with response -> perfect
        # 2. Backend 1 first, no response -> wait for Backend 2
        # 3. Backend 2 first -> wait for Backend 1 and prefer if response
        response, backend_id = result_queue.get()
        if backend_id == 1 and response == None:
            response, backend_id = result_queue.get()
        elif backend_id == 2:
            log.info("Backend 2 responded first -> give Backend 1 a chance, too")
            response_2, backend_id_2 = result_queue.get()
            if response_2 != None:
                response, backend_id = response_2, backend_id_2

        # We now have three cases:
        # 1. We have a response from  backend 1 (best case)
        # 2. No response from backend 1, but from backend 2 (fallback case)
        # 3. No response from either backend (worst case)
        if response != None and backend_id == 1:
            log.info("BEST CASE: Backend 1 responded in time")
        elif response != None and backend_id == 2:
            log.info("FALLBACK: Backend 1 did not respond in time, "
                     "taking result from Backend 2")
        else:
            log.info("WORST CASE: Neither backend responded in time :-(")

        # Return the response
        return response


def MakeRequestHandler(query_processor):
    """
    Returns a RequestHandler class for handling GET requests to the proxy for
    using the given query processor.

    NOTE: This function returns a class ("class factory pattern"). I did this so
    that I can give the class access to a particular QueryProcessor object,
    since the class has to be a subclass of http.server.BaseHTTPRequestHandler,
    I did not see an easier way (except making the QueryProcessor object global,
    which would be a ugly solution, however).
    """

    class RequestHandler(http.server.BaseHTTPRequestHandler):
        """
        Class for handling GET requests to the proxy.
        
        """
    
        def __init__(self, *args, **kwargs):
            """
            NOTE: The call to super().__init__ must come at the end because
            (confusingly) do_GET is called during BaseHTTPRequestHandler.__init__
            """
            self.query_processor = query_processor
            super(RequestHandler, self).__init__(*args, **kwargs)
    
        def log_message(self, format_string, *args):
            """ We do our own logging. """
            log.debug(format_string % args)
    
        def do_GET(self):
            """
            Handle GET request from the caller to the proxy. For SPARQL queries, ask
            both backends, for other requests (e.g. /?cmd=... or /?clear_cache)
            only ask backend 1.

            Measure the total time and log it.
            """

            start_time = time.time()
    
            # Process request.
            path = str(self.path)
            headers = re.sub("\n", " | ", str(self.headers))
            log.info("")
            log.info("Received GET request: %s"
                        % (path if len(path) < 50 else path[:47] + "..."))
            log.debug("Headers: %s" % headers)
    
            # Ask both backends or only the first, depending on the request.
            qp = self.query_processor
            if path.startswith("/?query="):
                # If YAML, only take the first query
                if path.startswith("/?query=yaml"):
                    try:
                        log.info("YAML with two queries, trying to parse it")
                        queries_yaml = urllib.parse.unquote(re.sub("^/\?query=", "", path))
                        queries_yaml = re.sub("\n(LIMIT)", "\n  footer: |-\n\\1", queries_yaml)
                        queries_yaml = re.sub("\n(PREFIX|LIMIT|OFFSET)", "\n    \\1", queries_yaml)
                        log.debug("YAML = \n" + queries_yaml)
                        queries = yaml.safe_load(queries_yaml)["yaml"]
                        log.debug("QUERIES = " + str(queries))
                        query_1 = queries["query_1"] + "\n" + queries["footer"]
                        query_2 = queries["query_2"] + "\n" + queries["footer"]
                        log.info("Query 1: " + re.sub("\s+", " ", query_1)[:30] + "..." + re.sub("\s+", " ", query_1)[-30:])
                        log.info("Query 2: " + re.sub("\s+", " ", query_2)[:30] + "..." + re.sub("\s+", " ", query_2)[-30:])
                        path_1 = "/?query=" + urllib.parse.quote(query_1)
                        path_2 = "/?query=" + urllib.parse.quote(query_2)
                        response = qp.query_both_backends_in_parallel(path_1, path_2)
                    except Exception as e:
                        log.info("\x1b[31mSomething went wrong parsing the YAML (%s)\x1b[0m" % str(e))
                        log.info("YAML = \n" + queries_yaml)
                        response = None
                else:
                    path_1 = path
                    path_2 = path
                    response = qp.query_both_backends_in_parallel(path_1, path_2)
            else:
                response = qp.query_first_backend_only(path)
    
            # If no response, send 404, otherwise forward to caller, including
            # selected headers.
            # NOTE: without the Access-Control-Allow-Origin, the QLever UI will
            # not accept the result.
            if response == None:
                self.send_response(404)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                log.info("\x1b[31mSending 404 to caller\x1b[0m")
            else:
                self.send_response(200)
                headers_preserved = ["Content-Type", "Access-Control-Allow-Origin"]
                for header_key in headers_preserved:
                    header_value = response.getheader(header_key)
                    if header_value != None:
                        self.send_header(header_key, header_value)
                self.end_headers()
                self.wfile.write(response.data)
                log.debug("Forwarded result to caller"
                          " (headers preserved: %s)" % ", ".join(headers_preserved))

            end_time = time.time()
            log.info("\x1b[1mTotal time spend on request: %dms\x1b[0m",
                     int(1000 * (end_time - start_time)))

    # Don't forget to return the class :-)
    return RequestHandler


def server_loop(hostname, port):
    """
    Create a HTTP server that listens and respond to queries for the given
    hostname under the given port, using the request handler above. Runs in an
    infinite loop.
    """

    server_address = (hostname, port)
    query_processor = QueryProcessor()
    request_handler_class = MakeRequestHandler(query_processor)
    server = http.server.HTTPServer(server_address, request_handler_class)
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
