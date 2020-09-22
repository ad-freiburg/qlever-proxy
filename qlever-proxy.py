"""
Copyright 2020, University of Freiburg,
Chair of Algorithms and Data Structures
Author: Hannah Bast <bast@cs.uni-freiburg.de>
"""

import argparse
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
import json


""" Global log """
log = logging.getLogger("proxy logger")
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(asctime)s.%(msecs)03d %(levelname)s  %(message)s", "%Y-%m-%d %H:%M:%S"))
log.addHandler(handler)
   

class QleverNameService:
    """
    Class for extending a SPARQL Query by adding name variables and triples. We
    use the following terminology:

    An "id variable" is a variable which stands for entities which have a name
    (via the given name predicate, e.g. @en@rdfs:label).

    A "name variable" is a variable which stand for literals which are names of
    a corresponding id variable.

    A "name triple" is a triple of the form <is variable> <name predicate> <name
    variable>.
    """

    def __init__(self, backend,
            name_predicate, name_predicate_prefix,
            var_suffix_id, var_suffix_name, addition_mode):
        """
        Create with given backend. For the meaning of the other three arguments,
        see the doctest for enhance_query below.
        """
        self.backend = backend
        self.name_predicate = name_predicate
        self.name_predicate_prefix = name_predicate_prefix
        self.var_suffix_id = var_suffix_id
        self.var_suffix_name = var_suffix_name
        self.addition_mode = addition_mode

        # Which of the two variable types (id and name) are we renaming?
        #
        # Note: if we do not rename id vars, we have to rename name vars, since
        # we cannot have the same name for both.
        self.rename_id_vars = var_suffix_id != ""
        self.rename_name_vars = var_suffix_name != "" or var_suffix_id == ""
    
    def get_query_parts(self, sparql_query):
        """
        Split a SPARQL query into parts. Returns the variables from the SELECT
        clause, the query without the final part (after the last closing curly
        brace), and the final part. Note that for this test, it's not important
        that the SPARQL query is syntactically correct and no backend is needed.

        >>> log.setLevel(logging.ERROR)
        >>> qns = QleverNameService(None, None, None, None, None, None)
        >>> parts = qns.get_query_parts(
        ...     " PREFIX a: <bla>  PREFIX bc: <http://y> \\n"
        ...     "SELECT ?x_  ( COUNT( ?y_2) AS ?yy)  WHERE \\n"
        ...     "{ ?x wd:P31 ?p31 { SELECT ... WHERE ... } ?p31 w:P279 ?y .} "
        ...     "GROUP BY ?yy ?x OFFSET 20 LIMIT 10") # doctest: +NORMALIZE_WHITESPACE
        >>> parts[0]
        ['PREFIX a: <bla>', 'PREFIX bc: <http://y>']
        >>> parts[1]
        '?x_ (COUNT(?y_2) AS ?yy)'
        >>> parts[2]
        ['?x_', '?yy']
        >>> parts[3]
        '?x wd:P31 ?p31 { SELECT ... WHERE ... } ?p31 w:P279 ?y'
        >>> parts[4]
        'GROUP BY ?yy ?x '
        >>> parts[5]
        'OFFSET 20 LIMIT 10'
        """
        # Get the query parts via this nice regex. Make sure that there are no
        # newline, or use the re.MULTILINE flag.
        match_groups = re.match(
            "^\s*(.*?)\s*SELECT\s+(\S[^{]*\S)\s*WHERE\s*{\s*(\S.*\S)\s*}\s*(.*?)\s*$",
            re.sub("\s+", " ", sparql_query))
        try:
            prefixes_list = re.split("\s+(?=PREFIX)", match_groups.group(1))
            select_vars_string = match_groups.group(2)
            select_vars_string = re.sub("\(\s+", "(", select_vars_string)
            select_vars_string = re.sub("\s+\)", ")", select_vars_string)
            # For something like "( COUNT( ?y_2) AS ?yy)" extract "?yy".
            select_vars_list = re.sub(
                "\(\s*[^(]+\s*\([^)]+\)\s*[aA][sS]\s*(\?[^)]+)\s*\)", "\\1",
                select_vars_string).split()
            body_string = match_groups.group(3)
            body_string = re.sub("\s+", " ", body_string)
            body_string = re.sub("\s*\.?\s*$", "", body_string)
            footer_string = match_groups.group(4)

            # If there is a GROUP BY, we need to separate it from the footer.
            #
            # First split by whitespace, then check whether the first two tokens
            # are GROUP BY and if yes, collect all variables (?...) that follow.
            # This is kind of ugly, but it works. It would be nicer to do this
            # with a proper regex. On the other hand, maybe that's not to
            # differnt from how a "real" parser would do it.
            footer_string_parts = footer_string.split()
            if len(footer_string_parts) > 2 \
                    and footer_string_parts[0] == "GROUP" \
                    and footer_string_parts[1] == "BY":
                group_by_string = "GROUP BY"
                i = 2
                while i < len(footer_string_parts) \
                        and footer_string_parts[i].startswith("?"):
                    i += 1
                group_by_string = " ".join(footer_string_parts[:i]) + " "
                footer_string = " ".join(footer_string_parts[i:])
            else:
                group_by_string = ""
            return [prefixes_list, select_vars_string, select_vars_list,
                      body_string, group_by_string, footer_string]
        except Exception as e:
            log.error("\x1b[31mProblem parsing SPARQL query (%s)\x1b[0m" % str(e))
            log.error("SPARQL query:\n%s" % sparql_query)
            log.error("Match groups: %s" % str(match_groups.groups()))
            return None

    def make_name_query_from_parts(self,
            prefixes_list, name_vars_list, name_triples_list,
            select_vars_string, body_string, group_by_string, footer_string):
        """
        Build query from the given parts. It should be self-explanatory from the
        return-statement, how the parts are synthesized. The arguments with
        suffix _list are lists, the other arguments are strings.
        """
        prefixes_string = "\n".join(prefixes_list)
        name_vars_string = " ".join(name_vars_list)
        name_triples_string = " .\n".join(name_triples_list)
        return f"{prefixes_string}\n" \
               f"SELECT {name_vars_string} WHERE {{\n" \
               f"  {{ SELECT {select_vars_string} WHERE {{\n" \
               f"    {body_string} }} {group_by_string}}}\n" \
               f"{name_triples_string}\n" \
               f"}} {footer_string}"


    def enhance_query(self, sparql_query):
        """
        Enhance the query, so that in the result for each columns with an ID
        that also has a name (via name_predicate) there is also a column (right
        next to the id column) with that name.

        >>> log.setLevel(logging.ERROR)
        >>> backend = Backend(
        ...     "https://qlever.cs.uni-freiburg.de:443/api/wikidata", 1, 0)
        >>> qns = QleverNameService(
        ...     backend,
        ...     "@en@rdfs:label",
        ...     "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
        ...     "_id", "_name", "add-all")
        >>> sparql_lines = qns.enhance_query(
        ...     "PREFIX wdt: <http://www.wikidata.org/prop/direct/> "
        ...     "PREFIX wd: <http://www.wikidata.org/entity/>  "
        ...     "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
        ...     "SELECT ?x ?y ?y_label WHERE {"
        ...     "  ?x wdt:P31 wd:Q5 ."
        ...     "  ?x wdt:P17 ?y ."
        ...     "  ?y rdfs:label ?y_label ."
        ...     "} LIMIT 10 ").split("\\n")
        >>> sparql_lines[:3] # doctest: +NORMALIZE_WHITESPACE
        ['PREFIX wdt: <http://www.wikidata.org/prop/direct/>',
         'PREFIX wd: <http://www.wikidata.org/entity/>',
         'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>']
        >>> sparql_lines[3]
        'SELECT ?x_id ?x_name ?y ?y_label WHERE {'
        >>> sparql_lines[4]
        '  { SELECT ?x_id ?y ?y_label WHERE {'
        >>> sparql_lines[5]
        '    ?x_id wdt:P31 wd:Q5 . ?x_id wdt:P17 ?y . ?y rdfs:label ?y_label } }'
        >>> sparql_lines[6]
        '  ?x_id @en@rdfs:label ?x_name'
        >>> sparql_lines[7]
        '} LIMIT 10'
        >>> len(sparql_lines)
        8
        """

        log.info("QLever Name Service: check which name triples can be added")
        start_time = time.time()

        # Get the various parts of the query (some as lists, some as strings).
        prefixes_list, select_vars_string, select_vars_list, body_string, \
                group_by_string, footer_string = self.get_query_parts(sparql_query)

        # Add name_predicate_prefix if not already in list of prefixes
        #
        # TODO: The following is not correct in case the prefix is already in
        # the list, but with a different definition. The result will be that no
        # result will be found for any of the name probe queries below and no
        # name triples will be added to the query.
        if not self.name_predicate_prefix in prefixes_list:
            prefixes_list.append(self.name_predicate_prefix)

        # For each variable in the SELECT clause of the original query check two
        # things:
        #
        # 1. Does it already have a "name triple" in the original query?
        #
        # 2. If not, check via a SPARQL query whether adding a name triple gives
        #    any results, that is whether it is an "id variable".
        # 
        # TODO: I first tried to check property 2 with a single SPARQL query
        # with all variables where property 1 holds at once, using OPTIONAL.
        # But that did not work, some fields were empty also for variables which
        # clearly had names. This even happened when I added only a single name
        # triple with OPTIONAL. Looks like a QLever bug to me.
        #
        new_select_vars_list = select_vars_list.copy()
        name_triples_list = []
        num_added = 0
        for i, var in enumerate(select_vars_list):
            # First check property 1. The regex captures which predicates we
            # count as name predicates when checking whether a "name triple"
            # already exists. Feel free to extend this.
            name_predicate_regex = "(@[a-z]+@)?(rdfs:label|schema:name)"
            if re.search(re.sub("\?", "\\?", var) + "\\s+"
                + name_predicate_regex, body_string) != None:
                continue

            # Now check property 2 via a SPARQL query (does it make sense to add
            # a name triple for this variable).
            name_var_test = var + "_test"
            name_triple_test = f"  {var} {self.name_predicate} {name_var_test}"
            name_test_query= self.make_name_query_from_parts(
                prefixes_list, [name_var_test], [name_triple_test],
                select_vars_string, body_string, group_by_string, "LIMIT 1")
            # SPARQL query in a separate try block, so that we can give a
            # specific error message for that.
            response = None
            try:
                response = self.backend.query("/?query=" +
                    urllib.parse.quote(name_test_query),
                    self.backend.timeout_seconds)
            except Exception as e:
                log.error("\x1b[31mCould not get result from backend\x1b[0m")
                log.error("Error message: %s" % str(e))
                log.error("Query was: %s" % name_test_query)
                add_name_for_this_var = False
            # If proper response, check the result size.
            if response != None and response.http_response != None:
                match = re.search("\"resultsize\"\s*:\s*(\d+)",
                    response.http_response.data.decode("utf-8"))
                add_name_for_this_var = True if match != None \
                                             and len(match.groups()) > 0 \
                                             and int(match.group(1)) > 0 \
                                        else False
            else:
                add_name_for_this_var = False

            # If both properties fulfilled, add the variable to the select
            # variables (at the right position) and add the name triple.
            if add_name_for_this_var:
                # Are we renaming id variables?
                if self.rename_id_vars:
                    id_var = var + self.var_suffix_id
                    log.info(f"Renaming {var} to {id_var}")
                    new_select_vars_list[i + num_added] = id_var
                    # Replace all occurrences of this variable (the \\b is there
                    # to make sure that only whole-word matches are replaced).
                    var_regex = re.sub("\\?", "\\?", var) + "\\b"
                    log.debug("Regex for re.sub is %s" % var_regex)
                    body_string = re.sub(var_regex, id_var, body_string)
                    group_by_string = re.sub(var_regex, id_var, group_by_string)
                    select_vars_string = re.sub(var_regex, id_var,
                            select_vars_string)
                else:
                    id_var = var
                # Are we renaming name variables?
                if self.rename_name_vars:
                    name_var = var + self.var_suffix_name
                else:
                    name_var = var
                # One of the two must be renamed
                assert(id_var != name_var)
                # Add new triple
                name_triple = f"  {id_var} {self.name_predicate} {name_var}"
                log.info("Adding triple \"%s\""
                        % re.sub("^\s+", "", name_triple))
                name_triples_list.append(name_triple)
                # CASE 1: Keep the id variable, add the name variable
                if self.addition_mode == "add-all" or \
                        (self.addition_mode == "add-first" and num_added == 0):
                    log.info("Adding name variable \"%s\"" % name_var)
                    num_added += 1
                    new_select_vars_list.insert(i + num_added, name_var)
                # CASE 2: Replace the id variable by the name variable
                else:
                    log.info("Replacing id variable \"%s\" "
                             "by name variable \"%s\"" % (id_var, name_var))
                    new_select_vars_list[i + num_added] = name_var


        # Add the name triples for the variables, where names exist.
        enhanced_query = self.make_name_query_from_parts(
                prefixes_list, new_select_vars_list, name_triples_list,
                select_vars_string, body_string, group_by_string, footer_string)
        end_time = time.time()
        log.info("Total time spent on name service: %dms",
                 int(1000 * (end_time - start_time)))

        return enhanced_query

class Response:
    """
    Own response class, so that we can also send a proper error response. Using
    HTTPResponse, I did not manage to set the data property myself.

    Contains an HTTPResponse object. If that object is None that signifies an
    error.
    """

    def __init__(self, **kwargs):
        """
        Three possible keywords arguments: http_response, query_path, error_msg.
        """
        self.http_response = kwargs.get("http_response", None)
        if self.http_response == None:
            query_path = kwargs.get("query_path", "[no query path specified]")
            error_msg = kwargs.get("error_msg", "[no error message specified]")
            query = urllib.parse.parse_qs(query_path[2:]).get("query",
                    "[no query specified]")
            error_msg = "QLever Proxy error: %s" % error_msg
            self.error_data = json.dumps({
                    "query": query,
                    "status": "ERROR",
                    "resultsize": "0",
                    "time": { "total": "0ms", "computeResult": "0ms" },
                    "exception": error_msg })
        else:
            self.error_data = None


class Backend:
    """
    Class for asking queries to one or several of the QLever backend.

    NOTE: Also the backend is now HTTPS, so we need to do some additional work
    with certificates here. We also had to do this for eval.py from the QLever
    evaluation, where a first version of this code has been copied from.
    """

    def __init__(self, backend_url, timeout_seconds, backend_id):
        """
        Create HTTP connection pool for asking request to this backend.
        """

        backend_url_parsed = urllib.parse.urlparse(backend_url)
        self.host, self.port = backend_url_parsed.netloc.split(':')
        self.base_path = backend_url_parsed.path if backend_url_parsed.path else "/"
        self.port = int(self.port) if self.port else 80
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
             backend_id, self.host, self.port, self.base_path, timeout_seconds)

    def query_and_write_to_queue(self, query_path, timeout, queue):
        """ 
        Like method query below, but append response to given queue (along with
        the backend id, so that we know which result comes from with backend).
        
        Note that queue.put automatically locks the queue during appending so
        that appends from different threads are serialized and don't interfere
        with each other.
        """
        response = self.query(query_path, timeout)
        queue.put((response, self.backend_id))

    def query(self, query_path, timeout):
        """ 
        Sent a GET request to the QLever backend, with the given path (which
        should always start with a / even if it's a relative path).
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
            return Response(http_response=response)
        except socket.timeout as e:
            error_msg = "%s Timeout (socket.timeout) after %.1f seconds" \
                    % (log_prefix, timeout)
            log.info(error_msg)
            return Response(query_path=query_path, error_msg=error_msg)
        except urllib3.exceptions.ReadTimeoutError as e:
            error_msg = "%s Timeout (ReadTimeoutError) after %.1f seconds" \
                    % (log_prefix, timeout)
            log.info(error_msg)
            return Response(query_path=query_path, error_msg=error_msg)
        except urllib3.exceptions.MaxRetryError as e:
            error_msg = "%s Timeout (MaxRetryError) after %.1f seconds" \
                    % (log_prefix, timeout)
            log.info(error_msg)
            return Response(query_path=query_path, error_msg=error_msg)
        except Exception as e:
            error_msg = "%s Error with request to %s (%s)" \
                    % (log_prefix, self.host, str(e))
            log.info(error_msg)
            return Response(query_path=query_path, error_msg=error_msg)


class QueryProcessor:
    """
    Class for sending a query to one or both backends, each with a timeout.

    If we get a response from the first backend before the specified timeout, we
    take that. Otherwise we take the response from the second backend (as a
    fallback). In the worst case, both backends fail.
    """

    def __init__(self,
            backend_1, backend_2, timeout_normal, qlever_name_service):
        """
        Create the two backends using the class above. Also create a
        QleverNameService in case we need it (costs nothing to create, just
        copies the arguments to the class, see above).
        """
        self.backend_1 = backend_1
        self.backend_2 = backend_2
        self.timeout_normal = timeout_normal
        self.qlever_name_service = qlever_name_service

    def query(self, path):
        """
        Decide what to do depending on the form of the query:

        1. If the query starts with yaml: then it's actually a yaml containing
        two queries, one for each backend

        2. Otherwise send the query to backend 1, with timeout_normal
        """
        # CASE 1: a YAML containing information about two SPARQL queries, one
        # for each backend.
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
                log.info("Query 1: " + re.sub("\s+", " ", query_1)[:30] \
                           + "..." + re.sub("\s+", " ", query_1)[-30:])
                log.info("Query 2: " + re.sub("\s+", " ", query_2)[:30] \
                           + "..." + re.sub("\s+", " ", query_2)[-30:])
                path_1 = "/?query=" + urllib.parse.quote(query_1)
                path_2 = "/?query=" + urllib.parse.quote(query_2)
                return self.query_backends_in_parallel(path_1, path_2)
            except Exception as e:
                error_msg = "\x1b[31mError parsing the YAML string (%s)\x1b[0m" % str(e)
                log.info(error_msg)
                log.info("YAML = \n" + queries_yaml)
                return Response(query_path=y_path, error_msg=error_msg)
        # CASE 2: An ordinary query, which we send to backend 1. This can be a
        # SPARQL query or a command like /?cmd=stats or /?cmd=clearcache
        else:
            # If SPARQL query and QLever Name Service active, see if we can add
            # name triples to the query. Note: there can be more & arguments in
            # the end
            if path.startswith("/?query=") and self.qlever_name_service:
                # Note that parse_qsl creates a list of key-value pairs and also
                # automatically unquotes (and urlencode quotes again).
                parameters = urllib.parse.parse_qsl(path[2:])
                sparql_query = parameters[0][1]
                # log.info("SPARQL query before enhancing:\n%s" % sparql_query)
                new_sparql_query = \
                        self.qlever_name_service.enhance_query(sparql_query)
                log.info("Enhanced SPARQL query: %s"
                        % re.sub("\s+", " ", new_sparql_query))
                parameters[0] = ("query", new_sparql_query)
                path = "/?" + urllib.parse.urlencode(parameters)
            return backend_1.query(path, self.timeout_normal)


    def query_backends_in_parallel(self, path_1, path_2):
        """
        Query both backends in parallel with a preference for a result from the
        first backend, as explained above.
        """

        # Send two concurrent requests to the two backends. Each of them will
        # append their response object to the given queue.
        result_queue = queue.Queue()
        thread_1 = threading.Thread(
                target=self.backend_1.query_and_write_to_queue,
                args=(path_1, self.backend_1.timeout_seconds, result_queue))
        thread_2 = threading.Thread(
                target=self.backend_2.query_and_write_to_queue,
                args=(path_2, self.backend_2.timeout_seconds, result_queue))
        for thread in [thread_1, thread_2]:
            thread.daemon = True
            thread.start()
         
        # Wait until the first of the two backends has responded (note that
        # queue.get is blocking). Then there are three cases:
        # 1. Backend 1 first, with response -> perfect
        # 2. Backend 1 first, no response -> wait for Backend 2
        # 3. Backend 2 first -> wait for Backend 1 and prefer if response
        response, backend_id = result_queue.get()
        if backend_id == 1 and response.http_response == None:
            response, backend_id = result_queue.get()
        elif backend_id == 2:
            log.info("Backend 2 responded first -> give Backend 1 a chance, too")
            response_2, backend_id_2 = result_queue.get()
            if response_2.http_response != None:
                response, backend_id = response_2, backend_id_2

        # We now have three cases:
        # 1. We have a response from  backend 1 (best case)
        # 2. No response from backend 1, but from backend 2 (fallback case)
        # 3. No response from either backend (worst case)
        if response.http_response != None and backend_id == 1:
            log.info("BEST CASE: Backend 1 responded in time")
        elif response.http_response != None and backend_id == 2:
            log.info("FALLBACK: Backend 1 did not respond in time, "
                     "taking result from Backend 2")
        else:
            log.info("WORST CASE: Neither backend responded in time :-(")

        # Return the response
        return response


def MakeRequestHandler(
        backend_1, backend_2, timeout_normal, qlever_name_service):
    """
    Returns a RequestHandler class for handling GET requests to the proxy for
    using the given backends.

    NOTE: This function returns a class ("class factory pattern"). I did this so
    that I can pass the information about the backends. This was not trivial,
    since the class has to be a subclass of http.server.BaseHTTPRequestHandler.
    I did not see an easier way (except for making the information about the
    backends global, which would be an ugly solution).
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
            self.backend_1 = backend_1
            self.backend_2 = backend_2
            self.query_processor = QueryProcessor(
                    backend_1, backend_2, timeout_normal, qlever_name_service)
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
    
            # Process query. The query process will decided whether to ask both
            # backends in parallel, whether to call the QLever Name Service,
            # etc. If something goes wrong, the response is None.
            response = self.query_processor.query(path)
    
            # For both case below, the QLever UI will only accept the response
            # when there is a Access-Control-Allow-Origin header.
            #
            # If we have a HTTPResponse, forward to caller, including selected headers.
            if response.http_response != None:
                self.send_response(200)
                headers_preserved = ["Content-Type", "Access-Control-Allow-Origin"]
                for header_key in headers_preserved:
                    header_value = response.http_response.getheader(header_key)
                    if header_value != None:
                        self.send_header(header_key, header_value)
                self.end_headers()
                self.wfile.write(response.http_response.data)
                log.debug("Forwarded result to caller"
                          " (headers preserved: %s)" % ", ".join(headers_preserved))
            # Otherwise, send an error message
            else:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(response.error_data.encode("utf-8"))
                log.info("\x1b[31mSending QLever error JSON to caller\x1b[0m")

            end_time = time.time()
            log.info("\x1b[1mTotal time spend on request: %dms\x1b[0m",
                     int(1000 * (end_time - start_time)))

    # Don't forget to return the class :-)
    return RequestHandler


def server_loop(hostname, port,
        backend_1, backend_2, timeout_normal, qlever_name_service):
    """
    Create a HTTP server that listens and respond to queries for the given
    hostname under the given port, using the request handler above. Runs in an
    infinite loop.
    """

    server_address = (hostname, port)
    request_handler_class = MakeRequestHandler(
            backend_1, backend_2, timeout_normal, qlever_name_service)
    server = http.server.HTTPServer(server_address, request_handler_class)
    log.info("Listening to GET requests on %s:%d" % (hostname, port))
    server.serve_forever()


if __name__ == "__main__":

    # Parse command line arguments + usage info.
    parser = argparse.ArgumentParser(
            description="QLever Proxy, see the README.md for more information")

    parser.add_argument(
            "--port", dest="port", type=int, default=8904,
            help="Run proxy on this port")
    parser.add_argument(
            "--backend-1", dest="backend_1", type=str,
            default="https://qlever.cs.uni-freiburg.de:443/api/wikidata",
            help="Primary backend (prefer if it responds in time)")
    parser.add_argument(
            "--backend-2", dest="backend_2", type=str,
            default="https://qlever.cs.uni-freiburg.de:443/api/wikidata-vulcano",
            help="Fallback backend (ask simpler query, when Backend 1"
            " does not respond in time")
    parser.add_argument(
            "--timeout-1", dest="timeout_1", type=float, default=0.5,
            help="Timeout for Backend 1, when asking parallel queries")
    parser.add_argument(
            "--timeout-2", dest="timeout_2", type=float, default=5.0,
            help="Timeout for Backend 2, when asking parallel queries")
    parser.add_argument(
            "--timeout", dest="timeout_normal", type=float, default=10.0,
            help="Timeout for Backend 1, when asking ordinary queries")
    parser.add_argument(
            "--add-name-triples", dest="add_name_triples", type=str,
            default="add_first",
            help="Add name triples for id variables. Options are:\n"
            " add-all     : have both name vars and id vars\n"
            " replace-all : replace all id vars by name vars\n"
            " add-first   : add name var for first id var, replace others\n"
            " [other]     : do not add name triples at all\n")
    parser.add_argument(
            "--id-suffix", dest="id_suffix", type=str, default="_id",
            help="Suffix for id variables (can be empty)")
    parser.add_argument(
            "--name-suffix", dest="name_suffix", type=str, default="_name",
            help="Suffix for name variables (can be empty)")


    args = parser.parse_args(sys.argv[1:])

    # Create backends. The third argument is the id (1 = primary, 2 = fallback)
    backend_1 = Backend(args.backend_1, args.timeout_1, 1)
    backend_2 = Backend(args.backend_2, args.timeout_2, 2)
    log.info("Timeout for single-backend queries is %.1fs" %
            args.timeout_normal)

    # Create Qlever Name Service (None if not asked for).
    if args.add_name_triples in ["add-all", "replace-all", "add-first"]:
        if args.id_suffix == args.name_suffix and \
                args.add_name_triples in ["add-all", "add-first"]:
                    log.error("If we add name triples, the id suffix "
                              "and the name suffix must be different")
                    sys.exit(1)
        qlever_name_service = QleverNameService(
                backend_2,
                "@en@rdfs:label",
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
                args.id_suffix, args.name_suffix, args.add_name_triples)
        log.info("\x1b[1mQLever Name Service is ACTIVE\x1b[0m")
    else:
        qlever_name_service = None
        log.info("\x1b[1mQLever Name Service is not active\x1b[0m"
                 " -> see usage info (--help) for how to activate")

    # Listen and respond to queries at that port, no matter to which hostname on
    # this machine the were directed.
    server_loop("0.0.0.0", args.port,
            backend_1, backend_2, args.timeout_normal, qlever_name_service)
