#!/usr/bin/python3
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


# Global log.
log = logging.getLogger("proxy logger")
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(asctime)s.%(msecs)03d %(levelname)-5s %(message)s", "%Y-%m-%d %H:%M:%S"))
log.addHandler(handler)

# Function for abbreviating long strings in log. When called with second
# argument unquote=True, urldecode the string and replace sequences of whitspace
# by a single space. When called with compact_ws=True, only do the latter.
# space.
def abbrev(long_string, **kwargs):
    max_length = kwargs.get("max_length", 80)
    long_string = "\"" + long_string + "\""
    if kwargs.get("unquote", False):
        long_string = re.sub("\n", " ",
                urllib.parse.unquote_plus(long_string)) + " [unquoted]"
    if kwargs.get("compact_ws", False):
        long_string = re.sub("\s+", " ", long_string)
    if len(long_string) <= max_length:
        return long_string
    else:
        k = max_length // 2 - 2
        return "%s ... %s" % (long_string[:k], long_string[-k:])
   
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

    class ConfigForAddTriple:
        """
        Configuration for adding a new triple to the query. See __init__ for the
        elements of the configuration.  """

        def __init__(self, predicate, suffix, position, **kwargs):
            """
            The first two arguments are the name of the predicate to be added
            and the suffix of the to-be-added variable that will be added to the
            existing variable (which appears as subject of the predicate).

            The third argument is the position at which the new variable will be
            added in the SELECT clause. 0 means to replace the subject variable
            of the new triple. A positive number k means to add it k to the
            right of the subject variable. A negative number -k means to add it
            at the end of the SELECT clause, where -1 means at the very end, -2
            means one before that, etc.
            
            For the keyword args, there are the following options and defaults:

            position_first: position for the first occurrence. 

            optional: Whether to put the new triple in OPTIONAL { ... } or not.
            The default is not to do this.

            predicate_exists_regex: The regex used to search whether the
            predicate or a similar one already exists. The default is to search
            for a triple with a predicate that matches either the full name or
            the suffix of the given "predicate", with an arbitrary prefix. For
            example, for predicate <http://www.wikidata.org/prop/direct/P18>,
            the default would be to search for triples with a predicate that
            either matches exactly this IRI or ends in :P18.
            """

            self.predicate = predicate
            self.suffix = suffix
            self.position = int(position)
            self.position_repeated = int(position)
            self.optional = kwargs.get("optional", False)
            self.predicate_exists_regex = kwargs.get("predicate_exists_regex",
                    "(%s|%s)" % (predicate,
                        re.sub("^.*[/#](.*)>", "\\S+:\\1", predicate)))
            # print("Predicate exists REGEX: ", self.predicate_exists_regex)

            # TODO: Hard-coded stuff, make configurable (it's not hard)

            # For name predicate, replace after first
            if predicate.find("label") != -1:
                self.position_repeated = 0

            # Images and coordinates should be OPTIONAL
            if suffix == "_image" or suffix == "_coords":
                self.optional = True

            # Only add triple for this select variable position.
            #
            # TODO: Cannot yet be controlled by command line options.
            self.select_variable_position = None
            if suffix == "_image":
                self.select_variable_position = 0
            elif suffix == "_coords":
                self.select_variable_position = -1

        def __repr__(self):
            """
            Config as human-readable string (used for logging).
            """

            suffix_show = self.suffix if self.suffix != "" else "None"
            return f"{self.predicate}, suffix: {suffix_show}" \
                   f", position: {self.position}, optional: {self.optional}"


    def __init__(self, backend, subject_var_suffix,
            configs_for_add_triple):
        """
        Create with given backendi and config.
        """
        self.backend = backend
        self.subject_var_suffix = subject_var_suffix
        self.configs_for_add_triple = configs_for_add_triple

    
    def get_query_parts(self, sparql_query):
        """
        Split a SPARQL query into parts. Returns the variables from the SELECT
        clause, the query without the final part (after the last closing curly
        brace), and the final part. Note that for this test, it's not important
        that the SPARQL query is syntactically correct and no backend is needed.

        >>> log.setLevel(logging.ERROR)
        >>> qns = QleverNameService(None, None, None)
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
        if match_groups == None or len(match_groups.groups()) != 4:
            log.error("\x1b[31mProblem parsing SPARQL query\x1b[0m"
                      "\x1b[90m\n%s\x1b[0m" % re.sub("\s*$", "", sparql_query))
            if match_groups == None:
                log.error("Parse regex does not match")
            else:
                log.error("Parse regex match groups: %s" % str(match_groups.groups()))
            return None
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


    def make_sparql_query_from_parts(self,
            prefixes_list,new_vars_list, new_triples_list,
            select_vars_string, body_string, group_by_string, footer_string):
        """
        Build SPARQL query from the given parts. It should be self-explanatory
        from the return-statement, how the parts are synthesized. The arguments
        with suffix _list are lists, the other arguments are strings.
        """
        prefixes_string = "\n".join(prefixes_list)
        new_vars_string = " ".join(new_vars_list)
        new_triples_string = " .\n".join(new_triples_list)
        return f"{prefixes_string}\n" \
               f"SELECT {new_vars_string} WHERE {{\n" \
               f"  {{ SELECT {select_vars_string} WHERE {{\n" \
               f"    {body_string} }} {group_by_string}}}\n" \
               f"{new_triples_string}\n" \
               f"}} {footer_string}"


    def enhance_query(self, sparql_query):
        """
        Enhance the query, so that in the result for each columns with an ID
        that also has a name (via name_predicate) there is also a column (right
        next to the id column) with that name.

        >>> log.setLevel(logging.ERROR)
        >>> backend = Backend(
        ...     "https://qlever.cs.uni-freiburg.de:443/api/wikidata", 1, 0)
        >>> config = QleverNameService.ConfigForAddTriple(
        ...     "@en@<http://www.w3.org/2000/01/rdf-schema#label>", "_name", "1")
        >>> qns = QleverNameService(backend, "_id", [config])
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
        '  ?x_id @en@<http://www.w3.org/2000/01/rdf-schema#label> ?x_name'
        >>> sparql_lines[7]
        '} LIMIT 10'
        >>> len(sparql_lines)
        8
        """

        log.info("QLever Name Service: check which name triples can be added")
        start_time = time.time()

        # Get the various parts of the query (some as lists, some as strings).
        query_parts = self.get_query_parts(sparql_query)
        if query_parts == None:
            log.error("QLever name service: query unchanged")
            return sparql_query
        prefixes_list, select_vars_string, select_vars_list, body_string, \
                group_by_string, footer_string = query_parts

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
        new_triples_list = []
        num_vars_added = 0
        num_triples_added_per_config = [0] * len(self.configs_for_add_triple)
        for var_index, var in enumerate(select_vars_list):

            # Keep track of whether this variable has been renamed (renamed at
            # most once) and the original name.
            var_has_been_renamed = False
            original_var = var

            # For each --add-triple configuration check if the corresponding
            # triple should be added and if yes, add it.
            for config_index, config in enumerate(self.configs_for_add_triple):

                # Some configs only apply to select variables in certain
                # positions.
                if config.select_variable_position != None:
                    select_variable_position = config.select_variable_position \
                            if config.select_variable_position >= 0 \
                            else len(select_vars_list) + \
                                     config.select_variable_position
                    if var_index != select_variable_position:
                        continue

                # Add name_predicate_prefix if not already in list of prefixes
                #
                # TODO: The following is not ideal for three reasons:
                #
                # 1. The running time is linear per added variable
                # 2. The prefix ends up in the enhanced query also if the new triple
                # is not added (because a related triple was already there)
                # 3. In case the prefix is already in the list, but with a
                # different definition. The result will be that no result will
                # be found for any of the name probe queries below and no name
                # triples will be added to the query.
                # if not new_predicate_prefix in prefixes_list:
                #     prefixes_list.append(new_predicate_prefix)

                # First check property 1. The regex captures which predicates we
                # count as name predicates when checking whether a "name triple"
                # already exists. Feel free to extend this.
                if re.search(re.sub("\?", "\\?", var) + "\\s+"
                    + config.predicate_exists_regex, body_string) != None:
                    continue

                # Now check property 2 via a SPARQL query (does it make sense to
                # add a name triple for this variable). Make sure to take the
                # current name of the variable (maybe it was renamed already).
                # 
                # TODO: Due to a current bug in PR#355 (as of 25.12.2020) when
                # the inner query has only a single triple, explicitly order the
                # inner query by {var}.
                group_by_string_enhanced = group_by_string + f"ORDER BY {var} "
                log.info("\x1b[35;1mEnhancing test query by \"ORDER BY {var}\""
                         " to circumvent bug in current PR355"
                         " (as of 25.12.2020)\x1b[0m")

                test_var = var + config.suffix + "_test"
                test_triple = f"  {var} {config.predicate} {test_var}"
                test_query = self.make_sparql_query_from_parts(
                    prefixes_list, [test_var], [test_triple],
                    select_vars_string, body_string, group_by_string_enhanced,
                    "LIMIT 1")
                log.debug(f"Test if adding \"{test_triple}\" gives result"
                          f"\n\x1b[90m{test_query}\x1b[0m")

                # SPARQL query in a separate try block, so that we can give a
                # specific error message for that. Make sure that the result and
                # sub-results of this query are NOT pinned to the cahce.
                response = None
                try:
                    response = self.backend.query("/?query=" +
                        urllib.parse.quote(test_query),
                        self.backend.timeout_seconds,
                        pin_results_override=False)
                except Exception as e:
                    log.error("\x1b[31mCould not get result from backend\x1b[0m")
                    log.error("Error message: %s" % str(e))
                    log.error("Query was: %s" % test_query)
                    add_new_triple = False
                # If proper response, check the result size.
                if response != None and response.http_response != None:
                    # log.info(f"\x1b[90mResponse data for {var} is %s\x1b[0m"
                    #         % re.sub("(\s|\\\\n)+", " ",
                    #             str(response.http_response.data.decode("utf-8"))))
                    match = re.search("\"resultsize\"\s*:\s*(\d+)",
                        response.http_response.data.decode("utf-8"))
                    add_new_triple = True if match != None \
                                      and len(match.groups()) > 0 \
                                      and int(match.group(1)) > 0 \
                                 else False
                else:
                    add_new_triple = False

                # If both properties are fulfilled, add the new triple.
                if add_new_triple:
                    # Are we renaming the original variable?
                    # 
                    # Note: id_var was initialized to None above to make sure
                    # that in case we test several triples for addition, the
                    # variable name is changed only once.
                    if self.subject_var_suffix != "" \
                            and not var_has_been_renamed:
                        var = original_var + self.subject_var_suffix
                        log.info(f"Renaming {original_var} to {var}")
                        new_select_vars_list[var_index + num_vars_added] = var
                        # Replace all occurrences of the original variable (the
                        # \\b is there to make sure that only whole-word matches
                        # are replaced).
                        original_var_regex = re.sub("\\?", "\\?", original_var) + "\\b"
                        log.debug("Regex for re.sub is %s" % original_var_regex)
                        body_string = re.sub(original_var_regex, var, body_string)
                        group_by_string = re.sub(original_var_regex, var, group_by_string)
                        select_vars_string = re.sub(original_var_regex, var,
                                select_vars_string)
                        var_has_been_renamed = True
                    # The name of the new variable.
                    new_var = original_var + config.suffix
                    # The id variable and the new variable must not have the
                    # same name.
                    assert(var != new_var)
                    # Add new triple
                    new_triple = f"{var} {config.predicate} {new_var}"
                    if config.optional:
                        new_triple = f"OPTIONAL {{ {new_triple} }}"
                    log.info("\x1b[0mAdding triple \"%s\"\x1b[0m"
                            % re.sub("^\s+", "", new_triple))
                    new_triples_list.append("  " + new_triple)
                    # Get position argument depending on how many triples we
                    # have already added for this config.
                    if num_triples_added_per_config[config_index] == 0:
                        position = config.position
                    else:
                        position = config.position_repeated
                    # CASE 1: Replace the id variable by the name variable
                    if position == 0:
                        log.debug(f"Replacing id variable \"{var}\" "
                                  f"by new variable \"{new_var}\"")
                        new_select_vars_list[var_index + num_vars_added] = new_var
                    # CASE 2: Keep the id variable, add the new variable. Do not
                    # count variables appended to the end towards
                    # num_vars_added.
                    else:
                        log.debug(f"Keeping id variable \"{var}\", "
                                  f"adding new variable \"{new_var}\"")
                        # Position +1 means right next to current position
                        # Position -1 means last position, -2 second to last.
                        if position > 0:
                            num_vars_added += 1
                            pos = var_index + num_vars_added + position - 1
                        else:
                            pos = len(new_select_vars_list) + position + 1
                        new_select_vars_list.insert(pos, new_var)
                    log.debug("\x1b[34mNew select var list: %s\x1b[0m" 
                                % " ".join(new_select_vars_list))

                    # Keep track of how many triples we add per predicate.
                    num_triples_added_per_config[config_index] += 1


        # Add the name triples for the variables, where names exist.
        enhanced_query = self.make_sparql_query_from_parts(
                prefixes_list, new_select_vars_list, new_triples_list,
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

        NOTE: In a prior version http_response == None was taken as equivalent to
        error. However that was not correct because some QLever errors like
        "allocated more than the specified limit" lead to a http_response !=
        None but with an error message.
        """

        self.http_response = kwargs.get("http_response", None)
        self.error_data = None

        # If http_response == None, create an error message.
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
                    "exception": error_msg }).encode("utf-8")

        # If http_response.data == "status": "ERROR" then set http_response to
        # Note that  None and set error_data instead.
        #
        # NOTE: Alternatively, we could have create an http_response object with
        # "status": "ERROR" in the case of an error, but I could not figure out
        # how to create an http_response object.
        if self.http_response != None:
            try:
                result = json.loads(self.http_response.data.decode("utf-8"))
                if result.get("status") == "ERROR":
                    error_msg = result.get("exception", "[error msg not found]")
                    log.info("\x1b[31mQLever response with ERROR: "
                            + re.sub("\s+", " ", error_msg) + "\x1b[0m")
                    self.error_data = self.http_response.data
                    self.http_response = None
            except Exception as e:
                log.info("\x1b[31mCould not parse QLever result ("
                             + str(e) + ")\x1b[0m")


class Backend:
    """
    Class for asking queries to one or several of the QLever backend.

    NOTE: Also the backend is now HTTPS, so we need to do some additional work
    with certificates here. We also had to do this for eval.py from the QLever
    evaluation, where a first version of this code has been copied from.
    """

    def __init__(self, backend_url, timeout_seconds, backend_id,
            pin_results=False, clear_cache=False, show_cache_stats=True):
        """
        Create HTTP connection pool for asking request to this backend.
        """

        backend_url_parsed = urllib.parse.urlparse(backend_url)
        if backend_url_parsed.netloc.find(":") != -1:
            self.host, self.port = backend_url_parsed.netloc.split(':')
            self.port = int(self.port)
        else:
            self.host = backend_url_parsed.netloc
            self.port = 443 if backend_url_parsed.scheme == "https" else 80
        self.base_path = backend_url_parsed.path if backend_url_parsed.path else "/"
        self.timeout_seconds = timeout_seconds
        self.backend_id = backend_id
        self.pin_results = pin_results
        self.clear_cache = clear_cache
        self.always_show_cache_stats = show_cache_stats
        self.log_prefix = "Backend %d:" % self.backend_id

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

        # Optionally clear the cache initially.
        if self.clear_cache:
            clear_cache_fields = { "cmd": "clearcachecomplete" }
            clear_cache_response = self.connection_pool.request(
                'GET', self.base_path, fields=clear_cache_fields)
            assert(clear_cache_response.status == 200)

        # Log what we have created.
        log.info("%s %s:%d%s with timeout %4.1fs%s", self.log_prefix,
             self.host, self.port, self.base_path, timeout_seconds,
             " [cache completely cleared]" if self.clear_cache else "")

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

    def query(self, query_path, timeout, **kwargs):
        """ 
        Sent a GET request to the QLever backend, with the given path (which
        should always start with a / even if it's a relative path).
        
        If never_pin_result=True, override self.pin_results.
        """

        # Pin result to cache if self.pin_results.
        #
        # Can be overridden by kwarg pin_results_override. This is used by
        # QleverNameService, for which we do not want to pin results.  The
        # reason is that these queries are arbitrary and the subtree results can
        # be huge. In contrast, the large subtree results for the agnostic
        # queries all come from a very small set.
        pin_results_params = "&pinresult=true&pinsubtrees=true" \
            if kwargs.get("pin_results_override", self.pin_results) else ""

        # Build Query URL.
        #
        # Note: We cannot easily use "fields=..." for the URL parameters in
        # connection_pool.request below because "query_path" may come from the
        # query received by the proxy and may or may not contain parameters, so
        # we cannot simply append with "?..." (which is what fields=... does).
        full_path = self.base_path + query_path + pin_results_params
        log.info("%s Sending GET request %s",
                  self.log_prefix, abbrev(full_path, unquote=True))

        try:
            # TODO: Understand why we need keep_alive=False?
            headers = urllib3.make_headers(keep_alive=False)
            response = self.connection_pool.request('GET', full_path,
                    fields=None, headers=headers, timeout=timeout)
            assert(response.status == 200)
            log.debug("%s Response data: %s", self.log_prefix,
                abbrev(str(response.data), max_length=500, compact_ws=True))
            # log.debug("Content type  : %s", response.getheader("Content-Type"))
            # log.debug("All headers   : %s", response.getheaders())

            # If query successful and we pinned the result, output cache stats.
            # If args.show_cache_stats_2, always show it. If
            # pin_results_override is set and False, never show it.
            if (self.pin_results or self.always_show_cache_stats) \
                    and kwargs.get("pin_results_override", True) == True:
                self.show_cache_stats()

            return Response(http_response=response)
        except socket.timeout as e:
            error_msg = "%s Timeout (socket.timeout) after %.1f seconds" \
                    % (self.log_prefix, timeout)
            log.info(error_msg)
            return Response(query_path=query_path, error_msg=error_msg)
        except urllib3.exceptions.ReadTimeoutError as e:
            error_msg = "%s Timeout (ReadTimeoutError) after %.1f seconds" \
                    % (self.log_prefix, timeout)
            log.info(error_msg)
            return Response(query_path=query_path, error_msg=error_msg)
        except urllib3.exceptions.MaxRetryError as e:
            error_msg = "%s Timeout (MaxRetryError) after %.1f seconds" \
                    % (self.log_prefix, timeout)
            log.info(error_msg)
            return Response(query_path=query_path, error_msg=error_msg)
        except Exception as e:
            error_msg = "%s Error with request to %s (%s)" \
                    % (self.log_prefix, self.host, str(e))
            log.info(error_msg)
            return Response(query_path=query_path, error_msg=error_msg)

    def show_cache_stats(self):
        try:
            # Get the response and throw exception if status != 200.
            cache_stats_fields = { "cmd": "cachestats" }
            cache_stats_response = self.connection_pool.request(
                'GET', self.base_path, fields=cache_stats_fields,
                timeout=self.timeout_seconds)
            assert(cache_stats_response.status == 200)
            # Show cache stats in a nicely readable way.
            cache_stats = eval(cache_stats_response.data.decode("utf-8"))
            num_results = cache_stats["num-cached-elements"]
            size_results_mb = cache_stats["num-cached-elements"] / 1e9
            num_pinned = cache_stats["num-pinned-elements"]
            size_pinned_mb = cache_stats["pinned-size"] / 1e9
            log.info("%s %d normally cached results in %.1f GB"
                     " + %d pinned results in %.1f GB"
                     % (self.log_prefix, num_results, size_results_mb,
                         num_pinned, size_pinned_mb))
        except Exception as e:
            error_msg = "%s Error getting cache statistics from %s:%d (%s)" \
                    % (self.log_prefix, self.host, self.port, str(e))
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
                log.info("Query 1: " + abbrev(query_1, compact_ws=True))
                log.info("Query 2: " + abbrev(query_2, compact_ws=True))
                path_1 = "/?query=" + urllib.parse.quote(query_1)
                path_2 = "/?query=" + urllib.parse.quote(query_2)
                return self.query_backends_in_parallel(path_1, path_2)
            except Exception as e:
                error_msg = "\x1b[31mError parsing the YAML string (%s)\x1b[0m" % str(e)
                log.info(error_msg)
                log.info("YAML = \n" + queries_yaml)
                return Response(query_path=path, error_msg=error_msg)
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
                log.info("QLever Name Service, result query: \x1b[90m%s\x1b[0m"
                        % re.sub("\s+", " ", new_sparql_query))
                parameters[0] = ("query", new_sparql_query)
                path = "/?" + urllib.parse.urlencode(parameters)
            else:
                log.info("Ordinary query, processed using backend 1")
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
            backend_1_error_data = response.error_data
            response, backend_id = result_queue.get()
        elif backend_id == 2:
            log.info("Backend 2 responded first -> give Backend 1 a chance, too")
            response_2, backend_id_2 = result_queue.get()
            backend_1_error_data = response_2.error_data
            if response_2.http_response != None:
                response, backend_id = response_2, backend_id_2

        # We now have three cases:
        # 1. We have a response from  backend 1 (best case)
        # 2. No response from backend 1, but from backend 2 (fallback case)
        # 3. No response from either backend (worst case)
        if response.http_response != None and backend_id == 1:
            log.info("BEST CASE: Backend 1 responded in time")
        elif response.http_response != None and backend_id == 2:
            log.info("FALLBACK: Backend 1 " +
                     ("responsed with an error"
                         if backend_1_error_data != None
                         else "did not not respond in time") + 
                     ", taking result from Backend 2")
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
            """
            This is called for example by send_response below. By doing nothing
            here, we suppress messages from BaseHTTPRequestHandler.
            """
            # log.debug(format_string % args)
    
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
            path_unquoted = urllib.parse.unquote(path)
            # log.info("")
            print()
            log.info("GET request received: %s"
                    % abbrev(path_unquoted, unquote=True, compact_ws=True))
            # log.debug("Headers: %s" % headers)
    
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
                self.wfile.write(response.error_data)
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
    log.info("Listening to GET requests on \x1b[1m%s:%d\x1b[0m"
                 % (socket.getfqdn(), port))
    server.serve_forever()

class MyArgumentParser(argparse.ArgumentParser):
    """
    Override the error message so that it prints the full help text if the
    script is called without arguments or with a wrong argument.
    """

    def error(self, message):
        print("ArgumentParser: %s\n" % message)
        self.print_help()
        sys.exit(1)


if __name__ == "__main__":

    # Parse command line arguments + usage info.
    parser = MyArgumentParser(
            # description="QLever Proxy",
            epilog="See the README.md for more information. Here is an example"
            " invocation for Wikidata:\n\n"
            "python3 qlever-proxy.py --port 8904"
            " --add-triple \"@en@<http://www.w3.org/2000/01/rdf-schema#label>||1\""
            " --backend-1 \"https://qlever.cs.uni-freiburg.de/api/wikidata\""
            " --backend-2 \"https://qlever.cs.uni-freiburg.de/api/wikidata\""
            " --timeout-1 0.5 --timeout-2 30.0 --pin-results-backend-2",
            formatter_class=argparse.RawDescriptionHelpFormatter)
            # formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            "--port", dest="port", type=int, required=True,
            help="Run proxy on this port")
    parser.add_argument(
            "--backend-1", dest="backend_1", type=str,
            default="https://qlever.cs.uni-freiburg.de:443/api/wikidata",
            help="Primary backend (prefer if it responds in time)")
    parser.add_argument(
            "--backend-2", dest="backend_2", type=str,
            default="",
            help="Fallback backend (ask simpler query, when Backend 1"
            " does not respond in time. When empty (which is the default)"
            ", same as Backend 1.")
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
            "--subject-var-suffix", dest="subject_var_suffix",
            type=str, default="_id",
            help="Suffix for subject variable of added triple (can be empty)")
    parser.add_argument(
            "--add-triple", dest="configs_for_add_triple",
            type=str, nargs="*",
            default=["@en@<http://www.w3.org/2000/01/rdf-schema#label>||1"],
            help="Configuration for adding a triple in the form"
            " <predicate>|<suffix>|<position>, where <predicate> is the"
            " name of the new predicate, suffix is what is added to the"
            " subject variable name to derive the new variable name"
            " (can be empty), and position is the placement of the new"
            " variable in the SELECT clause of the SPARQL query")
    parser.add_argument(
            "--log-level", dest="log_level", type=str,
            choices=["INFO", "DEBUG", "ERROR"], default="INFO",
            help="Log level (INFO, DEBUG, ERROR)")
    parser.add_argument(
            "--pin-results-backend-2", dest="pin_results_2",
            action="store_true", default=False,
            help="Pin results from backend 2 to the cache permanently"
            " (QLever URL parameter pinresult=true and pinsubtrees=true)")
    parser.add_argument(
            "--clear-cache-2", dest="clear_cache_2",
            action="store_true", default=False,
            help="Clear cache from backend 2 initially, including pinned"
            " results (QLever URL parameter cmd=clearcachecomplete)")
    parser.add_argument(
            "--show-cache-stats-2", dest="show_cache_stats_2",
            action="store_true", default=True,
            help="Show cache stats for backend 2 after every query")

    args = parser.parse_args(sys.argv[1:])
    log.setLevel(eval("logging.%s" % args.log_level))
    print()
    log.info("Log level is \x1b[1m%s\x1b[0m" % args.log_level)

    # Create backends. The third argument is the id (1 = primary, 2 = fallback)
    if args.backend_2 == "":
        args.backend_2 = args.backend_1
    backend_1 = Backend(args.backend_1, args.timeout_1, 1)
    backend_2 = Backend(args.backend_2, args.timeout_2, 2,
        args.pin_results_2, args.clear_cache_2, args.show_cache_stats_2)
    backend_2.show_cache_stats()
    log.info("Timeout for single-backend queries is %.1fs" %
            args.timeout_normal)

    # Parse arguments to --add-triple into ConfigForAddTriple objects.
    configs_for_add_triple = []
    for config_arg in args.configs_for_add_triple:
        config_parts = config_arg.split("|")
        if len(config_parts) != 3:
            log.error("Argument to --add-triple must be of the form"
                      " <predicate>|<suffix>|<position>, was: ",
                      config_arg)
            sys.exit(1)
        config = QleverNameService.ConfigForAddTriple(*config_parts)
        configs_for_add_triple.append(config)

    # Create Qlever Name Service (None if no --add-triple is specified).
    if len(configs_for_add_triple) > 0:
        qlever_name_service = QleverNameService(
                backend_2, args.subject_var_suffix, configs_for_add_triple)
        log.info("QLever Name Service is \x1b[1mACTIVE\x1b[0m"
                 " (only for queries to backend 1, using backend 2)"
                 ", configs are:")
        for config in configs_for_add_triple:
            log.info("\x1b[90m" + str(config) + "\x1b[0m")
    else:
        qlever_name_service = None
        log.info("QLever Name Service is \x1b[1mNOT active\x1b[0m"
                 " -> see usage info (--help) for how to activate")

    # Listen and respond to queries at that port, no matter to which hostname on
    # this machine the were directed.
    server_loop("0.0.0.0", args.port,
            backend_1, backend_2, args.timeout_normal, qlever_name_service)
