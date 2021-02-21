# QLever Proxy

This proxy can be used in place of a QLever backend (most of the functionality
works in fact for an arbitrary SPARQL engine). The proxy expects `GET` requests
with an argument `query` which contains a SPARQL query (or two of them, see
below) and currently serves *two* purposes:

1. It can check for a given predicate `<p>`, whether each variable `?x` in the
   SPARQL query has a hit if a triple `?x p ?x_p` is added to the SPARQL query.
   If there is hit and a triple of that form is not already contained in the
   SPARQL query, it is added to the body of the SPARQL query along with a new
   variable `?x_p` in the SELECT clause. The name of the new variable and its
   position in the SELECT clause can be configured, as well as whether the
   original variable should be kept in the SELECT clause.

   A typical use case is a predicate providing the name of an entity, for
   example `rdfs:label`. If configured accordingly, the proxy will then check
   for each variable, whether it has a name according to that predicate and if
   yes, add the computation and display of those names to the query. Other use
   cases are automatically adding an image if there is one (the proxy can also
   add triples with the keyword `OPTIONAL`) or a coordinate location if there is
   one. See Section "Usage" below for two example invocations.

2. It checks if the value of the query argument in the GET request starts with
   "yaml" (without the quotes). If it does, it assumes that the query argument
   is of the form

   ```
   yaml:
     query_1: |-
     ...
     query:2: |-
     ...
   ```

   If it is, it will parse the two queries and issue them both in parallel.
   Query 1 will be send to Backend 1 and Query 2 will be send to Backend 2. The
   two backends and their respective timeouts can be specified via command line
   arguments, see below and `python3 qlever-proxy.py --help`. If the first query
   is not answered within the specified timeout, the result from the second
   query is taken. The idea is that Query 2 is simpler, so that an answer is
   provided in any case. Note that the two backends can be the same. Indeed,
   this is the default when only one Backend 1 is specified via the command
   line.

## Usage

You can start the proxy as follows. As an absolute minimum, you must specify the
port to which the proxy listens and the URL of Backend 1.

```
python3 -m http.server --port <port> --backend-1 <url>
```

Here is an example invocation using two backends with different timeouts and a
name service that automatically adds a label triple (using the predicate before
the first |, the 1 means that the new variable is added one to the right of the
original variable in the SELECT clause) for every variable, for which such a
triple exists and which does not already have such a triple in the query:

```
python3 qlever-proxy.py
  --port 8904
  --backend-1 https://qlever.cs.uni-freiburg.de/api/wikidata
  --backend-2 https://qlever.cs.uni-freiburg.de/api/wikidata
  --timeout-1 0.5 --timeout-2 30.0
  --add-triple "@en@<http://www.w3.org/2000/01/rdf-schema#label>||1"
```

Here is another example, with a single backend and automatic triple addition for
three predicates. Since no timeout is specified, the default timeout for a
single backend is used (10 seconds). The first add-triple is like in the example
above, except that the new variable now replaces the old one (that is what the 0
means). The second add-triple checks is a variable has an image (via Wikidata's
P18 predicate) and if yes, adds a new variable with suffix `_image` at the end
of the SELECT clause (that is what the -1 means). In an analogous way, the third
add-triple adds a coordinate location if it exists.

```
python3 qlever-proxy.py
  --port 8904
  --backend-1 https://qlever.cs.uni-freiburg.de/api/wikidata
  --add-triple "@en@<http://www.w3.org/2000/01/rdf-schema#label>||0"
  --add-triple "<http://www.wikidata.org/prop/direct/P18>|_image|-1"
  --add-triple "<http://www.wikidata.org/prop/direct/P625>|_coords|-1"
```

## Public URL of the proxy if you are in a local network.

If you are in a local network and you want the proxy to be reachable via a
public URL, configure your web server accordingly. Here is an example for
Apache2, using the configuration file for https://qlever.cs.uni-freiburg.de,
which is located at `filicudi:etc/apache2/sites-available/qlever-ssl.conf`. 
Assume that the proxy is started on a machine with the local network name
`galera.informatik.privat` on port `8904` and serves as proxy for a QLever
backend for Wikidata. Then add the following line to the configuration file:

```
ProxyPass /api/wikidata-proxy http://galera.informatik.privat:8904
```

Restart the web sever using `sudo /etc/init.d/apaches2 restart` or `sudo service
apache2 restart`. The proxy is then publicly reachable under the URL
https://qlever.cs.uni-freiburg.de/api/wikidata-proxy . 
