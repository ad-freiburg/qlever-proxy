# QLever Proxy

This proxy can be used in place of a QLever backend. Its current functionality
is to check if the value of the query argument has a special format (see below)
and actually contains SPARQL queries, each with a backend API. In that case,
both queries are issued in parallel to the respecive backends. If the first
query is not answered within the specified timeout, the result from the second
query is taken.

You can start the proxy simply as follows:

```
python3 -m http.server <port> &
```

To get a nice API URL, add the usual Proxy directives to
`/etc/apache2/sites-available/qlever-ssl.conf`. For example, if the proxy is
started on galera on part 8903 and is meant for the Wikidata backend, add

```
ProxyPass /api/wikidata-proxy http://galera.informatik.privat:8904
ProxyPassReverse /api/wikidata-proxy http://galera.informatik.privat:8904
```

## Format of the query argument


