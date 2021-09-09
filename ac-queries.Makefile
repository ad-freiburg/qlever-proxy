SHELL = /bin/bash
# PIN WARMUP QUERIES TO CACHE (for the QLever UI)
# (c) Algorithms and Data Structures, University of Freiburg
# Originally written by Hannah Bast, 20.02.2021

# This Makefile provides the following targets:
#
# pin: Pin queries to cache, so that all autocompletion queries are fast, even
#      when "Clear cache" is clicked in the QLever UI (ther results for pinned 
#      queries will never be removed, unless ... see target clear).
#
# clear: Clear the cache completely (including pinned results). Note that this
#        can NOT be activated from the QLever UI.
#
# clear-unpinned: Clear all unpinned results from the cache. This is exactly
#                 what happens when clicking "Clear cache" in the QLever UI.
#
# show-all-ac-queries: Show the AC queries for subject, predicat, object for
#                      copy&paste in the QLever UI backend settings.

# This Makefile should be used as follows:
#
# 1. In the directors with the particular index, create a new Makefile
# 2. At the top add: include /local/data/qlever/qlever-indices/Makefile
#    (or wherever this Makefile - the master Makefile - resides)
# 3. Redefine API and FREQUENT_PREDICATES (see below) in the local Makefile
# 4. Redefine any of the patterns in the local Makefile
#    (the patterns below give a default functionality, which should work
#    for any knowledge base, but only using the raw IRIs, and no names,
#    aliases, or whatever special data the knowledge base has to offer.

# A prefix that identifies a particular build. This typically consists of a base
# name and optionally further specificataion separated by dots. For example:
# wikidata. Or: wikidata.2021-06-27
DB = 

# The base name of the prefix = the part before the first dot. Often, the name
# of the input (ttl) file or of the settings (json) file use only the basename
# and not the full prefix.
DB_BASE = $(firstword $(subst ., ,$(DB)))

# The port of the QLever backend.
PORT = 

# The slug used in the URL of the QLever API and the QleverUI API. This is
# typically the basename of the prefix. For example: wikidata. Or: freebase.
SLUG = $(DB_BASE)

# Memory for queries and for the cache (all in GB).
MEMORY_FOR_QUERIES = 30
CACHE_MAX_SIZE_GB = 30
CACHE_MAX_SIZE_GB_SINGLE_ENTRY = 5
CACHE_MAX_NUM_ENTRIES = 1000

# The URL of the QLever backend.
QLEVER_API = https://qlever.cs.uni-freiburg.de/api/$(SLUG)

# The URL of the QLever UI istance ... TODO: it's confusing that this also has
# /api/ in the name, it actually has nothing to do with the URLs from the QLever
# backends (which are defined in the Apache configuration of QLever).
WARMUP_API = $(subst /api/,/api/warmup/,$(QLEVER_API))

# Admin token
TOKEN = aof4Ad

# Frequent predicates that should be pinned to the cache (can be left empty).
# Separate by space. You can use all the prefixes from PREFIXES (e.g. wdt:P31 if
# PREFIXES defines the prefix for wdt), but you can also write full IRIs. Just
# see how it is used in target pin: below, it's very simple.
FREQUENT_PREDICATES =
FREQUENT_PATTERNS_WITHOUT_ORDER =

# The name of the docker image.
DOCKER_IMAGE = qlever.master

# The name of the docker container. Used for target memory-usage: below.
DOCKER_CONTAINER = qlever.$(DB)

show-config:
	@echo
	@echo "Basic configuration variables:"
	@echo
	@for VAR in DB DB_BASE SLUG CAT TTL \
	  PORT QLEVER_API WARMUP_API \
	  DOCKER_IMAGE DOCKER_CONTAINER \
	  MEMORY_FOR_QUERIES \
	  CACHE_MAX_SIZE_GB CACHE_MAX_SIZE_GB_SINGLE_ENTRY CACHE_MAX_NUM_ENTRIES; do \
	  printf "%-30s = %s\n" "$$VAR" "$${!VAR}"; done
	@echo
	@printf "All targets: "
	@grep "^[A-Za-z._]\+:" /local/data/qlever/qlever-proxy/ac-queries.Makefile | sed 's/://' | paste -sd" "
	@echo
	@echo "make index.THIS_WILL_OVERWRITE_AN_EXISTING_INDEX will execute the following:"
	@echo
	@$(MAKE) -sn index.THIS_WILL_OVERWRITE_AN_EXISTING_INDEX
	@echo

# Build an index

CAT = cat
TTL = $(DB_BASE).ttl

index.THIS_WILL_OVERWRITE_AN_EXISTING_INDEX:
	time ( docker run -it --rm -v $(shell pwd):/index --entrypoint bash --name qlever.$(DB)-index $(DOCKER_IMAGE) -c "cd /index && $(CAT) $(TTL) | IndexBuilderMain -F ttl -f - -l -i $(DB) -s $(DB_BASE).settings.json | tee $(DB).index-log.txt"; rm -f $(DB)*tmp* )

# START, STOP, and view LOG

start:
	-docker rm -f $(DOCKER_CONTAINER)
	docker run -d --restart=unless-stopped -v $(shell pwd):/index -p $(PORT):7001 -e INDEX_PREFIX=$(DB) -e MEMORY_FOR_QUERIES=$(MEMORY_FOR_QUERIES) -e CACHE_MAX_SIZE_GB=${CACHE_MAX_SIZE_GB} -e CACHE_MAX_SIZE_GB_SINGLE_ENTRY=${CACHE_MAX_SIZE_GB_SINGLE_ENTRY} -e CACHE_MAX_NUM_ENTRIES=${CACHE_MAX_NUM_ENTRIES} --name $(DOCKER_CONTAINER) $(DOCKER_IMAGE)

stop:
	docker stop $(DOCKER_CONTAINER)

log:
	docker logs -f --tail 100 $(DOCKER_CONTAINER)


# WARMUP queries. The .local target only works on the machine, where the
# qlever-ui Docker container is running. It has the advantage of being more
# interactive than the WARMUP_API call (which for pin: returns only after all
# warmup queries have been executed and times out if this takes too long, for
# reasons I have not fully understood yet, apparently there is a time out in one
# of the proxies involved).
HTML2ANSI = jq -r '.log|join("\n")' | sed 's|<strong>\(.*\)</strong>|\\033[1m\1\\033[0m|; s|<span style="color: blue">\(.*\)</span>|\\033[34m\1\\033[0m|' | xargs -0 echo -e

pin.remote:
	ssh -t galera docker exec -it qlever-ui bash -c \"python manage.py warmup $(SLUG) pin\"

pin.local:
	docker exec -it qlever-ui bash -c "python manage.py warmup $(SLUG) pin"

clear.local:
	docker exec -it qlever-ui bash -c "python manage.py warmup $(SLUG) clear"

clear_unpinned.local:
	docker exec -it qlever-ui bash -c "python manage.py warmup $(SLUG) clear_unpinned"

pin:
	@if ! curl -Gsf $(WARMUP_API)/pin?token=$(TOKEN) | $(HTML2ANSI); \
	  then curl -Gi $(WARMUP_API)/pin?token=$(TOKEN); fi

clear:
	@curl -Gs $(WARMUP_API)/clear?token=$(TOKEN) | $(HTML2ANSI)
	@# curl -Gs $(QLEVER_API) --data-urlencode "cmd=clearcachecomplete" > /dev/null

clear_unpinned:
	@curl -Gsf $(WARMUP_API)/clear_unpinned?token=$(TOKEN) | $(HTML2ANSI)
	@# curl -Gs $(QLEVER_API) --data-urlencode "cmd=clearcache" > /dev/null


# STATISTICS on cache, memory, and the number of triples per predicate.

disk_usage:
	du -hc $(DB).index.* $(DB).literals-index $(DB).vocabulary $(DB).prefixes

cachestats:
	@curl -Gs $(QLEVER_API) --data-urlencode "cmd=cachestats" \
	  | sed 's/[{}]//g; s/:/: /g; s/,/ , /g' | numfmt --field=2,5,8,11,14 --grouping && echo

memory_usage:
	@echo && docker stats --no-stream --format \
	  "Memory usage of docker container $(DOCKER_CONTAINER): {{.MemUsage}}" $(DOCKER_CONTAINER)


num_triples:
	@echo -e "\033[1mCompute total number of triples by computing the number of triples for each predicate\033[0m"
	curl -Gs $(QLEVER_API) --data-urlencode "query=SELECT ?p (COUNT(?p) AS ?count) WHERE { ?x ql:has-predicate ?p } GROUP BY ?p ORDER BY DESC(?count)" --data-urlencode "action=tsv_export" \
	  | cut -f1 | grep -v "QLever-internal-function" \
	  > $(DB).predicates.txt
	cat $(DB).predicates.txt \
	  | while read P; do \
	      $(MAKE) -s clear-unpinned > /dev/null; \
	      printf "$$P\t" && curl -Gs $(QLEVER_API) --data-urlencode "query=SELECT ?x ?y WHERE { ?x $$P ?y }" --data-urlencode "send=10" \
	        | grep resultsize | sed 's/[^0-9]//g'; \
	    done \
	  | tee $(DB).predicate-counts.tsv | numfmt --field=2 --grouping
	cut -f2 $(DB).predicate-counts.tsv | paste -sd+ | bc | numfmt --grouping \
	  | tee $(DB).num-triples.txt


# SETTINGS
settings:
	@curl -Gs $(QLEVER_API) --data-urlencode "cmd=get-settings" \
	  | sed 's/[{}]//g; s/:/: /g; s/,/ , /g' && echo

BB_FACTOR_SORTED = 100
BB_FACTOR_UNSORTED = 150
set_bb:
	@echo -e "\033[1mSet factor for BB FILTER cost estimate to $(BB_FACTOR)\033[0m"
	@curl -Gs $(QLEVER_API) --data-urlencode "bounding_box_filter_sorted_cost_estimate=$(BB_FACTOR_SORTED)" \
	                 --data-urlencode "bounding_box_filter_unsorted_cost_estimate=$(BB_FACTOR_UNSORTED)" \
	  > \dev\null
	@$(MAKE) -s settings




#
# The rest is DEPRECATED stuff related to warumup queries. These are now
# configured in the QLever UI and can be either triggered from there or via an
# API call (see target warmup above).
#

# The prefix definitions that are prepended to each warumup query. Note that
# define allows multline strings. Which is exactly why we use define here.
define PREFIXES
endef

# The patterns used in the warmup queries above. Feel free to redefine these as
# you like in your local Makefile. The idea ist that you only have to adapt a
# few and then the warmup queries and the AC queries just work out of the box.

define ENTITY_NAME_AND_ALIAS_PATTERN
BIND(?qleverui_entity AS ?name) BIND(?qleverui_entity AS ?alias)
endef

define ENTITY_SCORE_PATTERN
{ SELECT ?qleverui_entity (COUNT(?qleverui_tmp) AS ?count) WHERE { ?qleverui_entity ql:has-predicate ?qleverui_tmp } GROUP BY ?qleverui_entity }
endef

define PREDICATE_NAME_AND_ALIAS_PATTERN_WITHOUT_CONTEXT
BIND(?qleverui_entity AS ?name) BIND(?qleverui_entity AS ?alias)
endef

define PREDICATE_NAME_AND_ALIAS_PATTERN_WITH_CONTEXT
BIND(?qleverui_entity AS ?name) BIND(?qleverui_entity AS ?alias)
endef

# The NAME_AND_ALIAS patterns above are typically defined with KB-specific
# predicates such as rdfs:label or fb:type.object.name. However usually not all
# entities in a knowledge base have such names. As a fallback (default),
# therefore also names according to the following patterns are used. These can
# also be override. In the definition below, we simply take the whole IRI as
# name and alias.
#
# NOTE: These can also be used to *restrict* the set of predicates shown. We do
# this, for example, for Wikidata: without context, we only show wdt: predicates
# (not p: etc.) and a few select others (like schema:about). See the local
# Makefile in the subfolder wikidata.

define ENTITY_NAME_AND_ALIAS_PATTERN_DEFAULT
BIND(?qleverui_entity AS ?name) BIND(?qleverui_entity AS ?alias)
endef

define PREDICATE_NAME_AND_ALIAS_PATTERN_WITHOUT_CONTEXT_DEFAULT
BIND(?qleverui_entity AS ?name) BIND(?qleverui_entity AS ?alias)
endef

define PREDICATE_NAME_AND_ALIAS_PATTERN_WITH_CONTEXT_DEFAULT
BIND(?qleverui_entity AS ?name) BIND(?qleverui_entity AS ?alias)
endef

# The warmup queries. The used PREFIXES and several patterns defined afterwards.
# These warmup queries are written in such a way that for almost all knowledge
# bases, you have to adapat only the patterns, not these warmup query templates.

define WARMUP_QUERY_1
SELECT ?qleverui_entity (SAMPLE(?name) AS ?qleverui_name) (SAMPLE(?alias) AS ?qleverui_altname) (SAMPLE(?count) AS ?qleverui_count) WHERE {
  { SELECT ?qleverui_entity ?name ?alias ?count WHERE {
    $(ENTITY_SCORE_PATTERN)
    $(ENTITY_NAME_AND_ALIAS_PATTERN) }
  ORDER BY ?qleverui_entity }
} GROUP BY ?qleverui_entity ORDER BY DESC(?qleverui_count)
endef

define WARMUP_QUERY_2
SELECT ?qleverui_entity ?name ?alias ?count WHERE {
  $(ENTITY_SCORE_PATTERN)
  $(ENTITY_NAME_AND_ALIAS_PATTERN)
} ORDER BY ?alias
endef

define WARMUP_QUERY_3
SELECT ?qleverui_entity ?name ?alias ?count WHERE {
  $(ENTITY_SCORE_PATTERN)
  $(ENTITY_NAME_AND_ALIAS_PATTERN)
} ORDER BY ?qleverui_entity
endef

define WARMUP_QUERY_4
SELECT ?qleverui_entity ?name ?alias ?count_1 WHERE {
  { { SELECT ?qleverui_entity (COUNT(?qleverui_entity) AS ?count_1) WHERE { ?x ql:has-predicate ?qleverui_entity } GROUP BY ?qleverui_entity }
    $(PREDICATE_NAME_AND_ALIAS_PATTERN_WITHOUT_CONTEXT) .
    FILTER (?qleverui_entity != <QLever-internal-function/langtag>)
  } UNION {
    { SELECT ?qleverui_entity (COUNT(?qleverui_entity) AS ?count_1) WHERE { ?x ql:has-predicate ?qleverui_entity } GROUP BY ?qleverui_entity }
    $(PREDICATE_NAME_AND_ALIAS_PATTERN_WITHOUT_CONTEXT_DEFAULT) .
    FILTER (?qleverui_entity != <QLever-internal-function/langtag>)
  } }
endef

define WARMUP_QUERY_5
SELECT ?qleverui_entity ?name ?alias ?count_1 WHERE {
  { { SELECT ?qleverui_entity (COUNT(?qleverui_entity) AS ?count_1) WHERE { ?x ql:has-predicate ?qleverui_entity } GROUP BY ?qleverui_entity }
    $(PREDICATE_NAME_AND_ALIAS_PATTERN_WITH_CONTEXT) .
    FILTER (?qleverui_entity != <QLever-internal-function/langtag>)
  } UNION {
    { SELECT ?qleverui_entity (COUNT(?qleverui_entity) AS ?count_1) WHERE { ?x ql:has-predicate ?qleverui_entity } GROUP BY ?qleverui_entity }
    $(PREDICATE_NAME_AND_ALIAS_PATTERN_WITH_CONTEXT_DEFAULT) .
    FILTER (?qleverui_entity != <QLever-internal-function/langtag>)
  } }
endef

# Export all variables defined with define. That way, we can use them as in
# $$PREFIXES (only spaces, no newlines).
export

# Note: Without the @: (which is a no-op), there will be a message "make: ... is
# up to date. The $(info ...) shows the query with newlines.
show-warmup-query-%:
	@$(MAKE) -s help-show-warmup-query-$*

help-show-warmup-query-%:
	@:
	$(info $(PREFIXES))
	$(info $(WARMUP_QUERY_$*))

# Used to extract the result size from a QLever JSON results and pretty print it
# using thousands separators (uses the locale, e.g. en_US.utf8 works fine).
NUMFMT = egrep "\"(resultsize|exception)\"" | tail -1 | sed 's/^\s\+//' | numfmt --delimiter " " --field 2 --suffix , --invalid ignore --grouping | sed 's/,$$//' | xargs -0 printf "\033[34m%s\033[0m"

# Options for API calls to pin results, without actually sending them
PINRESULT = --data-urlencode "pinresult=true" --data-urlencode "send=10"

# Default target: completely clear the cache, then execute the warmup queries
# and pin the results, then clear the unpinned results. Show cache statistics
# and memory usage before and afterwards.
clear_and_pin.DEPRECATED:
	@$(MAKE) -s clear
	@$(MAKE) -s cachestats memory_usage
	@$(MAKE) -s pin.DEPRECATED
	@$(MAKE) -s clear_unpinned
	@$(MAKE) -s cachestats memory_usage
	@echo

# Pin warmup queries, so that AC queries in the QLever UI are always fast.
pin.DEPRECATED:
	@echo
	@echo -e "\033[1mPin: Entities names aliases score, ordered by score, full result for Subject AC query with empty prefix\033[0m"
	@$(MAKE) -s show-warmup-query-1
	curl -Gs $(QLEVER_API) --data-urlencode "query=$$PREFIXES $$WARMUP_QUERY_1" $(PINRESULT) | $(NUMFMT)
	@echo
	@echo -e "\033[1mPin: Entities names aliases score, ordered by alias, part of Subject AC query with non-empty prefix\033[0m"
	@$(MAKE) -s show-warmup-query-2
	curl -Gs $(QLEVER_API) --data-urlencode "query=$$PREFIXES $$WARMUP_QUERY_2" $(PINRESULT) | $(NUMFMT)
	@echo
	@echo -e "\033[1mPin: Entities names aliases score, ordered by entity, part of Object AC query\033[0m"
	@$(MAKE) -s show-warmup-query-3
	curl -Gs $(QLEVER_API) --data-urlencode "query=$$PREFIXES $$WARMUP_QUERY_3" $(PINRESULT) | $(NUMFMT)
	@echo
	@echo -e "\033[1mPin: Predicates names aliases score, without prefix (only wdt: and schema:about)\033[0m"
	@$(MAKE) -s show-warmup-query-4
	curl -Gs $(QLEVER_API) --data-urlencode "query=$$PREFIXES $$WARMUP_QUERY_4" $(PINRESULT) | $(NUMFMT)
	@echo
	@echo -e "\033[1mPin: Predicates names aliases score, with prefix (all predicates)\033[0m"
	@$(MAKE) -s show-warmup-query-5
	curl -Gs $(QLEVER_API) --data-urlencode "query=$$PREFIXES $$WARMUP_QUERY_5" $(PINRESULT) | $(NUMFMT)
	@for P in $(FREQUENT_PREDICATES); do \
	  echo; \
	  echo -e "\033[1mPin: $$P ordered by subject\033[0m"; \
	  echo -e "$$PREFIXES\nSELECT ?x ?y WHERE { ?x $$P ?y } ORDER BY ?x"; \
	  curl -Gs $(QLEVER_API) --data-urlencode "query=$$PREFIXES SELECT ?x ?y WHERE { ?x $$P ?y } ORDER BY ?x" $(PINRESULT) | $(NUMFMT); \
	  echo; \
	  echo -e "\033[1mPin: $$P ordered by object\033[0m"; \
	  echo -e "$$PREFIXES\nSELECT ?x ?y WHERE { ?x $$P ?y } ORDER BY ?x"; \
	  curl -Gs $(QLEVER_API) --data-urlencode "query=$$PREFIXES SELECT ?x ?y WHERE { ?x $$P ?y } ORDER BY ?y" $(PINRESULT) | $(NUMFMT); \
	  done
	@for P in $(FREQUENT_PATTERNS_WITHOUT_ORDER); do \
	  echo; \
	  echo -e "\033[1mPin: $$P without ORDER BY\033[0m"; \
	  echo -e "$$PREFIXES\nSELECT ?x ?y WHERE { ?x $$P ?y }"; \
	  curl -Gs $(QLEVER_API) --data-urlencode "query=$$PREFIXES SELECT ?x ?y WHERE { ?x $$P ?y }" $(PINRESULT) | $(NUMFMT); \
	  done
	@echo


DEPRECATED.show-subject-ac-query:
	@:
	$(info $(SUBJECT_AC_QUERY))

DEPRECATED.show-predicate-ac-query:
	@:
	$(info $(PREDICATE_AC_QUERY))

DEPRECATED.show-object-ac-query:
	@:
	$(info $(OBJECT_AC_QUERY))

DEPRECATED.show-all-ac-queries:
	@echo
	@echo -e "\033[1mSubject AC query\033[0m"
	@echo
	@$(MAKE) -s show-subject-ac-query
	@echo
	@echo -e "\033[1mPredicate AC query\033[0m"
	@echo
	@$(MAKE) -s show-predicate-ac-query
	@echo
	@echo -e "\033[1mObject AC query\033[0m"
	@echo
	@$(MAKE) -s show-object-ac-query
	@echo

define SUBJECT_AC_QUERY
$(PREFIXES)
# IF CURRENT_WORD_EMPTY #

$(WARMUP_QUERY_1)

# ELSE #

SELECT ?qleverui_entity (SAMPLE(?name) AS ?qleverui_name) (SAMPLE(?alias) AS ?qleverui_altname) (SAMPLE(?count) AS ?qleverui_count) WHERE {
  { $(WARMUP_QUERY_2) }
  # IF !CURRENT_WORD_EMPTY #
  FILTER (REGEX(?alias, "^\"%CURRENT_WORD%") || REGEX(?alias, "^<%CURRENT_WORD%"))
  # ENDIF #
} GROUP BY ?qleverui_entity ORDER BY DESC(?qleverui_count)

# ENDIF #
endef

define PREDICATE_AC_QUERY
%PREFIXES%
$(PREFIXES)
# IF !CURRENT_SUBJECT_VARIABLE #

SELECT ?qleverui_entity
              (MIN(?name) as ?qleverui_name)
              (MIN(?alias) as ?qleverui_altname)
              (SAMPLE(?count_2) as ?qleverui_count)
              (SAMPLE(?qleverui_reversed) as ?qleverui_reversed) WHERE {

  { { SELECT ?qleverui_entity (COUNT(?qleverui_tmp) AS ?count_2)
    WHERE { %CURRENT_SUBJECT% ?qleverui_entity ?qleverui_tmp  }
    GROUP BY ?qleverui_entity }
  BIND (0 AS ?qleverui_reversed) }
  UNION
  { { SELECT ?qleverui_entity (COUNT(?qleverui_tmp) AS ?count_2)
    WHERE { ?qleverui_tmp ?qleverui_entity %CURRENT_SUBJECT%  }
    GROUP BY ?qleverui_entity }
    BIND (1 AS ?qleverui_reversed) }
  { $(WARMUP_QUERY_5) }
  # IF !CURRENT_WORD_EMPTY #
  FILTER REGEX(?alias, "%CURRENT_WORD%", "i")
  # ENDIF #
} GROUP BY ?qleverui_entity ORDER BY DESC(?qleverui_count)

# ENDIF #

# IF CONNECTED_TRIPLES_EMPTY AND CURRENT_SUBJECT_VARIABLE #

SELECT ?qleverui_entity
              (MIN(?name) as ?qleverui_name)
              (MIN(?alias) as ?qleverui_altname)
              (SAMPLE(?count_1) as ?qleverui_count) WHERE {
  { $(WARMUP_QUERY_4) }
  # IF !CURRENT_WORD_EMPTY #
  FILTER REGEX(?alias, "%CURRENT_WORD%", "i")
  # ENDIF #
} GROUP BY ?qleverui_entity ORDER BY DESC(?qleverui_count)

# ENDIF #

# IF !CONNECTED_TRIPLES_EMPTY AND CURRENT_SUBJECT_VARIABLE #

SELECT ?qleverui_entity
              (MIN(?name) as ?qleverui_name)
              (MIN(?alias) as ?qleverui_altname)
              (SAMPLE(?count_2) as ?qleverui_count) WHERE {
  { SELECT ?qleverui_entity (COUNT(?qleverui_entity) AS ?count_2)
    WHERE { %CONNECTED_TRIPLES% %CURRENT_SUBJECT% ql:has-predicate ?qleverui_entity }
    GROUP BY ?qleverui_entity }
  { $(WARMUP_QUERY_5) }
  # IF !CURRENT_WORD_EMPTY #
  FILTER REGEX(?alias, "%CURRENT_WORD%", "i")
  # ENDIF #
} GROUP BY ?qleverui_entity ORDER BY DESC(?qleverui_count)

# ENDIF #
endef

define OBJECT_AC_QUERY
%PREFIXES%
$(PREFIXES)
SELECT ?qleverui_entity
              (MIN(?name) AS ?qleverui_name)
              (MIN(?alias) AS ?qleverui_altname)
              (MAX(?count_1) AS ?qleverui_count) WHERE {
  {

    { SELECT ?qleverui_entity ?name ?alias ?count_1 WHERE {
      { SELECT ?qleverui_entity (COUNT(?qleverui_entity) AS ?count_1) WHERE {
        %CONNECTED_TRIPLES% %CURRENT_SUBJECT% %CURRENT_PREDICATE% ?qleverui_entity .
      } GROUP BY ?qleverui_entity }
      { $(WARMUP_QUERY_3) }
      # IF !CURRENT_WORD_EMPTY #
      FILTER (REGEX(?alias, "^\"%CURRENT_WORD%") || REGEX(?alias, "^<%CURRENT_WORD%"))
      # ENDIF #
    } }

  } UNION {

    { SELECT ?qleverui_entity ?name ?alias ?count_1 WHERE {
      { SELECT ?qleverui_entity (COUNT(?qleverui_entity) AS ?count_1) WHERE {
        %CONNECTED_TRIPLES% %CURRENT_SUBJECT% %CURRENT_PREDICATE% ?qleverui_entity
      } GROUP BY ?qleverui_entity }
      $(ENTITY_NAME_AND_ALIAS_PATTERN_DEFAULT)
      # IF !CURRENT_WORD_EMPTY #
      FILTER (REGEX(?qleverui_entity, "^\"%CURRENT_WORD%") || REGEX(?qleverui_entity, "^<%CURRENT_WORD%"))
      # ENDIF #
    } }

  }
} GROUP BY ?qleverui_entity ORDER BY DESC(?qleverui_count)
endef
