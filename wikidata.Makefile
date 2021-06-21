include /local/data/qlever/qlever-proxy/ac-queries.Makefile

DB = wikidata.20210610
API = https://qlever.cs.uni-freiburg.de/api/wikidata
DOCKER_IMAGE = qlever.pr415
# DOCKER_IMAGE = qlever.master
# DOCKER_IMAGE = qlever.pr355-plus
# DOCKER_CONTAINER = qlever.wikidata
MEMORY_FOR_QUERIES = 70
CACHE_MAX_SIZE_GB = 40
CACHE_MAX_SIZE_GB_SINGLE_ENTRY = 5
CACHE_MAX_NUM_ENTRIES = 100
PORT = 7001

FREQUENT_PREDICATES = wdt:P31 wdt:P279 wdt:P31'|'wdt:P279 wdt:P131+ @en@rdfs:label wikibase:sitelinks schema:about

index.THIS_WILL_OVERWRITE_AN_EXISTING_INDEX:
	time ( docker run -it --rm -v $(shell pwd):/index --entrypoint bash --name qlever.$(DB)-index $(DOCKER_IMAGE) -c "bzcat /index/$(DB).ttl.bz2 | IndexBuilderMain -F ttl -f - -l -i /index/$(DB) -s /index/$(DB).settings.json | tee /index/$(DB).index-log.txt"; rm -f $(DB)*tmp* )

define PREFIXES
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX schema: <http://schema.org/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
endef

define ENTITY_NAME_AND_ALIAS_PATTERN
?qleverui_entity @en@rdfs:label ?name . ?qleverui_entity @en@rdfs:label|@en@skos:altLabel ?alias .
endef

define ENTITY_SCORE_PATTERN
{ SELECT ?qleverui_entity (COUNT(?qleverui_tmp) AS ?count) WHERE { ?qleverui_tmp schema:about ?qleverui_entity } GROUP BY ?qleverui_entity }
endef

define PREDICATE_NAME_AND_ALIAS_PATTERN_WITHOUT_CONTEXT
?entity wikibase:directClaim ?qleverui_entity . ?entity @en@rdfs:label ?name . ?entity @en@rdfs:label|@en@skos:altLabel ?alias
endef

define PREDICATE_NAME_AND_ALIAS_PATTERN_WITHOUT_CONTEXT_DEFAULT
VALUES ?qleverui_entity { schema:about_TEMPORARILY_DISABLED } . BIND(?qleverui_entity AS ?name) BIND(?qleverui_entity AS ?alias)
endef

define PREDICATE_NAME_AND_ALIAS_PATTERN_WITH_CONTEXT
?entity ?qleverui_tmp ?qleverui_entity . ?entity @en@rdfs:label ?name . ?entity @en@rdfs:label|@en@skos:altLabel ?alias
endef

define PREDICATE_NAME_AND_ALIAS_PATTERN_WITH_CONTEXT_DEFAULT
BIND(?qleverui_entity AS ?name) BIND(?qleverui_entity AS ?alias)
endef

export
