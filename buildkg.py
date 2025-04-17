from dotenv import load_dotenv
import os
from neo4j import GraphDatabase

load_dotenv()

#NEO4J CONNECTION
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
AUTH = (NEO4J_USERNAME,NEO4J_PASSWORD)

# All Cypher queries

#Drop existing constraints if needed
Drop_constraint_cyphers = [
"DROP CONSTRAINT paper_id IF EXISTS;",
"DROP CONSTRAINT author_id IF EXISTS;",
"DROP CONSTRAINT journal_id IF EXISTS;",
"DROP CONSTRAINT institution_name IF EXISTS;",
"DROP CONSTRAINT topic_name IF EXISTS;",
"DROP CONSTRAINT concept_name IF EXISTS;",
"DROP CONSTRAINT country_name IF EXISTS;",
"DROP CONSTRAINT license_name IF EXISTS;"
]

#Delete existing papers if needed
Drop_papers_cypher = "MATCH (n:Paper) DETACH DELETE n;"

#Constraints
Constraint_cyphers = [
"CREATE CONSTRAINT paper_id IF NOT EXISTS FOR (p:Paper) REQUIRE p.id IS UNIQUE;",
"CREATE CONSTRAINT author_id IF NOT EXISTS FOR (a:Author) REQUIRE a.id IS UNIQUE;",
"CREATE CONSTRAINT journal_id IF NOT EXISTS FOR (j:Journal) REQUIRE j.id IS UNIQUE;",
"CREATE CONSTRAINT topic_id IF NOT EXISTS FOR (t:Topic) REQUIRE t.id IS UNIQUE;",
"CREATE CONSTRAINT concept_id IF NOT EXISTS FOR (c:Concept) REQUIRE c.id IS UNIQUE;",
"CREATE INDEX paper_year_idx IF NOT EXISTS FOR (p:Paper) ON (p.publication_year);",
"CREATE INDEX paper_title_idx IF NOT EXISTS FOR (p:Paper) ON (p.title);",
"CREATE INDEX author_name_idx IF NOT EXISTS FOR (a:Author) ON (a.name);",
"CREATE INDEX journal_name_idx IF NOT EXISTS FOR (j:Journal) ON (j.name);"
]

#Papers
Load_papers_from_csv = """
LOAD CSV WITH HEADERS FROM 'file:///collected.csv' AS row
MERGE (p:Paper {id: row.id})
SET p.title = row.title,
    p.year = toInteger(row.publication_year),
    p.language = row.language,
    p.is_open_access = (row.is_oa = 'True'),
    p.oa_status = row.oa_status,
    p.paper_type = row.papertype,
    p.is_accepted = (row.is_accepted = 'True'),
    p.is_published = (row.is_published = 'True'),
    p.cited_by_count = toInteger(COALESCE(row.cited_by_count, '0')),
    p.fwci = CASE WHEN row.fwci <> '' THEN toFloat(row.fwci) ELSE null END,
    p.referenced_works_count = toInteger(COALESCE(row.referenced_works_count, '0'));
    """

#Journals
Load_journals_from_csv = """
LOAD CSV WITH HEADERS FROM 'file:///collected.csv' AS row
WITH row
WHERE row.journal_id IS NOT NULL AND row.journal_id <> ''
MERGE (j:Journal {id: row.journal_id})
SET j.name = row.journal_name
WITH row, j
MATCH (p:Paper {id: row.id})
MERGE (p)-[:PUBLISHED_IN]->(j);
"""

#Hosts
Load_hosts_from_csv = """
LOAD CSV WITH HEADERS FROM 'file:///collected.csv' AS row
WITH row
WHERE row.host_id IS NOT NULL AND row.host_id <> ''
MERGE (h:Host {id: row.host_id})
SET h.name = row.host_name
WITH row, h
MATCH (p:Paper {id: row.id})
MERGE (p)-[:HOSTED_BY]->(h);
"""

#Authors
Load_authors_from_csv = """
LOAD CSV WITH HEADERS FROM 'file:///collected.csv' AS row
WITH row
WHERE row.first_author_id IS NOT NULL AND row.first_author_id <> ''
MERGE (a:Author {id: row.first_author_id})
SET a.name = row.first_author_name,
    a.orcid = CASE WHEN row.first_author_orcid <> '' THEN row.first_author_orcid ELSE null END
WITH row, a
MATCH (p:Paper {id: row.id})
MERGE (a)-[:FIRST_AUTHORED]->(p);
"""

#Topics
Load_topics_from_csv = """
LOAD CSV WITH HEADERS FROM 'file:///collected.csv' AS row
MATCH (p:Paper {id: row.id})
WITH p, row
WHERE row.topics IS NOT NULL AND row.topics <> ''
WITH p, apoc.text.regexGroups(row.topics, '"(https://openalex.org/T\\d+)"\\s*,\\s*"([^"]+)"') AS topics
UNWIND topics AS topic
WITH p, topic
WHERE topic[1] IS NOT NULL
MERGE (t:Topic {id: topic[1]})
SET t.name = topic[2]
MERGE (p)-[:COVERS]->(t);
"""

#Concepts
Load_concepts_from_csv = """
LOAD CSV WITH HEADERS FROM 'file:///collected.csv' AS row
MATCH (p:Paper {id: row.id})
WITH p, row
WHERE row.paper_concepts IS NOT NULL AND row.paper_concepts <> ''
WITH p, apoc.text.regexGroups(row.paper_concepts, '"(https://openalex.org/C\\d+)"\\s*,\\s*"([^"]+)"') AS concepts
UNWIND concepts AS concept
WITH p, concept
WHERE concept[1] IS NOT NULL
MERGE (c:Concept {id: concept[1]})
SET c.name = concept[2]
MERGE (p)-[:DISCUSSES]->(c);
"""

#Keywords
Load_keywords_from_csv = """
LOAD CSV WITH HEADERS FROM 'file:///collected.csv' AS row
MATCH (p:Paper {id: row.id})
WITH p, row
WHERE row.paper_keywords IS NOT NULL AND row.paper_keywords <> ''
WITH p, apoc.text.regexGroups(row.paper_keywords, '"(https://openalex.org/keywords/[^"]+)"\\s*,\\s*"([^"]+)"') AS keywords
UNWIND keywords AS keyword
WITH p, keyword
WHERE keyword[1] IS NOT NULL
MERGE (k:Keyword {id: keyword[1]})
SET k.name = keyword[2]
MERGE (p)-[:HAS_KEYWORD]->(k);
"""

#Related works
Load_related_works_from_csv = """
LOAD CSV WITH HEADERS FROM 'file:///collected.csv' AS row
MATCH (p1:Paper {id: row.id})
WITH p1, row
WHERE row.related_works IS NOT NULL AND row.related_works <> ''
// Extract related work IDs with regex
WITH p1, apoc.text.regexGroups(row.related_works, '"(https://openalex.org/W\\d+)"') AS related_works
UNWIND related_works AS related
WITH p1, related
WHERE related[1] IS NOT NULL
MERGE (p2:Paper {id: related[1]})
MERGE (p1)-[:RELATED_TO]->(p2);
"""

with GraphDatabase.driver(NEO4J_URI,auth=AUTH) as driver:
    driver.verify_authentication()
    driver.verify_connectivity()
    print(driver.execute_query("RETURN apoc.version() AS output;"))
    for query in Constraint_cyphers:
        driver.execute_query(query)
    print(driver.execute_query("SHOW CONSTRAINTS;"))
    for query in [Load_papers_from_csv,Load_authors_from_csv,Load_concepts_from_csv,Load_hosts_from_csv,Load_journals_from_csv,
                  Load_keywords_from_csv,Load_related_works_from_csv,Load_topics_from_csv]:
        driver.execute_query(query)
    print(driver.execute_query("MATCH (n:Paper) RETURN count(n)")) #Can replace papers with other entities to check count.

#NODES: Paper, Author, Keyword, Concept, Topic, Journal, Host
#RELATIONSHIPS: RELATED_TO,HAS_KEYWORD,PUBLISHED_IN,HOSTED_BY,FIRST_AUTHORED,COVERS,DISCUSSES