from dotenv import load_dotenv
import os
from neo4j import GraphDatabase


load_dotenv()

#NEO4J CONNECTION
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
AUTH = (NEO4J_USERNAME,NEO4J_PASSWORD)

#INTIALIZE DRIVER

driver = GraphDatabase.driver(NEO4J_URI,auth=AUTH)
driver.verify_connectivity()
session = driver.session()
session._verify_authentication()

def papers_by_topic(topic):
    query = """
    MATCH (p:Paper)-[:COVERS]->(t:Topic {name: $topicName})
    MATCH (a:Author)-[:FIRST_AUTHORED]->(p)
    RETURN p.title AS title, a.name AS author_name, p.citation_count AS citation_count, p.publication_date AS date
    ORDER BY p.citation_count DESC
    LIMIT 10
    """
    papers = []
    try:
        result = session.run(query,parameters={"topicName": topic})
        for r in result:
            papers.append({"Title":r["title"],"Author Name":r["author_name"],"Citation Count":r["citation_count"],"Publication Date":r["date"]})
        return papers
    except Exception as e:
        print("Exception in function:"+str(e))
        return []

def papers_same_concept_by_id(paper_id):
    """Get ALL papers which use concepts as the given Paper ID's concepts."""
    papers = []
    try:
        query = """
        MATCH (p:Paper{id: $paperid})-[:DISCUSSES]->(c:Concept)<-[:DISCUSSES]-(p2:Paper)
        WHERE p2.id <> $paperid
        RETURN DISTINCT p2 AS answer_papers;
        """
        result = session.run(query,parameters={"paperid": paper_id})
        for r in result:
            papers.append(r["answer_papers"])
        return papers
    except Exception as e:
        print("Exception in function:"+str(e))
        return []

def hot_topics_past_years(number_of_years:int,number_of_topics:int):
    """Get the most trending research topics for the past N years as provided in the input.
    RETURNS a Tuple in the format: (Topic name, number of publications in the past N years).
    """
    topics = []
    try:
        query = """
                MATCH (p:Paper)-[:HAS_KEYWORD]->(k:Keyword)
                WHERE p.year >= date().year - $numberofyears 
                WITH k.name as topic, count(p) as publications
                ORDER BY publications DESC
                LIMIT $numberoftopics
                RETURN topic, publications;
                """
        result = session.run(query,parameters={"numberofyears":number_of_years,"numberoftopics": number_of_topics})
        for r in result:
            topics.append((r["topic"],r["publications"]))
        return topics
    except Exception as e:
        print("Exception in function: "+str(e))
        return []