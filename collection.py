# For now I will use OpenAlex via requests to get the data.
import requests
import csv
import time
import json

#CONFIG
KEYWORDS = ["Natural language processing","Large language models","Language models","Transformers","NLP","RAG","Knowledge graphs"]
RESULTS_LIMIT = 100  
CSV_OUTPUT = "researchdata/kg_triplets.csv"
TIME_SLEEP = 1
PAPER_IDS_FILE ="paperids/ids.txt"

def extract_triplets(paper):
    triplets = []
    paper_title = paper.get("display_name", "").strip()
    
    # Authors
    for author in paper.get("authorships", []):
        name = author["author"]["display_name"]
        triplets.append((paper_title, "AUTHORED_BY", name))

    # Concepts (filtering only those that match the filter topic)
    for concept in paper.get("concepts", []):
        if FILTER_TOPIC.lower() in concept["display_name"].lower():
            triplets.append((paper_title, "HAS_CONCEPT", concept["display_name"]))

    # Venue
    venue = paper.get("host_venue", {}).get("display_name")
    if venue:
        triplets.append((paper_title, "PUBLISHED_IN", venue))

    return triplets

def save_triplets_to_csv(triplets, filename):
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["subject", "predicate", "object"])
        writer.writerows(triplets)

def extract_id_from_url(url):
    return url.strip().split("/")[-1]

def search_openalex_keyword(keyword, per_page=RESULTS_LIMIT):
    print(f"Searching OpenAlex for keyword: '{keyword}'")
    url = "https://api.openalex.org/works"
    params = {
        "search": keyword,
        "per-page": per_page
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"Error: {response.status_code} - {response.text}")
    return response.json().get("results", [])

def search_openalex_id(id):
    url = f"https://api.openalex.org/works/{id}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Error: {response.status_code} - {response.text}")
    return response.json()

def get_all_papers_by_keyword(keyword_list = KEYWORDS, per_page=RESULTS_LIMIT):
    all_paper_ids = []
    for keyword in keyword_list:
        papers = search_openalex_keyword(keyword, per_page)
        for paper in papers:
            all_paper_ids.append(paper.get("id"))
    final_list = list(set(all_paper_ids))
    print(final_list)
    with open(PAPER_IDS_FILE,"w") as f:
        for id in final_list:
            f.write(id + "\n")
    print(f"✅ Saved {len(final_list)} paper ids to '{PAPER_IDS_FILE}'")

def fetch_metadata(paper_id):
    url = f"https://api.openalex.org/works/{paper_id}"
    try:
        resp = requests.get(url)
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"⚠️ Failed for {paper_id}: {resp.status_code}")
            return None
    except Exception as e:
        print(f"❌ Exception for {paper_id}: {e}")
        return None

def store_paper_metadata(paper_id):
    md = fetch_metadata(paper_id)
    if md is None:
        return None
    #Top level Fields first, as many nested JSONs are there as well.
    id = md.get('id')
    title = md.get('title')
    publication_year = md.get('publication_year')
    language = md.get('language')
    countries_distinct_count = md.get('countries_distinct_count')
    institutions_distinct_count = md.get('institutions_distinct_count')
    cited_by_count = md.get('cited_by_count')
    locations_count = md.get('locations_count')
    papertype = md.get('type')
    fwci = md.get('fwci')
    referenced_works_count = md.get('referenced_works_count')
    # From the Primary Location object.
    primary_location = md.get('primary_location',None)
    primary_source = None
    primary_is_oa = "None"
    primary_license = "None"
    primary_version = "None"
    is_accepted = "None"  
    is_published = "None"
    if primary_location is not None:
        primary_is_oa = primary_location.get('is_oa')
        primary_license = primary_location.get('license')
        primary_version = primary_location.get('version')
        is_accepted = primary_location.get('is_accepted')    
        is_published = primary_location.get('is_published')
        primary_source = primary_location.get('source',None)
    # From the Source Object inside the Primary Location object.
    journal_id = "None"
    journal_name = "None"
    host_id = "None"
    host_name = "None"
    if primary_source is not None:
        journal_id = primary_source.get('id')
        journal_name = primary_source.get('display_name')
        host_id = primary_source.get('host_organization')
        host_name = primary_source.get('host_organization_name')
    # From the Open Access object.
    open_access = md.get('open_access')
    is_oa = open_access.get('is_oa')
    oa_status = open_access.get('oa_status')
    # From the Authorship List
    authorships = md.get('authorships')
    #Authors Data and First author data
    first_author_id = "None"
    first_author_name = "None"
    first_author_orcid = "None"
    author_data_list = []
    try:
        if len(authorships) > 0:
            first_author_block = authorships[0]
            first_author_id = first_author_block.get('author').get('id')
            first_author_name = first_author_block.get('author').get('display_name')
            first_author_orcid = first_author_block.get('author').get('orcid')
        if len(authorships) > 1:
            for i in range(len(authorships)):
                author_block = authorships[i]
                author_id = author_block.get('author').get('id')
                author_name = author_block.get('author').get('display_name')
                author_orcid = author_block.get('author').get('orcid')
                author_data_list.append((author_id,author_name,author_orcid))
    except:
        author_data_list.append(("None","None","None"))
    #Paper Topic Data
    topics = []
    topic_data = md.get('topics',[])
    for topic in topic_data:
        topics.append((topic.get('id'),topic.get('display_name')))
    # Paper Keywords Data
    paper_keywords = []
    keyword_data = md.get('keywords',[])
    for k in keyword_data:
        paper_keywords.append((k.get('id'),k.get('display_name')))
    # Paper Concepts Data
    paper_concepts = []
    concepts_data = md.get('concepts',[])
    for concept in concepts_data:
        paper_concepts.append((concept.get('id'),concept.get('display_name')))
    #Related Works Data
    related_works = md.get("related_works",[])
    return [id,title,publication_year,language,primary_is_oa,primary_license,primary_version,is_accepted,is_published,papertype,is_oa,
            oa_status,countries_distinct_count,journal_id,journal_name,host_id,host_name,
            institutions_distinct_count,cited_by_count,fwci,locations_count,first_author_id,
            first_author_name,first_author_orcid,author_data_list,paper_keywords,topics,paper_concepts,related_works,referenced_works_count]

def main():     
    
    print("Fetching data for the followinf keywords:")
    print(KEYWORDS)
    print("\n")
    print("Check if you want to change the keywords.")
    
    get_all_papers_by_keyword()
    
    with open(PAPER_IDS_FILE,"r") as f:
        urls = f.readlines()
        paper_ids = list(set([extract_id_from_url(url) for url in urls]))
    print(f"🔍 Found {len(paper_ids)} unique OpenAlex IDs")
    print(store_paper_metadata(paper_ids[-1]))

    #Saving the Paper Data as a CSV.

    headers = [
    "id", "title", "publication_year", "language", "primary_is_oa", "primary_license", "primary_version", "is_accepted",
    "is_published", "papertype", "is_oa", "oa_status", "countries_distinct_count", "journal_id", "journal_name",
    "host_id", "host_name", "institutions_distinct_count", "cited_by_count", "fwci", "locations_count",
    "first_author_id", "first_author_name", "first_author_orcid", "author_data_list", "paper_keywords", "topics",
    "paper_concepts", "related_works","referenced_works_count"
    ]   
    main_data = []

    for paper in paper_ids:
        returned_data = store_paper_metadata(paper)
        returned_data = [json.dumps(x) if isinstance(x, (list, dict)) else x for x in returned_data]
        main_data.append(returned_data)
    
    print(main_data)
    with open("researchdata/collected.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(main_data)

if __name__ == "__main__":
    main()
