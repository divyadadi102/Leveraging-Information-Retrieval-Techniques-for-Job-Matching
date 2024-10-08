!pip install -U sentence-transformers -qq
!pip install pandas -qq
!pip install lshashpy3 -qq
!pip install numpy -qq
!pip install datasketch -qq

import pandas as pd
df = pd.read_csv("linkedInjobs_event_new.csv")

df.drop_duplicates(subset='Job ID', keep='first', inplace=True)

# https://ekzhu.com/datasketch/documentation.html

from sentence_transformers import SentenceTransformer
from datasketch import MinHashLSH, MinHash, MinHashLSHEnsemble

# Load pre-trained sentence transformer model
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# Function to generate MinHash from embeddings
def generate_minhash(embedding):
    m = MinHash(num_perm=256)
    for val in embedding:
        m.update(str(val).encode('utf-8'))
    return m

df.columns = df.columns.str.strip()

%%time
def initialize_lsh(df):
    lsh = MinHashLSH(threshold=0.8, num_perm=256)

    for index, row in df.iterrows():
        job_id = row['Job ID']
        company = row['Company']
        title = row['Title'].lower()
        minhash = generate_minhash(title)
        lsh.insert("job_id = {}, company = {}, title = {}".format( job_id, company, title), minhash)

    return lsh
lsh = initialize_lsh(df)

def query_lsh_by_title(lsh, title):
    minhash = generate_minhash(title.lower())
    results = []
    for key in lsh.query(minhash):
        results.append(key)
    for result in results:
        print(result)
    print("Total matches:", len(results))
query_lsh_by_title(lsh, 'data analyst')

def count_titles_with_words(lsh, title):
    title = title.lower()
    words_to_check = title.split()
    minhash = generate_minhash(title)
    count_containing_words = 0
    count = 0
    for title_line in lsh.query(minhash):
        count += 1
        extracted_title = title_line.split(", title = ")[1].split()
        if any(word in extracted_title for word in words_to_check):
            count_containing_words += 1
    print(f"Number of titles containing words from '{title}': {count_containing_words}, total titles queried: {count}")
count_titles_with_words(lsh, 'data analyst')