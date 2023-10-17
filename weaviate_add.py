from nltk.tokenize import sent_tokenize, word_tokenize
import nltk
from sentence_transformers import SentenceTransformer, util
import weaviate
import re
import json
from tqdm import tqdm
import time
import gc
import os

weaviate_client = weaviate.Client("http://localhost:8080")
weaviate_client.schema.create_class({'class': 'Wiki',
                                'properties': [
                                    {
                                        "dataType": ["text"],
                                        "name": "docid",
                                    },
                                    {
                                        "dataType": ["text"],
                                        "name": "text",
                                    },
                                    {
                                        "dataType": ["text"],
                                        "name": "title",
                                    },
                                    {
                                        "dataType": ["int"],
                                        "name": "uri",
                                    }
                                ],
                                'vectorIndexConfig': {
                                    "distance": "dot",
                                }
                           })

nltk.download('punkt')

def merged_sentence(words):
    '''
    Merge words back to sentence
    
    '''
    if type(words[0]) == str:
        sentence = ' '.join(words)
        sentence = re.sub(r' ([''`"'']),', r'\1,', sentence)
        sentence = re.sub(r'\s([.,;:!?])', r'\1', sentence)
        sentence = re.sub(r'\s([\'])\s', r'\1', sentence)
        sentence = re.sub(r'\s([(])\s', r' \1', sentence)
        sentence = re.sub(r'\s([)])\s', r'\1 ', sentence)
        return sentence
    else:
        return ' '.join([merged_sentence(w) for w in words])

# The Sentence Transformer model has can handle 512 tokens, and is trained using 250 tokens. Therefore, 128+32 words is a good approximation.
SENTENCE_SIZE = 128
OVERLAP_SIZE = 32
def chunk_text(text):
    '''
    Split large text into multiple chunks
    
    '''
    sentences = sent_tokenize(text)
    chunks = []
    current_chunk = []
    current_chunk_size = 0
    for sentence in sentences:
        words = word_tokenize(sentence)
        
        # Estimate if adding the next sentence will exceed the word limit
        if current_chunk_size + len(words) > SENTENCE_SIZE:
            # Add current_chunk to chunks and start a new chunk
            chunks.append(merged_sentence(current_chunk))
            
            # Get the overlap of the next chunk, previous sentence or previous 32 words
            overlap = current_chunk[-1][-OVERLAP_SIZE:] if len(current_chunk[-1]) > OVERLAP_SIZE else current_chunk[-1]
            
            # Preparing the next chunk
            current_chunk = [overlap, words]
            current_chunk_size = len(overlap) + len(words)
        else:
            # Add the sentence to the current chunk
            current_chunk.append(words)
            current_chunk_size += len(words)
            
    chunks.append(merged_sentence(current_chunk))
    return chunks

model = SentenceTransformer('sentence-transformers/multi-qa-mpnet-base-dot-v1')
def text_to_vector(chunks, title):
    '''
    Embed chunk of text
    
    '''
    doc_emb = model.encode([title+": "+c for c in chunks])
    return doc_emb.tolist()
    
batch_size = 500
batch_objects = 0
data_dir = "/app/T-Rex"
for file in os.listdir(data_dir):
    if '.json' in file and found:
        s_time = time.time()
        data = json.load(open(f"{data_dir}/{file}", "r+"))
        
        for d in tqdm(data):
            chunks = chunk_text(d['text'])
            embeddings = text_to_vector(chunks, d['title'])
            
            for chunk_text, chunk_vector in zip(chunks, embeddings):
                try:
                    weaviate_client.batch.add_data_object(
                        {
                            'docid': d['docid'],
                            'title': d['title'],
                            'text': chunk_text,
                            'uri': d['uri']
                        },
                        'Wiki',
                        vector = chunk_vector
                    )
                    
                    batch_objects += 1
                    if batch_objects >= batch_size:
                        weaviate_client.batch.create_objects()
                        batch_objects = 0
                except Exception as e:
                    print(e)

        print(f"Time for file {file} is {int(time.time() - s_time)}sec")
        del data
        gc.collect()
