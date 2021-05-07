import pandas as pd
import csv 
import os 
import json
from collections import defaultdict
cord_uid_to_text = defaultdict(list)


df = pd.read_csv('/Users/xuhuili/Desktop/ST_446_Distributed_Computing/project/2021-04-13/metadata.csv')


# open the file 
with open('/Users/xuhuili/Desktop/ST_446_Distributed_Computing/project/2021-04-13/metadata.csv') as f_in:
    
    reader = csv.DictReader(f_in)
        
    for row in reader:
        
        # access some metadata
        cord_uid = row['cord_uid']
        title = row['title']
        abstract = row['abstract']
        authors = row['authors'].split('; ')
        publish_time = row['publish_time']
        url = row['url']
        
        # access the full text for intro
        introduction = []
        conclusion = []
        if row['pdf_json_files']:
            for json_path in row['pdf_json_files'].split('; '):
                with open('/Users/xuhuili/Desktop/ST_446_Distributed_Computing/project/2021-04-13/' + json_path) as f_json:                    
                    full_text_dict = json.load(f_json)

                    # grab introduction section from *some* version of the full text
                    for paragraph_dict in full_text_dict['body_text']:
                        paragraph_text = paragraph_dict['text']
                        section_name = paragraph_dict['section']
                        if 'intro' in section_name.lower():
                            introduction.append(paragraph_text)

                    # stop search other copies of full text if already got introduction 
                    if introduction:
                        break
                        
        # save for later usage 
        cord_uid_to_text[cord_uid].append({
            'title': title,
            'authors': authors,
            'publish_time': publish_time,
            'url': url,
            'abstract': abstract,
            'introduction': introduction
        })
        
        
def get_df(CORD):
    cord_uid, title, authors, publish_time, abstract, introduction, url = list(), list(), list(), list(), list(), list(), list()
    for uid in CORD:
        cord_uid.append(uid)
        title.append(CORD[uid][0]['title'])
        authors.append(CORD[uid][0]['authors'])
        publish_time.append(CORD[uid][0]['publish_time'])
        abstract.append(CORD[uid][0]['abstract'])
        url.append(CORD[uid][0]['url'])
        try:
            introduction.append((CORD[uid][0]['introduction'][0]))
        except IndexError:
            introduction.append('')
    return pd.DataFrame({'cord_uid': cord_uid, 'title': title, 'publish_time': publish_time, 'url': url, 'abstract': abstract, 'introduction': introduction})
        
cord = get_df(cord_uid_to_text)
cord.to_csv('cord.csv') 