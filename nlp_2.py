import pandas as pd
import time
import re
from collections import Counter
import nltk
from nltk.corpus import stopwords


class Data_ETL:


    def __init__(self):

        self.engine = create_engine("mysql+mysqlconnector://root:mysql_dev123@localhost/provideosdb")
        self.data_df = pd.read_sql_table('soccerprovideos', self.engine)
        self.data_df['support'] = ''
        self.data_df['count'] = 0
        self.data_df['kw_dict'] = 'nan'
        self.data_df['matched_kw'] = ''
        self.data_df['score'] = 0
        print(self.data_df.head(2))
        self.stops = set(stopwords.words('english'))

    def reload_df(self):
        self.engine = create_engine("mysql+mysqlconnector://root:mysql_dev123@localhost/provideosdb")
        self.data_df = pd.read_sql_table('soccerprovideos', self.engine)
        self.data_df['support'] = ''
        self.data_df['count'] = 0
        self.data_df['kw_dict'] = 'nan'
        self.data_df['matched_kw'] = ''
        self.data_df['score'] = 0
        self.data_df = self.key_word_counter(self.data_df)


    def key_word_counter(self, input_df):
        st = time.time()
        for index, row in input_df.iterrows():
            if str(row['support']) == '':
                text = str(row['name']) + ' ' + str(row['summary']) + ' ' + str(row['description']) + ' ' + str(
                    row['position']) + ' ' + str(row['category'])
                input_df.at[index, 'kw_dict'] = word_freq_counter(text)
            else:
                text = str(row['name']) + ' ' + str(row['summary']) + ' ' + str(row['description']) + ' ' + str(
                    row['position']) + ' ' + str(row['category']) + ' ' + str(row['support'])
                input_df.at[index, 'kw_dict'] = word_freq_counter(text)
        print('Time to Run key_word_counter(sec) - ', time.time() - st)
        return input_df


    def word_freq_counter(self, input_string):
        output_list = Counter(input_string.split(' '))
        output_dict = {}
        for term, count in output_list.most_common():
            if term.lower() not in self.stops:
                output_dict[term.lower()] = count
        return output_dict


    def total_match_counter(self, query_string, word_freq_dict):
        total_match_count = 0
        matched_key_words = []
        for word in query_string.split(' '):
            if word.lower() not in self.stops:
                match_count = word_freq_dict.get(word.lower(), 0)
                if match_count != 0:
                    matched_key_words.append(word)
                    total_match_count += match_count
        return matched_key_words, total_match_count


    def search_pro(self, query, N=3):
        st = time.time()
        query_len = len(query)
        for index, row in self.data_df.iterrows():
            word_freq_dict = self.data_df.at[index, 'kw_dict']
            matched_kw, count = total_match_counter(query, word_freq_dict)
            #print(row['id'], matched_kw)
            self.data_df.at[index, 'matched_kw'] = " ".join(matched_kw)
            self.data_df.at[index, 'count'] = count
            self.data_df.at[index, 'score'] = count / query_len

        final_df = self.data_df.sort_values(by=['count'], ascending=False)
        output = []

        for index, row in final_df.head(N).iterrows():
            if row['score'] > 0.85 and row['score'] < 0.95:
                score = row['score'] + 0.9
            elif row['score'] > 0.75 and row['score'] < 0.85:
                score = row['score'] + 0.11
            elif row['score'] > 0.65 and row['score'] < 0.75:
                score = row['score'] + 0.13
            elif row['score'] > 0.30 and row['score'] < 0.65:
                score = row['score'] + 0.20
            else:
                score = 50.4758

            output_dict = {
                "id": row['id'],
                "name": row['name'],
                "url": row['url'],
                "ratio": score,
                "video_thubnail_id": int(row['videoThumbnail']),
                "thubnail": row['videoThumbnailUrl']
            }
            output.append(output_dict)

        print('Time for KW Search - ', time.time() - st)
        print('Output  -', output)

        return {
            'query': query,
            'output': output
        }


    def query_processor(self, query_string):
        query_list = []
        for word in query_string.split(' '):
            if word.lower() not in stops:
                query_list.append(word.lower())
        return " ".join(query_list)


    def add_support(self, id, query):
        try:
            value = self.data_df.loc[self.data_df['id'] == id, 'support'].values[0]
        except Exception as E:
            print('Error in add support - ', E)
            #logging.debug(" ********* Error in add support ********** ".format(E))
            return False

        if value == '':
            self.data_df.loc[self.data_df['id'] == id, 'support'] = self.query_processor(query)
        else:
            support_list = value.split(" ")
            support_list.extend(self.get_optimized_query_keywords(query).split(" "))
            new_value = list(set(support_list))
            value = " ".join(new_value)
            self.data_df.loc[self.data_df['id'] == id, 'support'] = value

        self.data_df.drop(columns=['kw_dict', 'score', 'count', 'matched_kw'], axis = 1, inplace=True)
        print(self.data_df.columns)
        self.data_df.to_sql('soccerprovideos', self.engine, schema=None, if_exists='replace', index=False, chunksize=None, dtype=None, method=None)
        self.data_df = self.key_word_counter(self.data_df)
        print('Updated Support Value -', self.data_df.loc[self.data_df['id'] == id, 'support'].values[0])
        return True
