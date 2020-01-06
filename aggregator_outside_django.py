import math
from itertools import groupby
from operator import itemgetter

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from aggregator import Aggregator

if __name__ == '__main__':
    records_df = pd.read_pickle('records.pkl')
    records_df['record_id'] = range(1, len(records_df) + 1)
    aggregated_records = records_df[['record_id', 'description']].values
    print(len(aggregated_records))
    only_descriptions = aggregated_records[:, 1]
    tf_idf_vectorizer = TfidfVectorizer()
    tf_idf_matrix = tf_idf_vectorizer.fit_transform(only_descriptions)
    description_similarity = np.array(
        [cosine_similarity(tf_idf_matrix[i: i+1], tf_idf_matrix)[0] for i in range(tf_idf_matrix.shape[0])])
    print(description_similarity)
    similarity_indices = np.transpose(aggregated_records[np.nonzero(description_similarity > 0.5), 0])
    similarity_indices_pdf = pd.DataFrame(similarity_indices, columns=['record_id', 'similar_record_id'])
    similar_records_pdf = similarity_indices_pdf.groupby('record_id', axis=0).agg(tuple)
    print(similar_records_pdf)
