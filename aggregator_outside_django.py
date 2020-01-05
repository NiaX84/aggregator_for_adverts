import math

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from aggregator import Aggregator

if __name__ == '__main__':

    records_df = pd.read_pickle('records.pkl')
    aggregated_records_df = Aggregator.group_records_by_description(records_df).copy()
    aggregated_records_df['similarity_id'] = np.nan
    aggregated_records = aggregated_records_df.values
    only_descriptions = aggregated_records[:, 1]
    tf_idf_vectorizer = TfidfVectorizer()
    tf_idf_matrix = tf_idf_vectorizer.fit_transform(only_descriptions)
    selected_records = []
    similarity_id = 0
    for i in range(0, tf_idf_matrix.shape[0]):
        if i+1 in selected_records:
            continue
        description_similarity = cosine_similarity(tf_idf_matrix[i:i+1], tf_idf_matrix)
        record_ids = aggregated_records[np.where(description_similarity > 0.5)[1], 0]
        similarity_id += 1
        aggregated_records_df.loc[aggregated_records_df.record_id.isin(record_ids), 'similarity_id'] = similarity_id
        selected_records.extend(record_ids)
    print(aggregated_records_df)
