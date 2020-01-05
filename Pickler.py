import dill as pickle
import pandas as pd

class Pickler:

    def __init__(self, class_to_pickle):
        self.class_to_pickle = class_to_pickle

    def pickle(self):
        aggregator = self.class_to_pickle()

        with open('zillion_aggregator/aggregator_model/aggregator.pk', 'wb') as file:
            pickle.dump(aggregator, file)

    @staticmethod
    def pickle_df(df, file_name='gps.pkl'):
        pd.to_pickle(df, file_name)


if __name__ == '__main__':
    with open('gps.csv', 'r', encoding='utf-8') as gps_file:
        df = pd.read_csv(gps_file)
        print(df.head())
    Pickler.pickle_df(df)
