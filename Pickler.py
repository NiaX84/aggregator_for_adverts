import dill as pickle

from aggregator import Aggregator


class Pickler:

    def __init__(self, class_to_pickle):
        self.class_to_pickle = class_to_pickle

    def pickle(self):
        aggregator = self.class_to_pickle()

        with open('zillion_aggregator/aggregator_model/aggregator.pk', 'wb') as file:
            pickle.dump(aggregator, file)


if __name__ == '__main__':
    Pickler(Aggregator).pickle()
