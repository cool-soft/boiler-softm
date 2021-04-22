import pandas as pd


class HeatingSystemRepository:

    def list(self) -> list:
        raise NotImplementedError

    def get_dataset(self,
                    dataset_id: str,
                    start_datetime: pd.Timestamp = None,
                    end_datetime: pd.Timestamp = None):
        raise NotImplementedError

    def set_dataset(self, dataset_id: str, dataset: pd.DataFrame):
        raise NotImplementedError

    def update_dataset(self, dataset_id: str, dataset: pd.DataFrame):
        raise NotImplementedError
