import pandas as pd


class TempRequirementsRepository:

    async def get_temp_requirements(self,
                                    start_datetime: pd.Timestamp = None,
                                    end_datetime: pd.Timestamp = None) -> pd.DataFrame:
        raise NotImplementedError

    async def set_temp_requirements(self, temp_requirements_df: pd.DataFrame):
        raise NotImplementedError

    async def update_temp_requirements(self, temp_requirements_df: pd.DataFrame):
        raise NotImplementedError

    async def delete_temp_requirements_older_than(self, datetime: pd.Timestamp):
        raise NotImplementedError

    async def get_max_timestamp(self) -> pd.Timestamp:
        raise NotImplementedError
