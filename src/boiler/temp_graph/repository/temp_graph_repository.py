import pandas as pd


class TempGraphRepository:

    async def get_temp_graph(self) -> pd.DataFrame:
        raise NotImplementedError

    async def set_temp_graph(self, temp_graph: pd.DataFrame):
        raise NotImplementedError
