from typing import Union, IO

import pandas as pd


class HeatingSystemDataParser:

    def parse(self, data: Union[str, IO]) -> pd.DataFrame:
        raise NotImplementedError
