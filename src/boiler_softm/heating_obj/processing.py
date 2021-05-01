from typing import Union

import pandas as pd
from boiler.heating_obj.processing import AbstractHeatingObjProcessor


class SoftMLysvaMKDProcessor(AbstractHeatingObjProcessor):

    def process_heating_obj(self,
                            heating_obj: pd.DataFrame,
                            min_required_timestamp: Union[pd.Timestamp, None],
                            max_required_timestamp: Union[pd.Timestamp, None]) -> pd.DataFrame:
        pass


class SoftMLysva308BoilerProcessor(AbstractHeatingObjProcessor):

    def process_heating_obj(self,
                            heating_obj: pd.DataFrame,
                            min_required_timestamp: Union[pd.Timestamp, None],
                            max_required_timestamp: Union[pd.Timestamp, None]) -> pd.DataFrame:
        pass
