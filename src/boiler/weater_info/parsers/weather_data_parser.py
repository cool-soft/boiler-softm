from typing import Union, IO

import pandas as pd


class WeatherDataParser:

    def parse_weather_data(self, weather_data: Union[str, IO]) -> pd.DataFrame:
        raise NotImplementedError
