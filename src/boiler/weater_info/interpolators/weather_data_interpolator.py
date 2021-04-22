
import pandas as pd


class WeatherDataInterpolator:
    def interpolate_weather_data(self,
                                 weather_data: pd.DataFrame,
                                 start_datetime=None,
                                 end_datetime=None,
                                 inplace=False) -> pd.DataFrame:
        raise NotImplementedError
