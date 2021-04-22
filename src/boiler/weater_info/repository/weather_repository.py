import pandas as pd


class WeatherRepository:

    async def get_weather_info(self, start_datetime: pd.Timestamp = None, end_datetime: pd.Timestamp = None):
        raise NotImplementedError

    async def set_weather_info(self, weather_df: pd.DataFrame):
        raise NotImplementedError

    async def update_weather_info(self, weather_df: pd.DataFrame):
        raise NotImplementedError

    async def delete_weather_info_older_than(self, datetime: pd.Timestamp):
        raise NotImplementedError
