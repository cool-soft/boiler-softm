from . import soft_m_column_names
from boiler.constants import column_names

DICT = {
    soft_m_column_names.SOFT_M_TEMP_GRAPH_WEATHER_TEMP: column_names.WEATHER_TEMP,
    soft_m_column_names.SOFT_M_TEMP_GRAPH_TEMP_AT_HOME_IN: column_names.FORWARD_PIPE_COOLANT_TEMP,
    soft_m_column_names.SOFT_M_TEMP_GRAPH_TEMP_AT_HOME_OUT: column_names.BACKWARD_PIPE_COOLANT_TEMP
}
