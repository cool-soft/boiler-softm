from boiler.constants import column_names

from boiler_softm.constants import column_names as softm_column_names

TEMP_GRAPH_COLUMN_NAMES_EQUALS = {
    softm_column_names.TEMP_GRAPH_WEATHER_TEMP: column_names.WEATHER_TEMP,
    softm_column_names.TEMP_GRAPH_TEMP_AT_IN: column_names.FORWARD_PIPE_COOLANT_TEMP,
    softm_column_names.TEMP_GRAPH_TEMP_AT_OUT: column_names.BACKWARD_PIPE_COOLANT_TEMP
}

WEATHER_INFO_COLUMN_EQUALS = {
    softm_column_names.WEATHER_TEMP: column_names.WEATHER_TEMP
}

HEATING_SYSTEM_COLUMN_NAMES_EQUALS = {
    softm_column_names.HEATING_SYSTEM_TIMESTAMP: column_names.TIMESTAMP,
    softm_column_names.HEATING_SYSTEM_CIRCUIT_ID: column_names.CIRCUIT_ID,
    softm_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_TEMP: column_names.FORWARD_PIPE_COOLANT_TEMP,
    softm_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_TEMP: column_names.BACKWARD_PIPE_COOLANT_TEMP,
    softm_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_VOLUME: column_names.FORWARD_PIPE_COOLANT_VOLUME,
    softm_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_VOLUME: column_names.BACKWARD_PIPE_COOLANT_VOLUME,
    softm_column_names.HEATING_SYSTEM_FORWARD_PIPE_COOLANT_PRESSURE: column_names.FORWARD_PIPE_COOLANT_PRESSURE,
    softm_column_names.HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_PRESSURE: column_names.BACKWARD_PIPE_COOLANT_PRESSURE
}
