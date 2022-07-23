from boiler.constants import column_names, circuit_types

from boiler_softm.constants import column_names as softm_column_names
from boiler_softm.constants.circuit_ids import LYSVA_HOT_WATER_CIRCUIT, LYSVA_HEATING_CIRCUIT

LYSVA_BOILER_AVAILABLE_COLUMNS = [
    column_names.TIMESTAMP,
    column_names.FORWARD_PIPE_COOLANT_TEMP,
    column_names.BACKWARD_PIPE_COOLANT_TEMP,
    column_names.FORWARD_PIPE_COOLANT_PRESSURE,
    column_names.BACKWARD_PIPE_COOLANT_PRESSURE,
    column_names.FORWARD_PIPE_COOLANT_VOLUME,
    column_names.BACKWARD_PIPE_COOLANT_VOLUME
]
LYSVA_BOILER_FLOAT_COLUMNS = [
    column_names.FORWARD_PIPE_COOLANT_TEMP,
    column_names.BACKWARD_PIPE_COOLANT_TEMP,
    column_names.FORWARD_PIPE_COOLANT_PRESSURE,
    column_names.BACKWARD_PIPE_COOLANT_PRESSURE,
    column_names.FORWARD_PIPE_COOLANT_VOLUME,
    column_names.BACKWARD_PIPE_COOLANT_VOLUME
]
LYSVA_APARTMENT_HOUSE_AVAILABLE_COLUMNS = [
    column_names.TIMESTAMP,
    column_names.FORWARD_PIPE_COOLANT_TEMP,
    column_names.BACKWARD_PIPE_COOLANT_TEMP,
]
LYSVA_APARTMENT_HOUSE_FLOAT_COLUMNS = [
    column_names.FORWARD_PIPE_COOLANT_TEMP,
    column_names.BACKWARD_PIPE_COOLANT_TEMP,
]
LYSVA_TEMP_GRAPH_COLUMN_NAMES_EQUALS = {
    softm_column_names.LYSVA_TEMP_GRAPH_WEATHER_TEMP: column_names.WEATHER_TEMP,
    softm_column_names.LYSVA_TEMP_GRAPH_TEMP_AT_IN: column_names.FORWARD_PIPE_COOLANT_TEMP,
    softm_column_names.LYSVA_TEMP_GRAPH_TEMP_AT_OUT: column_names.BACKWARD_PIPE_COOLANT_TEMP
}
LYSVA_WEATHER_INFO_COLUMN_EQUALS = {
    softm_column_names.LYSVA_WEATHER_TEMP: column_names.WEATHER_TEMP
}
LYSVA_HEATING_OBJ_COLUMN_NAMES_EQUALS = {
    softm_column_names.LYSVA_HEATING_SYSTEM_TIMESTAMP: column_names.TIMESTAMP,
    softm_column_names.LYSVA_HEATING_SYSTEM_FORWARD_PIPE_COOLANT_TEMP: column_names.FORWARD_PIPE_COOLANT_TEMP,
    softm_column_names.LYSVA_HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_TEMP: column_names.BACKWARD_PIPE_COOLANT_TEMP,
    softm_column_names.LYSVA_HEATING_SYSTEM_FORWARD_PIPE_COOLANT_VOLUME: column_names.FORWARD_PIPE_COOLANT_VOLUME,
    softm_column_names.LYSVA_HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_VOLUME: column_names.BACKWARD_PIPE_COOLANT_VOLUME,
    softm_column_names.LYSVA_HEATING_SYSTEM_FORWARD_PIPE_COOLANT_PRESSURE: column_names.FORWARD_PIPE_COOLANT_PRESSURE,
    softm_column_names.LYSVA_HEATING_SYSTEM_BACKWARD_PIPE_COOLANT_PRESSURE: column_names.BACKWARD_PIPE_COOLANT_PRESSURE
}
LYSVA_HEATING_OBJ_TIMESTAMP_PARSING_PATTERNS = (
    r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\s(?P<hours>\d{2}):(?P<minutes>\d{2}).{7}",
    r"(?P<day>\d{2})\.(?P<month>\d{2})\.(?P<year>\d{4})\s(?P<hours>\d{1,2}):(?P<minutes>\d{2})"
)
LYSVA_CIRCUIT_EQUALS = {
    LYSVA_HOT_WATER_CIRCUIT: circuit_types.HOT_WATER,
    LYSVA_HEATING_CIRCUIT: circuit_types.HEATING
}

CHERNUSHKA_TEMP_GRAPH_COLUMN_NAMES_EQUALS = {
    softm_column_names.CHERNUSHKA_TEMP_GRAPH_WEATHER_TEMP: column_names.WEATHER_TEMP,
    softm_column_names.CHERNUSHKA_TEMP_GRAPH_TEMP_AT_IN: column_names.FORWARD_PIPE_COOLANT_TEMP,
    softm_column_names.CHERNUSHKA_TEMP_GRAPH_TEMP_AT_OUT: column_names.BACKWARD_PIPE_COOLANT_TEMP
}