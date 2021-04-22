from ...constants import column_names
from . import soft_m_column_names

DICT = {
    soft_m_column_names.TIMESTAMP: column_names.TIMESTAMP,
    soft_m_column_names.CIRCUIT_ID: column_names.CIRCUIT_ID,
    soft_m_column_names.FORWARD_PIPE_COOLANT_TEMP: column_names.FORWARD_PIPE_COOLANT_TEMP,
    soft_m_column_names.BACKWARD_PIPE_COOLANT_TEMP: column_names.BACKWARD_PIPE_COOLANT_TEMP,
    soft_m_column_names.FORWARD_PIPE_COOLANT_VOLUME: column_names.FORWARD_PIPE_COOLANT_VOLUME,
    soft_m_column_names.BACKWARD_PIPE_COOLANT_VOLUME: column_names.BACKWARD_PIPE_COOLANT_VOLUME,
    soft_m_column_names.FORWARD_PIPE_COOLANT_PRESSURE: column_names.FORWARD_PIPE_COOLANT_PRESSURE,
    soft_m_column_names.BACKWARD_PIPE_COOLANT_PRESSURE: column_names.BACKWARD_PIPE_COOLANT_PRESSURE
}
