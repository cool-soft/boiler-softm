from boiler_softm.constants import circuit_ids as soft_m_circuit_ids
from boiler.constants import circuit_types

CIRCUIT_ID_EQUALS = {
    soft_m_circuit_ids.HOT_WATER_CIRCUIT: circuit_types.HOT_WATER,
    soft_m_circuit_ids.HEATING_CIRCUIT: circuit_types.HEATING
}
