import re
import unicodedata
from enum import Enum
from typing import TypedDict
from sqlalchemy import (
	Float,
	Integer,
	String,
)

PROFILE_COLUMNS = [
	'profile_fuelused',
	'profile_fuelcost',
	'profile_time',
	'profile_distanceWhilstConnectedToOBD',
	'profile_distance',
	'profile_date',
]

# --- Source data used to build COLUMN_SCHEMA below -------------------------
#
# _RAW_COLUMN_TYPES: every raw Torque CSV header variant ever seen (legacy
# underscored headers like "GPS_Time" as well as their canonical lowercase
# form like "gpstime") mapped to its SQLAlchemy column type. Where the same
# canonical column is reachable through more than one raw spelling, the type
# here is treated as authoritative (it drives the actual numeric coercion and
# ALTER TABLE dtype decisions in utils.py).
_RAW_COLUMN_TYPES = {
	"GPS_Time": String,
	"Device_Time": String,
	"longitude": Float,
	"latitude": Float,
	"GPS_Speed_Meterssecond": Float,
	"Horizontal_Dilution_of_Precision": Float,
	"Altitude": Float,
	"Bearing": Float,
	"Gx": Float,
	"Gy": Float,
	"Gz": Float,
	"Gcalibrated": Float,
	"Acceleration_SensorTotalg": Float,
	"Acceleration_SensorX_axisg": Float,
	"Acceleration_SensorY_axisg": Float,
	"Acceleration_SensorZ_axisg": Float,
	"Actual_engine_torque": Float,
	"Air_Fuel_RatioMeasured1": Float,
	"Android_device_Battery_Level": Integer,
	"Average_trip_speedwhilst_moving_onlykmh": Float,
	"Average_trip_speedwhilst_stopped_or_movingkmh": Float,
	"Barometric_pressure_from_vehiclepsi": Float,
	"CO_in_gkm_Averagegkm": Float,
	"CO_in_gkm_Instantaneousgkm": Float,
	"Distance_to_empty_Estimatedkm": Float,
	"Distance_travelled_with_MILCEL_litkm": Float,
	"Engine_Coolant_TemperatureC": Float,
	"Engine_kW_At_the_wheelskW": Float,
	"Engine_Load": Float,
	"Engine_RPMrpm": Float,
	"Fuel_cost_tripcost": Float,
	"Fuel_flow_ratehourlhr": Float,
	"Fuel_flow_rateminuteccmin": Float,
	"Fuel_Rail_Pressurepsi": Float,
	"Fuel_Remaining_Calculated_from_vehicle_profile": Float,
	"Fuel_used_tripl": Float,
	"GPS_Accuracym": Float,
	"GPS_Altitudem": Float,
	"GPS_Bearing": Float,
	"GPS_Latitude": Float,
	"GPS_Longitude": Float,
	"GPS_Satellites": Integer,
	"GPS_vs_OBD_Speed_differencekmh": Float,
	"Horsepower_At_the_wheelshp": Float,
	"Intake_Air_TemperatureC": Float,
	"Intake_Manifold_Pressurepsi": Float,
	"Kilometers_Per_LitreInstantkpl": Float,
	"Kilometers_Per_LitreLong_Term_Averagekpl": Float,
	"Litres_Per_100_KilometerInstantl100km": Float,
	"Litres_Per_100_KilometerLong_Term_Averagel100km": Float,
	"Mass_Air_Flow_Rategs": Float,
	"Miles_Per_GallonInstantmpg": Float,
	"Miles_Per_GallonLong_Term_Averagempg": Float,
	"O2_Sensor1_Wide_Range_CurrentmA": Float,
	"O2_Bank_1_Sensor_1_Wide_Range_Equivalence_Ratio": Float,
	"O2_Bank_1_Sensor_1_Wide_Range_VoltageV": Float,
	"Speed_GPSkmh": Float,
	"Speed_OBDkmh": Float,
	"TorqueNm": Float,
	"Trip_average_KPLkpl": Float,
	"Trip_average_Litres100_KMl100km": Float,
	"Trip_average_MPGmpg": Float,
	"Trip_Distancekm": Float,
	"Trip_distance_stored_in_vehicle_profilekm": Float,
	"Trip_TimeSince_journey_starts": Float,
	"Trip_timewhilst_movings": Float,
	"Trip_timewhilst_stationarys": Float,
	"Turbo_Boost_Vacuum_Gaugepsi": Float,
	"Voltage_OBD_AdapterV": Float,
	"Volumetric_Efficiency_Calculated": Float,
	"Ambient_air_tempC": Float,
	"Cost_per_milekm_Instantkm": Float,
	"Cost_per_milekm_Tripkm": Float,
	"Positive_Kinetic_Energy_PKEkmhr": Float,
	"Throttle_PositionManifold": Float,
	"Voltage_Control_ModuleV": Float,
	"O2_Sensor1_Wide_Range_Equivalence_Ratio": Float,
	"O2_Sensor1_Wide_Range_VoltageV": Float,
	"gpstime": String,
	"devicetime": String,
	"gpsspeedmeterssecond": Float,
	"horizontaldilutionofprecision": Float,
	"altitude": Float,
	"bearing": Float,
	"gx": Float,
	"gy": Float,
	"gz": Float,
	"gcalibrated": Float,
	"accelerationsensortotalg": Float,
	"accelerationsensorxaxisg": Float,
	"accelerationsensoryaxisg": Float,
	"accelerationsensorzaxisg": Float,
	"actualenginetorque": Float,
	"airfuelratiomeasured1": Float,
	"androiddevicebatterylevel": Integer,
	"averagetripspeedwhilstmovingonlykmh": Float,
	"averagetripspeedwhilststoppedormovingkmh": Float,
	"barometricpressurefromvehiclepsi": Float,
	"coingkmaveragegkm": Float,
	"coingkminstantaneousgkm": Float,
	"distancetoemptyestimatedkm": Float,
	"distancetravelledwithmilcellitkm": Float,
	"enginecoolanttemperaturec": Float,
	"enginekwatthewheelskw": Float,
	"engineload": Float,
	"enginerpmrpm": Float,
	"fuelcosttripcost": Float,
	"fuelflowratehourlhr": Float,
	"fuelflowrateminuteccmin": Float,
	"fuelrailpressurepsi": Float,
	"fuelremainingcalculatedfromvehicleprofile": Float,
	"fuelusedtripl": Float,
	"gpsaccuracym": Float,
	"gpsaltitudem": Float,
	"gpsbearing": Float,
	"gpslatitude": Float,
	"gpslongitude": Float,
	"gpssatellites": Integer,
	"gpsvsobdspeeddifferencekmh": Float,
	"horsepoweratthewheelshp": Float,
	"intakeairtemperaturec": Float,
	"intakemanifoldpressurepsi": Float,
	"kilometersperlitreinstantkpl": Float,
	"kilometersperlitrelongtermaveragekpl": Float,
	"litresper100kilometerinstantl100km": Float,
	"litresper100kilometerlongtermaveragel100km": Float,
	"massairflowrategs": Float,
	"milespergalloninstantmpg": Float,
	"milespergallonlongtermaveragempg": Float,
	"o2sensor1widerangecurrentma": Float,
	"o2bank1sensor1widerangeequivalenceratio": Float,
	"o2bank1sensor1widerangevoltagev": Float,
	"speedgpskmh": Float,
	"speedobdkmh": Float,
	"torquenm": Float,
	"tripaveragekplkpl": Float,
	"tripaveragelitres100kml100km": Float,
	"tripaveragempgmpg": Float,
	"tripdistancekm": Float,
	"tripdistancestoredinvehicleprofilekm": Float,
	"triptimesincejourneystarts": Float,
	"triptimewhilstmovings": Float,
	"triptimewhilststationarys": Float,
	"turboboostvacuumgaugepsi": Float,
	"voltageobdadapterv": Float,
	"volumetricefficiencycalculated": Float,
	"ambientairtempc": Float,
	"costpermilekminstantkm": Float,
	"costpermilekmtripkm": Float,
	"positivekineticenergypkekmhr": Float,
	"throttlepositionmanifold": Float,
	"voltagecontrolmodulev": Float,
	"o2sensor1widerangeequivalenceratio": Float,
	"o2sensor1widerangevoltagev": Float,
	"barometer_on_android_devicemb": Float,
	"barometricpressurefromvehiclekpa": Float,
	"catalyst_temperature_bank_1_sensor_1c": Float,
	"catalyst_temperature_bank_1_sensor_1f": Float,
	"catalyst_temperature_bank_1_sensor_2c": Float,
	"catalyst_temperature_bank_1_sensor_2f": Float,
	"catalyst_temperature_bank_2_sensor_1c": Float,
	"catalyst_temperature_bank_2_sensor_1f": Float,
	"catalyst_temperature_bank_2_sensor_2c": Float,
	"catalyst_temperature_bank_2_sensor_2f": Float,
	"charge_air_cooler_temperature_cactc": Float,
	"charge_air_cooler_temperature_cactf": Float,
	"commanded_equivalence_ratiolambda": Float,
	"cost_per_milekm_instantkm": Float,
	"cost_per_milekm_tripkm": Float,
	"distance_travelled_since_codes_clearedkm": Float,
	"dpf_pressurebar": Float,
	"dpf_pressurepsi": Float,
	"dpf_temperaturec": Float,
	"dpf_temperaturef": Float,
	"drivers_demand_engine__torque": Float,
	"egr_commanded": Float,
	"egr_error": Float,
	"eighthMileTime": Float,
	"engine_loadabsolute": Float,
	"engine_oil_temperaturec": Float,
	"engine_oil_temperaturef": Float,
	"engine_reference_torquenm": Float,
	"enginecoolanttemperaturef": Float,
	"ethanol_fuel_": Float,
	"evap_system_vapour_pressurepa": Float,
	"exhaust_gas_temp_bank_1_sensor_1c": Float,
	"exhaust_gas_temp_bank_1_sensor_1f": Float,
	"exhaust_gas_temp_bank_1_sensor_2c": Float,
	"exhaust_gas_temp_bank_1_sensor_2f": Float,
	"exhaust_gas_temp_bank_1_sensor_3c": Float,
	"exhaust_gas_temp_bank_1_sensor_3f": Float,
	"exhaust_gas_temp_bank_1_sensor_4c": Float,
	"exhaust_gas_temp_bank_1_sensor_4f": Float,
	"exhaust_gas_temp_bank_2_sensor_1c": Float,
	"exhaust_gas_temp_bank_2_sensor_1f": Float,
	"exhaust_gas_temp_bank_2_sensor_2c": Float,
	"exhaust_gas_temp_bank_2_sensor_2f": Float,
	"exhaust_gas_temp_bank_2_sensor_3c": Float,
	"exhaust_gas_temp_bank_2_sensor_3f": Float,
	"exhaust_gas_temp_bank_2_sensor_4c": Float,
	"exhaust_gas_temp_bank_2_sensor_4f": Float,
	"exhaust_pressurebar": Float,
	"exhaust_pressurepsi": Float,
	"fuel_level_from_engine_ecu": Float,
	"fuel_pressurepsi": Float,
	"fuel_rail_pressure_relative_to_manifold_vacuumkpa": Float,
	"fuel_rail_pressure_relative_to_manifold_vacuumpsi": Float,
	"fuel_rate_direct_from_eculm": Float,
	"fuel_trim_bank_1_long_term": Float,
	"fuel_trim_bank_1_sensor_1": Float,
	"fuelpressurekpa": Float,
	"fuelrailpressurekpa": Float,
	"gpsspeedkmh": Float,
	"gravityx": Float,
	"gravityxg": Float,
	"gravityy": Float,
	"gravityyg": Float,
	"gravityz": Float,
	"gravityzg": Float,
	"hybrid_battery_charge_": Float,
	"intakeairtemperaturef": Float,
	"intakemanifoldpressurekpa": Float,
	"kphTime0-100": Float,
	"kphTime0-200": Float,
	"kphTime0200": Float,
	"kphTime100-0": Float,
	"kphTime100-200": Float,
	"kphTime1000": Float,
	"kphTime100200": Float,
	"kphTime80-120": Float,
	"miletimes14": Float,
	"miletimes18": Float,
	"mphTime0-100": Float,
	"mphTime0-30": Float,
	"mphTime0-60": Float,
	"mphTime40-60": Float,
	"mphTime60-0": Float,
	"mphTime60-120": Float,
	"mphTime60-130": Float,
	"mphTime60-80": Float,
	"mphTime80-100": Float,
	"mphtimes01008": Float,
	"mphtimes030": Float,
	"mphtimes060": Float,
	"mphtimes60120": Float,
	"mphtimes60130": Float,
	"mphtimes6080": Float,
	"nox_post_scrppm": Float,
	"nox_pre_scrppm": Float,
	"o2_sensor1_equivalence_ratio": Float,
	"o2_sensor1_equivalence_ratioalternate": Float,
	"percentage_of_highway_driving": Float,
	"percentageofcitydriving": Float,
	"percentageofidledriving": Float,
	"quarterMileTime": Float,
	"relative_accelerator_pedal_position": Float,
	"relative_throttle_position": Float,
	"run_time_since_engine_starts": Float,
	"timing_advance": Float,
	"torqueftlb": Float,
	"transmission_temperaturemethod_1c": Float,
	"transmission_temperaturemethod_1f": Float,
	"transmission_temperaturemethod_2c": Float,
	"transmission_temperaturemethod_2f": Float,
	"turbo_pressure_controlbar": Float,
	"turbo_pressure_controlpsi": Float,
	"turboboostvacuumgaugebar": Float,
	"absolute_throttle_position_b": Float,
	"accelerator_pedalposition_d": Float,
	"accelerator_pedalposition_e": Float,
	"accelerator_pedalposition_f": Float,
	"air_fuel_ratiocommanded1": Float,
	"altitudem": Float,
	"ambientairtempf": Float,
}

# _RAW_COLUMN_MAPPING: raw Torque CSV header strings (as they appear in
# trackLog*.csv) mapped to the canonical column name used everywhere else
# (DB columns, COLUMN_SCHEMA keys, etc).
_RAW_COLUMN_MAPPING = {
	"0-100kph Time(s)": "kphTime0-100",
	"0-100mph Time(s)": "mphTime0-100",
	"0-200kph Time(s)": "kphTime0-200",
	"0-30mph Time(s)": "mphTime0-30",
	"0-60mph Time(s)": "mphTime0-60",
	"1/4 mile time(s)": "quarterMileTime",
	"1/8 mile time(s)": "eighthMileTime",
	"100-0kph Time(s)": "kphTime100-0",
	"100-200kph Time(s)": "kphTime100-200",
	"40-60mph Time(s)": "mphTime40-60",
	"60-0mph Time(s)": "mphTime60-0",
	"60-120mph Time(s)": "mphTime60-120",
	"60-130mph Time(s)": "mphTime60-130",
	"60-80mph Time(s)": "mphTime60-80",
	"80-100mph Time(s)": "mphTime80-100",
	"80-120kph Time(s)": "kphTime80-120",
	"0200kphtimes": "kphTime0200",
	"100200kphtimes": "kphTime100200",
	"030mphtimes": "mphtimes030",
	"060mphtimes": "mphtimes060",
	"60130mphtimes": "mphtimes60130",
	"8mphtimes0100": "mphtimes01008",
	"6080mphtimes": "mphtimes6080",
	"60120mphtimes": "mphtimes60120",
	"14miletimes": "miletimes14",
	"18miletimes": "miletimes18",
	"1000kphtimes": "kphTime1000",
	"Barometric pressure (from vehicle)(kpa)": "barometricpressurefromvehiclekpa",
	"Cost per mile/km (Instant)($/km)": "costpermilekminstantkm",
	"Cost per mile/km (Trip)($/km)": "costpermilekmtripkm",
	"Fuel pressure(kpa)": "fuelpressurekpa",
	"Torque(Nm)": "torquenm",
	"Acceleration Sensor(Total)(g)": "accelerationsensortotalg",
	"Acceleration Sensor(X axis)(g)": "accelerationsensorxaxisg",
	"Acceleration Sensor(Y axis)(g)": "accelerationsensoryaxisg",
	"Acceleration Sensor(Z axis)(g)": "accelerationsensorzaxisg",
	"Actual engine % torque(%)": "actualenginetorque",
	"Air Fuel Ratio(Measured)(:1)": "airfuelratiomeasured1",
	"Altitude": "altitude",
	"Altitude(m)": "altitudem",
	"Ambient air temp(°C)": "ambientairtempc",
	"Ambient air temp(°F)": "ambientairtempf",
	"Android device Battery Level(%)": "androiddevicebatterylevel",
	"Average trip speed(whilst moving only)(km/h)": "averagetripspeedwhilstmovingonlykmh",
	"Average trip speed(whilst stopped or moving)(km/h)": "averagetripspeedwhilststoppedormovingkmh",
	"Barometric pressure (from vehicle)(psi)": "barometricpressurefromvehiclepsi",
	"Bearing": "bearing",
	"CO₂ in g/km (Average)(g/km)": "coingkmaveragegkm",
	"CO₂ in g/km (Instantaneous)(g/km)": "coingkminstantaneousgkm",
	"Device Time": "devicetime",
	"Distance to empty (Estimated)(km)": "distancetoemptyestimatedkm",
	"Distance travelled with MIL/CEL lit(km)": "distancetravelledwithmilcellitkm",
	"Engine Coolant Temperature(°C)": "enginecoolanttemperaturec",
	"Engine Coolant Temperature(°F)": "enginecoolanttemperaturef",
	"Engine kW (At the wheels)(kW)": "enginekwatthewheelskw",
	"Engine Load(%)": "engineload",
	"Engine RPM(rpm)": "enginerpmrpm",
	"Fuel cost (trip)(cost)": "fuelcosttripcost",
	"Fuel flow rate/hour(l/hr)": "fuelflowratehourlhr",
	"Fuel flow rate/minute(cc/min)": "fuelflowrateminuteccmin",
	"Fuel Rail Pressure(kpa)": "fuelrailpressurekpa",
	"Fuel Rail Pressure(psi)": "fuelrailpressurepsi",
	"Fuel Remaining (Calculated from vehicle profile)(%)": "fuelremainingcalculatedfromvehicleprofile",
	"Fuel used (trip)(l)": "fuelusedtripl",
	"G(calibrated)": "gcalibrated",
	"G(x)": "gravityxg",
	"G(y)": "gravityyg",
	"G(z)": "gravityzg",
	"GPS Accuracy(m)": "gpsaccuracym",
	"GPS Altitude(m)": "gpsaltitudem",
	"GPS Bearing(°)": "gpsbearing",
	"GPS Latitude(°)": "gpslatitude",
	"GPS Longitude(°)": "gpslongitude",
	"GPS Satellites": "gpssatellites",
	"GPS Speed (Meters/second)": "gpsspeedmeterssecond",
	"GPS Speed(km/h)": "gpsspeedkmh",
	"GPS Time": "gpstime",
	"GPS vs OBD Speed difference(km/h)": "gpsvsobdspeeddifferencekmh",
	"Gravity X(G)": "gravityx",
	"Gravity Y(G)": "gravityy",
	"Gravity Z(G)": "gravityz",
	"Horizontal Dilution of Precision": "horizontaldilutionofprecision",
	"Horsepower (At the wheels)(hp)": "horsepoweratthewheelshp",
	"Intake Air Temperature(°C)": "intakeairtemperaturec",
	"Intake Air Temperature(°F)": "intakeairtemperaturef",
	"Intake Manifold Pressure(kpa)": "intakemanifoldpressurekpa",
	"Intake Manifold Pressure(psi)": "intakemanifoldpressurepsi",
	"Kilometers Per Litre(Instant)(kpl)": "kilometersperlitreinstantkpl",
	"Kilometers Per Litre(Long Term Average)(kpl)": "kilometersperlitrelongtermaveragekpl",
	"Latitude": "latitude",
	"Litres Per 100 Kilometer(Instant)(l/100km)": "litresper100kilometerinstantl100km",
	"Litres Per 100 Kilometer(Long Term Average)(l/100km)": "litresper100kilometerlongtermaveragel100km",
	"Longitude": "longitude",
	"Mass Air Flow Rate(g/s)": "massairflowrategs",
	"Miles Per Gallon(Instant)(mpg)": "milespergalloninstantmpg",
	"Miles Per Gallon(Long Term Average)(mpg)": "milespergallonlongtermaveragempg",
	"O2 Bank 1 Sensor 1 Wide Range Equivalence Ratio(λ)": "o2bank1sensor1widerangeequivalenceratio",
	"O2 Bank 1 Sensor 1 Wide Range Voltage(V)": "o2bank1sensor1widerangevoltagev",
	"O2 Sensor1 Wide Range Current(mA)": "o2sensor1widerangecurrentma",
	"O2 Sensor1 Wide Range Equivalence Ratio": "o2sensor1widerangeequivalenceratio",
	"O2 Sensor1 Wide Range Voltage(V)": "o2sensor1widerangevoltagev",
	"Positive Kinetic Energy (PKE)(km/hr²)": "positivekineticenergypkekmhr",
	"Speed (GPS)(km/h)": "speedgpskmh",
	"Speed (OBD)(km/h)": "speedobdkmh",
	"Throttle Position(Manifold)(%)": "throttlepositionmanifold",
	"Torque(ft-lb)": "torqueftlb",
	"Trip average KPL(kpl)": "tripaveragekplkpl",
	"Trip average Litres/100 KM(l/100km)": "tripaveragelitres100kml100km",
	"Trip average MPG(mpg)": "tripaveragempgmpg",
	"Trip distance (stored in vehicle profile)(km)": "tripdistancestoredinvehicleprofilekm",
	"Trip Distance(km)": "tripdistancekm",
	"Trip Time(Since journey start)(s)": "triptimesincejourneystarts",
	"Trip time(whilst moving)(s)": "triptimewhilstmovings",
	"Trip time(whilst stationary)(s)": "triptimewhilststationarys",
	"Turbo Boost & Vacuum Gauge(bar)": "turboboostvacuumgaugebar",
	"Turbo Boost & Vacuum Gauge(psi)": "turboboostvacuumgaugepsi",
	"Voltage (Control Module)(V)": "voltagecontrolmodulev",
	"Voltage (OBD Adapter)(V)": "voltageobdadapterv",
	"Volumetric Efficiency (Calculated)(%)": "volumetricefficiencycalculated",
	"Accelerator PedalPosition F(%)": "accelerator_pedalposition_f",
	"Relative Accelerator Pedal Position(%)": "relative_accelerator_pedal_position",
	"Exhaust gas temp Bank 2 Sensor 3(°C)": "exhaust_gas_temp_bank_2_sensor_3c",
	"Exhaust gas temp Bank 2 Sensor 4(°C)": "exhaust_gas_temp_bank_2_sensor_4c",
	"Percentage of Idle driving(%)": "percentageofidledriving",
	"Fuel trim bank 1 sensor 1(%)": "fuel_trim_bank_1_sensor_1",
	"Fuel Rate (direct from ECU)(L/m)": "fuel_rate_direct_from_eculm",
	"Catalyst Temperature (Bank 2 Sensor 2)(°C)": "catalyst_temperature_bank_2_sensor_2c",
	"Catalyst Temperature (Bank 2 Sensor 2)(°F)": "catalyst_temperature_bank_2_sensor_2f",
	"Fuel pressure(psi)": "fuel_pressurepsi",
	"Percentage of Highway driving(%)": "percentage_of_highway_driving",
	"Exhaust gas temp Bank 1 Sensor 2(°C)": "exhaust_gas_temp_bank_1_sensor_2c",
	"Barometer (on Android device)(mb)": "barometer_on_android_devicemb",
	"Cost per mile/km (Trip)(£/km)": "cost_per_milekm_tripkm",
	"EGR Commanded(%)": "egr_commanded",
	"Exhaust gas temp Bank 1 Sensor 1(°C)": "exhaust_gas_temp_bank_1_sensor_1c",
	"NOx Post SCR(ppm)": "nox_post_scrppm",
	"Commanded Equivalence Ratio(lambda)": "commanded_equivalence_ratiolambda",
	"NOx Pre SCR(ppm)": "nox_pre_scrppm",
	"Catalyst Temperature (Bank 2 Sensor 1)(°C)": "catalyst_temperature_bank_2_sensor_1c",
	"Catalyst Temperature (Bank 2 Sensor 1)(°F)": "catalyst_temperature_bank_2_sensor_1f",
	"Exhaust gas temp Bank 1 Sensor 3(°C)": "exhaust_gas_temp_bank_1_sensor_3c",
	"Engine Oil Temperature(°C)": "engine_oil_temperaturec",
	"Catalyst Temperature (Bank 1 Sensor 2)(°C)": "catalyst_temperature_bank_1_sensor_2c",
	"Exhaust gas temp Bank 1 Sensor 4(°C)": "exhaust_gas_temp_bank_1_sensor_4c",
	"Fuel Rail Pressure (relative to manifold vacuum)(psi)": "fuel_rail_pressure_relative_to_manifold_vacuumpsi",
	"O2 Sensor1 Equivalence Ratio(alternate)": "o2_sensor1_equivalence_ratioalternate",
	"Engine Load(Absolute)(%)": "engine_loadabsolute",
	"Charge air cooler temperature (CACT)(°C)": "charge_air_cooler_temperature_cactc",
	"Charge air cooler temperature (CACT)(°F)": "charge_air_cooler_temperature_cactf",
	"DPF Pressure(bar)": "dpf_pressurebar",
	"DPF Temperature(°F)": "dpf_temperaturef",
	"Engine Oil Temperature(°F)": "engine_oil_temperaturef",
	"Run time since engine start(s)": "run_time_since_engine_starts",
	"Exhaust gas temp Bank 2 Sensor 1(°C)": "exhaust_gas_temp_bank_2_sensor_1c",
	"Relative Throttle Position(%)": "relative_throttle_position",
	"Timing Advance(°)": "timing_advance",
	"Transmission Temperature(Method 2)(°C)": "transmission_temperaturemethod_2c",
	"Hybrid Battery Charge (%)(%)": "hybrid_battery_charge_",
	"Catalyst Temperature (Bank 1 Sensor 1)(°C)": "catalyst_temperature_bank_1_sensor_1c",
	"Catalyst Temperature (Bank 1 Sensor 1)(°F)": "catalyst_temperature_bank_1_sensor_1f",
	"Catalyst Temperature (Bank 1 Sensor 2)(°F)": "catalyst_temperature_bank_1_sensor_2f",
	"Accelerator PedalPosition D(%)": "accelerator_pedalposition_d",
	"Evap System Vapour Pressure(Pa)": "evap_system_vapour_pressurepa",
	"Exhaust gas temp Bank 2 Sensor 2(°C)": "exhaust_gas_temp_bank_2_sensor_2c",
	"Turbo Pressure Control(psi)": "turbo_pressure_controlpsi",
	"O2 Sensor1 Equivalence Ratio": "o2_sensor1_equivalence_ratio",
	"Absolute Throttle Position B(%)": "absolute_throttle_position_b",
	"Exhaust Pressure(psi)": "exhaust_pressurepsi",
	"DPF Pressure(psi)": "dpf_pressurepsi",
	"DPF Temperature(°C)": "dpf_temperaturec",
	"EGR Error(%)": "egr_error",
	"Fuel Trim Bank 1 Long Term(%)": "fuel_trim_bank_1_long_term",
	"Percentage of City driving(%)": "percentageofcitydriving",
	"Transmission Temperature(Method 1)(°C)": "transmission_temperaturemethod_1c",
	"Cost per mile/km (Instant)(£/km)": "cost_per_milekm_instantkm",
	"Accelerator PedalPosition E(%)": "accelerator_pedalposition_e",
	"Ethanol Fuel %(%)": "ethanol_fuel_",
	"Drivers demand engine % torque(%)": "drivers_demand_engine__torque",
	"Distance travelled since codes cleared(km)": "distance_travelled_since_codes_clearedkm",
	"Air Fuel Ratio(Commanded)(:1)": "air_fuel_ratiocommanded1",
	"Fuel Level (From Engine ECU)(%)": "fuel_level_from_engine_ecu",
	"Engine reference torque(Nm)": "engine_reference_torquenm",
	"Exhaust gas temp Bank 1 Sensor 1(°F)": "exhaust_gas_temp_bank_1_sensor_1f",
	"Exhaust gas temp Bank 1 Sensor 2(°F)": "exhaust_gas_temp_bank_1_sensor_2f",
	"Exhaust gas temp Bank 1 Sensor 3(°F)": "exhaust_gas_temp_bank_1_sensor_3f",
	"Exhaust gas temp Bank 1 Sensor 4(°F)": "exhaust_gas_temp_bank_1_sensor_4f",
	"Exhaust gas temp Bank 2 Sensor 1(°F)": "exhaust_gas_temp_bank_2_sensor_1f",
	"Exhaust gas temp Bank 2 Sensor 2(°F)": "exhaust_gas_temp_bank_2_sensor_2f",
	"Exhaust gas temp Bank 2 Sensor 3(°F)": "exhaust_gas_temp_bank_2_sensor_3f",
	"Exhaust gas temp Bank 2 Sensor 4(°F)": "exhaust_gas_temp_bank_2_sensor_4f",
	"Exhaust Pressure(bar)": "exhaust_pressurebar",
	"Fuel Rail Pressure (relative to manifold vacuum)(kpa)": "fuel_rail_pressure_relative_to_manifold_vacuumkpa",
	"Transmission Temperature(Method 1)(°F)": "transmission_temperaturemethod_1f",
	"Transmission Temperature(Method 2)(°F)": "transmission_temperaturemethod_2f",
	"Turbo Pressure Control(bar)": "turbo_pressure_controlbar",
}

# _LEGACY_METRIC_NAMES: the curated subset of canonical metric names that made
# up the original "dataschema" dict. Kept as a scope flag (`legacy_metric`)
# rather than expanded to every known column, so index creation
# (updatetripdata.update_indexes) and null-ratio stats collection
# (updatetripdata.collect_db_columnstats) keep covering exactly the same
# columns as before -- not every rare sensor Torque has ever emitted.
_LEGACY_METRIC_NAMES = frozenset({
	"gpstime",
	"devicetime",
	"longitude",
	"latitude",
	"gpsspeedkmh",
	"horizontaldilutionofprecision",
	"altitude",
	"altitudem",
	"bearing",
	"gravityxg",
	"gravityyg",
	"gravityzg",
	"gcalibrated",
	"accelerationsensortotalg",
	"accelerationsensorxaxisg",
	"accelerationsensoryaxisg",
	"accelerationsensorzaxisg",
	"actualenginetorque",
	"airfuelratiomeasured1",
	"androiddevicebatterylevel",
	"averagetripspeedwhilstmovingonlykmh",
	"averagetripspeedwhilststoppedormovingkmh",
	"barometricpressurefromvehiclepsi",
	"coingkmaveragegkm",
	"coingkminstantaneousgkm",
	"distancetoemptyestimatedkm",
	"distancetravelledwithmilcellitkm",
	"enginecoolanttemperaturec",
	"enginecoolanttemperaturef",
	"enginekwatthewheelskw",
	"engineload",
	"enginerpmrpm",
	"fuelcosttripcost",
	"fuelflowratehourlhr",
	"fuelflowrateminuteccmin",
	"fuelrailpressurepsi",
	"fuelremainingcalculatedfromvehicleprofile",
	"fuelusedtripl",
	"fuelrailpressurekpa",
	"gpsaccuracym",
	"gpsaltitudem",
	"gpsbearing",
	"gpslatitude",
	"gpslongitude",
	"gpssatellites",
	"gpsspeedmeterssecond",
	"gpsvsobdspeeddifferencekmh",
	"horsepoweratthewheelshp",
	"intakeairtemperaturec",
	"intakemanifoldpressurepsi",
	"kilometersperlitreinstantkpl",
	"kilometersperlitrelongtermaveragekpl",
	"litresper100kilometerinstantl100km",
	"litresper100kilometerlongtermaveragel100km",
	"massairflowrategs",
	"milespergalloninstantmpg",
	"milespergallonlongtermaveragempg",
	"o2sensor1widerangecurrentma",
	"o2bank1sensor1widerangeequivalenceratio",
	"o2bank1sensor1widerangevoltagev",
	"speedgpskmh",
	"speedobdkmh",
	"torquenm",
	"torqueftlb",
	"tripaveragekplkpl",
	"tripaveragelitres100kml100km",
	"tripaveragempgmpg",
	"tripdistancekm",
	"tripdistancestoredinvehicleprofilekm",
	"triptimesincejourneystarts",
	"triptimewhilstmovings",
	"triptimewhilststationarys",
	"turboboostvacuumgaugepsi",
	"turboboostvacuumgaugebar",
	"voltageobdadapterv",
	"volumetricefficiencycalculated",
	"ambientairtempc",
	"costpermilekminstantkm",
	"costpermilekmtripkm",
	"positivekineticenergypkekmhr",
	"throttlepositionmanifold",
	"voltagecontrolmodulev",
	"o2sensor1widerangeequivalenceratio",
	"o2sensor1widerangevoltagev",
})

# _TRIP_METRIC_NAMES: canonical metric names selected (of the full set) to
# aggregate per-trip into torqtrips. Alternatives considered but not enabled
# are commented out.
_TRIP_METRIC_NAMES = [
	# "ambientairtempc",
	"accelerationsensortotalg",
	# "accelerationsensorxaxisg",
	# "accelerationsensoryaxisg",
	# "accelerationsensorzaxisg",
	# "androiddevicebatterylevel",
	# "averagetripspeedwhilststoppedormovingkmh",
	"coingkmaveragegkm",
	# "distancetoemptyestimatedkm",
	"engineload",
	"enginerpmrpm",
	"fuelcosttripcost",
	"fuelflowratehourlhr",
	"fuelflowrateminuteccmin",
	# "fuelrailpressurepsi",
	# "fuelusedtripl",
	# "fuelremainingcalculatedfromvehicleprofile",
	"gpsaltitudem",
	"gpsspeedkmh",
	# "gpsvsobdspeeddifferencekmh",
	# "horizontaldilutionofprecision",
	"kilometersperlitreinstantkpl",
	"kilometersperlitrelongtermaveragekpl",
	"litresper100kilometerlongtermaveragel100km",
	"massairflowrategs",
	# "milespergalloninstantmpg",
	# "milespergallonlongtermaveragempg",
	"speedgpskmh",
	"speedobdkmh",
	# "enginecoolanttemperaturec",
	# "intakeairtemperaturec",
	"tripaveragekplkpl",
	"tripaveragelitres100kml100km",
	"tripaveragempgmpg",
	"tripdistancekm",
	"tripdistancestoredinvehicleprofilekm",
	"triptimesincejourneystarts",
	"triptimewhilstmovings",
	"triptimewhilststationarys",
	# "voltageobdadapterv",
	# "volumetricefficiencycalculated",
]


class ColumnSchemaEntry(TypedDict):
	"""Metadata for a single canonical Torque metric column."""

	type: type  # SQLAlchemy column type class: Float, Integer, or String
	aliases: tuple[str, ...]  # raw CSV header / legacy variants resolving to this column
	mapped_column: bool  # grown into the torqlogs table when importing CSVs
	legacy_metric: bool  # part of the original curated dataschema subset
	trip_metric: bool  # aggregated per-trip into torqtrips


def _normalize_col_key(value: str) -> str:
	return re.sub(r"[^A-Za-z0-9]+", "", value).lower()


def _build_column_schema() -> dict[str, ColumnSchemaEntry]:
	# Canonical columns are every value column_mapping ever produces, plus a
	# couple of legacy standalone headers ("Gx"/"Gy"/"Gz") that never got a
	# column_mapping entry of their own.
	mapped_names = set(_RAW_COLUMN_MAPPING.values())
	canonical_names = mapped_names | {"gx", "gy", "gz"}

	reverse_mapping: dict[str, set[str]] = {}
	for header, canonical in _RAW_COLUMN_MAPPING.items():
		reverse_mapping.setdefault(canonical, set()).add(header)

	normalized_aliases: dict[str, set[str]] = {}
	for raw_key in _RAW_COLUMN_TYPES:
		normalized = _normalize_col_key(raw_key)
		if normalized in canonical_names and raw_key != normalized:
			normalized_aliases.setdefault(normalized, set()).add(raw_key)

	schema: dict[str, ColumnSchemaEntry] = {}
	for name in sorted(canonical_names):
		aliases = reverse_mapping.get(name, set()) | normalized_aliases.get(name, set())
		schema[name] = ColumnSchemaEntry(
			type=_RAW_COLUMN_TYPES[name],
			aliases=tuple(sorted(aliases)),
			mapped_column=name in mapped_names,
			legacy_metric=name in _LEGACY_METRIC_NAMES,
			trip_metric=name in _TRIP_METRIC_NAMES,
		)
	return schema


# Single unified source of truth for every canonical Torque metric column:
# its SQL type, known raw header aliases, and which processing stages
# (table growth / legacy indexing+stats / per-trip aggregation) include it.
COLUMN_SCHEMA: dict[str, ColumnSchemaEntry] = _build_column_schema()


class MetricCategory(Enum):
	"""Categories for different types of vehicle metrics."""

	SPEED = "Speed"
	ENGINE = "Engine"
	FUEL = "Fuel"
	GPS = "GPS/Location"
	ACCELERATION = "Acceleration/G-Force"
	TEMPERATURE = "Temperature"
	PRESSURE = "Pressure"
	TRIP = "Trip Summary"
	EFFICIENCY = "Efficiency"
	EMISSION = "Emissions"
	OTHER = "Other"


# Metric categorization map: normalized name -> (category, display_name, unit)
METRIC_CATEGORIES = {
	# Speed metrics
	"speedgpskmh": (MetricCategory.SPEED, "GPS Speed", "km/h"),
	"speedobdkmh": (MetricCategory.SPEED, "OBD Speed", "km/h"),
	"gpsspeedmeterssecond": (MetricCategory.SPEED, "GPS Speed", "m/s"),
	"averagetripspeedwhilstmovingkmh": (
		MetricCategory.SPEED,
		"Avg Speed (Moving)",
		"km/h",
	),
	"averagetripspeedwhilststoppedormovingkmh": (
		MetricCategory.SPEED,
		"Avg Speed (All)",
		"km/h",
	),
	"gpsvsObDspeeddifferencekmh": (MetricCategory.SPEED, "GPS-OBD Speed Diff", "km/h"),
	# Engine metrics
	"enginerpmrpm": (MetricCategory.ENGINE, "Engine RPM", "rpm"),
	"engineload": (MetricCategory.ENGINE, "Engine Load", "%"),
	"actualenginetorque": (MetricCategory.ENGINE, "Engine Torque", "Nm"),
	"enginekwatthewheelskw": (MetricCategory.ENGINE, "Power Output", "kW"),
	"horsepoweratthewheelshp": (MetricCategory.ENGINE, "Horsepower", "hp"),
	"volumetricefficiencycalculated": (
		MetricCategory.ENGINE,
		"Volumetric Efficiency",
		"%",
	),
	# Fuel metrics
	"fuelflowratehourlhr": (MetricCategory.FUEL, "Fuel Flow Rate (hourly)", "l/h"),
	"fuelflowrateminuteccmin": (
		MetricCategory.FUEL,
		"Fuel Flow Rate (minute)",
		"cc/min",
	),
	"fuelusedtripl": (MetricCategory.FUEL, "Fuel Used", "l"),
	"fuelremainingcalculatedfromvehicleprofile": (
		MetricCategory.FUEL,
		"Fuel Remaining",
		"l",
	),
	"fuelcosttripcost": (MetricCategory.FUEL, "Fuel Cost", "cost"),
	"costpermilekminstantkm": (MetricCategory.FUEL, "Cost per km (instant)", "cost/km"),
	"costpermilekmtripkm": (MetricCategory.FUEL, "Cost per km (trip)", "cost/km"),
	"distancetoemptyestimatedkm": (MetricCategory.FUEL, "Distance to Empty", "km"),
	# Efficiency metrics
	"milespergalloninstantmpg": (MetricCategory.EFFICIENCY, "Instant MPG", "mpg"),
	"milespergallonlongtermaveragempg": (
		MetricCategory.EFFICIENCY,
		"Long-term Avg MPG",
		"mpg",
	),
	"tripaveragempgmpg": (MetricCategory.EFFICIENCY, "Trip Avg MPG", "mpg"),
	"kilometersperlitreinstantkpl": (MetricCategory.EFFICIENCY, "Instant KPL", "km/l"),
	"kilometersperlitrelongtermaveragekpl": (
		MetricCategory.EFFICIENCY,
		"Long-term Avg KPL",
		"km/l",
	),
	"tripaveragekplkpl": (MetricCategory.EFFICIENCY, "Trip Avg KPL", "km/l"),
	"litresper100kilometerinstantl100km": (
		MetricCategory.EFFICIENCY,
		"Instant L/100km",
		"l/100km",
	),
	"litresper100kilometerlongtermaveragel100km": (
		MetricCategory.EFFICIENCY,
		"Long-term L/100km",
		"l/100km",
	),
	"tripaveragelitres100kml100km": (
		MetricCategory.EFFICIENCY,
		"Trip Avg L/100km",
		"l/100km",
	),
	# GPS/Location metrics
	"gpslatitude": (MetricCategory.GPS, "Latitude", "°"),
	"gpslongitude": (MetricCategory.GPS, "Longitude", "°"),
	"gpsaltitudem": (MetricCategory.GPS, "Altitude", "m"),
	"gpsbearing": (MetricCategory.GPS, "Bearing", "°"),
	"gpsaccuracym": (MetricCategory.GPS, "GPS Accuracy", "m"),
	"gpssatellites": (MetricCategory.GPS, "Satellites", "count"),
	"horizontaldilutionofprecision": (MetricCategory.GPS, "HDOP", ""),
	"latitude": (MetricCategory.GPS, "Latitude", "°"),
	"longitude": (MetricCategory.GPS, "Longitude", "°"),
	"bearing": (MetricCategory.GPS, "Bearing", "°"),
	"altitude": (MetricCategory.GPS, "Altitude", "m"),
	# Acceleration/G-Force metrics
	"accelerationsensortotalg": (MetricCategory.ACCELERATION, "Total G-Force", "g"),
	"accelerationsensorxaxisg": (
		MetricCategory.ACCELERATION,
		"X-axis Acceleration",
		"g",
	),
	"accelerationsensoryaxisg": (
		MetricCategory.ACCELERATION,
		"Y-axis Acceleration",
		"g",
	),
	"accelerationsensorzaxisg": (
		MetricCategory.ACCELERATION,
		"Z-axis Acceleration",
		"g",
	),
	"gravityxg": (MetricCategory.ACCELERATION, "Gravity X", "g"),
	"gravityyg": (MetricCategory.ACCELERATION, "Gravity Y", "g"),
	"gravityzg": (MetricCategory.ACCELERATION, "Gravity Z", "g"),
	"gcalibrated": (MetricCategory.ACCELERATION, "G-Force (Calibrated)", "g"),
	# Temperature metrics
	"enginecoolanttemperaturef": (MetricCategory.TEMPERATURE, "Coolant Temp", "°F"),
	"intakeairtemperaturef": (MetricCategory.TEMPERATURE, "Intake Air Temp", "°F"),
	"ambientairtempf": (MetricCategory.TEMPERATURE, "Ambient Air Temp", "°F"),
	# Pressure metrics
	"intakemanifoldpressurekpa": (
		MetricCategory.PRESSURE,
		"Intake Manifold Pressure",
		"kPa",
	),
	"barometricpressurefromvehiclekpa": (
		MetricCategory.PRESSURE,
		"Barometric Pressure",
		"kPa",
	),
	"fuelrailpressurekpa": (MetricCategory.PRESSURE, "Fuel Rail Pressure", "kPa"),
	"fuelpressurekpa": (MetricCategory.PRESSURE, "Fuel Pressure", "kPa"),
	"turboboostvacuumgaugebar": (MetricCategory.PRESSURE, "Turbo Boost/Vacuum", "bar"),
	# Emissions metrics
	"coaingkmaveragegkm": (MetricCategory.EMISSION, "CO Avg", "g/km"),
	"coaingkminstantaneousgkm": (MetricCategory.EMISSION, "CO Instant", "g/km"),
	"coingkminstantaneousgkm": (MetricCategory.EMISSION, "CO (inst)", "g/km"),
	# Trip summary metrics
	"tripdistancekm": (MetricCategory.TRIP, "Trip Distance", "km"),
	"tripdistancestoredinvehicleprofilekm": (
		MetricCategory.TRIP,
		"Vehicle Profile Distance",
		"km",
	),
	"triptimesincejourneystarts": (MetricCategory.TRIP, "Trip Time", "s"),
	"triptimewhilstmovings": (MetricCategory.TRIP, "Time Moving", "s"),
	"triptimewhilststationarys": (MetricCategory.TRIP, "Time Stationary", "s"),
	# Other metrics
	"voltageobdadapterv": (MetricCategory.OTHER, "OBD Adapter Voltage", "V"),
	"voltagecontrolmodulev": (MetricCategory.OTHER, "Control Module Voltage", "V"),
	"androiddevicebatterylevel": (MetricCategory.OTHER, "Device Battery", "%"),
	"massairflowrategs": (MetricCategory.OTHER, "Mass Air Flow", "g/s"),
	"airfuelratiomeasured1": (MetricCategory.OTHER, "Air-Fuel Ratio", "ratio"),
	"o2sensor1widerangecurrentma": (MetricCategory.OTHER, "O2 Sensor Current", "mA"),
	"o2sensor1widerangeequivalenceratio": (
		MetricCategory.OTHER,
		"O2 Equivalence Ratio",
		"ratio",
	),
	"o2sensor1widerangevoltagev": (MetricCategory.OTHER, "O2 Sensor Voltage", "V"),
	"o2bank1sensor1widerangeequivalenceratio": (
		MetricCategory.OTHER,
		"O2 Bank 1 Equivalence",
		"ratio",
	),
	"o2bank1sensor1widerangevoltagev": (MetricCategory.OTHER, "O2 Bank 1 Voltage", "V"),
	"throttlepositionmanifold": (MetricCategory.OTHER, "Throttle Position", "%"),
	"positivekineticenergypkekmhr": (
		MetricCategory.OTHER,
		"Positive Kinetic Energy",
		"km/h",
	),
	"distancetravelledwithmilcellitkm": (MetricCategory.OTHER, "MIL Distance", "km"),
}


class AnalysisSuggestion(TypedDict):
	"""Suggestion for how to analyze and visualize a metric."""

	analysis_type: str
	visualization: str
	unit: str
	description: str


# Analysis suggestions for each category
ANALYSIS_SUGGESTIONS = {
	MetricCategory.SPEED: AnalysisSuggestion(
		analysis_type="Speed Profile Analysis",
		visualization="Line plot over time + color-coded map scatter",
		unit="km/h",
		description="Analyze acceleration patterns, speed distribution, and cornering speeds. Compare GPS vs OBD for validation.",
	),
	MetricCategory.ENGINE: AnalysisSuggestion(
		analysis_type="Engine Performance Analysis",
		visualization="RPM vs Load scatter, Power output timeline",
		unit="mixed",
		description="Monitor engine stress, operational modes, and power efficiency. Identify sustained high-load conditions.",
	),
	MetricCategory.FUEL: AnalysisSuggestion(
		analysis_type="Fuel Consumption Analysis",
		visualization="Flow rate timeline, Cost accumulation curve",
		unit="l/h or l/min",
		description="Track fuel consumption patterns, identify inefficient driving segments, calculate cost per trip.",
	),
	MetricCategory.GPS: AnalysisSuggestion(
		analysis_type="Route & Location Analysis",
		visualization="Geographic map with altitude/accuracy overlays",
		unit="mixed",
		description="Examine route geometry, elevation changes, and GPS signal quality. Useful for route optimization.",
	),
	MetricCategory.ACCELERATION: AnalysisSuggestion(
		analysis_type="Driving Behavior & Dynamics Analysis",
		visualization="3D acceleration vectors, G-force distribution histogram",
		unit="g",
		description="Assess driving smoothness, aggressive acceleration/braking, and cornering forces.",
	),
	MetricCategory.TEMPERATURE: AnalysisSuggestion(
		analysis_type="Thermal Performance Analysis",
		visualization="Temperature timeline, thermal stress heat map",
		unit="°F or °C",
		description="Monitor engine coolant health, intake temperature variations, and thermal management.",
	),
	MetricCategory.PRESSURE: AnalysisSuggestion(
		analysis_type="System Pressure Analysis",
		visualization="Pressure timelines, pressure vs load correlation",
		unit="kPa or bar",
		description="Track fuel system, intake, and boost pressures for diagnostics.",
	),
	MetricCategory.TRIP: AnalysisSuggestion(
		analysis_type="Trip Summary Statistics",
		visualization="Trip overview cards, duration/distance gauge",
		unit="mixed",
		description="High-level trip metrics for quick assessment.",
	),
	MetricCategory.EFFICIENCY: AnalysisSuggestion(
		analysis_type="Fuel Economy Analysis",
		visualization="MPG/KPL timeline, efficiency vs speed correlation",
		unit="mpg/kpl",
		description="Identify optimal driving speeds and conditions for best fuel economy.",
	),
	MetricCategory.EMISSION: AnalysisSuggestion(
		analysis_type="Emissions Analysis",
		visualization="Emission levels on map, correlation with engine load",
		unit="g/km",
		description="Track pollutant levels and identify high-emission driving segments.",
	),
	MetricCategory.OTHER: AnalysisSuggestion(
		analysis_type="System Diagnostics",
		visualization="Voltage/sensor signal quality charts",
		unit="varies",
		description="Monitor vehicle system health and sensor calibration.",
	),
}


def _strip_accents(value: str) -> str:
	return "".join(
		ch
		for ch in unicodedata.normalize("NFKD", value)
		if not unicodedata.combining(ch)
	)


def _normalize_lookup_key(value: str) -> str:
	value = str(value).strip().replace("﻿", "")
	value = value.replace("Â", "")
	value = _strip_accents(value)
	value = re.sub(r"\s+", " ", value)
	return value


def _fallback_column_name(value: str) -> str:
	key = _normalize_lookup_key(value).lower()
	return re.sub(r"[^a-z0-9]+", "", key)


COLUMN_MAP = {
	_normalize_lookup_key(alias): canonical
	for canonical, entry in COLUMN_SCHEMA.items()
	for alias in entry["aliases"]
}


def canonicalize_column_name(column_name: str) -> str:
	lookup = _normalize_lookup_key(column_name)
	mapped = COLUMN_MAP.get(lookup)
	if mapped:
		return mapped
	return _fallback_column_name(lookup)


def canonicalize_columns(columns: list[str]) -> dict[str, str]:
	return {col: canonicalize_column_name(col) for col in columns}


if __name__ == "__main__":
	pass
