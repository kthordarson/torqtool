import re
import unicodedata
from enum import Enum
from typing import TypedDict
from sqlalchemy import (
	Column,
	DateTime,
	Float,
	ForeignKey,
	Integer,
	Text,
	text,
	String,
	LargeBinary,
	UniqueConstraint,
)

PROFILE_COLUMNS = [
	'profile_fuelused',
	'profile_fuelcost',
	'profile_time',
	'profile_distanceWhilstConnectedToOBD',
	'profile_distance',
	'profile_date',
]

dataschema = {
	"gpstime": Float,
	"devicetime": Float,
	"longitude": Float,
	"latitude": Float,
	"gpsspeedkmh": Float,
	"horizontaldilutionofprecision": Float,
	"altitude": Float,
	"altitudem": Float,
	"bearing": Float,
	"gravityxg": Float,
	"gravityyg": Float,
	"gravityzg": Float,
	"gcalibrated": Float,
	"accelerationsensortotalg": Float,
	"accelerationsensorxaxisg": Float,
	"accelerationsensoryaxisg": Float,
	"accelerationsensorzaxisg": Float,
	"actualenginetorque": String,
	"airfuelratiomeasured1": Float,
	"androiddevicebatterylevel": Integer,
	"averagetripspeedwhilstmovingonlykmh": Float,
	"averagetripspeedwhilststoppedormovingkmh": Float,
	"barometricpressurefromvehiclepsi": Float,
	"coingkmaveragegkm": Float,
	"coingkminstantaneousgkm": Float,
	"distancetoemptyestimatedkm": Integer,
	"distancetravelledwithmilcellitkm": Integer,
	"enginecoolanttemperaturec": Integer,
	"enginecoolanttemperaturef": Integer,
	"enginekwatthewheelskw": Float,
	"engineload": Float,
	"enginerpmrpm": Float,
	"fuelcosttripcost": Float,
	"fuelflowratehourlhr": Float,
	"fuelflowrateminuteccmin": Float,
	"fuelrailpressurepsi": Float,
	"fuelremainingcalculatedfromvehicleprofile": Integer,
	"fuelusedtripl": Float,
	"fuelrailpressurekpa": Float,
	"gpsaccuracym": Float,
	"gpsaltitudem": Float,
	"gpsbearing": Float,
	"gpslatitude": Float,
	"gpslongitude": Float,
	"gpssatellites": Integer,
	"gpsspeedmeterssecond": Float,
	"gpsvsobdspeeddifferencekmh": Float,
	"horsepoweratthewheelshp": Float,
	"intakeairtemperaturec": Integer,
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
	"speedobdkmh": Integer,
	"torquenm": Float,
	"torqueftlb": Float,
	"tripaveragekplkpl": Float,
	"tripaveragelitres100kml100km": Float,
	"tripaveragempgmpg": Float,
	"tripdistancekm": Float,
	"tripdistancestoredinvehicleprofilekm": Float,
	"triptimesincejourneystarts": Float,
	"triptimewhilstmovings": Float,
	"triptimewhilststationarys": Float,
	"turboboostvacuumgaugepsi": Float,
	"turboboostvacuumgaugebar": Float,
	"voltageobdadapterv": Float,
	"volumetricefficiencycalculated": Integer,
	"ambientairtempc": Float,
	"costpermilekminstantkm": Float,
	"costpermilekmtripkm": Float,
	"positivekineticenergypkekmhr": Float,
	"throttlepositionmanifold": Float,
	"voltagecontrolmodulev": Float,
	"o2sensor1widerangeequivalenceratio": Float,
	"o2sensor1widerangevoltagev": Float,
}

column_mapping = {
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

TRIP_METRIC_COLUMNS = [
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
	value = str(value).strip().replace("\ufeff", "")
	value = value.replace("Â", "")
	value = _strip_accents(value)
	value = re.sub(r"\s+", " ", value)
	return value


def _fallback_column_name(value: str) -> str:
	key = _normalize_lookup_key(value).lower()
	return re.sub(r"[^a-z0-9]+", "", key)


COLUMN_MAP = {_normalize_lookup_key(k): v for k, v in column_mapping.items()}


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
