import polars as pl

ncc = {
    "0-100kph Time(s)": "kphTime0-100",
    "0-100mph Time(s)": "mphTime0-100",
    "0-200kph Time(s)": "kphTime0-200",
    "0-30mph Time(s)": "mphTime0-300",
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
    "Barometric pressure (from vehicle)(kpa)": "barometricpressurefromvehiclekpa",
    "Cost per mile/km (Instant)($/km)": "costpermilekminstantkm",
    "Cost per mile/km (Trip)($/km)": "costpermilekmtripkm",
    "Fuel pressure(kpa)": "fuelpressurekpa",
    "Torque(Nm)": "torquefnm",
    "Acceleration Sensor(Total)(g)": "accelerationsensortotalg",
    "Acceleration Sensor(X axis)(g)": "accelerationsensorxaxisg",
    "Acceleration Sensor(Y axis)(g)": "accelerationsensoryaxisg",
    "Acceleration Sensor(Z axis)(g)": "accelerationsensorzaxisg",
    "Actual engine % torque(%)": "actualenginetorque",
    "Air Fuel Ratio(Measured)(:1)": "airfuelrationmeasure",
    "Altitude": "altitudem",
    "Altitude(m)": "altitudem",
    "Ambient air temp(°C)": "ambientairtempc",
    "Ambient air temp(°F)": "ambientairtempf",
    "Android device Battery Level(%)": "androiddevicebatterylevel",
    "Average trip speed(whilst moving only)(km/h)": "averagetripspeedwhilstmovingonlykmh",
    "Average trip speed(whilst stopped or moving)(km/h)": "averagetripspeedwhilststoppedormovingkmh",
    "Barometric pressure (from vehicle)(psi)": "barometricpressurefromvpsi",
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
    "Gravity X(G)": "gravityxg",
    "Gravity Y(G)": "gravityyg",
    "Gravity Z(G)": "gravityzg",
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
    "Catalyst Temperature (Bank 1 Sensor 2)(°F)": "catalyst_temperature_bank_2_sensor_1f",
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

schema_datatypes = {
    'barometricpressurefromvehiclepsi': pl.Float64,
    'airfuelratiomeasured1': pl.Float64,
    'barometricpressurefromvehiclekpa': pl.Float64,
    'altitudem': pl.Float64,
    'ambientairtempf': pl.Float64,
    'gpsspeedkmh': pl.Float64,
    'o2bank1sensor1widerangevoltagev': pl.Float64,
    'o2sensor1widerangecurrentma': pl.Float64,
    "accelerationsensortotalg": pl.Float64,
    "accelerationsensorxaxisg": pl.Float64,
    "accelerationsensoryaxisg": pl.Float64,
    "accelerationsensorzaxisg": pl.Float64,
    "actualenginetorque": pl.Float64,
    "altitude": pl.Float64,
    "ambientairtempc": pl.Float64,
    "androiddevicebatterylevel": pl.Float64,
    "averagetripspeedwhilstmovingonlykmh": pl.Float64,
    "averagetripspeedwhilststoppedormovingkmh": pl.Float64,
    "barometeronandroiddevicemb": pl.Float64,
    "bearing": pl.Float64,
    "coingkmaveragegkm": pl.Float64,
    "coingkminstantaneousgkm": pl.Float64,
    "costpermilekminstantkm": pl.Float64,
    "costpermilekmtripkm": pl.Float64,
    "distancetoemptyestimatedkm": pl.Float64,
    "distancetravelledsincecodesclearedkm": pl.Float64,
    "distancetravelledwithmilcellitkm": pl.Float64,
    "dpfpressurepsi": pl.Float64,
    "dpftemperaturec": pl.Float64,
    "egrcommanded": pl.Float64,
    "egrerror": pl.Float64,
    "enginecoolanttemperaturec": pl.Float64,
    "enginecoolanttemperaturef": pl.Float64,
    "enginekwatthewheelskw": pl.Float64,
    "engineload": pl.Float64,
    "engineloadabsolute": pl.Float64,
    "enginerpmrpm": pl.Float64,
    "ethanolfuel": pl.Float64,
    "evapsystemvapourpressurepa": pl.Float64,
    "fuelcosttripcost": pl.Float64,
    "fuelflowratehourlhr": pl.Float64,
    "fuelflowrateminuteccmin": pl.Float64,
    "fuellevelfromengineecu": pl.Float64,
    "fuelpressurepsi": pl.Float64,
    "fuelrailpressurekpa": pl.Float64,
    "fuelrailpressurepsi": pl.Float64,
    "fuelrailpressurerelativetomanifoldvacuumpsi": pl.Float64,
    "fuelratedirectfromeculm": pl.Float64,
    "fuelremainingcalculatedfromvehicleprofile": pl.Float64,
    "fueltrimbank1sensor1": pl.Float64,
    "fuelusedtripl": pl.Float64,
    "gcalibrated": pl.Float64,
    "gpsaccuracym": pl.Float64,
    "gpsaltitudem": pl.Float64,
    "gpsbearing": pl.Float64,
    "gpslatitude": pl.Float64,
    "gpslongitude": pl.Float64,
    "gpssatellites": pl.Float64,
    "gpsspeedmeterssecond": pl.Float64,
    "gpsvsobdspeeddifferencekmh": pl.Float64,
    "gx": pl.Float64,
    "gy": pl.Float64,
    "gz": pl.Float64,
    "horizontaldilutionofprecision": pl.Float64,
    "horsepoweratthewheelshp": pl.Float64,
    "intakeairtemperaturec": pl.Float64,
    "intakeairtemperaturef": pl.Float64,
    "intakemanifoldpressurekpa": pl.Float64,
    "intakemanifoldpressurepsi": pl.Float64,
    "kilometersperlitreinstantkpl": pl.Float64,
    "kilometersperlitrelongtermaveragekpl": pl.Float64,
    "latitude": pl.Float64,
    "litresper100kilometerinstantl100km": pl.Float64,
    "litresper100kilometerlongtermaveragel100km": pl.Float64,
    "longitude": pl.Float64,
    "massairflowrategs": pl.Float64,
    "milespergalloninstantmpg": pl.Float64,
    "milespergallonlongtermaveragempg": pl.Float64,
    "noxpostscrppm": pl.Float64,
    "noxprescrppm": pl.Float64,
    "o2bank1sensor1widerangeequivalenceratio": pl.Float64,
    "o2sensor1equivalenceratio": pl.Float64,
    "o2sensor1equivalenceratioalternate": pl.Float64,
    "percentageofcitydriving": pl.Float64,
    "percentageofhighwaydriving": pl.Float64,
    "percentageofidledriving": pl.Float64,
    "positivekineticenergypkekmhr": pl.Float64,
    "relativeacceleratorpedalposition": pl.Float64,
    "relativethrottleposition": pl.Float64,
    "speedgpskmh": pl.Float64,
    "speedobdkmh": pl.Float64,
    "throttlepositionmanifold": pl.Float64,
    "timingadvance": pl.Float64,
    "torqueftlb": pl.Float64,
    "torquenm": pl.Float64,
    "tripaveragekplkpl": pl.Float64,
    "tripaveragelitres100kml100km": pl.Float64,
    "tripaveragempgmpg": pl.Float64,
    "tripdistancekm": pl.Float64,
    "tripdistancestoredinvehicleprofilekm": pl.Float64,
    "turboboostvacuumgaugebar": pl.Float64,
    "turboboostvacuumgaugepsi": pl.Float64,
    "voltagecontrolmodulev": pl.Float64,
    "voltageobdadapterv": pl.Float64,
    "volumetricefficiencycalculated": pl.Float64,
}


def merge_colum_data(new_ncc: list, ncc: dict):
    import re

    # Convert new_ncc to a dictionary with sanitized keys
    new_ncc_dict = {s: re.sub(r"\W+", "", s.lower().replace(" ", "_")) for s in new_ncc}

    # Merge ncc and new_ncc_dict
    merged_ncc = {**ncc, **new_ncc_dict}
    return merged_ncc


if __name__ == "__main__":
    pass
