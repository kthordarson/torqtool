import polars as pl

ncc ={'0-100kph Time(s)': 'kphTime0-100',
 '0-100mph Time(s)': 'mphTime0-100',
 '0-200kph Time(s)': 'kphTime0-200',
 '0-30mph Time(s)': 'mphTime0-300',
 '0-60mph Time(s)': 'mphTime0-60',
 '1/4 mile time(s)': 'quarterMileTime',
 '1/8 mile time(s)': 'eighthMileTime',
 '100-0kph Time(s)': 'kphTime100-0',
 '100-200kph Time(s)': 'kphTime100-200',
 '40-60mph Time(s)': 'mphTime40-60',
 '60-0mph Time(s)': 'mphTime60-0',
 '60-120mph Time(s)': 'mphTime60-120',
 '60-130mph Time(s)': 'mphTime60-130',
 '60-80mph Time(s)': 'mphTime60-80',
 '80-100mph Time(s)': 'mphTime80-100',
 '80-120kph Time(s)': 'kphTime80-120',
 'Barometric pressure (from vehicle)(kpa)': 'barometricpressurefromvehiclekpa',
 'Cost per mile/km (Instant)($/km)': 'costpermilekminstantkm',
 'Cost per mile/km (Trip)($/km)': 'costpermilekmtripkm',
 'Fuel pressure(kpa)': 'fuelpressurekpa',
 'Torque(Nm)': 'torquefnm',
 'Acceleration Sensor(Total)(g)': 'accelerationsensortotalg',
 'Acceleration Sensor(X axis)(g)': 'accelerationsensorxaxisg',
 'Acceleration Sensor(Y axis)(g)': 'accelerationsensoryaxisg',
 'Acceleration Sensor(Z axis)(g)': 'accelerationsensorzaxisg',
 'Actual engine % torque(%)': 'actualenginetorque',
 'Air Fuel Ratio(Measured)(:1)': 'airfuelrationmeasure',
 'Altitude': 'altitudem',
 'Altitude(m)': 'altitudem',
 'Ambient air temp(°C)': 'ambientairtempc',
 'Ambient air temp(°F)': 'ambientairtempf',
 'Android device Battery Level(%)': 'androiddevicebatterylevel',
 'Average trip speed(whilst moving only)(km/h)': 'averagetripspeedwhilstmovingonlykmh',
 'Average trip speed(whilst stopped or moving)(km/h)': 'averagetripspeedwhilststoppedormovingkmh',
 'Barometric pressure (from vehicle)(psi)': 'barometricpressurefromvpsi',
 'Bearing': 'bearing',
 'CO₂ in g/km (Average)(g/km)': 'coingkmaveragegkm',
 'CO₂ in g/km (Instantaneous)(g/km)': 'coingkminstantaneousgkm',
 'Device Time': 'devicetime',
 'Distance to empty (Estimated)(km)': 'distancetoemptyestimatedkm',
 'Distance travelled with MIL/CEL lit(km)': 'distancetravelledwithmilcellitkm',
 'Engine Coolant Temperature(°C)': 'enginecoolanttemperaturec',
 'Engine Coolant Temperature(°F)': 'enginecoolanttemperaturef',
 'Engine kW (At the wheels)(kW)': 'enginekwatthewheelskw',
 'Engine Load(%)': 'engineload',
 'Engine RPM(rpm)': 'enginerpmrpm',
 'Fuel cost (trip)(cost)': 'fuelcosttripcost',
 'Fuel flow rate/hour(l/hr)': 'fuelflowratehourlhr',
 'Fuel flow rate/minute(cc/min)': 'fuelflowrateminuteccmin',
 'Fuel Rail Pressure(kpa)': 'fuelrailpressurekpa',
 'Fuel Rail Pressure(psi)': 'fuelrailpressurepsi',
 'Fuel Remaining (Calculated from vehicle profile)(%)': 'fuelremainingcalculatedfromvehicleprofile',
 'Fuel used (trip)(l)': 'fuelusedtripl',
 'G(calibrated)': 'gcalibrated',
 'G(x)': 'gravityxg',
 'G(y)': 'gravityyg',
 'G(z)': 'gravityzg',
 'GPS Accuracy(m)': 'gpsaccuracym',
 'GPS Altitude(m)': 'gpsaltitudem',
 'GPS Bearing(°)': 'gpsbearing',
 'GPS Latitude(°)': 'gpslatitude',
 'GPS Longitude(°)': 'gpslongitude',
 'GPS Satellites': 'gpssatellites',
 'GPS Speed (Meters/second)': 'gpsspeedmeterssecond',
 'GPS Speed(km/h)': 'gpsspeedkmh',
 'GPS Time': 'gpstime',
 'GPS vs OBD Speed difference(km/h)': 'gpsvsobdspeeddifferencekmh',
 'Gravity X(G)': 'gravityxg',
 'Gravity Y(G)': 'gravityyg',
 'Gravity Z(G)': 'gravityzg',
 'Horizontal Dilution of Precision': 'horizontaldilutionofprecision',
 'Horsepower (At the wheels)(hp)': 'horsepoweratthewheelshp',
 'Intake Air Temperature(°C)': 'intakeairtemperaturec',
 'Intake Air Temperature(°F)': 'intakeairtemperaturef',
 'Intake Manifold Pressure(kpa)': 'intakemanifoldpressurekpa',
 'Intake Manifold Pressure(psi)': 'intakemanifoldpressurepsi',
 'Kilometers Per Litre(Instant)(kpl)': 'kilometersperlitreinstantkpl',
 'Kilometers Per Litre(Long Term Average)(kpl)': 'kilometersperlitrelongtermaveragekpl',
 'Latitude': 'latitude',
 'Litres Per 100 Kilometer(Instant)(l/100km)': 'litresper100kilometerinstantl100km',
 'Litres Per 100 Kilometer(Long Term Average)(l/100km)': 'litresper100kilometerlongtermaveragel100km',
 'Longitude': 'longitude',
 'Mass Air Flow Rate(g/s)': 'massairflowrategs',
 'Miles Per Gallon(Instant)(mpg)': 'milespergalloninstantmpg',
 'Miles Per Gallon(Long Term Average)(mpg)': 'milespergallonlongtermaveragempg',
 'O2 Bank 1 Sensor 1 Wide Range Equivalence Ratio(λ)': 'o2bank1sensor1widerangeequivalenceratio',
 'O2 Bank 1 Sensor 1 Wide Range Voltage(V)': 'o2bank1sensor1widerangevoltagev',
 'O2 Sensor1 Wide Range Current(mA)': 'o2sensor1widerangecurrentma',
 'Positive Kinetic Energy (PKE)(km/hr²)': 'positivekineticenergypkekmhr',
 'Speed (GPS)(km/h)': 'speedgpskmh',
 'Speed (OBD)(km/h)': 'speedobdkmh',
 'Throttle Position(Manifold)(%)': 'throttlepositionmanifold',
 'Torque(ft-lb)': 'torqueftlb',
 'Trip average KPL(kpl)': 'tripaveragekplkpl',
 'Trip average Litres/100 KM(l/100km)': 'tripaveragelitres100kml100km',
 'Trip average MPG(mpg)': 'tripaveragempgmpg',
 'Trip distance (stored in vehicle profile)(km)': 'tripdistancestoredinvehicleprofilekm',
 'Trip Distance(km)': 'tripdistancekm',
 'Trip Time(Since journey start)(s)': 'triptimesincejourneystarts',
 'Trip time(whilst moving)(s)': 'triptimewhilstmovings',
 'Trip time(whilst stationary)(s)': 'triptimewhilststationarys',
 'Turbo Boost & Vacuum Gauge(bar)': 'turboboostvacuumgaugebar',
 'Turbo Boost & Vacuum Gauge(psi)': 'turboboostvacuumgaugepsi',
 'Voltage (Control Module)(V)': 'voltagecontrolmodulev',
 'Voltage (OBD Adapter)(V)': 'voltageobdadapterv',
 'Volumetric Efficiency (Calculated)(%)': 'volumetricefficiencycalculated',
 'Accelerator PedalPosition F(%)': 'accelerator_pedalposition_f',
 'Relative Accelerator Pedal Position(%)': 'relative_accelerator_pedal_position',
 'Exhaust gas temp Bank 2 Sensor 3(°C)': 'exhaust_gas_temp_bank_2_sensor_3c',
 'Exhaust gas temp Bank 2 Sensor 4(°C)': 'exhaust_gas_temp_bank_2_sensor_4c',
 'Percentage of Idle driving(%)': 'percentageofidledriving',
 'Fuel trim bank 1 sensor 1(%)': 'fuel_trim_bank_1_sensor_1',
 'Fuel Rate (direct from ECU)(L/m)': 'fuel_rate_direct_from_eculm',
 'Catalyst Temperature (Bank 2 Sensor 2)(°C)': 'catalyst_temperature_bank_2_sensor_2c',
 'Catalyst Temperature (Bank 2 Sensor 2)(°F)': 'catalyst_temperature_bank_2_sensor_2f',
 'Fuel pressure(psi)': 'fuel_pressurepsi',
 'Percentage of Highway driving(%)': 'percentage_of_highway_driving',
 'Exhaust gas temp Bank 1 Sensor 2(°C)': 'exhaust_gas_temp_bank_1_sensor_2c',
 'Barometer (on Android device)(mb)': 'barometer_on_android_devicemb',
 'Cost per mile/km (Trip)(£/km)': 'cost_per_milekm_tripkm',
 'EGR Commanded(%)': 'egr_commanded',
 'Exhaust gas temp Bank 1 Sensor 1(°C)': 'exhaust_gas_temp_bank_1_sensor_1c',
 'NOx Post SCR(ppm)': 'nox_post_scrppm',
 'Commanded Equivalence Ratio(lambda)': 'commanded_equivalence_ratiolambda',
 'NOx Pre SCR(ppm)': 'nox_pre_scrppm',
 'Catalyst Temperature (Bank 2 Sensor 1)(°C)': 'catalyst_temperature_bank_2_sensor_1c',
 'Catalyst Temperature (Bank 2 Sensor 1)(°F)': 'catalyst_temperature_bank_2_sensor_1f',
 'Exhaust gas temp Bank 1 Sensor 3(°C)': 'exhaust_gas_temp_bank_1_sensor_3c',
 'Engine Oil Temperature(°C)': 'engine_oil_temperaturec',
 'Catalyst Temperature (Bank 1 Sensor 2)(°C)': 'catalyst_temperature_bank_1_sensor_2c',
 'Exhaust gas temp Bank 1 Sensor 4(°C)': 'exhaust_gas_temp_bank_1_sensor_4c',
 'Fuel Rail Pressure (relative to manifold vacuum)(psi)': 'fuel_rail_pressure_relative_to_manifold_vacuumpsi',
 'O2 Sensor1 Equivalence Ratio(alternate)': 'o2_sensor1_equivalence_ratioalternate',
 'Engine Load(Absolute)(%)': 'engine_loadabsolute',
 'Charge air cooler temperature (CACT)(°C)': 'charge_air_cooler_temperature_cactc',
 'Charge air cooler temperature (CACT)(°F)': 'charge_air_cooler_temperature_cactf',
 'DPF Pressure(bar)': 'dpf_pressurebar',
 'DPF Temperature(°F)': 'dpf_temperaturef',
 'Engine Oil Temperature(°F)': 'engine_oil_temperaturef',
 'Run time since engine start(s)': 'run_time_since_engine_starts',
 'Exhaust gas temp Bank 2 Sensor 1(°C)': 'exhaust_gas_temp_bank_2_sensor_1c',
 'Relative Throttle Position(%)': 'relative_throttle_position',
 'Timing Advance(°)': 'timing_advance',
 'Transmission Temperature(Method 2)(°C)': 'transmission_temperaturemethod_2c',
 'Hybrid Battery Charge (%)(%)': 'hybrid_battery_charge_',
 'Catalyst Temperature (Bank 1 Sensor 1)(°C)': 'catalyst_temperature_bank_1_sensor_1c',
 'Catalyst Temperature (Bank 1 Sensor 1)(°F)': 'catalyst_temperature_bank_1_sensor_1f',
 'Catalyst Temperature (Bank 1 Sensor 2)(°F)': 'catalyst_temperature_bank_2_sensor_1f',
 'Accelerator PedalPosition D(%)': 'accelerator_pedalposition_d',
 'Evap System Vapour Pressure(Pa)': 'evap_system_vapour_pressurepa',
 'Exhaust gas temp Bank 2 Sensor 2(°C)': 'exhaust_gas_temp_bank_2_sensor_2c',
 'Turbo Pressure Control(psi)': 'turbo_pressure_controlpsi',
 'O2 Sensor1 Equivalence Ratio': 'o2_sensor1_equivalence_ratio',
 'Absolute Throttle Position B(%)': 'absolute_throttle_position_b',
 'Exhaust Pressure(psi)': 'exhaust_pressurepsi',
 'DPF Pressure(psi)': 'dpf_pressurepsi',
 'DPF Temperature(°C)': 'dpf_temperaturec',
 'EGR Error(%)': 'egr_error',
 'Fuel Trim Bank 1 Long Term(%)': 'fuel_trim_bank_1_long_term',
 'Percentage of City driving(%)': 'percentageofcitydriving',
 'Transmission Temperature(Method 1)(°C)': 'transmission_temperaturemethod_1c',
 'Cost per mile/km (Instant)(£/km)': 'cost_per_milekm_instantkm',
 'Accelerator PedalPosition E(%)': 'accelerator_pedalposition_e',
 'Ethanol Fuel %(%)': 'ethanol_fuel_',
 'Drivers demand engine % torque(%)': 'drivers_demand_engine__torque',
 'Distance travelled since codes cleared(km)': 'distance_travelled_since_codes_clearedkm',
 'Air Fuel Ratio(Commanded)(:1)': 'air_fuel_ratiocommanded1',
 'Fuel Level (From Engine ECU)(%)': 'fuel_level_from_engine_ecu',
 'Engine reference torque(Nm)': 'engine_reference_torquenm',
 'Exhaust gas temp Bank 1 Sensor 1(°F)': 'exhaust_gas_temp_bank_1_sensor_1f',
 'Exhaust gas temp Bank 1 Sensor 2(°F)': 'exhaust_gas_temp_bank_1_sensor_2f',
 'Exhaust gas temp Bank 1 Sensor 3(°F)': 'exhaust_gas_temp_bank_1_sensor_3f',
 'Exhaust gas temp Bank 1 Sensor 4(°F)': 'exhaust_gas_temp_bank_1_sensor_4f',
 'Exhaust gas temp Bank 2 Sensor 1(°F)': 'exhaust_gas_temp_bank_2_sensor_1f',
 'Exhaust gas temp Bank 2 Sensor 2(°F)': 'exhaust_gas_temp_bank_2_sensor_2f',
 'Exhaust gas temp Bank 2 Sensor 3(°F)': 'exhaust_gas_temp_bank_2_sensor_3f',
 'Exhaust gas temp Bank 2 Sensor 4(°F)': 'exhaust_gas_temp_bank_2_sensor_4f',
 'Exhaust Pressure(bar)': 'exhaust_pressurebar',
 'Fuel Rail Pressure (relative to manifold vacuum)(kpa)': 'fuel_rail_pressure_relative_to_manifold_vacuumkpa',
 'Transmission Temperature(Method 1)(°F)': 'transmission_temperaturemethod_1f',
 'Transmission Temperature(Method 2)(°F)': 'transmission_temperaturemethod_2f',
 'Turbo Pressure Control(bar)': 'turbo_pressure_controlbar'}



schema_pl_org = {
    "GPS Time": pl.Datetime,
    "Device Time": pl.Datetime,
    # "Longitude": pl.Float32,
    "Latitude": pl.Float32,
    "GPS Speed(km/h)": pl.Float32,
    "Horizontal Dilution of Precision": pl.Float32,
    "Altitude(m)": pl.Float32,
    "Bearing": pl.Float32,
    "Gravity X(G)": pl.Float32,
    "Gravity Y(G)": pl.Float32,
    "Gravity Z(G)": pl.Float32,
    "Acceleration Sensor(Total)(g)": pl.Float32,
    "Acceleration Sensor(X axis)(g)": pl.Float32,
    "Acceleration Sensor(Y axis)(g)": pl.Float32,
    "Acceleration Sensor(Z axis)(g)": pl.Float32,
    "Actual engine % torque(%)": pl.Float32,
    "Android device Battery Level(%)": pl.Float32,
    "Average trip speed(whilst moving only)(km/h)": pl.Float32,
    "Average trip speed(whilst stopped or moving)(km/h)": pl.Float32,
    "CO₂ in g/km (Average)(g/km)": pl.Float32,
    "CO₂ in g/km (Instantaneous)(g/km)": pl.Float32,
    "Distance to empty (Estimated)(km)": pl.Float32,
    "Distance travelled with MIL/CEL lit(km)": pl.Float32,
    "Engine Coolant Temperature(°F)": pl.Float32,
    "Engine kW (At the wheels)(kW)": pl.Float32,
    "Engine Load(%)": pl.Float32,
    "Engine RPM(rpm)": pl.Float32,
    "Fuel cost (trip)(cost)": pl.Float32,
    "Fuel flow rate/hour(l/hr)": pl.Float32,
    "Fuel flow rate/minute(cc/min)": pl.Float32,
    "Fuel Rail Pressure(kpa)": pl.Float32,
    "Fuel Remaining (Calculated from vehicle profile)(%)": pl.Float32,
    "Fuel used (trip)(l)": pl.Float32,
    "GPS Accuracy(m)": pl.Float32,
    "GPS Altitude(m)": pl.Float32,
    "GPS Bearing(°)": pl.Float32,
    "GPS Latitude(°)": pl.Float32,
    # "GPS Longitude(°)": pl.Float32,
    "GPS Satellites": pl.Float32,
    "GPS vs OBD Speed difference(km/h)": pl.Float32,
    "Horsepower (At the wheels)(hp)": pl.Float32,
    "Intake Air Temperature(°F)": pl.Float32,
    "Intake Manifold Pressure(kpa)": pl.Float32,
    "Kilometers Per Litre(Instant)(kpl)": pl.Float32,
    "Kilometers Per Litre(Long Term Average)(kpl)": pl.Float32,
    "Litres Per 100 Kilometer(Instant)(l/100km)": pl.Float32,
    "Litres Per 100 Kilometer(Long Term Average)(l/100km)": pl.Float32,
    "Mass Air Flow Rate(g/s)": pl.Float32,
    "Miles Per Gallon(Instant)(mpg)": pl.Float32,
    "Miles Per Gallon(Long Term Average)(mpg)": pl.Float32,
    "Speed (GPS)(km/h)": pl.Float32,
    "Speed (OBD)(km/h)": pl.Float32,
    "Torque(ft-lb)": pl.Float32,
    "Trip average KPL(kpl)": pl.Float32,
    "Trip average Litres/100 KM(l/100km)": pl.Float32,
    "Trip average MPG(mpg)": pl.Float32,
    "Trip Distance(km)": pl.Float32,
    "Trip distance (stored in vehicle profile)(km)": pl.Float32,
    "Trip Time(Since journey start)(s)": pl.Float32,
    "Trip time(whilst moving)(s)": pl.Float32,
    "Trip time(whilst stationary)(s)": pl.Float32,
    "Turbo Boost & Vacuum Gauge(bar)": pl.Float32,
    "Voltage (OBD Adapter)(V)": pl.Float32,
    "Volumetric Efficiency (Calculated)(%)": pl.Float32,
    "TripDistancekm": pl.Float32,
}

schema_pl_clean_names = {
    "GPSTime": pl.Datetime,
    "DeviceTime": pl.Datetime,
    # "Longitude": pl.Float32,
    "Latitude": pl.Float32,
    "GPSSpeedkmh": pl.Float32,
    "HorizontalDilutionofPrecision": pl.Float32,
    "Altitudem": pl.Float32,
    "Bearing": pl.Float32,
    "GravityXG": pl.Float32,
    "GravityYG": pl.Float32,
    "GravityZG": pl.Float32,
    "AccelerationSensorTotalg": pl.Float32,
    "AccelerationSensorXaxisg": pl.Float32,
    "AccelerationSensorYaxisg": pl.Float32,
    "AccelerationSensorZaxisg": pl.Float32,
    "Actualenginetorque": pl.Float32,
    "AndroiddeviceBatteryLevel": pl.Float32,
    "Averagetripspeedwhilstmovingonlykmh": pl.Float32,
    "Averagetripspeedwhilststoppedormovingkmh": pl.Float32,
    "COingkmAveragegkm": pl.Float32,
    "COingkmInstantaneousgkm": pl.Float32,
    "DistancetoemptyEstimatedkm": pl.Float32,
    "DistancetravelledwithMILCELlitkm": pl.Float32,
    "EngineCoolantTemperatureF": pl.Float32,
    "EnginekWAtthewheelskW": pl.Float32,
    "EngineLoad": pl.Float32,
    "EngineRPMrpm": pl.Float32,
    "Fuelcosttripcost": pl.Float32,
    "Fuelflowratehourlhr": pl.Float32,
    "Fuelflowrateminuteccmin": pl.Float32,
    "FuelRailPressurekpa": pl.Float32,
    "FuelRemainingCalculatedfromvehicleprofile": pl.Float32,
    "Fuelusedtripl": pl.Float32,
    "GPSAccuracym": pl.Float32,
    "GPSAltitudem": pl.Float32,
    "GPSBearing": pl.Float32,
    "GPSLatitude": pl.Float32,
    #"GPSLongitude": pl.Float32,
    "GPSSatellites": pl.Float32,
    "GPSvsOBDSpeeddifferencekmh": pl.Float32,
    "HorsepowerAtthewheelshp": pl.Float32,
    "IntakeAirTemperatureF": pl.Float32,
    "IntakeManifoldPressurekpa": pl.Float32,
    "KilometersPerLitreInstantkpl": pl.Float32,
    "KilometersPerLitreLongTermAveragekpl": pl.Float32,
    "LitresPer100KilometerInstantl100km": pl.Float32,
    "LitresPer100KilometerLongTermAveragel100km": pl.Float32,
    "MassAirFlowRategs": pl.Float32,
    "MilesPerGallonInstantmpg": pl.Float32,
    "MilesPerGallonLongTermAveragempg": pl.Float32,
    "SpeedGPSkmh": pl.Float32,
    "SpeedOBDkmh": pl.Float32,
    "Torqueftlb": pl.Float32,
    "TripaverageKPLkpl": pl.Float32,
    "TripaverageLitres100KMl100km": pl.Float32,
    "TripaverageMPGmpg": pl.Float32,
    "TripDistancekm": pl.Float32,
    "Tripdistancestoredinvehicleprofilekm": pl.Float32,
    "TripTimeSincejourneystarts": pl.Float32,
    "Triptimewhilstmovings": pl.Float32,
    "Triptimewhilststationarys": pl.Float32,
    "TurboBoostVacuumGaugebar": pl.Float32,
    "VoltageOBDAdapterV": pl.Float32,
    "VolumetricEfficiencyCalculated": pl.Float32,
}

schema_pl_clean_names_lowercase = {
    "gpstime": pl.Datetime,
    "devicetime": pl.Datetime,
    #"longitude": pl.Float32,
    "latitude": pl.Float32,
    "gpsspeedkmh": pl.Float32,
    "horizontaldilutionofprecision": pl.Float32,
    "altitudem": pl.Float32,
    "bearing": pl.Float32,
    "gravityxg": pl.Float32,
    "gravityyg": pl.Float32,
    "gravityzg": pl.Float32,
    "accelerationsensortotalg": pl.Float32,
    "accelerationsensorxaxisg": pl.Float32,
    "accelerationsensoryaxisg": pl.Float32,
    "accelerationsensorzaxisg": pl.Float32,
    "actualenginetorque": pl.Float32,
    "androiddevicebatterylevel": pl.Float32,
    "averagetripspeedwhilstmovingonlykmh": pl.Float32,
    "averagetripspeedwhilststoppedormovingkmh": pl.Float32,
    "coingkmaveragegkm": pl.Float32,
    "coingkminstantaneousgkm": pl.Float32,
    "distancetoemptyestimatedkm": pl.Float32,
    "distancetravelledwithmilcellitkm": pl.Float32,
    "enginecoolanttemperaturef": pl.Float32,
    "enginekwatthewheelskw": pl.Float32,
    "engineload": pl.Float32,
    "enginerpmrpm": pl.Float32,
    "fuelcosttripcost": pl.Float32,
    "fuelflowratehourlhr": pl.Float32,
    "fuelflowrateminuteccmin": pl.Float32,
    "fuelrailpressurekpa": pl.Float32,
    "fuelremainingcalculatedfromvehicleprofile": pl.Float32,
    "fuelusedtripl": pl.Float32,
    "gpsaccuracym": pl.Float32,
    "gpsaltitudem": pl.Float32,
    "gpsbearing": pl.Float32,
    "gpslatitude": pl.Float32,
    #"gpslongitude": pl.Float32,
    "gpssatellites": pl.Float32,
    "gpsvsobdspeeddifferencekmh": pl.Float32,
    "horsepoweratthewheelshp": pl.Float32,
    "intakeairtemperaturef": pl.Float32,
    "intakemanifoldpressurekpa": pl.Float32,
    "kilometersperlitreinstantkpl": pl.Float32,
    "kilometersperlitrelongtermaveragekpl": pl.Float32,
    "litresper100kilometerinstantl100km": pl.Float32,
    "litresper100kilometerlongtermaveragel100km": pl.Float32,
    "massairflowrategs": pl.Float32,
    "milespergalloninstantmpg": pl.Float32,
    "milespergallonlongtermaveragempg": pl.Float32,
    "speedgpskmh": pl.Float32,
    "speedobdkmh": pl.Float32,
    "torqueftlb": pl.Float32,
    "tripaveragekplkpl": pl.Float32,
    "tripaveragelitres100kml100km": pl.Float32,
    "tripaveragempgmpg": pl.Float32,
    "tripdistancekm": pl.Float32,
    "tripdistancestoredinvehicleprofilekm": pl.Float32,
    "triptimesincejourneystarts": pl.Float32,
    "triptimewhilstmovings": pl.Float32,
    "triptimewhilststationarys": pl.Float32,
    "turboboostvacuumgaugebar": pl.Float32,
    "voltageobdadapterv": pl.Float32,
    "volumetricefficiencycalculated": pl.Float32,
}

def merge_colum_data(new_ncc: list, ncc: dict):

    import re

    # Convert new_ncc to a dictionary with sanitized keys
    new_ncc_dict = {s: re.sub(r'\W+', '', s.lower().replace(' ', '_')) for s in new_ncc}

    # Merge ncc and new_ncc_dict
    merged_ncc = {**ncc, **new_ncc_dict}
    return merged_ncc

if __name__ == "__main__":
    pass
