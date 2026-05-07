"""
Metric categorization and analysis suggestion system for TorqTool.
Categorizes metrics and provides visualization/analysis recommendations.
"""

from enum import Enum
from typing import TypedDict

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

class AnalysisSuggestion(TypedDict):
	"""Suggestion for how to analyze and visualize a metric."""
	analysis_type: str
	visualization: str
	unit: str
	description: str

# Metric categorization map: normalized name -> (category, display_name, unit)
METRIC_CATEGORIES = {
	# Speed metrics
	"speedgpskmh": (MetricCategory.SPEED, "GPS Speed", "km/h"),
	"speedobdkmh": (MetricCategory.SPEED, "OBD Speed", "km/h"),
	"gpsspeedmeterssecond": (MetricCategory.SPEED, "GPS Speed", "m/s"),
	"averagetripspeedwhilstmovingkmh": (MetricCategory.SPEED, "Avg Speed (Moving)", "km/h"),
	"averagetripspeedwhilststoppedormovingkmh": (MetricCategory.SPEED, "Avg Speed (All)", "km/h"),
	"gpsvsObDspeeddifferencekmh": (MetricCategory.SPEED, "GPS-OBD Speed Diff", "km/h"),

	# Engine metrics
	"enginerpmrpm": (MetricCategory.ENGINE, "Engine RPM", "rpm"),
	"engineload": (MetricCategory.ENGINE, "Engine Load", "%"),
	"actualenginetorque": (MetricCategory.ENGINE, "Engine Torque", "Nm"),
	"enginekwatthewheelskw": (MetricCategory.ENGINE, "Power Output", "kW"),
	"horsepoweratthewheelshp": (MetricCategory.ENGINE, "Horsepower", "hp"),
	"volumetricefficiencycalculated": (MetricCategory.ENGINE, "Volumetric Efficiency", "%"),

	# Fuel metrics
	"fuelflowratehourlhr": (MetricCategory.FUEL, "Fuel Flow Rate (hourly)", "l/h"),
	"fuelflowrateminuteccmin": (MetricCategory.FUEL, "Fuel Flow Rate (minute)", "cc/min"),
	"fuelusedtripl": (MetricCategory.FUEL, "Fuel Used", "l"),
	"fuelremainingcalculatedfromvehicleprofile": (MetricCategory.FUEL, "Fuel Remaining", "l"),
	"fuelcosttripcost": (MetricCategory.FUEL, "Fuel Cost", "cost"),
	"costpermilekminstantkm": (MetricCategory.FUEL, "Cost per km (instant)", "cost/km"),
	"costpermilekmtripkm": (MetricCategory.FUEL, "Cost per km (trip)", "cost/km"),
	"distancetoemptyestimatedkm": (MetricCategory.FUEL, "Distance to Empty", "km"),

	# Efficiency metrics
	"milespergalloninstantmpg": (MetricCategory.EFFICIENCY, "Instant MPG", "mpg"),
	"milespergallonlongtermaveragempg": (MetricCategory.EFFICIENCY, "Long-term Avg MPG", "mpg"),
	"tripaveragempgmpg": (MetricCategory.EFFICIENCY, "Trip Avg MPG", "mpg"),
	"kilometersperlitreinstantkpl": (MetricCategory.EFFICIENCY, "Instant KPL", "km/l"),
	"kilometersperlitrelongtermaveragekpl": (MetricCategory.EFFICIENCY, "Long-term Avg KPL", "km/l"),
	"tripaveragekplkpl": (MetricCategory.EFFICIENCY, "Trip Avg KPL", "km/l"),
	"litresper100kilometerinstantl100km": (MetricCategory.EFFICIENCY, "Instant L/100km", "l/100km"),
	"litresper100kilometerlongtermaveragel100km": (MetricCategory.EFFICIENCY, "Long-term L/100km", "l/100km"),
	"tripaveragelitres100kml100km": (MetricCategory.EFFICIENCY, "Trip Avg L/100km", "l/100km"),

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
	"accelerationsensorxaxisg": (MetricCategory.ACCELERATION, "X-axis Acceleration", "g"),
	"accelerationsensoryaxisg": (MetricCategory.ACCELERATION, "Y-axis Acceleration", "g"),
	"accelerationsensorzaxisg": (MetricCategory.ACCELERATION, "Z-axis Acceleration", "g"),
	"gravityxg": (MetricCategory.ACCELERATION, "Gravity X", "g"),
	"gravityyg": (MetricCategory.ACCELERATION, "Gravity Y", "g"),
	"gravityzg": (MetricCategory.ACCELERATION, "Gravity Z", "g"),
	"gcalibrated": (MetricCategory.ACCELERATION, "G-Force (Calibrated)", "g"),

	# Temperature metrics
	"enginecoolanttemperaturef": (MetricCategory.TEMPERATURE, "Coolant Temp", "°F"),
	"intakeairtemperaturef": (MetricCategory.TEMPERATURE, "Intake Air Temp", "°F"),
	"ambientairtempf": (MetricCategory.TEMPERATURE, "Ambient Air Temp", "°F"),

	# Pressure metrics
	"intakemanifoldpressurekpa": (MetricCategory.PRESSURE, "Intake Manifold Pressure", "kPa"),
	"barometricpressurefromvehiclekpa": (MetricCategory.PRESSURE, "Barometric Pressure", "kPa"),
	"fuelrailpressurekpa": (MetricCategory.PRESSURE, "Fuel Rail Pressure", "kPa"),
	"fuelpressurekpa": (MetricCategory.PRESSURE, "Fuel Pressure", "kPa"),
	"turboboostvacuumgaugebar": (MetricCategory.PRESSURE, "Turbo Boost/Vacuum", "bar"),

	# Emissions metrics
	"coaingkmaveragegkm": (MetricCategory.EMISSION, "CO Avg", "g/km"),
	"coaingkminstantaneousgkm": (MetricCategory.EMISSION, "CO Instant", "g/km"),
	"coingkminstantaneousgkm": (MetricCategory.EMISSION, "CO (inst)", "g/km"),

	# Trip summary metrics
	"tripdistancekm": (MetricCategory.TRIP, "Trip Distance", "km"),
	"tripdistancestoredinvehicleprofilekm": (MetricCategory.TRIP, "Vehicle Profile Distance", "km"),
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
	"o2sensor1widerangeequivalenceratio": (MetricCategory.OTHER, "O2 Equivalence Ratio", "ratio"),
	"o2sensor1widerangevoltagev": (MetricCategory.OTHER, "O2 Sensor Voltage", "V"),
	"o2bank1sensor1widerangeequivalenceratio": (MetricCategory.OTHER, "O2 Bank 1 Equivalence", "ratio"),
	"o2bank1sensor1widerangevoltagev": (MetricCategory.OTHER, "O2 Bank 1 Voltage", "V"),
	"throttlepositionmanifold": (MetricCategory.OTHER, "Throttle Position", "%"),
	"positivekineticenergypkekmhr": (MetricCategory.OTHER, "Positive Kinetic Energy", "km/h"),
	"distancetravelledwithmilcellitkm": (MetricCategory.OTHER, "MIL Distance", "km"),
}

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


def categorize_metric(metric_name: str) -> tuple[MetricCategory, str, str]:
	"""
	Categorize a metric and return its category, display name, and unit.
	
	Args:
		metric_name: The metric column name (normalized or original)
		
	Returns:
		Tuple of (category, display_name, unit)
	"""
	normalized = "".join(ch.lower() for ch in str(metric_name) if ch.isalnum())
	if normalized in METRIC_CATEGORIES:
		return METRIC_CATEGORIES[normalized]
	return (MetricCategory.OTHER, metric_name, "")


def get_analysis_suggestion(category: MetricCategory) -> AnalysisSuggestion:
	"""Get analysis suggestion for a metric category."""
	return ANALYSIS_SUGGESTIONS.get(category, ANALYSIS_SUGGESTIONS[MetricCategory.OTHER])


def group_metrics_by_category(metric_names: list[str]) -> dict[MetricCategory, list[tuple[str, str, str]]]:
	"""
	Group metrics by category.
	
	Args:
		metric_names: List of metric column names
		
	Returns:
		Dict mapping category to list of (display_name, unit, original_name) tuples
	"""
	grouped: dict[MetricCategory, list[tuple[str, str, str]]] = {}
	for metric in metric_names:
		category, display_name, unit = categorize_metric(metric)
		if category not in grouped:
			grouped[category] = []
		grouped[category].append((display_name, unit, metric))
	
	# Sort by category enum value for consistent ordering
	return dict(sorted(grouped.items(), key=lambda x: x[0].value))


def format_metric_info(metric_name: str, min_val: float, avg_val: float, max_val: float) -> str:
	"""Format metric stats into readable string."""
	category, display_name, unit = categorize_metric(metric_name)
	unit_suffix = f" {unit}" if unit else ""
	return f"{display_name}: {min_val:.2f} / {avg_val:.2f} / {max_val:.2f}{unit_suffix}"
