"""
Metric categorization and analysis suggestion system for TorqTool.
Categorizes metrics and provides visualization/analysis recommendations.
"""

from schemas import MetricCategory, ANALYSIS_SUGGESTIONS, METRIC_CATEGORIES, AnalysisSuggestion

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
