from __future__ import annotations
from types import SimpleNamespace
import pandas as pd
import numpy as np
from skill_framework import (
    SkillInput,
    SkillVisualization,
    skill,
    SkillParameter,
    SkillOutput,
    ParameterDisplayDescription
)
from skill_framework.skills import ExportData
from skill_framework.layouts import wire_layout
from ar_analytics import DriverAnalysisTemplateParameterSetup, ArUtils
from ar_analytics.driver_analysis import DriverAnalysis
from ar_analytics.defaults import get_table_layout_vars
import jinja2
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Database Configuration
DATABASE_ID = "83E664CE-BD47-40C1-B3D6-C58B27714E8F"
DATASET_ID = "07fa2c04-5484-4c05-afbf-5868019431c7"


def _filter_metric_hierarchy_by_groups(current_metric, metric_hierarchy, metric_hierarchy_groups):
    """Filter metric_hierarchy based on metric_hierarchy_groups"""
    if not current_metric or not metric_hierarchy_groups or not metric_hierarchy:
        return metric_hierarchy

    target_group = None
    for group in metric_hierarchy_groups:
        if current_metric in group:
            target_group = group
            break

    if not target_group:
        return metric_hierarchy

    # Filter metric_hierarchy to only include metrics from the target group
    filtered_hierarchy = []
    for item in metric_hierarchy:
        metric_name = item.get('metric')
        peers = item.get('peer_metrics') or []

        # keep if the metric itself is in the group OR if any peers are in the group
        if (metric_name in target_group) or any(peer in target_group for peer in peers):
            filtered_item = item.copy()
            if peers:
                filtered_item['peer_metrics'] = [peer for peer in peers if peer in target_group]
            filtered_hierarchy.append(filtered_item)

    return filtered_hierarchy


# Default prompts
DEFAULT_MAX_PROMPT = """
Based on the following variance analysis facts:
{% for fact_list in facts %}
{% for fact in fact_list %}
- {{ fact }}
{% endfor %}
{% endfor %}

Provide a concise executive summary (2-3 sentences) highlighting the most significant variance drivers.
"""

DEFAULT_INSIGHT_PROMPT = """
Analyze the following variance analysis data:
{% for fact_list in facts %}
{% for fact in fact_list %}
- {{ fact }}
{% endfor %}
{% endfor %}

Provide detailed insights covering:
1. Key variance drivers (Price, Volume, Mix)
2. Top contributing dimensions
3. Actionable recommendations for stakeholders
4. Areas requiring immediate attention

Format the insights in clear markdown with bullet points.
"""


# Layout template for waterfall chart visualization
WATERFALL_CHART_LAYOUT = """
{
    "layoutJson": {
        "type": "Document",
        "rows": 90,
        "columns": 160,
        "rowHeight": "1.11%",
        "colWidth": "0.625%",
        "gap": "0px",
        "style": {
            "backgroundColor": "#ffffff",
            "width": "100%",
            "height": "max-content",
            "padding": "15px",
            "gap": "20px"
        },
        "children": [
            {
                "name": "CardContainer0",
                "type": "CardContainer",
                "children": "",
                "minHeight": "80px",
                "rows": 2,
                "columns": 1,
                "style": {
                    "border-radius": "11.911px",
                    "background": "#2563EB",
                    "padding": "10px",
                    "fontFamily": "Arial"
                },
                "hidden": false
            },
            {
                "name": "Header0",
                "type": "Header",
                "children": "",
                "text": "Variance Analysis",
                "style": {
                    "fontSize": "20px",
                    "fontWeight": "700",
                    "color": "#ffffff",
                    "textAlign": "left",
                    "alignItems": "center"
                },
                "parentId": "CardContainer0",
                "hidden": false
            },
            {
                "name": "Paragraph0",
                "type": "Paragraph",
                "children": "",
                "text": "Price-Volume-Mix Decomposition",
                "style": {
                    "fontSize": "15px",
                    "fontWeight": "normal",
                    "textAlign": "center",
                    "verticalAlign": "start",
                    "color": "#fafafa",
                    "border": "null",
                    "textDecoration": "null",
                    "writingMode": "horizontal-tb",
                    "alignItems": "center"
                },
                "parentId": "CardContainer0",
                "hidden": false
            },
            {
                "name": "HighchartsChart0",
                "type": "HighchartsChart",
                "minHeight": "600px",
                "chartOptions": {},
                "options": {
                    "chart": {
                        "type": "waterfall",
                        "height": 600
                    },
                    "title": {
                        "text": "",
                        "style": {
                            "fontSize": "18px",
                            "fontWeight": "bold"
                        }
                    },
                    "xAxis": {
                        "categories": [],
                        "title": {
                            "text": ""
                        }
                    },
                    "yAxis": {
                        "title": {
                            "text": ""
                        }
                    },
                    "series": [],
                    "credits": {
                        "enabled": false
                    },
                    "legend": {
                        "enabled": false
                    },
                    "tooltip": {
                        "pointFormat": "<b>{point.name}</b>: {point.formatted}"
                    }
                },
                "hidden": false
            },
            {
                "name": "DataTable0",
                "type": "DataTable",
                "children": "",
                "columns": [],
                "data": [],
                "caption": "",
                "styles": {
                    "td": {
                        "vertical-align": "middle"
                    }
                }
            }
        ]
    },
    "inputVariables": [
        {
            "name": "sub_headline",
            "isRequired": false,
            "defaultValue": null,
            "targets": [
                {
                    "elementName": "Paragraph0",
                    "fieldName": "text"
                }
            ]
        },
        {
            "name": "headline",
            "isRequired": false,
            "defaultValue": null,
            "targets": [
                {
                    "elementName": "Header0",
                    "fieldName": "text"
                }
            ]
        },
        {
            "name": "chart_categories",
            "isRequired": false,
            "defaultValue": null,
            "targets": [
                {
                    "elementName": "HighchartsChart0",
                    "fieldName": "options.xAxis.categories"
                }
            ]
        },
        {
            "name": "chart_y_axis",
            "isRequired": false,
            "defaultValue": null,
            "targets": [
                {
                    "elementName": "HighchartsChart0",
                    "fieldName": "options.yAxis"
                }
            ]
        },
        {
            "name": "chart_data",
            "isRequired": false,
            "defaultValue": null,
            "targets": [
                {
                    "elementName": "HighchartsChart0",
                    "fieldName": "options.series"
                }
            ]
        },
        {
            "name": "chart_title",
            "isRequired": false,
            "defaultValue": null,
            "targets": [
                {
                    "elementName": "HighchartsChart0",
                    "fieldName": "options.title.text"
                }
            ]
        },
        {
            "name": "data",
            "isRequired": false,
            "defaultValue": null,
            "targets": [
                {
                    "elementName": "DataTable0",
                    "fieldName": "data"
                }
            ]
        },
        {
            "name": "col_defs",
            "isRequired": false,
            "defaultValue": null,
            "targets": [
                {
                    "elementName": "DataTable0",
                    "fieldName": "columns"
                }
            ]
        }
    ]
}
"""

# Horizontal bar chart layout for dimensional breakouts
HORIZONTAL_BAR_LAYOUT = """
{
    "layoutJson": {
        "type": "Document",
        "rows": 90,
        "columns": 160,
        "rowHeight": "1.11%",
        "colWidth": "0.625%",
        "gap": "0px",
        "style": {
            "backgroundColor": "#ffffff",
            "width": "100%",
            "height": "max-content",
            "padding": "15px",
            "gap": "20px"
        },
        "children": [
            {
                "name": "CardContainer0",
                "type": "CardContainer",
                "children": "",
                "minHeight": "80px",
                "rows": 2,
                "columns": 1,
                "style": {
                    "border-radius": "11.911px",
                    "background": "#2563EB",
                    "padding": "10px",
                    "fontFamily": "Arial"
                },
                "hidden": false
            },
            {
                "name": "Header0",
                "type": "Header",
                "children": "",
                "text": "Dimensional Breakout",
                "style": {
                    "fontSize": "20px",
                    "fontWeight": "700",
                    "color": "#ffffff",
                    "textAlign": "left",
                    "alignItems": "center"
                },
                "parentId": "CardContainer0",
                "hidden": false
            },
            {
                "name": "Paragraph0",
                "type": "Paragraph",
                "children": "",
                "text": "Variance by Dimension",
                "style": {
                    "fontSize": "15px",
                    "fontWeight": "normal",
                    "textAlign": "center",
                    "verticalAlign": "start",
                    "color": "#fafafa",
                    "border": "null",
                    "textDecoration": "null",
                    "writingMode": "horizontal-tb",
                    "alignItems": "center"
                },
                "parentId": "CardContainer0",
                "hidden": false
            },
            {
                "name": "HighchartsChart0",
                "type": "HighchartsChart",
                "minHeight": "400px",
                "chartOptions": {},
                "options": {
                    "chart": {
                        "type": "bar"
                    },
                    "title": {
                        "text": "",
                        "style": {
                            "fontSize": "18px",
                            "fontWeight": "bold"
                        }
                    },
                    "xAxis": {
                        "categories": [],
                        "title": {
                            "text": ""
                        }
                    },
                    "yAxis": {
                        "title": {
                            "text": ""
                        }
                    },
                    "series": [],
                    "credits": {
                        "enabled": false
                    },
                    "legend": {
                        "enabled": true,
                        "align": "center",
                        "verticalAlign": "bottom",
                        "layout": "horizontal"
                    },
                    "plotOptions": {
                        "bar": {
                            "dataLabels": {
                                "enabled": false
                            }
                        }
                    },
                    "tooltip": {
                        "pointFormat": "<b>{series.name}</b>: {point.y:,.0f}"
                    }
                },
                "hidden": false
            },
            {
                "name": "DataTable0",
                "type": "DataTable",
                "children": "",
                "columns": [],
                "data": [],
                "caption": "",
                "styles": {
                    "td": {
                        "vertical-align": "middle"
                    }
                }
            }
        ]
    },
    "inputVariables": [
        {
            "name": "sub_headline",
            "isRequired": false,
            "defaultValue": null,
            "targets": [
                {
                    "elementName": "Paragraph0",
                    "fieldName": "text"
                }
            ]
        },
        {
            "name": "headline",
            "isRequired": false,
            "defaultValue": null,
            "targets": [
                {
                    "elementName": "Header0",
                    "fieldName": "text"
                }
            ]
        },
        {
            "name": "chart_categories",
            "isRequired": false,
            "defaultValue": null,
            "targets": [
                {
                    "elementName": "HighchartsChart0",
                    "fieldName": "options.xAxis.categories"
                }
            ]
        },
        {
            "name": "chart_y_axis",
            "isRequired": false,
            "defaultValue": null,
            "targets": [
                {
                    "elementName": "HighchartsChart0",
                    "fieldName": "options.yAxis"
                }
            ]
        },
        {
            "name": "chart_data",
            "isRequired": false,
            "defaultValue": null,
            "targets": [
                {
                    "elementName": "HighchartsChart0",
                    "fieldName": "options.series"
                }
            ]
        },
        {
            "name": "chart_title",
            "isRequired": false,
            "defaultValue": null,
            "targets": [
                {
                    "elementName": "HighchartsChart0",
                    "fieldName": "options.title.text"
                }
            ]
        },
        {
            "name": "data",
            "isRequired": false,
            "defaultValue": null,
            "targets": [
                {
                    "elementName": "DataTable0",
                    "fieldName": "data"
                }
            ]
        },
        {
            "name": "col_defs",
            "isRequired": false,
            "defaultValue": null,
            "targets": [
                {
                    "elementName": "DataTable0",
                    "fieldName": "columns"
                }
            ]
        }
    ]
}
"""


def format_number(value, is_currency=True, decimals=1):
    """Format numbers with M/K/B abbreviations"""
    if pd.isna(value) or not isinstance(value, (int, float)):
        return str(value)

    abs_value = abs(value)

    if abs_value >= 1_000_000_000:
        formatted = f"{value / 1_000_000_000:.{decimals}f}B"
    elif abs_value >= 1_000_000:
        formatted = f"{value / 1_000_000:.{decimals}f}M"
    elif abs_value >= 1_000:
        formatted = f"{value / 1_000:.{decimals}f}K"
    else:
        formatted = f"{value:.{decimals}f}"

    if is_currency:
        formatted = f"${formatted}"

    return formatted


def format_display_name(name):
    """
    Format technical names to display names
    Examples:
        gross_revenue -> Gross Revenue
        region_l2 -> Region L2
        customer_type -> Customer Type
        market_type_1 -> Market Type 1
    """
    if not name:
        return name

    # Handle special cases
    special_cases = {
        'region_l1': 'Region L1',
        'region_l2': 'Region L2',
        'market_type_1': 'Market Type 1',
        'customer_type': 'Customer Type',
        'gross_revenue': 'Gross Revenue',
        'net_revenue': 'Net Revenue',
        'gross_profit': 'Gross Profit',
        'brand_contribution_margin': 'Brand Contribution Margin',
        'units_carton': 'Units (Carton)',
        'end_date': 'End Date',
        'start_date': 'Start Date',
    }

    if name.lower() in special_cases:
        return special_cases[name.lower()]

    # Default: replace underscores with spaces and title case
    return name.replace('_', ' ').title()


class FPAVarianceAnalysis:
    """FP&A Variance Analysis with Price-Volume-Mix Decomposition"""

    def __init__(self, client, metric, period, comparison_type, breakout_dimensions=None,
                 top_n=10, other_filters=None):
        self.client = client
        self.metric = metric
        self.period = period
        self.comparison_type = comparison_type  # 'Budget', 'Forecast', 'Prior Period'
        self.breakout_dimensions = breakout_dimensions or []
        self.top_n = top_n
        self.other_filters = other_filters or []

        self.actuals_df = None
        self.comparison_df = None
        self.pvm_results = None
        self.breakout_results = {}
        self.facts = []

    def get_comparison_scenario(self):
        """Map comparison type to scenario value

        Note: Prior Period doesn't use a scenario, it uses date math
        """
        mapping = {
            'Budget': 'budget',
            'Forecast': 'forecast'
        }
        return mapping.get(self.comparison_type, 'budget')

    def build_filter_clause(self):
        """Build SQL WHERE clause from filters

        Uses case-insensitive matching (UPPER()) for string comparisons
        """
        clauses = []

        if self.other_filters:
            for filter_dict in self.other_filters:
                dim = filter_dict.get('dim')
                op = filter_dict.get('op', '=')
                val = filter_dict.get('val')

                if dim and val:
                    # Handle list values - extract first element or use IN clause
                    if isinstance(val, list):
                        if len(val) == 1:
                            # Single value in list - use case-insensitive equality
                            clauses.append(f"UPPER({dim}) {op} UPPER('{val[0]}')")
                        else:
                            # Multiple values - use case-insensitive IN clause
                            val_str = ", ".join([f"UPPER('{v}')" for v in val])
                            clauses.append(f"UPPER({dim}) IN ({val_str})")
                    elif isinstance(val, str):
                        # Case-insensitive string comparison
                        clauses.append(f"UPPER({dim}) {op} UPPER('{val}')")
                    else:
                        # Numeric comparison - no need for UPPER()
                        clauses.append(f"{dim} {op} {val}")

        return " AND " + " AND ".join(clauses) if clauses else ""

    def parse_period_to_date_range(self, period_str):
        """Convert period string to date range for SQL query"""
        from dateutil.parser import parse
        from datetime import datetime

        if not period_str:
            raise ValueError("Period is required but was not provided")

        period_lower = period_str.lower().strip()

        # Handle quarters (Q1 2024, Q2 2025, etc.)
        if period_lower.startswith('q'):
            parts = period_str.split()
            quarter = int(parts[0][1])  # Extract quarter number
            year = int(parts[1])

            quarter_map = {
                1: ('01-01', '03-31'),
                2: ('04-01', '06-30'),
                3: ('07-01', '09-30'),
                4: ('10-01', '12-31')
            }
            start_month_day, end_month_day = quarter_map[quarter]
            return f"{year}-{start_month_day}", f"{year}-{end_month_day}"

        # Handle single months (January 2025, Jan 2025, 2025-01, etc.)
        try:
            parsed_date = parse(period_str, fuzzy=True)
            year = parsed_date.year
            month = parsed_date.month

            # Get last day of month
            if month == 12:
                last_day = 31
            elif month in [4, 6, 9, 11]:
                last_day = 30
            elif month == 2:
                # Check for leap year
                if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
                    last_day = 29
                else:
                    last_day = 28
            else:
                last_day = 31

            return f"{year}-{month:02d}-01", f"{year}-{month:02d}-{last_day}"
        except:
            # If can't parse, return as-is
            return period_str, period_str

    def query_data(self):
        """Query actuals and comparison data from database"""
        logger.info(f"Querying data for metric: {self.metric}, period: {self.period}")

        filter_clause = self.build_filter_clause()

        # Parse period to date range
        start_date, end_date = self.parse_period_to_date_range(self.period)
        logger.info(f"Parsed period '{self.period}' to date range: {start_date} to {end_date}")

        # Query actuals
        actuals_query = f"""
        SELECT *
        FROM read_csv('fpa_360_v1.csv')
        WHERE scenario = 'actuals'
        AND end_date BETWEEN '{start_date}' AND '{end_date}'
        {filter_clause}
        """

        logger.info(f"Actuals query: {actuals_query}")
        result = self.client.data.execute_sql_query(
            database_id=DATABASE_ID,
            sql_query=actuals_query,
            row_limit=10000
        )
        self.actuals_df = result.df if hasattr(result, 'df') else None

        # Query comparison data - handle Prior Period differently
        if self.comparison_type == 'Prior Period':
            # Calculate prior year dates (go back 12 months)
            from dateutil.relativedelta import relativedelta
            from datetime import datetime

            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')

            prior_start = (start_dt - relativedelta(years=1)).strftime('%Y-%m-%d')
            prior_end = (end_dt - relativedelta(years=1)).strftime('%Y-%m-%d')

            comparison_query = f"""
            SELECT *
            FROM read_csv('fpa_360_v1.csv')
            WHERE scenario = 'actuals'
            AND end_date BETWEEN '{prior_start}' AND '{prior_end}'
            {filter_clause}
            """
            logger.info(f"Prior Period query (LY): {prior_start} to {prior_end}")
        else:
            # Budget or Forecast
            comparison_scenario = self.get_comparison_scenario()
            comparison_query = f"""
            SELECT *
            FROM read_csv('fpa_360_v1.csv')
            WHERE scenario = '{comparison_scenario}'
            AND end_date BETWEEN '{start_date}' AND '{end_date}'
            {filter_clause}
            """

        logger.info(f"Comparison query: {comparison_query}")
        result = self.client.data.execute_sql_query(
            database_id=DATABASE_ID,
            sql_query=comparison_query,
            row_limit=10000
        )
        self.comparison_df = result.df if hasattr(result, 'df') else None

        logger.info(f"Actuals shape: {self.actuals_df.shape if self.actuals_df is not None else 'None'}")
        logger.info(f"Comparison shape: {self.comparison_df.shape if self.comparison_df is not None else 'None'}")

        # Check if we got any comparison data
        if self.comparison_df is None or self.comparison_df.empty:
            comparison_label = "prior year actuals" if self.comparison_type == 'Prior Period' else f"{self.get_comparison_scenario()} data"
            logger.error(f"No {comparison_label} found for period {self.period} ({start_date} to {end_date})")
            raise ValueError(f"No {comparison_label} available for period {self.period}")

    def calculate_price_volume_mix(self):
        """
        Calculate Price-Volume-Mix decomposition using category-level detail

        This implementation calculates PVM at the category level then aggregates up
        to properly capture mix effects (changes in product mix proportions).

        Formula:
        - Volume Impact = Sum across categories: (Actual Volume - Budget Volume) * Budget Price
        - Mix Impact = Sum across categories: (Actual Volume * Budget Price) * (Actual Share - Budget Share)
        - Price Impact = Sum across categories: (Actual Price - Budget Price) * Actual Volume

        Where Share = Category Revenue / Total Revenue
        """
        logger.info("Calculating Price-Volume-Mix decomposition with dimensional detail")

        if self.actuals_df is None or self.comparison_df is None:
            logger.error("Data not loaded. Call query_data() first.")
            return None

        # Use units_carton as volume measure (dataset has units_carton, not volume)
        volume_col = 'units_carton' if 'units_carton' in self.actuals_df.columns else 'volume'

        # Identify dimension to use for mix calculation (prefer category, fallback to first available)
        mix_dimension = None
        potential_dims = ['category', 'product', 'region_l2', 'customer_type']
        for dim in potential_dims:
            if dim in self.actuals_df.columns and dim in self.comparison_df.columns:
                mix_dimension = dim
                break

        # Calculate totals first
        actual_revenue = self.actuals_df[self.metric].sum()
        comparison_revenue = self.comparison_df[self.metric].sum()
        total_variance = actual_revenue - comparison_revenue

        # Category-level PVM calculation to properly capture mix effects
        if mix_dimension and volume_col in self.actuals_df.columns:
            logger.info(f"Using category-level PVM calculation with dimension: {mix_dimension}")

            # Aggregate by category
            actual_by_cat = self.actuals_df.groupby(mix_dimension).agg({
                self.metric: 'sum',
                volume_col: 'sum'
            }).reset_index()
            actual_by_cat['price'] = actual_by_cat[self.metric] / actual_by_cat[volume_col]

            comparison_by_cat = self.comparison_df.groupby(mix_dimension).agg({
                self.metric: 'sum',
                volume_col: 'sum'
            }).reset_index()
            comparison_by_cat['price'] = comparison_by_cat[self.metric] / comparison_by_cat[volume_col]

            # Merge to align categories
            merged = pd.merge(
                actual_by_cat,
                comparison_by_cat,
                on=mix_dimension,
                how='outer',
                suffixes=('_actual', '_comparison')
            ).fillna(0)

            # Calculate total volumes
            total_actual_volume = actual_by_cat[volume_col].sum()
            total_comparison_volume = comparison_by_cat[volume_col].sum()

            # Step 1: Calculate Mix first - change in category shares at comparison prices
            mix_impact = 0
            for _, row in merged.iterrows():
                actual_share = row[f'{volume_col}_actual'] / total_actual_volume if total_actual_volume > 0 else 0
                comparison_share = row[f'{volume_col}_comparison'] / total_comparison_volume if total_comparison_volume > 0 else 0
                share_change = actual_share - comparison_share

                # Mix = share change × comparison price × actual total volume
                mix_impact += share_change * row['price_comparison'] * total_actual_volume

            # Step 2: Calculate Volume - total volume change at comparison average price
            comparison_avg_price = comparison_revenue / total_comparison_volume if total_comparison_volume > 0 else 0
            volume_impact = (total_actual_volume - total_comparison_volume) * comparison_avg_price

            # Step 3: Price is residual
            price_impact = total_variance - volume_impact - mix_impact

            logger.info(f"Category-level PVM - Dimension: {mix_dimension}, Categories: {len(merged)}")

        else:
            # Fallback to simple aggregate calculation
            logger.info("Using simple aggregate PVM calculation")

            actual_volume = self.actuals_df[volume_col].sum() if volume_col in self.actuals_df.columns else 0
            actual_price = actual_revenue / actual_volume if actual_volume > 0 else 0

            comparison_volume = self.comparison_df[volume_col].sum() if volume_col in self.comparison_df.columns else 0
            comparison_price = comparison_revenue / comparison_volume if comparison_volume > 0 else 0

            # Calculate impacts using standard PVM formula
            volume_impact = (actual_volume - comparison_volume) * comparison_price
            price_impact = (actual_price - comparison_price) * actual_volume
            mix_impact = total_variance - volume_impact - price_impact

        logger.info(f"PVM: Volume={volume_impact:,.0f}, Price={price_impact:,.0f}, Mix={mix_impact:,.0f}")

        self.pvm_results = {
            'starting_value': comparison_revenue,
            'volume_impact': volume_impact,
            'price_impact': price_impact,
            'mix_impact': mix_impact,
            'ending_value': actual_revenue,
            'total_variance': total_variance
        }

        # Create facts
        self.facts.append({
            'fact': f"Total variance: {format_number(total_variance)} ({total_variance/comparison_revenue*100:.1f}%)",
            'category': 'overall'
        })
        self.facts.append({
            'fact': f"Volume impact: {format_number(volume_impact)} ({volume_impact/abs(total_variance)*100 if total_variance != 0 else 0:.1f}% of variance)",
            'category': 'pvm'
        })
        self.facts.append({
            'fact': f"Price impact: {format_number(price_impact)} ({price_impact/abs(total_variance)*100 if total_variance != 0 else 0:.1f}% of variance)",
            'category': 'pvm'
        })
        self.facts.append({
            'fact': f"Mix impact: {format_number(mix_impact)} ({mix_impact/abs(total_variance)*100 if total_variance != 0 else 0:.1f}% of variance)",
            'category': 'pvm'
        })

        logger.info(f"PVM Results: {self.pvm_results}")
        return self.pvm_results

    def calculate_dimensional_breakout(self, dimension):
        """Calculate variance attribution by dimension"""
        logger.info(f"Calculating breakout for dimension: {dimension}")

        if self.actuals_df is None or self.comparison_df is None:
            logger.error("Data not loaded. Call query_data() first.")
            return None

        # Merge actuals and comparison
        actuals_agg = self.actuals_df.groupby(dimension)[self.metric].sum().reset_index()
        actuals_agg.columns = [dimension, 'actual']

        comparison_agg = self.comparison_df.groupby(dimension)[self.metric].sum().reset_index()
        comparison_agg.columns = [dimension, 'comparison']

        merged = pd.merge(actuals_agg, comparison_agg, on=dimension, how='outer').fillna(0)

        # Calculate variance
        merged['variance'] = merged['actual'] - merged['comparison']
        merged['variance_pct'] = merged['variance'] / merged['comparison'] * 100

        # Rank by absolute variance
        merged['abs_variance'] = merged['variance'].abs()
        merged = merged.sort_values('abs_variance', ascending=False)

        # Take top N
        top_n_df = merged.head(self.top_n).copy()

        self.breakout_results[dimension] = top_n_df

        # Add facts for top contributors
        for idx, row in top_n_df.head(3).iterrows():
            self.facts.append({
                'fact': f"{dimension} '{row[dimension]}': {format_number(row['variance'])} variance ({row['variance_pct']:.1f}%)",
                'category': f'breakout_{dimension}'
            })

        logger.info(f"Breakout results for {dimension}: {top_n_df.shape}")
        return top_n_df

    def create_waterfall_chart_data(self):
        """Create Highcharts waterfall chart configuration with pretty formatting"""
        if not self.pvm_results:
            return None

        categories = [
            f"{self.comparison_type}",
            "Volume",
            "Price",
            "Mix",
            "Actuals"
        ]

        metric_display = format_display_name(self.metric)

        # Format values - millions for large values, thousands for Mix
        def format_millions(value):
            return f"${value / 1_000_000:.2f}M"

        def format_thousands(value):
            return f"${value / 1_000:.1f}K"

        # Determine colors: green for positive, red for negative, blue for totals
        def get_color(value):
            if value >= 0:
                return '#4ade80'  # Green for positive
            else:
                return '#ef4444'  # Red for negative

        volume_val = int(self.pvm_results['volume_impact'])
        price_val = int(self.pvm_results['price_impact'])
        mix_val = int(self.pvm_results['mix_impact'])

        # Convert values to millions for cleaner display (except Mix in thousands)
        starting_m = self.pvm_results['starting_value'] / 1_000_000
        volume_m = volume_val / 1_000_000
        price_m = price_val / 1_000_000
        mix_m = mix_val / 1_000_000  # Still in millions for chart Y axis consistency
        ending_m = self.pvm_results['ending_value'] / 1_000_000

        # Waterfall chart data with colors and formatted labels
        data_series = [{
            'name': metric_display,
            'data': [
                {
                    'name': self.comparison_type,
                    'y': starting_m,
                    'color': '#3b82f6',  # Blue for starting value
                    'dataLabels': {
                        'enabled': True,
                        'format': format_millions(self.pvm_results['starting_value'])
                    }
                },
                {
                    'name': 'Volume',
                    'y': volume_m,
                    'color': get_color(volume_val),
                    'dataLabels': {
                        'enabled': True,
                        'format': format_millions(volume_val)
                    }
                },
                {
                    'name': 'Price',
                    'y': price_m,
                    'color': get_color(price_val),
                    'dataLabels': {
                        'enabled': True,
                        'format': format_millions(price_val)
                    }
                },
                {
                    'name': 'Mix',
                    'y': mix_m,
                    'color': get_color(mix_val),
                    'dataLabels': {
                        'enabled': True,
                        'format': format_thousands(mix_val)  # Format Mix in thousands
                    }
                },
                {
                    'name': 'Actuals',
                    'isSum': True,
                    'y': ending_m,
                    'color': '#3b82f6',  # Blue for ending value
                    'dataLabels': {
                        'enabled': True,
                        'format': format_millions(self.pvm_results['ending_value'])
                    }
                }
            ],
            'dataLabels': {
                'enabled': True,
                'style': {
                    'fontWeight': 'bold',
                    'color': '#000000',
                    'textOutline': 'none'
                }
            },
            'tooltip': {
                'pointFormat': '<b>{point.name}</b>: {point.y:.2f}M'
            }
        }]

        # Smart Y-axis: don't start at 0, let Highcharts calculate based on data range
        # Find min/max values to set appropriate axis range
        min_val = min(starting_m, ending_m, starting_m + volume_m, starting_m + volume_m + price_m, starting_m + volume_m + price_m + mix_m)
        max_val = max(starting_m, ending_m, starting_m + volume_m, starting_m + volume_m + price_m, starting_m + volume_m + price_m + mix_m)

        # Add 10% padding
        padding = (max_val - min_val) * 0.1

        return {
            'chart_categories': categories,
            'chart_data': data_series,
            'chart_y_axis': {
                'title': {'text': metric_display},
                'labels': {'format': '${value:,.0f}M'},
                'min': min_val - padding,
                'max': max_val + padding
            },
            'chart_title': ''
        }

    def create_horizontal_bar_chart_data(self, dimension):
        """Create Highcharts horizontal bar chart for dimension breakout"""
        if dimension not in self.breakout_results:
            return None

        df = self.breakout_results[dimension]

        categories = df[dimension].tolist()
        # Convert to millions for cleaner display
        actual_data = [x / 1_000_000 for x in df['actual'].tolist()]
        comparison_data = [x / 1_000_000 for x in df['comparison'].tolist()]

        return {
            'chart_categories': categories,
            'chart_data': [
                {
                    'name': 'Actuals',
                    'data': actual_data,
                    'color': '#5DADE2'
                },
                {
                    'name': self.comparison_type,
                    'data': comparison_data,
                    'color': '#F8C471'
                }
            ],
            'chart_y_axis': {
                'title': {'text': format_display_name(self.metric)},
                'labels': {'format': '${value:,.0f}M'}
            },
            'chart_title': f'{format_display_name(dimension)} Variance Analysis'
        }

    def get_summary_table(self):
        """Create driver analysis table with Current Period, Compare Period, Change columns

        Hardcoded metrics to display:
        - gross_revenue (main metric)
        - brand_contribution_margin
        - brand_contribution_margin (%)
        - gross_profit
        - net_revenue
        - price
        - units_carton
        """
        if not self.pvm_results:
            return None

        # Hardcoded metrics to show in table
        metrics_to_show = [
            ('Gross Revenue', 'gross_revenue', True),  # (Display Name, Column Name, Is Currency)
            ('  Brand Contribution Margin', 'brand_contribution_margin', True),
            ('  Brand Contribution Margin %', 'brand_contribution_margin', False),  # Will calculate percentage
            ('  Gross Profit', 'gross_profit', True),
            ('  Net Revenue', 'net_revenue', True),
            ('  Price', 'price', True),
            ('  Units (Carton)', 'units_carton', False)
        ]

        data = []

        for display_name, metric_col, is_currency in metrics_to_show:
            # Get actual and comparison values for this metric
            if metric_col in self.actuals_df.columns and metric_col in self.comparison_df.columns:
                # Special handling for Price - calculate weighted average, not sum
                if display_name == '  Price':
                    actual_gross_rev = self.actuals_df['gross_revenue'].sum()
                    actual_volume = self.actuals_df['units_carton'].sum()
                    comparison_gross_rev = self.comparison_df['gross_revenue'].sum()
                    comparison_volume = self.comparison_df['units_carton'].sum()

                    actual_value = actual_gross_rev / actual_volume if actual_volume > 0 else 0
                    comparison_value = comparison_gross_rev / comparison_volume if comparison_volume > 0 else 0
                else:
                    actual_value = self.actuals_df[metric_col].sum()
                    comparison_value = self.comparison_df[metric_col].sum()

                # For Brand Contribution Margin %, calculate percentage of gross revenue
                if display_name == '  Brand Contribution Margin %':
                    actual_gross_rev = self.actuals_df['gross_revenue'].sum()
                    comparison_gross_rev = self.comparison_df['gross_revenue'].sum()

                    if actual_gross_rev != 0 and comparison_gross_rev != 0:
                        actual_value = (actual_value / actual_gross_rev) * 100
                        comparison_value = (comparison_value / comparison_gross_rev) * 100

                        variance_amount = actual_value - comparison_value
                        variance_pct_display = f"{variance_amount:+.1f} pts" if variance_amount != 0 else "0.0 pts"

                        data.append([
                            display_name,
                            f"{actual_value:.1f}%",
                            f"{comparison_value:.1f}%",
                            variance_pct_display,
                            variance_pct_display
                        ])
                    continue

                # Calculate variance
                variance_amount = actual_value - comparison_value
                variance_pct = (variance_amount / comparison_value * 100) if comparison_value != 0 else 0

                # Format values
                if is_currency:
                    actual_formatted = format_number(actual_value)
                    comparison_formatted = format_number(comparison_value)
                    variance_formatted = format_number(variance_amount)
                else:
                    actual_formatted = f"{actual_value:,.0f}"
                    comparison_formatted = f"{comparison_value:,.0f}"
                    variance_formatted = f"{variance_amount:+,.0f}"

                data.append([
                    display_name,
                    actual_formatted,
                    comparison_formatted,
                    variance_formatted,
                    f"{variance_pct:+.1f}%"
                ])
            else:
                # Metric not available in data
                data.append([
                    display_name,
                    "N/A",
                    "N/A",
                    "N/A",
                    "N/A"
                ])

        columns = [
            {'name': ''},
            {'name': 'Current Period'},
            {'name': f'{self.comparison_type}'},
            {'name': 'Change ($)'},
            {'name': 'Change (%)'}
        ]

        return {'data': data, 'col_defs': columns}

    def get_breakout_table(self, dimension):
        """Create variance table for dimension breakout"""
        if dimension not in self.breakout_results:
            return None

        df = self.breakout_results[dimension]

        data = []
        for _, row in df.iterrows():
            data.append([
                row[dimension],
                format_number(row['actual']),
                format_number(row['comparison']),
                format_number(row['variance']),
                f"{row['variance_pct']:.1f}%"
            ])

        columns = [
            {'name': dimension},
            {'name': 'Actuals'},
            {'name': self.comparison_type},
            {'name': 'Variance'},
            {'name': 'Variance %'}
        ]

        return {'data': data, 'col_defs': columns}

    def run_analysis(self):
        """Run complete variance analysis"""
        logger.info("Starting FPA variance analysis")

        # Query data
        self.query_data()

        # Calculate PVM
        self.calculate_price_volume_mix()

        # Calculate dimensional breakouts
        for dim in self.breakout_dimensions:
            self.calculate_dimensional_breakout(dim)

        logger.info("Analysis complete")
        return self


@skill(
    name="FP&A Drivers",
    llm_name="Metric Drivers with Price-Volume-Mix Decomposition",
    description="Analyze variance drivers using Price-Volume-Mix decomposition with waterfall charts and dimensional breakouts. Compare actuals vs Budget, Forecast, or Prior Period.",
    capabilities="Price-Volume-Mix variance decomposition. Waterfall chart visualization of variance components. Dimensional breakout analysis with horizontal bar charts. Top contributor identification and ranking. Multi-dimensional variance attribution. Comparison vs Budget, Forecast, or Prior Period.",
    limitations="Requires 'scenario' column in dataset with values: actuals, budget, forecast. Requires 'volume' column for accurate PVM decomposition. Limited to configured metrics and dimensions.",
    example_questions="What are the revenue drivers for Q3 2024 vs budget? Show me price-volume-mix analysis for sales vs forecast. Which regions contributed most to the revenue variance? Analyze variance drivers by category and customer type. What caused the variance in Q4 vs prior period?",
    parameter_guidance="Select the metric to analyze (e.g., Revenue, Profit, Units). Choose the time period for analysis (e.g., Q3 2024, 2024, Jan 2024). Choose Budget, Forecast, or Prior Period for comparison. Select dimensions for detailed breakout (e.g., Region, Category, Customer). Specify number of top contributors to display (default 10). Add additional filters as needed (e.g., Region = North, Product = Electronics).",
    parameters=[
        SkillParameter(
            name="metric",
            constrained_to="metrics",
            is_multi=False,
            description="Metric to analyze (e.g., Revenue, Profit)"
        ),
        SkillParameter(
            name="period",
            constrained_to="date_filter",
            is_multi=False,
            description="Time period in format 'Q3 2024', '2024', 'Jan 2024', etc."
        ),
        SkillParameter(
            name="comparison_type",
            constrained_to=None,
            constrained_values=["Budget", "Forecast", "Prior Period"],
            description="Comparison type: Budget, Forecast, or Prior Period",
            default_value="Budget"
        ),
        SkillParameter(
            name="breakout_dimensions",
            constrained_to="dimensions",
            is_multi=True,
            description="Dimensions for breakout analysis (e.g., Region, Category, Customer Type)"
        ),
        SkillParameter(
            name="top_n",
            description="Number of top contributors to display",
            default_value=10
        ),
        SkillParameter(
            name="other_filters",
            constrained_to="filters",
            is_multi=True,
            description="Additional filters to apply to the analysis"
        ),
        SkillParameter(
            name="max_prompt",
            parameter_type="prompt",
            description="Prompt for executive summary",
            default_value=DEFAULT_MAX_PROMPT
        ),
        SkillParameter(
            name="insight_prompt",
            parameter_type="prompt",
            description="Prompt for detailed insights",
            default_value=DEFAULT_INSIGHT_PROMPT
        )
    ]
)
def metric_drivers(parameters: SkillInput):
    """Execute FP&A Variance Analysis with Price-Volume-Mix decomposition"""

    logger.info(f"Skill received parameters: {parameters.arguments}")

    # Extract parameters
    metric = getattr(parameters.arguments, 'metric', None)
    period = getattr(parameters.arguments, 'period', None)
    comparison_type = getattr(parameters.arguments, 'comparison_type', 'Budget')

    # HARDCODED: Always show these 5 breakout dimensions
    breakout_dimensions = ['category', 'region_l2', 'country', 'customer_type', 'market_type_1']

    top_n = int(getattr(parameters.arguments, 'top_n', 10) or 10)
    other_filters = getattr(parameters.arguments, 'other_filters', [])
    max_prompt = getattr(parameters.arguments, 'max_prompt', DEFAULT_MAX_PROMPT)
    insight_prompt = getattr(parameters.arguments, 'insight_prompt', DEFAULT_INSIGHT_PROMPT)

    # Validate required parameters
    if not metric:
        return SkillOutput(
            final_prompt="Please select a metric to analyze (e.g., gross_revenue, net_revenue).",
            narrative="**Missing Parameter**: A metric is required for variance analysis.",
            visualizations=[],
            warnings=["Metric parameter is required"]
        )

    if not period:
        return SkillOutput(
            final_prompt="Please select a time period for analysis (e.g., Q3 2024, Jan 2025).",
            narrative="**Missing Parameter**: A time period is required for variance analysis.",
            visualizations=[],
            warnings=["Period parameter is required"]
        )

    # Get AnswerRocketClient
    try:
        from answer_rocket import AnswerRocketClient
        from ar_analytics import ArUtils

        client = AnswerRocketClient()
        ar_utils = ArUtils()
    except Exception as e:
        logger.error(f"Failed to initialize AnswerRocketClient: {e}")
        return SkillOutput(
            final_prompt=f"Failed to initialize client: {str(e)}",
            warnings=[str(e)]
        )

    # Run analysis
    analysis = FPAVarianceAnalysis(
        client=client,
        metric=metric,
        period=period,
        comparison_type=comparison_type,
        breakout_dimensions=breakout_dimensions,
        top_n=top_n,
        other_filters=other_filters
    )

    try:
        analysis.run_analysis()
    except ValueError as e:
        logger.error(f"Analysis failed: {e}")
        return SkillOutput(
            final_prompt=f"Analysis could not be completed: {str(e)}. Please try a different time period or check that budget/forecast data exists for the selected period.",
            narrative=f"**Error**: {str(e)}\n\nPlease select a time period where both actuals and {comparison_type.lower()} data are available.",
            visualizations=[],
            warnings=[str(e)]
        )

    # Generate insights
    facts_list = [pd.DataFrame(analysis.facts)]
    insight_template = jinja2.Template(insight_prompt).render(facts=[facts_list])
    max_response_prompt = jinja2.Template(max_prompt).render(facts=[facts_list])

    try:
        if ar_utils:
            insights = ar_utils.get_llm_response(insight_template)
        else:
            insights = "Variance analysis complete. Review the waterfall chart and dimensional breakouts for detailed insights."
    except:
        insights = "Variance analysis complete. Review the waterfall chart and dimensional breakouts for detailed insights."

    # Create visualizations
    viz_list = []
    export_data = {}

    # Tab 1: Waterfall Chart + Summary Table
    waterfall_data = analysis.create_waterfall_chart_data()
    summary_table = analysis.get_summary_table()

    logger.info(f"Waterfall data: {waterfall_data}")
    logger.info(f"Summary table: {summary_table}")

    if waterfall_data and summary_table:
        metric_display = format_display_name(metric)
        general_vars = {
            "headline": f"{metric_display} Variance Analysis",
            "sub_headline": f"{period} | Actuals vs {comparison_type}",
            "exec_summary": insights
        }

        layout_vars = {**general_vars, **waterfall_data, **summary_table}

        logger.info(f"Layout vars keys: {layout_vars.keys()}")
        logger.info(f"Chart data sample: {layout_vars.get('chart_data', 'MISSING')}")
        logger.info(f"Table data sample: {layout_vars.get('data', 'MISSING')}")

        rendered = wire_layout(json.loads(WATERFALL_CHART_LAYOUT), layout_vars)
        viz_list.append(SkillVisualization(title=f"{metric_display} Analysis", layout=rendered))
        export_data["PVM_Summary"] = pd.DataFrame(summary_table['data'], columns=['', 'Current Period', comparison_type, 'Change ($)', 'Change (%)'])
    else:
        logger.error(f"Missing waterfall data or summary table - waterfall: {waterfall_data is not None}, table: {summary_table is not None}")

    # Tab 2+: Horizontal Bar Charts for each dimension
    for dimension in breakout_dimensions:
        bar_data = analysis.create_horizontal_bar_chart_data(dimension)
        table_data = analysis.get_breakout_table(dimension)

        if bar_data and table_data:
            dimension_display = format_display_name(dimension)
            general_vars = {
                "headline": f"{dimension_display} Breakout",
                "sub_headline": f"Top {top_n} Contributors to Variance"
            }

            layout_vars = {**general_vars, **bar_data, **table_data}
            rendered = wire_layout(json.loads(HORIZONTAL_BAR_LAYOUT), layout_vars)
            viz_list.append(SkillVisualization(title=dimension_display, layout=rendered))
            export_data[f"{dimension}_Variance"] = analysis.breakout_results[dimension]

    # Create parameter display - format as "Key: Value" in the value field
    metric_display = format_display_name(metric)
    dimensions_display = ", ".join([format_display_name(d) for d in breakout_dimensions]) if breakout_dimensions else "None"

    param_info = [
        ParameterDisplayDescription(key="", value=f"Metric: {metric_display}"),
        ParameterDisplayDescription(key="", value=f"Period: {period}"),
        ParameterDisplayDescription(key="", value=f"Comparison: {comparison_type}"),
        ParameterDisplayDescription(key="", value=f"Dimensions: {dimensions_display}")
    ]

    # Add any user-specified filters to parameter display
    if other_filters:
        filter_descriptions = []
        for f in other_filters:
            dim = format_display_name(f.get('dim', ''))
            op = f.get('op', '=')
            val = f.get('val', '')
            filter_descriptions.append(f"{dim} {op} {val}")
        param_info.append(
            ParameterDisplayDescription(key="", value=f"Filters: {'; '.join(filter_descriptions)}")
        )

    return SkillOutput(
        final_prompt=max_response_prompt,
        narrative=insights,
        visualizations=viz_list,
        parameter_display_descriptions=param_info,
        export_data=[ExportData(name=name, data=df) for name, df in export_data.items()]
    )
