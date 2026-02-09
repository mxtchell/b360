from __future__ import annotations
from types import SimpleNamespace

from skill_framework import SkillVisualization, skill, SkillParameter, SkillInput, SkillOutput, ParameterDisplayDescription, SuggestedQuestion
from skill_framework.skills import ExportData
from skill_framework.layouts import wire_layout

from ar_analytics import ArUtils
from ar_analytics.defaults import default_table_layout, kpi_performance_config
from ar_analytics.kpi_performance import KPIPerformance, KPIPerformanceTemplateParameterSetup, get_table_layout_vars_kpi

import pandas as pd
import jinja2
import logging
import json

logger = logging.getLogger(__name__)

@skill(
    name=kpi_performance_config.name,
    description=kpi_performance_config.description,
    llm_name=kpi_performance_config.llm_name,  
    capabilities=kpi_performance_config.capabilities,  
    limitations=kpi_performance_config.limitations,   
    example_questions=kpi_performance_config.example_questions,  
    parameter_guidance=kpi_performance_config.parameter_guidance,  
    parameters=[
        SkillParameter(
            name="metrics",
            is_multi=True,
            constrained_to="metrics",
            description="metrics that are desired in output columns"
        ),
        SkillParameter(
            name="metric_groups",
            is_multi=True,
            constrained_values=[],
            description="Metric group used to pull grouped metrics"
        ),
        SkillParameter(
            name="breakouts",
            is_multi=True,
            constrained_to="dimensions",
            description="breakout dimension(s) for analysis."
        ),
        SkillParameter(
            name="periods",
            is_multi=True,
            constrained_to="date_filter",
            description=""
        ),
        SkillParameter(
            name="growth_type",
            is_multi=False,
            constrained_to=None,
            constrained_values=["none", "Y/Y", "P/P"],
            default_value="none",
            description="growth type to apply to the query"
        ),
        SkillParameter(
            name="growth_trend",
            constrained_to=None,
            constrained_values=["fastest growing", "highest growing", "highest declining", "fastest declining",
                                "smallest overall", "biggest overall"],
            description="indicates the trend type (fastest, highest, overall size) within a specified growth metric (year over year, period over period) for entities being analyzed."
        ),
        SkillParameter(
            name="other_filters",
            constrained_to="filters"
        ),
        SkillParameter(
            name="max_prompt",
            parameter_type="prompt",
            description="Prompt being used for max response.",
            default_value=kpi_performance_config.max_prompt
        ),
        SkillParameter(
            name="insight_prompt",
            parameter_type="prompt",
            description="Prompt being used for detailed insights.",
            default_value=kpi_performance_config.insight_prompt
        ),
        SkillParameter(
            name="table_viz_layout",
            parameter_type="visualization",
            description="Table Viz Layout",
            default_value=default_table_layout
        ),
        SkillParameter(
            name="limit_n",
            is_multi=False,
            constrained_to=None,
            default_value=10,
            description="Maximum number of dimension values to display (Top N filtering)"
        )
    ]
)
def kpi_performance(parameters: SkillInput):
    """Process KPI performance analysis for Kantar survey data.
    
    Args:
        parameters: SkillInput containing analysis parameters
        
    Returns:
        SkillOutput containing analysis results
    """
    print(f"Skill received following parameters: {parameters.arguments}")
    
    env = SimpleNamespace(**parameters.arguments.__dict__)
    KPIPerformanceTemplateParameterSetup(env=env)
    
    env.kpi_performance = KPIPerformance.from_env(env=env)
    df = env.kpi_performance.run_from_env()

    tables, footnotes = env.kpi_performance.get_display_tables(drop_rank=env.kpi_performance.include_ranks, breakouts=env.kpi_parameters["breakouts"])
    param_info = [ParameterDisplayDescription(key=k, value=v) for k, v in env.kpi_performance.paramater_display_infomation.items()]

    # Limit facts to top 5 for LLM insights using first table
    first_table = list(tables.values())[0] if tables else df
    facts_df, facts_limitation_note = env.kpi_performance.limit_facts_to_top_n(
        df=first_table, 
        top_n=5
    )

    insights_dfs = [env.kpi_performance.df_notes, facts_df]
    followups = env.kpi_performance.get_suggestions()

    # Add facts limitation note to warnings if present
    warning_message = env.kpi_performance.warning_message
    if facts_limitation_note:
        if warning_message:
            warning_message = warning_message + " " + facts_limitation_note
        else:
            warning_message = facts_limitation_note

    viz, _, final_prompt, export_data = render_layout(tables,
                                                            env.kpi_performance.title,
                                                            env.kpi_performance.subtitle,
                                                            env.kpi_performance.dimensions[0],
                                                            insights_dfs,
                                                            warning_message,
                                                            footnotes,
                                                            parameters.arguments.max_prompt,
                                                            parameters.arguments.insight_prompt, 
                                                            parameters.arguments.table_viz_layout)

    return SkillOutput(
        final_prompt=final_prompt,
        narrative=None,
        visualizations=viz,
        parameter_display_descriptions=param_info,
        followup_questions=[SuggestedQuestion(label=f.get("label"), question=f.get("question")) for f in followups if f.get("label")],
        export_data=[ExportData(name=name, data=df) for name, df in export_data.items()]
    )

def render_layout(tables, title, subtitle, dim_col_label, insights_dfs, warnings, footnotes, max_prompt, insight_prompt, viz_layout):
    facts = []

    for i_df in insights_dfs:
        facts.append(i_df.to_dict(orient='records'))

    insight_template = jinja2.Template(insight_prompt).render(**{"facts": facts})
    max_response_prompt = jinja2.Template(max_prompt).render(**{"facts": facts})

    # adding insights
    ar_utils = ArUtils()
    insights = ar_utils.get_llm_response(insight_template)

    # Base variables for all visualizations
    general_vars = {
        "headline": title if title else "Total",
        "sub_headline": subtitle or "Trend Analysis",
        "hide_growth_warning": False if warnings else True,
        "exec_summary": insights if insights else "No Insight.",
        "warning": warnings
    }

    viz_list = []
    export_data = {}

    for name, table in tables.items():
        export_data[name] = table

        table_vars = get_table_layout_vars_kpi(table,  dim_col_label=dim_col_label)
        
        dim_note = None
        if name in footnotes:
            dim_note = footnotes.get(name)
        
        table_vars["hide_footer"] = False if dim_note else True
        table_vars["footer"] = f"{dim_note.strip()}" if dim_note else "No additional info."

        viz_layout = json.loads(viz_layout)
        rendered_layout = wire_layout(viz_layout, {**general_vars, **table_vars})
        viz_list.append(SkillVisualization(title=name, layout=rendered_layout))

    return viz_list, insights, max_response_prompt, export_data

