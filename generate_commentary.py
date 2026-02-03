import json
from openai import OpenAI

with open("storage/secrets.json", "r") as f:
    secrets = json.load(f)
oai = OpenAI(api_key=secrets["openai_key"])


def generate_commentary(client):
    payload = {
        "inputs": {
            "plans_90_day": client['plan_json'],
            "performance": client['funnel_data'],
            "ga4_context": client['site_context'],
            "report_start_date": client['start_date_string'],
            "report_end_date": client['end_date_string'],
            "monthly_budget": client['budget'],
            "comparison_period": client['report_dates'],
            "client_context": client['client_context']
        }
        }


    schema = {
        "name": "weekly_marketing_commentary",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "plan_overview": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "tasks": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "task": {"type": "string"},
                                    "description": {"type": "string"},
                                    "status": {"type": "string"},
                                    "start_date": {"type": "string"},
                                    "end_date": {"type": "string"},
                                    "summary": {"type": "string"},
                                },
                                "required": ["task", "description", "status", "start_date", "end_date", "summary"],
                            },
                        }
                    },
                    "required": ["tasks"],
                },
                "performance_overview": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "summary": {"type": "string"},
                    },
                    "required": ["summary"],
                },
                "performance_points": {
                    "type": "array",
                    "minItems": 3,
                    "maxItems": 3,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "title": {"type": "string"},
                            "summary": {"type": "string"},
                        },
                        "required": ["title", "summary"],
                    },
                },
            },
            "required": ["plan_overview", "performance_overview", "performance_points"],
        },
        "strict": True,
    }



    response = oai.responses.create(
        model="gpt-4o-mini",
        instructions=(
            "You are a senior performance marketing manager writing weekly client-facing commentary.\n"
            "We want critical commentary, but when expanding, we don't want scathing commentary. We want optimistic and plan aligned explainers\n"
            "Write in British English. Be direct, but human.\n"
            "Use ONLY the provided data. If you don’t have enough evidence to claim something, say so and put it in risks_and_watchouts. "
            "The report must respect the provided report_start_date and report_end_date; comparisons can reference prior, especially if we can link performance to actions made previously, but the focus is the current reporting period.\n"
            "The client_context variable can be used to get some more information on what the client is, what their offering is and previous trends that the exec on the account has noticed, don't lean on it for every point but for overall wide arching points it can be useful to reference.\n"
            "When using data from 'ga4_context', only use it for context when analysing performance data, it should never be a point on its own, we are reporting on paid media performance here, not site wide performance\n"
            "STYLE REQUIREMENTS:\n"
            "- Write paragraphs (human readable) for the summaries, not bullet lists.\n"
            "- Evidence must be specific numbers from the performance / ga4_context inputs (e.g., revenue, ROAS, CPA, CR, AOV, spend).\n"
            "- When comparing two data points from different time periods, ensure that the right comparison is used, the comparison can be found in 'comparison_period', if it is 'yoy' then we are comparing year on year, if it is 'mom' then we are comparing month on month\n"
            "- Avoid using dates when referring to periods, use 'current period' and if comparing to the previous period use 'previous month' or 'previous year' (depending on the 'comparison_period' field,if it is 'yoy' then we are comparing year on year, if it is 'mom' then we are comparing month on month) \n"
            "- Explicitly reference the 90-day plan where relevant: if an initiative from plans_90_day plausibly links to a performance movement, call it out and explain the linkage. If you can’t find a relevant plan item, then don't reference it.\n"
            "- For each performance point, include a 2-3 sentence summary that combines all the points, this will be the part that gets sent to the client.\n"
            "- For acronyms (e.g. ROAS, TBD) style in all caps, make sure this is only for acronyms, not all metric names\n"
            "- when referencing dates, don't use iso format, use brtish standard date format (dd/mm/yyyy)\n"
        ),
        input=[
            {
                "role": "user",
                "content": (
                    "Return JSON that matches the schema exactly.\n\n"
                    "Deliverables:\n"
                        "1) plan_overview\n"
                            "This is a direct reference to the 90 day plans which have been sent over through the plans_90_day json. Use this to create the following for EACH TASK that is attached in the JSON, irregardless of what category it falls under.:\n"
                                "task, description, status: These are all just the corresponding values that have been sent over in the JSON\n"
                                "start_date, end_date: convert these from iso format into the british standard date format (dd/mm/yyyy)\n"
                                "summary: this is a more basic client friendly version of teh description, make it a one sentence summary that is to the point and not wrapped in marketing fluff\n"
                        "2) performance_overview:\n"
                            "- A sentence summary of overall performance for the current period vs the previous period, incorporating GA4 context (CR/AOV where available). Include a sentence on how budget / run rate is on track and whether we need to pull back or push on spend\n"
                        "3) performance_points: \n"
                            "- Exactly 3 points with a headline, a description of the point and evidence (only include evidence if there is an outstanding figure that requires reporting, it is not necessary for a point to have data, just allude to the metric increasing / decreasing (e.g. impressions are up.)). The can be either good or bad, we want to highlight the ones with the most impact. If there are any ways to tie back into the previous actions from the actions from past initiatives then do this \n"
                    "Input JSON:\n"
                    + json.dumps(payload, ensure_ascii=False)
                ),
            }
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": schema["name"],
                "schema": schema["schema"],
                "strict": schema["strict"],
            }
        },
    )
    return json.loads(response.output_text)
