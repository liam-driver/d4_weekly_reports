import json
from openai import OpenAI

with open("storage/secrets.json", "r") as f:
    secrets = json.load(f)
oai = OpenAI(api_key=secrets["openai_key"])


def generate_commentary(client):
    payload = {
        "inputs": {
            "plans_90_day": client['plan_json'],
            "paid_data": client['llm_data'],
            "overall_data": client['overall_data'],
            "report_start_date": client['start_date_string'],
            "report_end_date": client['end_date_string'],
            "monthly_budget": client['budget'],
            "run_rate": client['run_rate'],
            "cost_to_date": client['paid_data']['Total']['Cost']['curr'],
            "reporting_period": client['comparison_dates'],
            "client_context": client['client_context'],
            "holistic_plans": client['holistic_plans'],
            "paid_plans": client['paid_plans'],
            "kpis": client['kpis'],
            "seasonality": client['seasonality'],
            "historical_context": client['historical_context']
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
                    "minItems": 4,
                    "maxItems": 10,
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
            "We want critical commentary, but when expanding, we don't want scathing commentary, we want productive commentary that will help us get on track with our KPIs\n"
            "Write in British English. Be direct, but human.\n"
            "The report must respect the provided report_start_date and report_end_date; comparisons can reference prior, especially if we can link performance to actions made previously, but the focus is the current reporting period.\n"
            
            "Attached is a json object called 'inputs' this has attached all the context that will be used for the commentary that has been provided to you, the definition of each variable can be found below"
            "DEFINITION OF VAIRABLES:"
            "- plans_90_day: this is a json object that contains the plans for the current period as well as the plans from previous periods, giving you context on what we have done and what we are planning to do.\n"
            "- paid_data: this is a json object that has all of the ppc data (e.g. Google ads) from the current period, compared to our comparison period. This has the data that we need to translate into insights, so it is the most importnat variable that you should factor in"
            "- overall_data: this is a json object that has the overall site data, taken from GA4, from the current period, compared to our comparison period. This should be used for extra context for paid data so that we can compare it to other channels, like Organic, and lets us review how holistic plans are performing\n"
            "- report_start_date: This is the date where the reporting period begins/\n"
            "- report_end_date: This is the date where the reporting period ends\n"
            "- monthly_budget: this is the monthly budget for the current reporting period\n"
            "- run_rate: this is the current run rate for the period"
            "- cost_to_date: this is the total media spend for the current month"
            "- reporting_period: this states the period of the report and states the data we are using as comparison. It can be one of three things:\n" \
            "   - MTD Yearly Comparison: this means that the data period is month to date, and the comparison is the same date range last year\n"
            "   - MTD Monthly Comparison: this means that the data period is month to date, and the comparison is the same date range last month\n"
            "   - WTD Weekly Comparison: this means that the data period is week to date, is 7 day period prior to the last 7 day period\n"
            "- client_context: this is context on the client and what their offering is"
            "- holistic_plans: these are the goals for the entire website for the current year, we are striving to achieve these goals for our clients"
            "- paid_plans: these are the goals for the ppc channel for the current year, we are striving to achieve these goals for our clients within the context of ppc channels"
            "- kpis: these are the key performance indicators we are using to measure how the effectively we are achieving the goals that we are trying to hit"
            "- seasonality: this is extra context for the account based on how it performs within the context of external factors"
            "- historical_context: this is an overview on the performance on the account over previous years, giving extra context as to why we have made these plans"
            
            "It is essential that the context provided through the 'holistic_plans', 'paid_plans', 'kpis', 'seasonality' and 'historical_context' are used. We are comparing ourselves to ourselves and we want to stay algined to or goals for the year\n"
            "When looking at volume metrics, make sure that we are comparing it to the amount we've spent, if conversions or transaction revenue is down, then we need to account for the cost as the interplay between volume metrics and cost are too important to just reference volume metrics on their own\n"
            "When referring to data in a point, make sure that we are identfying where that data has come from, is it from the paid dataset, or the overall dataset? Furthermore, if we are referencing data from a specific dimension, make sure that is stated in the commentary\n"
            "When using the comparisons, make sure that we are referring to the correct period, so if the comparison period is monthly, we are comparing to 'last month', if the compariosn period is yearly, we are comparing to 'last year', etc.\n"

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
                                "- Ensure that the 'status' of the plan is set to 'current' if we are incorporating it into our plans, we only use the 'old' status plans as context\n"
                                "- Task, description, status: These are all just the corresponding values that have been sent over in the JSON\n"
                                "- Start_date, end_date: convert these from iso format into the british standard date format (dd/mm/yyyy)\n"
                                "- Summary: this is a more basic client friendly version of the description, make it a one sentence summary that is to the point and not wrapped in marketing fluff\n"
                        "2) performance_overview:\n"
                            "- A 2-4 sentence summary of paid_data and overall_data compared to the the 'holistic_plans' and 'paid_plans' how are we doing when it comes to achieving these goals. We want the focus to be on paid points, but frame it within the context of the holistic plans and the overall data we are seeing on the webstie\n"
                            "- When bringing in actual data from paid_data and overall_data, only use data that explicitly aligns with the kpis that have been given to you is the 'kpis' secion.\n"
                            "- You are allowed to use data to back up the statements, but be sparing, this is a quick human readable paaragraph for stakeholders\n"
                            "- Include a sentence on current spend to date. Use cost_to_date for the total spend this month, run_rate to see what the predicted cost at the end of the month, and budget to find the monthly budget for the client.\n"
                        "3) performance_points: \n"
                            "- The title should be outline what point is being made and reference the context of the data, e.g. For Microsoft Ads: Increased Microsoft Ads Impressions, if looking at overall (Overall decrease in Conversion Rate)\n"
                            "- For the summary, outline the problem, show the evidence from the data and assess why this may have happened using all the context provided to you. If there are any potential suggestions provide them\n"
                            "- Try to be concise with the description, the points need to be quite glanceable\n"
                            "- The first 2 points will be related to the 'kpis' provided, you don't need to reference every kpi, it can be whichever you deem most relevant\n"
                            "- The rest of the points will be related to specific ad channels. Look at the each channel in the paid data and output at least 1 point (license to do more than 1 up to 2) for each channel that has been defined, if there are 5 channels, then there needs to be at least 5 channel specific points.If there are any channels that are seeing sharp changes in performance, if any metrics are seeing any sharp shifts when compared to the comparison period, if there are any potential gaps you feel you've identified\n"
                            "- Points can only be made based off the data in the paid_data section, the overall_data can be used as context, but should never be used as a main point"
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