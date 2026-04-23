import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import shutil
import json
import pandas
from datetime import datetime
from pptx import Presentation
from pptx.util import Pt
from PIL import Image
from pptx.util import Inches
from lxml import etree
from core.config_dates import config_dates
from core.get_funnel_data import get_funnel_data
from core.get_run_rate import get_run_rate
from core.generate_commentary import generate_monthly_slide_content
from monthly_reports.generate_visualisation import render_graph, initialise_brand, BRAND

initialise_brand()

# Initialise the names of the slide layouts
SLD_LAYOUT_COVER = 0
SLD_LAYOUT_SECTION_SEPARATOR = 1
SLD_LAYOUT_SUNSET_SECTION_SEPARATOR = 2
SLD_LAYOUT_BURNT_ORANGE_SECTION_SEPARATOR = 3
SLD_LAYOUT_QUOTATION = 4
SLD_LAYOUT_TITLE_AND_BODY = 5
SLD_LAYOUT_TITLE_AND_BODY_1 = 6
SLD_LAYOUT_TITLE_AND_BODY_2 = 7
SLD_LAYOUT_TITLE_AND_BODY_3 = 8
SLD_LAYOUT_2_COLUMN = 9
SLD_LAYOUT_3_COLUMN = 10
SLD_LAYOUT_4_COLUMN = 11
SLD_LAYOUT_BIG_NUMBER = 12
SLD_LAYOUT_BLANK = 13



def generate_ppt():
    # Init the template
    shutil.copy('slides/template.pptx', 'slides/test.pptx')
    prs = Presentation('slides/test.pptx')


    # Get Clients
    with open("storage/config.json", "r") as config_json:
            clients = json.load(config_json)
    for client in clients:
        if client['name'] != 'Paintnuts':
            continue
        # client = config_dates(client)
        # if client["plan"] != "":
        #     with open("storage/plans.json", "r") as plans_json:
        #         plans = json.load(plans_json)
        #     client["plan_json"] = plans[client["name"]]
        # if client['account_type'] == 'Lead Gen':
        #     client['paid_data'] = get_funnel_data(client, 'paid_lead_gen')
        #     client['llm_data'] = get_funnel_data(client, 'llm_lead_gen')
        #     client['timeseries_data'] = get_funnel_data(client, 'time_series_lead_gen')
        #     client['overall_data'] = get_funnel_data(client, 'overall_lead_gen')
        # if client['account_type'] == 'Ecommerce':
        #     client['paid_data'] = get_funnel_data(client, 'paid_ecommerce')
        #     client['llm_data'] = get_funnel_data(client, 'llm_ecommerce')
        #     client['timeseries_data'] = get_funnel_data(client, 'time_series_ecommerce')
        #     client['overall_data'] = get_funnel_data(client, 'overall_ecommerce')
        # client['run_rate'] = get_run_rate(client)
        # client['slide_content'] = generate_monthly_slide_content(client)
        # with open('storage/monthly_content.json', 'w', encoding='utf-8') as f:
        #     json.dump(client['slide_content'], f, ensure_ascii=False, indent=4)

        with open('storage/monthly_content.json', 'r', encoding='utf-8') as f:
            client['slide_content'] = json.load(f)
        # Remove the single placeholder slide
        slide_rid = prs.slides._sldIdLst[0].get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
        prs.part.drop_rel(slide_rid)
        prs.slides._sldIdLst.remove(prs.slides._sldIdLst[0])

        # Title Slides
        slide = prs.slides.add_slide(prs.slide_layouts[SLD_LAYOUT_COVER])
        slide.placeholders[0].text = f'{client['name']} Monthly Deck'

        # Paid Separator Slide
        slide = prs.slides.add_slide(prs.slide_layouts[SLD_LAYOUT_SECTION_SEPARATOR])
        slide.placeholders[0].text = 'Paid Media'


# Overall Summary Slide
        slide = prs.slides.add_slide(prs.slide_layouts[SLD_LAYOUT_SUNSET_SECTION_SEPARATOR])
        slide.placeholders[0].text = 'Performance Overview'
        slide = prs.slides.add_slide(prs.slide_layouts[SLD_LAYOUT_TITLE_AND_BODY])
        slide.placeholders[0].text = 'Top Level View'
        tf = slide.placeholders[4].text_frame
        tf.text = client['slide_content']['overview']['summary']
        for p in tf.paragraphs:
            for run in p.runs:
                run.font.size = Pt(14)
        tf = slide.placeholders[1].text_frame
        tf.clear()

        for i, bullet in enumerate(client['slide_content']['overview']['bullets']):
            if i == 0:
                p = tf.paragraphs[0]
            else:
                p = tf.add_paragraph()
            p.text = bullet['point']
            p.level = 0
        for p in tf.paragraphs:
            for run in p.runs:
                run.font.size = Pt(12)
        # chart_path = render_graph(client,)
        # add_chart_to_slide(slide, chart_path)

        # Trends
        slide = prs.slides.add_slide(prs.slide_layouts[SLD_LAYOUT_SUNSET_SECTION_SEPARATOR])
        slide.placeholders[0].text = 'Top Level Trends'
        for trend in client['slide_content']['trends']:
            slide = prs.slides.add_slide(prs.slide_layouts[SLD_LAYOUT_TITLE_AND_BODY])
            slide.placeholders[0].text = trend['title']
            slide.placeholders[2].text = f"{trend['graph']['date_range']['start']} to {trend['graph']['date_range']['end']}"
            tf = slide.placeholders[4].text_frame
            tf.text = trend['summary']
            for p in tf.paragraphs:
                for run in p.runs:
                    run.font.size = Pt(14)
            tf = slide.placeholders[1].text_frame
            tf.clear()
            for i, bullet in enumerate(trend['bullets']):
                if i == 0:
                    p = tf.paragraphs[0]
                else:
                    p = tf.add_paragraph()
                p.text = bullet['point']
                p.level = 0
            for p in tf.paragraphs:
                for run in p.runs:
                    run.font.size = Pt(12)
            chart_path = render_graph(client, trend['graph'])
            add_chart_to_slide(slide, chart_path)
        # Actions
        slide = prs.slides.add_slide(prs.slide_layouts[SLD_LAYOUT_SUNSET_SECTION_SEPARATOR])
        # current_month = client['start_date'].normalize().strftime("%B")
        slide.placeholders[0].text = f'March Actions'
        for action in client['slide_content']['actions']:
            slide = prs.slides.add_slide(prs.slide_layouts[SLD_LAYOUT_TITLE_AND_BODY])
            slide.placeholders[0].text = action['task']
            slide.placeholders[1].text = action['summary']
            tf = slide.placeholders[4].text_frame
            tf.text = action['status']
            for p in tf.paragraphs:
                for run in p.runs:
                    run.font.size = Pt(14)

            if action['graph'] is not None:
                slide.placeholders[2].text = f"{action['graph']['date_range']['start']} to {action['graph']['date_range']['end']}"
                chart_path = render_graph(client, action['graph'])
                add_chart_to_slide(slide, chart_path)

        prs.save('slides/test.pptx')

        print(f"Saved with {len(prs.slides)} slide(s)")


def add_chart_to_slide(slide, chart_path, left=Inches(5), top=Inches(1.5), width=Inches(4.5)):
    """
    Adds a chart image to a slide preserving its aspect ratio.
    
    Args:
        slide:       the pptx slide object to add the image to
        chart_path:  path to the chart image file
        left:        distance from left edge of slide (default 5 inches)
        top:         distance from top of slide (default 1.5 inches)
        width:       desired width of the chart (default 4.5 inches)
    """
    with Image.open(chart_path) as img:
        img_width, img_height = img.size
        aspect_ratio = img_height / img_width

    height = int(width * aspect_ratio)

    slide.shapes.add_picture(
        chart_path,
        left=left,
        top=top,
        width=width,
        height=height
    )

generate_ppt()