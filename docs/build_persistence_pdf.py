"""Build the concise persistence guide from its Markdown source."""
import html
from pathlib import Path
import re

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Preformatted, Spacer, Table, TableStyle
from pypdf import PdfReader

base=Path(__file__).parent
source=base/'HA_Lab_Data_Persistence.md'
destination=base/'HA_Lab_Data_Persistence.pdf'
text=source.read_text(encoding='utf-8')
body=ParagraphStyle('Body',fontName='Helvetica',fontSize=10,leading=13.2,spaceAfter=6,textColor=colors.HexColor('#233047'))
title=ParagraphStyle('Title',parent=body,fontName='Helvetica-Bold',fontSize=19,leading=23,spaceAfter=10,textColor=colors.HexColor('#123e60'))
heading=ParagraphStyle('Heading',parent=body,fontName='Helvetica-Bold',fontSize=12,leading=16,spaceBefore=8,spaceAfter=5,keepWithNext=True,textColor=colors.HexColor('#123e60'))
code=ParagraphStyle('Code',fontName='Courier',fontSize=7.8,leading=10,spaceBefore=3,spaceAfter=9,backColor=colors.HexColor('#f0f5fa'),borderPadding=7)
cell=ParagraphStyle('Cell',parent=body,fontSize=9,leading=12,spaceAfter=0)
header=ParagraphStyle('Header',parent=cell,fontName='Helvetica-Bold',textColor=colors.white)


def inline(s):
    s=html.escape(s)
    s=re.sub(r'`([^`]+)`',lambda m:'<font name="Courier">'+m.group(1)+'</font>',s)
    return re.sub(r'\*\*(.+?)\*\*',r'<b>\1</b>',s)


story=[]
lines=text.splitlines()
i=0
while i<len(lines):
    line=lines[i]
    if not line.strip():
        i+=1
        continue
    if line.startswith('```'):
        i+=1
        block=[]
        while i<len(lines) and not lines[i].startswith('```'):
            block.append(lines[i]);i+=1
        story.append(Preformatted('\n'.join(block),code))
        i+=1
        continue
    if line.startswith('|'):
        rows=[]
        while i<len(lines) and lines[i].startswith('|'):
            parts=[p.strip() for p in lines[i].strip('|').split('|')]
            if not all(re.fullmatch(r'[:\- ]+',p or '-') for p in parts):
                style=header if not rows else cell
                rows.append([Paragraph(inline(p),style) for p in parts])
            i+=1
        table=Table(rows,colWidths=[137,A4[0]-72-137],repeatRows=1,hAlign='LEFT')
        table.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,0),colors.HexColor('#123e60')),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#f0f5fa'),colors.white]),
            ('VALIGN',(0,0),(-1,-1),'TOP'),('BOX',(0,0),(-1,-1),.4,colors.HexColor('#ccd7e2')),
            ('INNERGRID',(0,0),(-1,-1),.3,colors.HexColor('#dce4ed')),
            ('LEFTPADDING',(0,0),(-1,-1),7),('RIGHTPADDING',(0,0),(-1,-1),7),
            ('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6)]))
        story.extend([table,Spacer(1,8)])
        continue
    if line.startswith('## '):
        story.append(Paragraph(inline(line[3:]),heading));i+=1;continue
    if line.startswith('# '):
        story.append(Paragraph(inline(line[2:]),title));i+=1;continue
    paragraph=[line]
    i+=1
    while i<len(lines) and lines[i].strip() and not lines[i].startswith(('#','|','```')):
        paragraph.append(lines[i]);i+=1
    story.append(Paragraph(inline(' '.join(paragraph)),body))


def footer(canvas,doc):
    canvas.setStrokeColor(colors.HexColor('#ccd7e2'))
    canvas.line(36,30,A4[0]-36,30)
    canvas.setFont('Helvetica',8)
    canvas.setFillColor(colors.HexColor('#64748b'))
    canvas.drawString(36,18,'HA Trading Lab | Data persistence and recovery | 27 October 2026')
    canvas.drawRightString(A4[0]-36,18,str(doc.page))


document=SimpleDocTemplate(str(destination),pagesize=A4,leftMargin=36,rightMargin=36,topMargin=30,bottomMargin=42,
    title='HA Trading Lab: Data Persistence and Recovery',author='HA Trading Lab')
document.build(story,onFirstPage=footer,onLaterPages=footer)
pdf=PdfReader(destination)
print(f'Created {destination.name}: {len(pdf.pages)} pages, {len(text.split())} source words')
assert 2<=len(pdf.pages)<=4
contents='\n'.join(p.extract_text() or '' for p in pdf.pages)
for term in ('Outbox relay','Kafka','RabbitMQ','synchronous_mode_strict','at least once','memory-only'):
    assert term in contents,term
print('Verified all key sections are present in PDF text')
