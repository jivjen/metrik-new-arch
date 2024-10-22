from openai import OpenAI
from tavily import TavilyClient
from typing import List, Optional, Callable, Dict, Tuple
from pydantic import BaseModel, Field
from googleapiclient.discovery import build
import google.generativeai as genai
import requests
import fitz
import json
import typing_extensions as typing
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID")

google_search = build("customsearch", "v1", developerKey=GOOGLE_API_KEY).cse()

genai.configure(api_key=os.getenv("GOOGLE_GEMINI_API_KEY"))
model = genai.GenerativeModel('gemini-1.5-pro-latest')

openai = OpenAI(api_key="sk-SDZjr6f9IFZ7ZL6gEiduD_8n34LSV_xZHd6HkKxj5fT3BlbkFJiuDxYN1KT2lZoz-zayJbTibSkfyj2COHcDvxlMuz4A")

os.environ["OPENAI_API_KEY"] = "sk-SDZjr6f9IFZ7ZL6gEiduD_8n34LSV_xZHd6HkKxj5fT3BlbkFJiuDxYN1KT2lZoz-zayJbTibSkfyj2COHcDvxlMuz4A"


def generate_table(user_input: str):
    table_generator_system_prompt = """
    Role: You are an expert researcher and critical thinker.
    Task: Your task is to analyze the user's input and create a hypothetical table that would contain all the required information from the user query.

    Instructions:
    1. Create a hypothetical table from the user query, such that all the information asked in the user query are blank cells.
    2. The table must be constructed so that the individual cells can be answered with a single word, number, or at most a short sentence.
    3. Ensure that rows, columns, and blank cells are NOT created for any extra information that IS NOT asked for in the user input
    4. Make the row and column headers descriptive enough that they contain sufficient information for a search agent to instantly query the needed information from the internet.
    5. Output the table in Markdown format ONLY.
    6. ONLY If the number of rows or columns are not available in the user input, keep it as MAX = 5
    """

    table_generator_user_content = f"Analyze and generate a table for the following user input: {user_input}"

    class TableGeneration(BaseModel):
        table: str = Field(description="Markdown formatted table")

    table_generator_response = openai.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": table_generator_system_prompt},
            {"role": "user", "content": table_generator_user_content}
        ],
        response_format=TableGeneration
    )

    with open("table.md", "w") as f:
        f.write(table_generator_response.choices[0].message.parsed.table)

    return table_generator_response.choices[0].message.parsed.table

def generate_sub_questions(user_input, table):
    class SubQuestion(BaseModel):
        question: str = Field(description="The sub-question")

    sub_question_generator_system_prompt = """
    Role: You are an expert researcher and critical thinker.
    Task: Your task is to analyze the given table and create sub-questions that will help gather the information needed to fill the empty cells in the table.

    Instructions:
    0. If the row headers or column headers are missing, your first sub question must be a query that provides for the missing header. For this you will use the user prompt for reference to create the row.
    1. ONLY after both row headers and column headers are available for each cell, proceed to the next step.
    2. For each EMPTY cell in the table, create a standalone query which will provide the answer for that cell. This query must be such that a simple search query of the question should produce the answer.
    3. If any sub-question reference information from another cell, ALWAYS use the cell's position (e.g., A1, B2) as a placeholder instead of plain english placeholders.
    4. Ensure all sub-questions are unique and specific to each empty cell.
    5. Output a list of sub-questions, each corresponding to a specific empty cell in the table.
    6. The subquestions are processed linearly, so if ANY subquestion is answered by a previous answer, remove it.
    7. If ALL cells in the table are already filled, return an empty list of questions.
    """

    sub_question_generator_user_content = f"""
    Analyze the following table and generate sub-questions:

    {table}

    Please generate sub-questions only for the empty cells in the table. Cells with content are already filled and should be skipped.
    Here is the user input needed whenever the prompt asks for it: {user_input}
    """

    class SubQuestionGeneration(BaseModel):
        questions: List[SubQuestion] = Field(description="List of generated sub-questions")

    sub_questions_response = openai.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": sub_question_generator_system_prompt},
            {"role": "user", "content": sub_question_generator_user_content}
        ],
        response_format=SubQuestionGeneration
    )

    print(f"SUB QUESTIONS: \n")
    sub_questions = []
    sq = sub_questions_response.choices[0].message.parsed.questions
    for q in sq:
      sub_questions.append(q.question)
      print(f"QUESTION: \n {q.question}")

    return sub_questions

CELLS_FILLED_CHECKER_SYSTEM_PROMPT = """
You will be provided with the content of a Markdown table. Your task is to analyze this table and determine if all cells are filled or if there are any empty cells.

Instructions:
1. Carefully examine the provided Markdown table.
2. Check each cell in the table for content.
3. Ignore any leading or trailing whitespace in cells.
4. Consider a cell empty if it contains only whitespace or is completely blank.
5. Markdown table separators (rows of dashes) should be ignored and not considered as part of the data.
6. Use A1 notation to refer to cells, where letters represent columns and numbers represent rows. The first row of actual data (not counting the header) is considered row 1, and the leftmost column is column A.

Provide your response in the following format according to the input schema:
{
  "allCellsFilled": string,
  "emptyCells": [string]
}

Where:
- "allCellsFilled" is "yes" if all cells contain content, "no" otherwise.
- "emptyCells" is an array of strings, each representing the A1 notation of an empty cell. This should be an empty array if all cells are filled.

Example input:
| Header 1 | Header 2 | Header 3 |
|----------|----------|----------|
| Data 1   | Data 2   |          |
| Data 3   |          | Data 4   |

Example output:
{
  "allCellsFilled": "no",
  "emptyCells": ["C1", "B2"]
}

Now, analyze the provided Markdown table and report on any empty cells using this format.
"""

class CellCheckerResponse(BaseModel):
    allCellsFilled: str = Field(description="Status whether all cells are filled or not")
    emptyCells: List[str] = Field(description="List of empty cells")

def check_if_all_cells_are_filled(job_id: str):
  table = ""
  with open(f"jobs/{job_id}/table.md", "r") as f:
    table = f.read()
  CELLS_FILLED_CHECKER_USER_PROMPT = f"""
  Input Markdown Table:
  {table}
  """

  cell_checker_response = openai.beta.chat.completions.parse(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": CELLS_FILLED_CHECKER_SYSTEM_PROMPT},
        {"role": "user", "content": CELLS_FILLED_CHECKER_USER_PROMPT}
    ],
    response_format=CellCheckerResponse
  )

  return cell_checker_response.choices[0].message.parsed.allCellsFilled.lower() == "yes"


def generate_keywords(user_input: str, sub_question: str) -> List[str]:
    keyword_generator_system_prompt = """
        Role: You are a professional Google search researcher.
        Task: Given a main user query for context and a specific sub-question, your primary task is to generate 5 unique Google search keywords that will help gather detailed information primarily related to the sub-question.

        Instructions:
        1. Focus ONLY on the sub-question when generating keywords. The main query serves merely as context but should not dominate the keyword selection.
        2. Ensure that at least 4 out of 5 keywords are directly relevant to the sub-question.
        3. You may use 1 keyword to bridge the sub-question with the broader context of the main query if relevant.
        4. Generate keywords that aim to concisely answer the sub-question, including but not limited to: specific details, expert opinions, case studies, recent developments, and historical context (if any are applicable).
        5. Aim for a mix of broad and specific keywords related to the sub-question to ensure comprehensive coverage.
        6. Ensure all keywords are unique

        Main Aim of Creating Keywords for Search Engines: To ensure that any piece of information present on the internet pertinent to answering the sub-question is always found.
    """

    class KeywordGeneration(BaseModel):
        keywords: List[str] = Field(description="List of generated keywords")

    keyword_generator_user_prompt = f"Main query (for context): {user_input}\nSub-question (primary focus): {sub_question}\nPlease generate keywords primarily addressing the sub-question, while considering the main query as context."

    keyword_generator_response = openai.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": keyword_generator_system_prompt},
            {"role": "user", "content": keyword_generator_user_prompt}
        ],
        response_format=KeywordGeneration
    )

    return keyword_generator_response.choices[0].message.parsed.keywords

def search_web(search_term):
    """Search the Web and obtain a list of web results."""
    google_search_result = google_search.list(q=search_term, cx=GOOGLE_CSE_ID).execute()
    urls = []
    search_chunk = {}
    for result in google_search_result["items"]:
        urls.append(result["link"])
    for url in urls:
        search_url = f'https://r.jina.ai/{url}'
        headers = {
            "Authorization": f"Bearer {os.getenv('JINA_API_KEY')}"
        }
        response = requests.get(search_url, headers=headers)
        if response.status_code == 200:
            print("Successfully converted")
            search_chunk[url] = response.text
        else:
            print(f"Jina returned an error: {response.status_code}")
            continue
    return json.dumps(search_chunk)

def analyze_search_results(search_results: Dict[str, str], markdown_table: str, sub_question: str) -> Dict[str, str]:
    search_analyser_system_prompt = """
    Role: You are an expert AI assistant specialized in analyzing search results and extracting precise information.
    Task: Given a specific sub-question, a markdown table for context, and a set of search results, your primary task is to determine if the answer to the sub-question can be found within the provided information.

    Instructions:
    1. Carefully analyze the content of each search result, focusing on finding information that directly answers the sub-question.
    2. Pay attention to the markdown table, as it may provide additional context for interpreting the search results.
    3. If you find the answer:
       a. Respond with 'yes' for subQuestionAnswered.
       b. Provide a concise, accurate answer based on the information found.
       c. Include the exact URL of the source where the answer was found in brackets along with the answer.
    4. If you cannot find the answer:
       a. Respond with 'no' for subQuestionAnswered.
       b. Leave the result empty.
    5. Ensure that your response is based solely on the information provided in the search results and markdown table.
    6. Do not make assumptions or provide information that is not explicitly stated in the given data.

    Main Aim: To provide accurate, source-backed answers to sub-questions when the information is available, and to clearly indicate when the required information cannot be found in the given search results.
    """

    search_analyser_user_prompt = f"""
    Sub-question: {sub_question}

    Markdown Table:
    {markdown_table}

    Search Results:
    {search_result}

    Please analyze the search results and determine if the answer to the sub-question can be found.
    """

    class AnalysisResult(BaseModel):
        subQuestionAnswered: str = Field(description="'yes' if the sub-question is answered, 'no' otherwise")
        result: str = Field(description="The answer with the source in brackets if found, or an empty string if not found")

    analysis_response = openai.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": search_analyser_system_prompt},
            {"role": "user", "content": search_analyser_user_prompt}
        ],
        response_format=AnalysisResult
    )

    result = analysis_response.choices[0].message.parsed

    if result.subQuestionAnswered == "yes":
        print(f"Sub-question answered: {result.subQuestionAnswered}")
        print(f"Result: {result.result}")

    return {
        "subQuestionAnswered": result.subQuestionAnswered,
        "result": result.result
    }

def update_markdown_table(markdown_table: str, sub_question: str, answer: str) -> str:
    system_prompt = """
    Role: You are an AI assistant specialized in updating markdown tables with precise information and source references.
    Task: Given a markdown table, a specific sub-question, and an answer (which may include source references), your task is to update the markdown table by filling the appropriate cell(s) with the provided answer and its source.

    Instructions:
    1. Analyze the structure of the given markdown table.
    2. Identify the row where the sub-question belongs.
    3. Determine if the answer should be split across multiple cells:
      - If the answer is a list, contains multiple distinct items, or is separated by commas, ALWAYS split it into separate cells.
      - Each item should occupy its own cell, even if this means adding new columns to the table.
      - Never place multiple items in a single cell, even if they belong to the same category.
    4. For each value in the answer:
      - If a source reference is provided, format the cell content as: "value [source_url]"
      - If no source is provided, just include the value.
    5. Replace the content of the identified cell(s) with the provided answer and sources, ensuring each item is in its own cell.
    6. If new columns need to be added to accommodate the split answer, add them while maintaining the table structure.
    7. Ensure that the markdown table structure remains intact and properly formatted.
    8. If the sub-question doesn't match any existing row, do not modify the table.
    9. Preserve all other information in the table that is not related to the sub-question.
    10. Return the entire updated markdown table as a string.

    Main Aim: To accurately update the markdown table with the new information, ALWAYS splitting answers with multiple items across separate cells, including source references where provided, while maintaining its structure and existing content.

    Examples:
    1. Multiple items and source references:
      If given the sub-question "Energy drink market data" and the answer "Red Bull: 43% market share [https://example.com/redbull], Monster: 38% market share [https://example.com/monster], Industry growth rate: 15% annually [https://example.com/growth]", the table should be updated like this:

      | Energy Drink Brand | Market Share (%)                    | Industry Growth Rate (%)            |
      |--------------------|-------------------------------------|-------------------------------------|
      | Red Bull           | 43% [https://example.com/redbull]   |                                     |
      | Monster            | 38% [https://example.com/monster]   |                                     |
      |                    |                                     | 15% [https://example.com/growth]    |

    2. Splitting multiple items in a category:
      If given the sub-question "Leading energy drink brands" and the answer "Red Bull, Monster, Rockstar, Reign", the table should be updated like this:

      | Energy Drink Brand | Market Share (%) | Growth Rate (%) |
      |--------------------|------------------|-----------------|
      | Red Bull           |                  |                 |
      | Monster            |                  |                 |
      | Rockstar           |                  |                 |
      | Reign              |                  |                 |

      NOT like this:

      | Energy Drink Brand                   | Market Share (%) | Growth Rate (%) |
      |--------------------------------------|------------------|-----------------|
      | Red Bull, Monster, Rockstar, Reign   |                  |                 |

    Always ensure each item is in its own cell, adding columns if necessary, and include source references directly next to the values when provided. Never combine multiple items into a single cell, even if they belong to the same category.
    """

    user_prompt = f"""
    Markdown Table:
    {markdown_table}

    Sub-question: {sub_question}
    Answer: {answer}

    Please update the markdown table by filling the appropriate cell(s) with the provided answer. If the answer contains multiple items (like a list), split them into separate cells.
    """

    class UpdatedTable(BaseModel):
        updated_table: str = Field(description="The updated markdown table as a string")

    update_response = openai.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        response_format=UpdatedTable
    )

    return update_response.choices[0].message.parsed.updated_table

import uuid
import os
import threading
import logging
from logging.handlers import RotatingFileHandler

# Global dictionary to store job status
job_status = {}

def setup_logger(job_id):
    logger = logging.getLogger(f"job_{job_id}")
    logger.setLevel(logging.INFO)
    os.makedirs("logs", exist_ok=True)
    file_handler = RotatingFileHandler(f"logs/job_{job_id}.log", maxBytes=10*1024*1024, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

def process_research(user_input: str, job_id: str):
    logger = setup_logger(job_id)
    logger.info(f"Continuing research job with ID: {job_id}")
    
    job_status[job_id] = "running"
    
    while not check_if_all_cells_are_filled(job_id) and job_status[job_id] == "running":
        logger.info("Starting a new iteration to fill empty cells")
        with open(f"jobs/{job_id}/table.md", "r") as f:
            table = f.read()
        logger.info("Generating sub-questions")
        sub_questions = generate_sub_questions(user_input, table)
        sub_question = sub_questions[0]
        logger.info(f"Selected sub-question: {sub_question}")
        logger.info("Generating keywords")
        keywords = generate_keywords(user_input, sub_question)
        logger.info(f"Generated keywords: {keywords}")
        for keyword in keywords:
            if job_status[job_id] != "running":
                logger.info("Job status changed, breaking keyword loop")
                break
            logger.info(f"Searching web for keyword: {keyword}")
            search_result = search_web(keyword)
            logger.info("Analyzing search results")
            analysis_result = analyze_search_results(search_result, table, sub_question)
            if analysis_result["subQuestionAnswered"] == "yes":
                logger.info("Sub-question answered, updating table")
                table = update_markdown_table(table, sub_question, analysis_result["result"])
                with open(f"jobs/{job_id}/table.md", "w") as f:
                    f.write(table)
                logger.info("Table updated and saved")
                break
            else:
                logger.info("Sub-question not answered with this keyword")

    if job_status[job_id] == "running":
        job_status[job_id] = "completed"
        logger.info("Job completed successfully")
    elif job_status[job_id] == "stopping":
        job_status[job_id] = "stopped"
        logger.info("Job stopped by user request")

    return job_id

def get_job_status(job_id: str):
    logger = logging.getLogger(f"job_{job_id}")
    if job_id not in job_status:
        logger.warning(f"Status requested for non-existent job: {job_id}")
        return {"status": "not_found"}
    
    status = job_status[job_id]
    logger.info(f"Status requested for job {job_id}: {status}")
    if status == "completed":
        try:
            with open(f"jobs/{job_id}/table.md", "r") as f:
                table = f.read()
            logger.info(f"Returning completed table for job {job_id}")
            return {"status": status, "table": table}
        except FileNotFoundError:
            logger.error(f"Table file not found for completed job {job_id}")
            return {"status": "not_found"}
    else:
        return {"status": status}

def stop_job(job_id: str):
    logger = logging.getLogger(f"job_{job_id}")
    if job_id in job_status and job_status[job_id] == "running":
        job_status[job_id] = "stopping"
        logger.info(f"Stopping job {job_id}")
        return True
    logger.warning(f"Attempt to stop non-running job {job_id}")
    return False



