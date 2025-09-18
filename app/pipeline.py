import fitz  # PyMuPDF
import json
from PIL import Image
from openai import OpenAI
import base64
from io import BytesIO
import os 
import json
import re
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)


# ========== CONFIGURATION ==========
os.environ["OPENAI_API_KEY"] =  os.getenv("OPENAI_API_KEY")
client = OpenAI()

# --------------------------
# Helper: Convert PDF to first page image
# --------------------------
def pdf_to_image(pdf_path):
    doc = fitz.open(pdf_path)
    page = doc.load_page(0)
    pix = page.get_pixmap(dpi=300)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    return img


# --------------------------
# Helper: Convert Image to base64
# --------------------------
def image_to_base64(pil_img):
    buffered = BytesIO()
    pil_img.save(buffered, format="PNG")
    return base64.b64encode(buffered.getvalue()).decode()



def extract_json_block(llm_response: str) -> dict:
    """
    Extract the JSON object from a string that may include ```json blocks or extra text.
    """
    # Use regex to find JSON block inside triple backticks
    match = re.search(r"```json\s*(\{.*?\})\s*```", llm_response, re.DOTALL)
    if not match:
        # If no backticks, try to find the first {...} block
        match = re.search(r"(\{.*?\})", llm_response, re.DOTALL)
    
    if match:
        json_str = match.group(1)
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            raise ValueError("Extracted text is not valid JSON.")
    else:
        raise ValueError("No JSON object found in the response.")
    

# --------------------------
# Step 1: OCR + Extract JSON via OpenAI Vision
# --------------------------
def extract_json_from_pdf(pdf_path, keys):
    img = pdf_to_image(pdf_path)
    b64 = image_to_base64(img)

    prompt = f"""
Extract the following fields from the document and return as JSON:
{keys}
Also for each key, give confidence score as 'key'_confident_score. Explaining how confident are you about this match. Ranging between 0-1
Only return JSON. No explanation.
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {
                        "url": f"data:image/png;base64,{b64}",
                        "detail": "high"
                    }},
                ]
            }
        ],
        max_tokens=500,
        temperature=0
    )
    
    try:
        json_block = extract_json_block(response.choices[0].message.content)
        return json_block
    except Exception:
        return {"error": "Failed to parse JSON from response"}
    
# --------------------------
# Step 2: Compare PDFs Visually
# --------------------------
def compare_pdf_format_with_llm(reference_pdf_path, user_pdf_path, reference_json_keys):
    img_ref = pdf_to_image(reference_pdf_path)
    b64_ref = image_to_base64(img_ref)

    img_user = pdf_to_image(user_pdf_path)
    b64_user = image_to_base64(img_user)


    prompt = f"""
        You are given two images of documents. Your task is to decide if they follow the same general **layout and formatting style**.

        Please make a soft comparison — do NOT be strict.

        Focus on:
        - General background layout and visual style
        - Rough positioning of key elements like photo, signature, and key fields (e.g., Name, DOB, DL number, etc.)
        - Overall document structure and form style (like an ID, license, certificate, etc.)

        Ignore:
        - Exact text content or field values
        - Small shifts in alignment, spacing, font, or color
        - Presence or absence of some optional fields
        - Differences in handwriting, personal details, or signatures

        The goal is to determine if these two documents could be considered the **same type of form** (i.e., visually from the same template family).


        Respond only in this JSON format:
        {{
        - "match": true or false
        - "reason": short explanation with which field is misplaced. 
        - "confidance_score": A decimal number, representing a match score
        }}
        If False, validate again and return
        """
    print("Prompt for pdf comparision: ",prompt)
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64_ref}", "detail": "high"}},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64_user}", "detail": "high"}},
            ]}
        ],
        max_tokens=100,
        temperature=1,
    )

    try:
        json_block = extract_json_block(response.choices[0].message.content)
        return json_block
    except Exception:
        return {"error": "Failed to parse JSON from response"}


# --------------------------
# Step 3: Compare OCR JSON vs User Provided JSON
# --------------------------
def compare_jsons(extracted, provided): 
    result = {}
    # keys_list = list(set(extracted.keys()).union(provided.keys()))
    for key in provided:
        extracted_val = str(extracted.get(key, "")).strip().lower()
        extracted_confident_score = str(extracted.get(f"{key}_confident_score", "No score")).strip().lower()
        provided_val = str(provided.get(key, "")).strip().lower()

        result[key] = {
            "match": extracted_val == provided_val,
            "extracted": extracted_val,
            "extracted_confident_score": extracted_confident_score,
            "provided": provided_val
        }
    return result

# --------------------------
# Wrapper Pipeline
# --------------------------
def run_pipeline(reference_json_keys, reference_pdf_path, user_pdf_path, user_provided_json):
    print("Extracting fields from user PDF using GPT-4o-mini Vision...")
    extracted_json = extract_json_from_pdf(user_pdf_path, reference_json_keys)
    print("OCR output: ",extracted_json)

    print("Comparing reference and user PDFs (visual match)...")
    pdf_match_result = compare_pdf_format_with_llm(reference_pdf_path, user_pdf_path, reference_json_keys)

    print("Comparing extracted JSON with user-provided JSON...")
    json_comparison = compare_jsons(extracted_json, user_provided_json)

    return {
        "extracted_json": extracted_json,
        "pdf_match": pdf_match_result,
        "json_match": json_comparison
    }