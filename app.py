"""
Anaïs Email Intake Service
Replaces Make.com Flow 1: Email intake, AI extraction, Notion task creation

This service:
1. Receives forwarded emails via webhook
2. Parses email headers and body
3. Computes fingerprint for deduplication
4. Checks Supabase for duplicates
5. Extracts tasks using Claude API (email body) and Gemini API (attachments)
6. Creates tasks in Notion
7. Logs to Supabase audit trail
"""

import os
import re
import json
import hashlib
import logging
import base64
import tempfile
from datetime import datetime, timedelta
from urllib.parse import quote

from flask import Flask, request, jsonify
import anthropic
import requests

# Google Gemini AI
import google.generativeai as genai

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

NOTION_API_KEY = os.environ.get('NOTION_API_KEY')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY')
TODOIST_API_KEY = os.environ.get('TODOIST_API_KEY')
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY')

# Configure Gemini
if GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)

# Notion Database IDs
TASKS_DATABASE_ID = "2aee43055e06803dbf90d54231711e61"
CASES_DATABASE_ID = "2aee43055e0681ad8e43e6e67825f9dd"
CALENDAR_DATABASE_ID = "2cbe43055e06806d88bafb4b136061b5"
MAPPINGS_DATABASE_ID = "2aee43055e06802782abfee92f846d88"
MATTER_ACTIVITY_DATABASE_ID = "2aee43055e06800fb4860009681c5fe"  # Will need to verify this ID

# Matter Activity Property IDs (from Flow 3 blueprint)
MATTER_ACTIVITY_PROPS = {
    "name": "title",           # Title property (Name)
    "type": "%3EgJ%5D",         # Type: Note, Completed, Promoted, Proposed
    "source": "uam_",          # Source: System, Manual, Todoist, Email
    "case": "j%3FrS",          # Case relation
    "related_task": "rdqM",    # Related Task ID relation
    "actor": "%3DOq%3E",       # Actor: User, System
    "description": "%5Ek%40h", # Description rich_text
}

# Google Calendar (for Flow 4 - optional)
GOOGLE_CALENDAR_ID = os.environ.get('GOOGLE_CALENDAR_ID', 'primary')

# Notion Property IDs (from your Make.com blueprint)
NOTION_PROPS = {
    "status": "Ehyz",
    "subtasks_json": "FMmh",
    "llm_rationale": "PjOx",
    "source_email_url": "dLrJ",
    "priority": "jczD",
    "matter": "rwWE",
    "relative_deadline": "%5EGGe",  # ^GGe
    "message_fingerprint": "%7BkjL",  # {kjL
    "due_date": "OPN%5C",  # OPN\
    "llm_model": "%40v%40s",  # @v@s
    "applicable_rule": "MP%3F%60",  # MP?`
    "original_sender": "Gh%40%5D",  # Gh@]
}

# =============================================================================
# GEMINI DOCUMENT EXTRACTION PROMPT (from Appendix A)
# =============================================================================

GEMINI_DOCUMENT_PROMPT = """You are a legal document analysis assistant. Extract ALL information from this court document or attachment.

Today's date: {current_date}

REQUIRED EXTRACTIONS:

1. DOCUMENT METADATA
   - document_type: Classify as scheduling_order, court_order, motion, notice, correspondence, pleading, discovery_demand, or other
   - index_number: The court index/case number (e.g., "2024-974", "123456/2024")
   - caption: The case name if visible (e.g., "Walker v. Metro Ten Hotel")

2. TASKS (Deadlines requiring work product)
   Extract EACH deadline as a separate task:
   - Filings (answers, replies, motions, notes of issue)
   - Responses (discovery responses, demands for bills of particulars)
   - Discovery obligations (document production, interrogatories, admissions)
   - Compliance requirements (expert disclosure, IME scheduling, EBT completion)
   - Motion practice (MSJ filing, opposition deadlines)
   
   For each task:
   - title: Clear description of the required action
   - due_date: Explicit date in YYYY-MM-DD format (only if stated in document)
   - relative_deadline: Description if date requires calculation (e.g., "20 days from service", "60 days after plaintiff EBT")
   - priority: P0/P1/P2/P3 based on guidelines below
   - extraction_rationale: Quote the specific document text you relied on
   - confidence: 0.0-1.0 score for your extraction accuracy

3. CALENDAR ITEMS (Events requiring appearance)
   Extract events with specific dates/times:
   - Court conferences (compliance, settlement, pre-trial)
   - Hearings and oral arguments
   - Depositions (EBTs when scheduled with date/time)
   - Appearances before the court
   
   For each event:
   - title: Description of the event
   - event_date: YYYY-MM-DD format
   - event_time: HH:MM format (e.g., "10:00", "14:30")
   - location: Courtroom, address, or virtual link
   - extraction_rationale: Quote the document text
   - confidence: 0.0-1.0 score

SUBTASK DETECTION:
If the document indicates a complex deliverable with component work items, extract them as subtasks. Look for patterns like:
- Explicit lists labeled "Required documents:", "Components:", "To be filed:", or similar
- MSJ preparation requirements (Notice of Motion, Supporting Affidavits/Affirmations, Memorandum of Law, Statement of Material Facts)
- Appeal requirements (Notice of Appeal, Record on Appeal, Appellant's Brief)
- Discovery responses with multiple categories (interrogatories, document requests, requests for admission)
- Complex filings with multiple required attachments or exhibits

For each subtask, provide:
- title: Brief description of the component (e.g., "Draft Notice of Motion", "Prepare Statement of Material Facts")
- offset_days: Days relative to parent due date (negative = before parent deadline)
  - Example: Parent due 2025-03-15, subtask due 3 days before → offset_days: -3
  - Use -7 for work due one week before, -3 for three days before, -1 for day before

If subtasks are not explicitly indicated or cannot be reasonably inferred, return an empty subtasks array [].

DEADLINE RULE IDENTIFICATION:
If the deadline is governed by a statute or court rule, identify it in the applicable_rule field. Use exact citation format:

Common statutory deadlines:
- "CPLR 3012(a) — Answer to Complaint"
- "CPLR 3042(a) — Response to Demand for Bill of Particulars"
- "CPLR 3122(a) — Discovery Response"
- "CPLR 3212 — MSJ Filing Deadline"
- "IME Scheduling"
- "IME Report"

Only populate applicable_rule when:
- The document explicitly cites the statute/rule (e.g., "pursuant to CPLR 3122(a)")
- The deadline type is clearly statutory (e.g., "answer to complaint" = CPLR 3012(a))
- You have high confidence in the rule citation

Leave applicable_rule null or empty if:
- Deadline is court-ordered without statutory basis (e.g., "So Ordered" deadline in scheduling order)
- Deadline is case-specific or negotiated (e.g., "by agreement of the parties")
- You are uncertain about which rule applies

CRITICAL: Do NOT calculate the due date from the rule. If a deadline requires rule-based calculation (e.g., "within 20 days of service of the complaint"), leave due_date null. The attorney will calculate manually using the identified rule. Only populate due_date if an explicit date appears in the document.

FORMAT REQUIREMENTS:
- due_date: YYYY-MM-DD (e.g., "2025-02-28") - only if explicitly stated
- event_date: YYYY-MM-DD
- event_time: HH:MM 24-hour format (e.g., "10:00", "14:30")
- relative_deadline: Plain text description (e.g., "20 days from service", "60 days after plaintiff's EBT")
- confidence: Number between 0.0-1.0
- extraction_rationale: Direct quote from document

PRIORITY LEVELS:
- P0: Court-ordered deadlines with "So Ordered", emergency motions, show cause orders, contempt proceedings
- P1: Discovery deadlines, EBT completion, motion filing deadlines, compliance conference requirements
- P2: Standard responses, routine filings, administrative requirements
- P3: Informational items, suggested dates, courtesy copies

Extract information conservatively. If unsure whether something is a task or event, include it. Better to have the attorney dismiss a false positive than miss a deadline.

Respond with valid JSON matching this schema:
{{
  "document_type": {{"value": "string", "confidence": 0.0}},
  "index_number": {{"value": "string or null", "confidence": 0.0}},
  "caption": {{"value": "string or null", "confidence": 0.0}},
  "tasks": [
    {{
      "title": {{"value": "string", "confidence": 0.0}},
      "due_date": {{"value": "YYYY-MM-DD or null", "confidence": 0.0}},
      "relative_deadline": {{"value": "string or null", "confidence": 0.0}},
      "priority": {{"value": "P0|P1|P2|P3", "confidence": 0.0}},
      "applicable_rule": "string or null",
      "extraction_rationale": "string",
      "subtasks": [
        {{"title": "string", "offset_days": -3}}
      ]
    }}
  ],
  "calendar_items": [
    {{
      "title": {{"value": "string", "confidence": 0.0}},
      "event_type": {{"value": "hearing|conference|appearance|deadline|trial|deposition|meeting", "confidence": 0.0}},
      "event_date": {{"value": "YYYY-MM-DD", "confidence": 0.0}},
      "event_time": {{"value": "HH:MM or null", "confidence": 0.0}},
      "location": {{"value": "string or null", "confidence": 0.0}},
      "extraction_rationale": "string"
    }}
  ],
  "confidence_overall": 0.0,
  "extraction_notes": "string"
}}"""


# =============================================================================
# GEMINI DOCUMENT PROCESSING FUNCTIONS
# =============================================================================

def extract_with_gemini(file_data_base64, file_name, mime_type):
    """
    Upload document to Gemini and extract tasks/calendar items.
    
    Args:
        file_data_base64: Base64-encoded file content
        file_name: Original filename
        mime_type: MIME type (e.g., "application/pdf", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    
    Returns:
        dict with document_type, index_number, caption, tasks[], calendar_items[]
    """
    if not GOOGLE_API_KEY:
        logger.warning("GOOGLE_API_KEY not configured - skipping Gemini extraction")
        return None
    
    try:
        # Decode base64 to bytes
        file_bytes = base64.b64decode(file_data_base64)
        
        # Write to temp file (Gemini SDK requires file path)
        with tempfile.NamedTemporaryFile(delete=False, suffix=get_file_extension(mime_type)) as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name
        
        try:
            # Upload file to Gemini
            logger.info(f"Uploading {file_name} ({mime_type}) to Gemini...")
            uploaded_file = genai.upload_file(tmp_path, mime_type=mime_type)
            logger.info(f"Uploaded file URI: {uploaded_file.uri}")
            
            # Create model and generate
            model = genai.GenerativeModel('gemini-2.5-pro-preview-05-06')
            
            # Format prompt with current date
            prompt = GEMINI_DOCUMENT_PROMPT.format(current_date=datetime.now().strftime('%Y-%m-%d'))
            
            # Generate response with document
            response = model.generate_content([uploaded_file, prompt])
            
            # Parse JSON response
            response_text = response.text
            
            # Clean up markdown code blocks if present
            if response_text.startswith('```'):
                response_text = re.sub(r'^```(?:json)?\n?', '', response_text)
                response_text = re.sub(r'\n?```$', '', response_text)
            
            result = json.loads(response_text)
            
            # Add source model to each task and calendar item
            for task in result.get('tasks', []):
                task['source_model'] = 'gemini-2.5-pro'
            for item in result.get('calendar_items', []):
                item['source_model'] = 'gemini-2.5-pro'
            
            logger.info(f"Gemini extracted {len(result.get('tasks', []))} tasks, {len(result.get('calendar_items', []))} calendar items")
            
            # Clean up uploaded file
            try:
                genai.delete_file(uploaded_file.name)
            except:
                pass  # Ignore cleanup errors
            
            return result
            
        finally:
            # Clean up temp file
            try:
                os.unlink(tmp_path)
            except:
                pass
                
    except Exception as e:
        logger.error(f"Gemini extraction failed: {e}", exc_info=True)
        return None


def get_file_extension(mime_type):
    """Get file extension from MIME type."""
    extensions = {
        'application/pdf': '.pdf',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
        'application/msword': '.doc',
        'image/png': '.png',
        'image/jpeg': '.jpg',
        'image/jpg': '.jpg',
        'image/tiff': '.tiff',
        'image/gif': '.gif',
    }
    return extensions.get(mime_type, '.bin')


def find_matter_by_index_number(index_number):
    """
    Query Legal Cases database by index_number property.
    
    Returns matter_id if found, None otherwise.
    """
    if not index_number:
        return None
    
    # Clean up index number (remove spaces, normalize format)
    index_clean = index_number.strip()
    
    url = f"https://api.notion.com/v1/databases/{CASES_DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # Query by Index_number property (rich_text type)
    data = {
        "filter": {
            "property": "Index_number",
            "rich_text": {
                "contains": index_clean
            }
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        results = response.json().get('results', [])
        
        if results:
            matter_id = results[0].get('id')
            logger.info(f"Found matter by index number '{index_clean}': {matter_id}")
            return matter_id
        else:
            logger.info(f"No matter found for index number: {index_clean}")
            return None
            
    except Exception as e:
        logger.error(f"Index number lookup failed: {e}")
        return None


def find_matter_by_case_name(case_name):
    """
    Query Legal Cases database by Case Name (title property).
    
    Case name is typically the first plaintiff's last name or company name.
    E.g., "Smith" from "Smith v. Anderson"
    
    Returns matter_id if found, None otherwise.
    """
    if not case_name:
        return None
    
    # Normalize case name for comparison
    case_name_clean = case_name.strip()
    
    url = f"https://api.notion.com/v1/databases/{CASES_DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # Query by Case Name (title property) - case insensitive contains
    data = {
        "filter": {
            "property": "Case Name",
            "title": {
                "equals": case_name_clean
            }
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        results = response.json().get('results', [])
        
        if results:
            matter_id = results[0].get('id')
            logger.info(f"Found matter by case name '{case_name_clean}': {matter_id}")
            return matter_id
        
        # Try contains match if exact match fails (handles minor variations)
        data["filter"]["title"] = {"contains": case_name_clean}
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        results = response.json().get('results', [])
        
        if results:
            matter_id = results[0].get('id')
            logger.info(f"Found matter by case name (partial) '{case_name_clean}': {matter_id}")
            return matter_id
        
        logger.info(f"No matter found for case name: {case_name_clean}")
        return None
            
    except Exception as e:
        logger.error(f"Case name lookup failed: {e}")
        return None


def find_matter_for_email(sender_email):
    """
    Find a matter by matching sender's email domain.
    
    This is a fallback when no index number is present in the email.
    Currently returns None (tasks created without matter link).
    
    Future enhancement: Query Legal Cases for matching domain in contacts.
    
    Args:
        sender_email: The sender's email address (e.g., "jsmith@lawfirm.com")
    
    Returns:
        matter_id if found, None otherwise
    """
    if not sender_email:
        return None
    
    # Extract domain from email
    try:
        domain = sender_email.split('@')[1].lower() if '@' in sender_email else None
    except (IndexError, AttributeError):
        domain = None
    
    if not domain:
        logger.info(f"Could not extract domain from sender: {sender_email}")
        return None
    
    logger.info(f"Domain matching not yet implemented for: {domain}")
    # TODO: Query Legal Cases database for matters with matching domain
    # This would require adding a "Contact Domains" property to the Cases database
    
    return None


def parse_email(payload):
    """
    Extract email components from Mailhook payload.
    Handles Make.com's mailhook format directly.
    """
    # Handle if payload is wrapped in an array
    if isinstance(payload, list) and len(payload) > 0:
        payload = payload[0]
    
    text = payload.get('text', '') or ''
    headers = payload.get('headers', {}) or {}
    from_data = payload.get('from', {}) or {}
    
    # Get sender - either from 'from' object or parse from text
    if isinstance(from_data, dict):
        direct_sender = from_data.get('address', '')
    elif isinstance(from_data, str):
        direct_sender = from_data
    else:
        direct_sender = ''
    
    # Try to extract original sender from forwarded email body
    sender_match = re.search(r'From:\s*(?:[^<]*<)?([^>@\s]+@[^>\s]+)', text)
    original_sender = sender_match.group(1) if sender_match else direct_sender
    
    # Get date
    received_at = payload.get('date', '')
    
    # Get subject
    subject = payload.get('subject', '')
    
    # Extract user notes (content before signature)
    notes_match = re.search(r'^(.*?)(?=\n\nSteven E\. Mach|\n\n_{10,}|\n\n-{10,})', text, re.DOTALL)
    user_notes = notes_match.group(1).strip() if notes_match else ''
    
    # Use full text as body
    body = text
    
    # Get message ID from headers (for fingerprint)
    # Make.com sends headers as a dict with various keys
    message_id = ''
    if isinstance(headers, dict):
        message_id = headers.get('in-reply-to', '') or headers.get('message-id', '') or ''
        # If message-id is a string, use it directly
        if isinstance(message_id, str):
            message_id = message_id.strip('<>').strip()
    
    # Check for attachments
    attachments = payload.get('attachments', []) or []
    has_attachment = len(attachments) > 0
    attachment_names = []
    for a in attachments:
        if isinstance(a, dict):
            attachment_names.append(a.get('fileName', a.get('filename', 'unknown')))
        elif isinstance(a, str):
            attachment_names.append(a)
    
    return {
        'message_id': message_id,
        'original_sender': original_sender,
        'received_at': received_at,
        'subject': subject,
        'user_notes': user_notes,
        'body': body,
        'full_text': text,
        'has_attachment': has_attachment,
        'attachment_names': attachment_names,
    }


def normalize_subject(subject):
    """
    Normalize subject for fingerprint computation.
    Replicates Modules 10, 15, 16, 17 regex replacements.
    """
    # Module 10: Lowercase and remove re:/fw:/fwd: prefixes
    normalized = subject.lower()
    normalized = re.sub(r'^((re:|fw:|fwd:)\s*)+', '', normalized, flags=re.IGNORECASE)
    
    # Module 15: Collapse whitespace
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Module 16: Trim leading/trailing whitespace
    normalized = normalized.strip()
    
    # Module 17: Remove special characters (keep only a-z, 0-9, space, dash, underscore)
    normalized = re.sub(r'[^a-z0-9\s\-_]', '', normalized)
    
    return normalized


def compute_fingerprint(message_id, sender, received_at):
    """
    Compute SHA256 fingerprint for deduplication.
    Replicates Module 19: sha256(in-reply-to + sender + unix_timestamp)
    """
    # Parse received_at to unix timestamp
    try:
        # Try common date formats
        for fmt in [
            '%A, %B %d, %Y %I:%M %p',  # "Saturday, November 30, 2025 10:30 AM"
            '%B %d, %Y %I:%M %p',       # "November 30, 2025 10:30 AM"
            '%Y-%m-%dT%H:%M:%S.%fZ',    # ISO format
            '%Y-%m-%dT%H:%M:%SZ',       # ISO format without microseconds
        ]:
            try:
                dt = datetime.strptime(received_at.strip(), fmt)
                unix_ts = str(int(dt.timestamp()))
                break
            except ValueError:
                continue
        else:
            # Fallback: use current timestamp
            unix_ts = str(int(datetime.now().timestamp()))
    except Exception:
        unix_ts = str(int(datetime.now().timestamp()))
    
    # Concatenate and hash
    fingerprint_input = f"{message_id}{sender}{unix_ts}"
    fingerprint = hashlib.sha256(fingerprint_input.encode()).hexdigest()
    
    return fingerprint


# =============================================================================
# SUPABASE OPERATIONS (Same as Make.com Modules 25, 29)
# =============================================================================

def check_duplicate(fingerprint):
    """
    Check if email already processed.
    Replicates Module 25: Supabase search for fingerprint.
    """
    url = f"{SUPABASE_URL}/rest/v1/email_receipts"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    params = {
        "fingerprint": f"eq.{fingerprint}",
        "limit": 1
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        results = response.json()
        return len(results) > 0
    except Exception as e:
        logger.error(f"Supabase duplicate check failed: {e}")
        return False  # Proceed if check fails


def log_email_receipt(fingerprint, sender, received_at, message_id, subject):
    """
    Log email receipt to Supabase.
    Replicates Module 29: Insert into email_receipts.
    """
    url = f"{SUPABASE_URL}/rest/v1/email_receipts"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    data = {
        "fingerprint": fingerprint,
        "sender": sender,
        "received_at_utc": received_at,
        "headers_json": {
            "message_id": message_id,
            "subject": subject
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        logger.info(f"Logged email receipt: {fingerprint[:16]}...")
    except Exception as e:
        logger.error(f"Failed to log email receipt: {e}")


def log_task_event(fingerprint, task_title, event_type, notion_page_id=None):
    """
    Log task creation to Supabase audit trail.
    """
    url = f"{SUPABASE_URL}/rest/v1/task_events"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    data = {
        "fingerprint": fingerprint,
        "task_title": task_title,
        "event_type": event_type,
        "notion_row_id": notion_page_id,
        "ts": datetime.utcnow().isoformat(),
        "actor": "System"
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to log task event: {e}")


# =============================================================================
# TASK CONTENT FINGERPRINT (Semantic Deduplication)
# =============================================================================

def normalize_task_title(title):
    """
    Normalize task title for content fingerprinting.
    Reduces variations like "File answer" vs "file  Answer" to same form.
    """
    if not title:
        return ""
    t = title.lower().strip()
    t = re.sub(r'[^a-z0-9\s]', '', t)  # Remove punctuation
    t = re.sub(r'\s+', ' ', t)          # Collapse whitespace
    return t


def compute_task_content_fingerprint(title, due_date, matter_id):
    """
    Compute a content-based fingerprint for semantic deduplication.
    
    Two tasks with the same normalized title, due date, and matter are 
    considered duplicates regardless of their source (Flow 1 vs manual Todoist).
    
    Returns 64-character SHA256 hash.
    """
    normalized_title = normalize_task_title(title)
    due_date_str = due_date or "no-date"
    matter_str = matter_id or "no-matter"
    
    content = f"{normalized_title}|{due_date_str}|{matter_str}"
    return hashlib.sha256(content.encode()).hexdigest()


def store_task_content_fingerprint(content_fingerprint, title, matter_id, notion_page_id, source):
    """
    Store task content fingerprint in Supabase for deduplication.
    
    Uses task_events table with event_type='ContentFingerprint'.
    """
    url = f"{SUPABASE_URL}/rest/v1/task_events"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    data = {
        "fingerprint": content_fingerprint,
        "task_title": title,
        "event_type": "ContentFingerprint",
        "notion_row_id": notion_page_id,
        "ts": datetime.utcnow().isoformat(),
        "actor": "System",
        "details_json": {
            "matter_id": matter_id,
            "source": source
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        logger.info(f"Stored content fingerprint: {content_fingerprint[:16]}...")
    except Exception as e:
        logger.error(f"Failed to store content fingerprint: {e}")


def check_task_content_fingerprint_exists(content_fingerprint):
    """
    Check if a task with this content fingerprint already exists.
    
    Returns True if duplicate, False if new.
    """
    url = f"{SUPABASE_URL}/rest/v1/task_events"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    params = {
        "fingerprint": f"eq.{content_fingerprint}",
        "event_type": "eq.ContentFingerprint",
        "select": "fingerprint",
        "limit": "1"
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        results = response.json()
        exists = len(results) > 0
        if exists:
            logger.info(f"Content fingerprint exists: {content_fingerprint[:16]}... (duplicate)")
        return exists
    except Exception as e:
        logger.error(f"Content fingerprint check failed: {e}")
        return False  # Fail open - allow creation if check fails


# =============================================================================
# CLAUDE EXTRACTION (Replaces Gemini Modules 89/102)
# =============================================================================

EXTRACTION_PROMPT = """You are a legal task extraction assistant. Analyze this email and extract structured information.

Today's date: {today}

Email details:
From: {sender}
Subject: {subject}
Received: {received_at}
User Notes: {user_notes}
Body: {body}

REQUIRED EXTRACTIONS:

1. DOCUMENT METADATA
   - document_type: Classify as email, correspondence, notice, or other
   - index_number: Court case number if mentioned (e.g., "2024-974", "123456/2024")
   - caption: Case name if mentioned (e.g., "Walker v. Metro Ten Hotel")

2. TASKS (Deadlines requiring work product)
   Extract EACH actionable task with a deadline:
   - Filings (answers, motions, notes of issue)
   - Responses (discovery responses, demands for bills of particulars)
   - Document production and discovery obligations
   - Client deliverables with specific deadlines
   - Internal deadlines mentioned in the email
   
   For each task:
   - title: Clear description of the required action
   - due_date: Explicit date in YYYY-MM-DD format (only if stated in email)
   - relative_deadline: Description if date requires calculation (e.g., "20 days from service", "by end of week")
   - priority: P0/P1/P2/P3 based on guidelines below
   - extraction_rationale: Quote the specific email text you relied on
   - confidence: 0.0-1.0 score for your extraction accuracy

3. CALENDAR ITEMS (Events requiring appearance)
   Extract events with specific dates/times:
   - Court conferences and hearings
   - Depositions (EBTs) with scheduled date/time
   - Client meetings with specific scheduling
   - Attorney appearances
   
   For each event:
   - title: Description of the event
   - event_date: YYYY-MM-DD format
   - event_time: HH:MM format (e.g., "10:00", "14:30")
   - location: Courtroom, address, or virtual platform
   - extraction_rationale: Quote the email text
   - confidence: 0.0-1.0 score

SUBTASK DETECTION:
If the email indicates a complex deliverable with component work items, extract them as subtasks. Look for patterns like:
- Explicit lists with "Subtasks:", "To do:", "Steps:", "Components:", or "Notes:"
- MSJ preparation mentions (Notice of Motion, Affidavits, Memorandum of Law, Statement of Facts)
- Appeal preparation (Notice of Appeal, Record on Appeal, Brief)
- Discovery responses with multiple categories

For each subtask, provide:
- title: Brief description of the component
- offset_days: Days relative to parent due date (negative = before parent deadline)

If subtasks are not mentioned or implied, return an empty subtasks array [].

DEADLINE RULE IDENTIFICATION:
If the deadline is governed by a statute or court rule, identify it in the applicable_rule field:
- "CPLR 3012(a) — Answer to Complaint"
- "CPLR 3042(a) — Response to Demand for Bill of Particulars"
- "CPLR 3122(a) — Discovery Response"
- "CPLR 3212 — MSJ Filing Deadline"

Only populate applicable_rule when you have high confidence.

PRIORITY LEVELS:
- P0: Court-imposed deadlines, emergency matters, orders to show cause
- P1: Discovery deadlines, motion return dates, client-critical matters
- P2: Standard filings, routine responses, scheduled tasks
- P3: Administrative, informational, FYI items

Return your response as JSON with this structure:
{{
  "document_type": {{"value": "email", "confidence": 0.95}},
  "index_number": {{"value": "2024-974", "confidence": 0.9}},
  "caption": {{"value": "Martinez v. ABC Corp", "confidence": 0.9}},
  "tasks": [
    {{
      "title": {{"value": "Respond to discovery demands", "confidence": 0.95}},
      "due_date": {{"value": "2025-12-30", "confidence": 0.95}},
      "relative_deadline": {{"value": null, "confidence": 1.0}},
      "priority": {{"value": "P1", "confidence": 0.9}},
      "extraction_rationale": "Responses are due by December 30, 2025",
      "applicable_rule": "CPLR 3122(a) — Discovery Response",
      "subtasks": []
    }}
  ],
  "calendar_items": []
}}

Extract information conservatively. If unsure whether something is a task or event, include it. Better to have the attorney dismiss a false positive than miss a deadline."""


def extract_tasks_with_claude(email_data):
    """
    Use Claude API to extract tasks from email.
    Replaces Gemini extractStructuredData (Modules 89/102).
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    
    prompt = EXTRACTION_PROMPT.format(
        today=datetime.now().strftime('%Y-%m-%d'),
        sender=email_data['original_sender'],
        subject=email_data['subject'],
        received_at=email_data['received_at'],
        user_notes=email_data['user_notes'],
        body=email_data['body']
    )
    
    # Add note about attachments if present
    if email_data['has_attachment']:
        prompt += f"\n\nNOTE: This email has attachments: {', '.join(email_data['attachment_names'])}. Flag this in your extraction - the attorney will need to review the attachments manually."
    
    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # Extract JSON from response
        response_text = message.content[0].text
        
        # Try to parse JSON (handle potential markdown code blocks)
        json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            json_str = response_text
        
        extraction = json.loads(json_str)
        return extraction
        
    except Exception as e:
        logger.error(f"Claude extraction failed: {e}")
        # Return minimal extraction on failure
        return {
            "document_type": {"value": "email", "confidence": 0.0},
            "index_number": {"value": None, "confidence": 0.0},
            "caption": {"value": None, "confidence": 0.0},
            "tasks": [{
                "title": {"value": f"[EXTRACTION FAILED] Review email: {email_data['subject']}", "confidence": 0.0},
                "due_date": {"value": None, "confidence": 0.0},
                "relative_deadline": {"value": None, "confidence": 0.0},
                "priority": {"value": "P1", "confidence": 0.0},
                "extraction_rationale": f"Extraction failed: {str(e)}",
                "applicable_rule": None,
                "subtasks": []
            }],
            "calendar_items": []
        }


# =============================================================================
# NOTION OPERATIONS (Replaces Modules 92, 98)
# =============================================================================

def search_case_by_index(index_number):
    """
    Search Legal Cases database by index number.
    Replicates Module 98: Notion Search.
    """
    if not index_number:
        return None
    
    url = f"https://api.notion.com/v1/databases/{CASES_DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    data = {
        "filter": {
            "property": "Index_number",
            "rich_text": {
                "equals": index_number
            }
        },
        "page_size": 1
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        results = response.json().get('results', [])
        if results:
            return results[0]['id']
        return None
    except Exception as e:
        logger.error(f"Notion case search failed: {e}")
        return None


def extract_first_plaintiff(caption):
    """
    Extract the first plaintiff's last name or company name from a case caption.
    Examples:
        "Johnson v. State Farm" -> "Johnson"
        "Martinez v. ABC Corp" -> "Martinez"
        "ABC Corp v. Smith" -> "ABC Corp"
    """
    if not caption:
        return None
    
    # Split on common "v." or "vs." or "vs" patterns
    match = re.split(r'\s+v\.?\s+|\s+vs\.?\s+', caption, maxsplit=1, flags=re.IGNORECASE)
    if match and len(match) >= 1:
        first_party = match[0].strip()
        # Clean up any leading/trailing punctuation
        first_party = re.sub(r'^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$', '', first_party)
        return first_party if first_party else None
    return None


def create_stub_matter(index_number, caption, venue=None):
    """
    Create a stub matter in Legal Cases database when no match is found.
    Returns the new matter's Notion page ID.
    """
    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # Extract first plaintiff for Case Name
    case_name = extract_first_plaintiff(caption) or caption or "Unknown Matter"
    
    properties = {
        "Case Name": {
            "title": [{"text": {"content": case_name}}]
        },
        "Index_number": {
            "rich_text": [{"text": {"content": index_number or ""}}]
        },
        "Title": {
            "rich_text": [{"text": {"content": caption or ""}}]
        },
        "Status": {
            "select": {"name": "Pending"}
        },
        "Date Opened": {
            "date": {"start": datetime.now().strftime('%Y-%m-%d')}
        }
    }
    
    # Add venue if extracted
    if venue:
        properties["Venue"] = {
            "rich_text": [{"text": {"content": venue}}]
        }
    
    data = {
        "parent": {"database_id": CASES_DATABASE_ID},
        "properties": properties
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        logger.info(f"Created stub matter: {case_name} (Index: {index_number})")
        return result['id']
    except Exception as e:
        logger.error(f"Failed to create stub matter: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        return None


def determine_status(task, matter_id, has_attachment, attachment_processed=False):
    """
    Determine task status based on confidence, due date, and matter match.
    Replicates Module 68 status logic.
    
    Args:
        task: Task dict with confidence and due_date
        matter_id: Linked matter ID (or None)
        has_attachment: Whether email had attachments
        attachment_processed: Whether attachments were processed by Gemini (if True, don't force review)
    """
    # Check confidence
    confidence = task.get('title', {}).get('confidence', 0)
    
    # Check if due date is within 3 days (force review)
    force_review = False
    due_date_str = task.get('due_date', {}).get('value')
    if due_date_str:
        try:
            due_date = datetime.strptime(due_date_str, '%Y-%m-%d')
            days_until_due = (due_date - datetime.now()).days
            if days_until_due <= 3:
                force_review = True
        except ValueError:
            pass
    
    # Determine status
    if force_review:
        return "Needs Review"
    elif has_attachment and not attachment_processed:
        return "Needs Review"  # Unprocessed attachments need review
    elif not matter_id:
        return "Needs Review"  # No matter linked - needs manual review
    elif confidence >= 0.8:
        return "Proposed"
    else:
        return "Needs Review"


def create_notion_task(task, email_data, fingerprint, matter_id, status, llm_model="claude-sonnet-4-20250514"):
    """
    Create task in Notion Tasks (Proposed) database.
    Replicates Module 92: Notion Create Database Item.
    
    Args:
        task: Task dict from extraction
        email_data: Parsed email data
        fingerprint: Email fingerprint
        matter_id: Notion page ID of linked matter (or None)
        status: Task status (Proposed, Needs Review, etc.)
        llm_model: Model that extracted this task (claude-sonnet-4-20250514 or gemini-2.5-pro)
    """
    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # Build properties object
    properties = {
        "title": {
            "title": [{"text": {"content": task.get('title', {}).get('value', 'Untitled Task')}}]
        },
        NOTION_PROPS["status"]: {
            "status": {"name": status}
        },
        NOTION_PROPS["priority"]: {
            "select": {"name": task.get('priority', {}).get('value', 'P2')}
        },
        NOTION_PROPS["llm_rationale"]: {
            "rich_text": [{"text": {"content": task.get('extraction_rationale', '')[:2000]}}]
        },
        NOTION_PROPS["source_email_url"]: {
            "url": f"https://outlook.office.com/mail/deeplink/read/{quote(email_data['message_id'])}"
        },
        NOTION_PROPS["message_fingerprint"]: {
            "rich_text": [{"text": {"content": fingerprint}}]
        },
        NOTION_PROPS["llm_model"]: {
            "rich_text": [{"text": {"content": llm_model}}]
        },
    }
    
    # Add due date if present
    due_date = task.get('due_date', {}).get('value')
    if due_date:
        properties[NOTION_PROPS["due_date"]] = {
            "date": {"start": due_date}
        }
    
    # Add relative deadline if present
    relative_deadline = task.get('relative_deadline', {}).get('value')
    if relative_deadline:
        properties[NOTION_PROPS["relative_deadline"]] = {
            "rich_text": [{"text": {"content": relative_deadline}}]
        }
    
    # Add applicable rule if present
    applicable_rule = task.get('applicable_rule')
    if applicable_rule:
        properties[NOTION_PROPS["applicable_rule"]] = {
            "rich_text": [{"text": {"content": applicable_rule}}]
        }
    
    # Add subtasks JSON if present
    subtasks = task.get('subtasks', [])
    if subtasks:
        properties[NOTION_PROPS["subtasks_json"]] = {
            "rich_text": [{"text": {"content": json.dumps(subtasks)}}]
        }
    
    # Add matter relation if found
    if matter_id:
        properties[NOTION_PROPS["matter"]] = {
            "relation": [{"id": matter_id}]
        }
    
    # Add original sender
    properties[NOTION_PROPS["original_sender"]] = {
        "email": email_data['original_sender']
    }
    
    # Add note about attachments if present
    if email_data['has_attachment']:
        current_rationale = properties[NOTION_PROPS["llm_rationale"]]["rich_text"][0]["text"]["content"]
        attachment_note = f"\n\n⚠️ EMAIL HAS ATTACHMENTS: {', '.join(email_data['attachment_names'])}. Review source email."
        properties[NOTION_PROPS["llm_rationale"]]["rich_text"][0]["text"]["content"] = current_rationale + attachment_note
    
    data = {
        "parent": {"database_id": TASKS_DATABASE_ID},
        "properties": properties
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        logger.info(f"Created Notion task: {task.get('title', {}).get('value', 'Untitled')}")
        return result['id']
    except Exception as e:
        logger.error(f"Failed to create Notion task: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        return None


def create_notion_calendar_item(event, email_data, fingerprint, matter_id, status, llm_model="claude-sonnet-4-20250514"):
    """
    Create calendar item in Notion Calendar Items (Proposed) database.
    
    Args:
        event: Calendar event dict from extraction
        email_data: Parsed email data
        fingerprint: Email fingerprint
        matter_id: Notion page ID of linked matter (or None)
        status: Item status (Proposed, Needs Review, etc.)
        llm_model: Model that extracted this item (claude-sonnet-4-20250514 or gemini-2.5-pro)
    """
    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # Build properties object
    properties = {
        "Title": {
            "title": [{"text": {"content": event.get('title', {}).get('value', 'Untitled Event')}}]
        },
        "Status": {
            "status": {"name": status}
        },
    }
    
    # Add LLM Model if property exists
    # Using same property ID pattern as tasks - may need adjustment based on Calendar database schema
    properties["LLM Model"] = {
        "rich_text": [{"text": {"content": llm_model}}]
    }
    
    # Add event date if present
    event_date = event.get('event_date', {}).get('value')
    event_time = event.get('event_time', {}).get('value')
    if event_date:
        date_value = {"start": event_date}
        if event_time:
            date_value["start"] = f"{event_date}T{event_time}:00"
        properties["Event Date"] = {"date": date_value}
    
    # Add event type if we can determine it
    title_lower = event.get('title', {}).get('value', '').lower()
    if 'deposition' in title_lower or 'ebt' in title_lower:
        properties["Event_type"] = {"select": {"name": "Deposition"}}
    elif 'conference' in title_lower:
        properties["Event_type"] = {"select": {"name": "Compliance Conference"}}
    elif 'hearing' in title_lower:
        properties["Event_type"] = {"select": {"name": "Hearing"}}
    elif 'trial' in title_lower:
        properties["Event_type"] = {"select": {"name": "Trial"}}
    
    # Add location if present
    location = event.get('location', {}).get('value')
    if location:
        properties["Location"] = {
            "rich_text": [{"text": {"content": location}}]
        }
    
    # Add extraction rationale
    rationale = event.get('extraction_rationale', '')
    if rationale:
        properties["Extraction Rationale"] = {
            "rich_text": [{"text": {"content": rationale[:2000]}}]
        }
    
    # Add case relation if found
    if matter_id:
        properties["Case"] = {
            "relation": [{"id": matter_id}]
        }
    
    data = {
        "parent": {"database_id": CALENDAR_DATABASE_ID},
        "properties": properties
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        logger.info(f"Created Notion calendar item: {event.get('title', {}).get('value', 'Untitled')}")
        return result['id']
    except Exception as e:
        logger.error(f"Failed to create Notion calendar item: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        return None


# =============================================================================
# FLOW 2: PROMOTION TO TODOIST
# =============================================================================

def compute_promotion_key(notion_page_id, title, due_date, todoist_project_id):
    """
    Compute unique promotion key for idempotency.
    Combines: Notion page ID + normalized title + due date + Todoist project ID
    """
    # Normalize title
    normalized_title = title.lower().strip() if title else ""
    normalized_title = re.sub(r'\s+', ' ', normalized_title)
    
    # Build key components
    components = [
        notion_page_id or "",
        normalized_title,
        due_date or "",
        todoist_project_id or ""
    ]
    
    key_input = "|".join(components)
    return hashlib.sha256(key_input.encode()).hexdigest()


def check_promotion_exists(promotion_key):
    """
    Check if this promotion_key already exists in task_events.
    Returns True if already promoted (idempotency check).
    """
    url = f"{SUPABASE_URL}/rest/v1/task_events"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    params = {
        "promotion_key": f"eq.{promotion_key}",
        "event_type": "eq.Promoted",
        "limit": 1
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        results = response.json()
        return len(results) > 0
    except Exception as e:
        logger.error(f"Promotion check failed: {e}")
        return False  # Proceed if check fails (will create duplicate, but better than losing task)


# =============================================================================
# TODOIST PROJECT MANAGEMENT
# =============================================================================

def list_todoist_projects():
    """
    Get all Todoist projects for the user.
    Returns list of {id, name} dicts.
    """
    url = "https://api.todoist.com/rest/v2/projects"
    headers = {
        "Authorization": f"Bearer {TODOIST_API_KEY}",
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        projects = response.json()
        return [{"id": p.get("id"), "name": p.get("name")} for p in projects]
    except Exception as e:
        logger.error(f"Failed to list Todoist projects: {e}")
        return []


def find_todoist_project_by_name(project_name):
    """
    Search Todoist projects by name (case-insensitive).
    Returns project ID if found, None otherwise.
    """
    projects = list_todoist_projects()
    normalized_name = project_name.lower().strip()
    
    for project in projects:
        if project.get("name", "").lower().strip() == normalized_name:
            logger.info(f"Found existing Todoist project: {project['name']} ({project['id']})")
            return str(project["id"])
    
    return None


def create_todoist_project(project_name):
    """
    Create a new Todoist project.
    Returns project ID on success.
    """
    url = "https://api.todoist.com/rest/v2/projects"
    headers = {
        "Authorization": f"Bearer {TODOIST_API_KEY}",
        "Content-Type": "application/json"
    }
    
    data = {
        "name": project_name
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        logger.info(f"Created Todoist project: {project_name} ({result.get('id')})")
        return str(result.get("id"))
    except Exception as e:
        logger.error(f"Failed to create Todoist project: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        return None


def get_case_name_from_notion(matter_id):
    """
    Get the case name/caption from Legal Cases database.
    """
    url = f"https://api.notion.com/v1/pages/{matter_id}"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": "2022-06-28"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        page = response.json()
        
        props = page.get('properties', {})
        
        # Try common property names for case title
        for prop_name in ['Name', 'Title', 'Caption', 'Case Name']:
            prop = props.get(prop_name, {})
            if prop.get('title'):
                titles = prop['title']
                if titles:
                    return titles[0].get('plain_text', '')
        
        return None
    except Exception as e:
        logger.error(f"Failed to get case name: {e}")
        return None


def create_mapping_entry(matter_id, todoist_project_id, case_name):
    """
    Create entry in Mappings database linking matter to Todoist project.
    """
    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    properties = {
        "Name": {
            "title": [{"text": {"content": case_name or "Auto-mapped"}}]
        },
        "Case": {
            "relation": [{"id": matter_id}]
        },
        "Todoist_project_id": {
            "rich_text": [{"text": {"content": str(todoist_project_id)}}]
        }
    }
    
    data = {
        "parent": {"database_id": MAPPINGS_DATABASE_ID},
        "properties": properties
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        logger.info(f"Created mapping entry for {case_name}: {matter_id} → {todoist_project_id}")
        return result.get('id')
    except Exception as e:
        logger.error(f"Failed to create mapping entry: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        return None


def get_or_create_todoist_project_for_matter(matter_id):
    """
    Get Todoist project for a matter, creating one if it doesn't exist.
    
    Deduplication logic:
    1. Check Mappings database - if mapping exists, use that project ID
    2. Get case name from Notion
    3. Check Todoist for project with that name - if exists, create mapping and use it
    4. If neither exists, create new project and mapping
    
    Returns (todoist_project_id, mapping_id, was_created)
    """
    if not matter_id:
        return None, None, False
    
    # Step 1: Check Mappings database
    todoist_project_id, mapping_id = get_todoist_project_for_matter(matter_id)
    if todoist_project_id:
        logger.info(f"Found existing mapping for matter {matter_id}")
        return todoist_project_id, mapping_id, False
    
    # Step 2: Get case name from Notion
    case_name = get_case_name_from_notion(matter_id)
    if not case_name:
        logger.warning(f"Could not get case name for matter {matter_id}")
        return None, None, False
    
    logger.info(f"No mapping found for '{case_name}'. Checking Todoist...")
    
    # Step 3: Check if Todoist project already exists with this name
    existing_project_id = find_todoist_project_by_name(case_name)
    
    if existing_project_id:
        # Project exists in Todoist but no mapping - create mapping
        logger.info(f"Found existing Todoist project '{case_name}'. Creating mapping...")
        mapping_id = create_mapping_entry(matter_id, existing_project_id, case_name)
        return existing_project_id, mapping_id, False
    
    # Step 4: Create new Todoist project and mapping
    logger.info(f"Creating new Todoist project for '{case_name}'...")
    new_project_id = create_todoist_project(case_name)
    
    if not new_project_id:
        logger.error(f"Failed to create Todoist project for {case_name}")
        return None, None, False
    
    # Create mapping entry
    mapping_id = create_mapping_entry(matter_id, new_project_id, case_name)
    
    return new_project_id, mapping_id, True


def get_todoist_project_for_matter(matter_id):
    """
    Query Mappings database to find Todoist project ID for a matter.
    Returns (project_id, mapping_id) or (None, None) if no mapping found.
    
    NOTE: This only checks existing mappings. Use get_or_create_todoist_project_for_matter
    to auto-create projects when needed.
    """
    if not matter_id:
        return None, None  # Will use Todoist inbox
    
    url = "https://api.notion.com/v1/databases/" + MAPPINGS_DATABASE_ID + "/query"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # Query for mapping with this matter
    data = {
        "filter": {
            "property": "Case",
            "relation": {
                "contains": matter_id
            }
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        results = response.json().get('results', [])
        
        if results:
            mapping = results[0]
            props = mapping.get('properties', {})
            
            # Extract Todoist project ID from the mapping
            # Assuming there's a "Todoist_project_id" property (text or number)
            project_prop = props.get('Todoist_project_id', {})
            if project_prop.get('type') == 'rich_text':
                texts = project_prop.get('rich_text', [])
                if texts:
                    return texts[0].get('plain_text'), mapping.get('id')
            elif project_prop.get('type') == 'number':
                return str(int(project_prop.get('number', 0))), mapping.get('id')
        
        return None, None
        
    except Exception as e:
        logger.error(f"Mappings lookup failed: {e}")
        return None, None


def get_matter_for_todoist_project(todoist_project_id):
    """
    Reverse lookup: Query Mappings database to find matter ID for a Todoist project.
    Returns (matter_id, case_name) or (None, None) if no mapping found.
    
    Used by reverse sync to determine if a Todoist task belongs to a legal matter.
    If no mapping exists, the project is personal (Home, Inbox, etc.) and should be ignored.
    """
    if not todoist_project_id:
        return None, None
    
    url = "https://api.notion.com/v1/databases/" + MAPPINGS_DATABASE_ID + "/query"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # Query for mapping with this Todoist project ID
    data = {
        "filter": {
            "property": "Todoist_project_id",
            "rich_text": {
                "equals": str(todoist_project_id)
            }
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        results = response.json().get('results', [])
        
        if results:
            mapping = results[0]
            props = mapping.get('properties', {})
            
            # Get the Case relation
            case_relation = props.get('Case', {}).get('relation', [])
            if case_relation:
                matter_id = case_relation[0].get('id')
                
                # Get case name from Title property
                title_prop = props.get('Title', props.get('Name', {}))
                case_name = None
                if title_prop.get('type') == 'title':
                    titles = title_prop.get('title', [])
                    if titles:
                        case_name = titles[0].get('plain_text', '')
                
                logger.info(f"Found matter {matter_id} for Todoist project {todoist_project_id}")
                return matter_id, case_name
        
        logger.debug(f"No mapping found for Todoist project {todoist_project_id} - personal project")
        return None, None
        
    except Exception as e:
        logger.error(f"Reverse mapping lookup failed: {e}")
        return None, None


def check_notion_task_exists_by_todoist_id(todoist_task_id):
    """
    Check if a Notion task already exists with this Todoist Task ID.
    Used to prevent duplicate creation during reverse sync.
    """
    # Reuse existing function
    task = find_notion_task_by_todoist_id(str(todoist_task_id))
    return task is not None


def create_notion_task_from_todoist(todoist_task_id, title, due_date, project_id, matter_id, description=None):
    """
    Create a Notion task from a manually-added Todoist task (reverse sync).
    
    This creates an "Approved" task in Notion with the Todoist Task ID pre-populated,
    so completion sync will work correctly.
    """
    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # Build properties - task is already in Todoist so status is "Approved"
    properties = {
        "title": {
            "title": [{"text": {"content": title}}]
        },
        NOTION_PROPS["status"]: {
            "status": {"name": "Approved"}
        },
        NOTION_PROPS["priority"]: {
            "select": {"name": "P2"}  # Default priority for manual tasks
        },
        NOTION_PROPS["llm_model"]: {
            "rich_text": [{"text": {"content": "manual (Todoist)"}}]
        },
        NOTION_PROPS["llm_rationale"]: {
            "rich_text": [{"text": {"content": "Task created directly in Todoist - synced to Notion for tracking."}}]
        },
        # Store Todoist Task ID for completion sync
        "Todoist Task ID": {
            "rich_text": [{"text": {"content": str(todoist_task_id)}}]
        },
    }
    
    # Add due date if present
    if due_date:
        properties[NOTION_PROPS["due_date"]] = {
            "date": {"start": due_date}
        }
    
    # Add matter relation
    if matter_id:
        properties[NOTION_PROPS["matter"]] = {
            "relation": [{"id": matter_id}]
        }
    
    data = {
        "parent": {"database_id": TASKS_DATABASE_ID},
        "properties": properties
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        notion_page_id = result['id']
        logger.info(f"Created Notion task from Todoist: {title} ({notion_page_id})")
        return notion_page_id
    except Exception as e:
        logger.error(f"Failed to create Notion task from Todoist: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        return None
    """
    Create task in Todoist via REST API.
    Returns Todoist task ID on success.
    """
    url = "https://api.todoist.com/rest/v2/tasks"
    headers = {
        "Authorization": f"Bearer {TODOIST_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # Map priority: Notion P0-P3 to Todoist 4-1 (Todoist 4 is highest)
    priority_map = {"P0": 4, "P1": 3, "P2": 2, "P3": 1}
    todoist_priority = priority_map.get(priority, 2)
    
    data = {
        "content": title,
        "priority": todoist_priority
    }
    
    if due_date:
        data["due_date"] = due_date
    
    if project_id:
        data["project_id"] = project_id
    
    if description:
        data["description"] = description
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        logger.info(f"Created Todoist task: {result.get('id')} - {title}")
        return result.get('id'), result.get('url')
    except Exception as e:
        logger.error(f"Todoist task creation failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        return None, None


def create_todoist_subtask(title, parent_id, due_date=None, project_id=None):
    """
    Create subtask in Todoist with parent_id reference.
    """
    url = "https://api.todoist.com/rest/v2/tasks"
    headers = {
        "Authorization": f"Bearer {TODOIST_API_KEY}",
        "Content-Type": "application/json"
    }
    
    data = {
        "content": title,
        "parent_id": parent_id,
        "priority": 2  # Default priority for subtasks
    }
    
    if due_date:
        data["due_date"] = due_date
    
    if project_id:
        data["project_id"] = project_id
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        logger.info(f"Created Todoist subtask: {result.get('id')} - {title}")
        return result.get('id')
    except Exception as e:
        logger.error(f"Todoist subtask creation failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        return None


def calculate_subtask_due_date(parent_due_date, offset_days):
    """
    Calculate subtask due date based on parent due date and offset.
    offset_days is negative for days BEFORE parent deadline.
    """
    if not parent_due_date:
        return None
    
    try:
        parent_dt = datetime.strptime(parent_due_date, '%Y-%m-%d')
        subtask_dt = parent_dt + timedelta(days=offset_days)
        return subtask_dt.strftime('%Y-%m-%d')
    except Exception as e:
        logger.error(f"Failed to calculate subtask due date: {e}")
        return None
    """
    Update Notion task with Todoist task ID after promotion.
    """
    url = f"https://api.notion.com/v1/pages/{notion_page_id}"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    data = {
        "properties": {
            "Todoist Task ID": {
                "rich_text": [{"text": {"content": str(todoist_task_id)}}]
            },
            "Status": {
                "status": {"name": status}
            }
        }
    }
    
    try:
        response = requests.patch(url, headers=headers, json=data)
        response.raise_for_status()
        logger.info(f"Updated Notion task {notion_page_id} with Todoist ID {todoist_task_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to update Notion task: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        return False


def log_promotion_event(notion_page_id, task_title, todoist_task_id, promotion_key):
    """
    Log promotion to Supabase audit trail.
    """
    url = f"{SUPABASE_URL}/rest/v1/task_events"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    data = {
        "notion_row_id": notion_page_id,
        "event_type": "Promoted",
        "ts": datetime.utcnow().isoformat(),
        "actor": "System",
        "details_json": json.dumps({
            "task_title": task_title,
            "todoist_task_id": todoist_task_id
        }),
        "promotion_key": promotion_key
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        logger.info(f"Logged promotion event for {notion_page_id}")
    except Exception as e:
        logger.error(f"Failed to log promotion event: {e}")


def create_matter_activity(matter_id, activity_type, description, related_task_id=None, source="System"):
    """
    Create Matter Activity entry in Notion.
    Uses property IDs from blueprint for reliability.
    """
    if not matter_id:
        logger.warning("No matter_id provided for activity logging")
        return None
    
    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # Use property IDs from MATTER_ACTIVITY_PROPS
    properties = {
        # Title (Name) - use "title" key
        "title": {
            "title": [{"text": {"content": description}}]
        },
        # Type: Note, Completed, Promoted, Proposed
        MATTER_ACTIVITY_PROPS["type"]: {
            "select": {"name": activity_type}
        },
        # Source: System, Manual, Todoist, Email
        MATTER_ACTIVITY_PROPS["source"]: {
            "select": {"name": source}
        },
        # Case relation
        MATTER_ACTIVITY_PROPS["case"]: {
            "relation": [{"id": matter_id}]
        }
    }
    
    # Related Task ID relation
    if related_task_id:
        properties[MATTER_ACTIVITY_PROPS["related_task"]] = {
            "relation": [{"id": related_task_id}]
        }
    
    data = {
        "parent": {"database_id": MATTER_ACTIVITY_DATABASE_ID},
        "properties": properties
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        logger.info(f"Created Matter Activity: {description}")
        return result.get('id')
    except Exception as e:
        logger.error(f"Failed to create Matter Activity: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        return None


# =============================================================================
# FLOW 3: COMPLETION SYNC
# =============================================================================

def find_notion_task_by_todoist_id(todoist_task_id):
    """
    Search Tasks (Proposed) database for task with matching Todoist Task ID.
    """
    url = f"https://api.notion.com/v1/databases/{TASKS_DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    data = {
        "filter": {
            "property": "Todoist Task ID",
            "rich_text": {
                "equals": str(todoist_task_id)
            }
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        results = response.json().get('results', [])
        
        if results:
            task = results[0]
            props = task.get('properties', {})
            
            # Extract task details
            title_prop = props.get('Name', {}) or props.get('Title', {})
            title = ""
            if title_prop.get('title'):
                title = title_prop['title'][0]['plain_text'] if title_prop['title'] else ""
            
            # Get matter relation
            matter_prop = props.get('Matter', {})
            matter_id = None
            if matter_prop.get('relation'):
                relations = matter_prop['relation']
                if relations:
                    matter_id = relations[0].get('id')
            
            return {
                "id": task.get('id'),
                "title": title,
                "matter_id": matter_id
            }
        
        return None
        
    except Exception as e:
        logger.error(f"Notion task search failed: {e}")
        return None


def mark_notion_task_completed(notion_page_id):
    """
    Update Notion task with completed_at timestamp and status.
    """
    url = f"https://api.notion.com/v1/pages/{notion_page_id}"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    data = {
        "properties": {
            "Status": {
                "status": {"name": "Completed"}
            },
            "Completed At": {
                "date": {"start": datetime.utcnow().isoformat()}
            }
        }
    }
    
    try:
        response = requests.patch(url, headers=headers, json=data)
        response.raise_for_status()
        logger.info(f"Marked Notion task {notion_page_id} as completed")
        return True
    except Exception as e:
        logger.error(f"Failed to mark task completed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        return False


def log_completion_event(notion_page_id, task_title, todoist_task_id):
    """
    Log completion to Supabase audit trail.
    """
    url = f"{SUPABASE_URL}/rest/v1/task_events"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    data = {
        "notion_row_id": notion_page_id,
        "event_type": "Completed",
        "ts": datetime.utcnow().isoformat(),
        "actor": "Todoist",
        "details_json": json.dumps({
            "task_title": task_title,
            "todoist_task_id": todoist_task_id
        })
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        logger.info(f"Logged completion event for {notion_page_id}")
    except Exception as e:
        logger.error(f"Failed to log completion event: {e}")


def log_orphan_completion(todoist_task_id, task_content):
    """
    Log completion of a task that wasn't found in Notion (orphan).
    """
    url = f"{SUPABASE_URL}/rest/v1/task_events"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    data = {
        "event_type": "CompletedOrphan",
        "ts": datetime.utcnow().isoformat(),
        "actor": "Todoist",
        "details_json": json.dumps({
            "todoist_task_id": todoist_task_id,
            "task_content": task_content
        })
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        logger.info(f"Logged orphan completion for Todoist task {todoist_task_id}")
    except Exception as e:
        logger.error(f"Failed to log orphan completion: {e}")


# =============================================================================
# MAIN WEBHOOK ENDPOINT
# =============================================================================

@app.route('/webhook', methods=['POST'])
def webhook():
    """
    Main endpoint for email intake.
    Replicates entire Flow 1 logic with dual-model extraction:
    - Attachments → Gemini 2.5 Pro (native multimodal)
    - Email body → Claude (text extraction)
    """
    try:
        # Get payload - handle various formats
        if request.is_json:
            payload = request.json
        else:
            # Try to parse as JSON anyway
            try:
                payload = json.loads(request.data.decode('utf-8'))
            except:
                payload = request.form.to_dict()
        
        # Handle if payload is wrapped in an array
        if isinstance(payload, list) and len(payload) > 0:
            payload = payload[0]
        
        logger.info(f"Received webhook payload type: {type(payload)}")
        logger.info(f"Received email subject: {payload.get('subject', 'No subject')}")
        
        # Step 1: Parse email
        email_data = parse_email(payload)
        logger.info(f"Parsed email from: {email_data['original_sender']}")
        
        # Step 2: Compute fingerprint
        fingerprint = compute_fingerprint(
            email_data['message_id'],
            email_data['original_sender'],
            email_data['received_at']
        )
        logger.info(f"Fingerprint: {fingerprint[:16]}...")
        
        # Step 3: Check for duplicate
        if check_duplicate(fingerprint):
            logger.info("Duplicate email detected, skipping")
            return jsonify({"status": "duplicate", "fingerprint": fingerprint})
        
        # Step 4: Log email receipt
        log_email_receipt(
            fingerprint,
            email_data['original_sender'],
            email_data['received_at'],
            email_data['message_id'],
            normalize_subject(email_data['subject'])
        )
        
        # =================================================================
        # DUAL-MODEL EXTRACTION
        # =================================================================
        
        all_tasks = []
        all_calendar_items = []
        attachment_processed = False
        gemini_index_number = None
        gemini_caption = None
        
        # Step 5a: Process attachments with Gemini (if present)
        attachments = payload.get('attachments', []) or []
        if attachments and GOOGLE_API_KEY:
            logger.info(f"Processing {len(attachments)} attachment(s) with Gemini...")
            
            for attachment in attachments:
                # Get attachment data
                if isinstance(attachment, dict):
                    file_data = attachment.get('data', attachment.get('content', ''))
                    file_name = attachment.get('fileName', attachment.get('filename', 'document'))
                    mime_type = attachment.get('contentType', attachment.get('mimeType', 'application/pdf'))
                else:
                    logger.warning(f"Unexpected attachment format: {type(attachment)}")
                    continue
                
                if not file_data:
                    logger.warning(f"No data in attachment: {file_name}")
                    continue
                
                # Extract with Gemini
                gemini_result = extract_with_gemini(file_data, file_name, mime_type)
                
                if gemini_result:
                    attachment_processed = True
                    
                    # Collect tasks (already have source_model set)
                    all_tasks.extend(gemini_result.get('tasks', []))
                    all_calendar_items.extend(gemini_result.get('calendar_items', []))
                    
                    # Use index_number from document for case matching (prefer first found)
                    if not gemini_index_number:
                        idx = gemini_result.get('index_number', {})
                        if isinstance(idx, dict):
                            gemini_index_number = idx.get('value')
                        elif isinstance(idx, str):
                            gemini_index_number = idx
                    
                    # Use caption from document
                    if not gemini_caption:
                        cap = gemini_result.get('caption', {})
                        if isinstance(cap, dict):
                            gemini_caption = cap.get('value')
                        elif isinstance(cap, str):
                            gemini_caption = cap
                    
                    logger.info(f"Gemini extracted from {file_name}: {len(gemini_result.get('tasks', []))} tasks, {len(gemini_result.get('calendar_items', []))} calendar items")
        
        elif attachments and not GOOGLE_API_KEY:
            logger.warning("Attachments present but GOOGLE_API_KEY not configured - skipping document extraction")
        
        # Step 5b: Extract from email body with Claude (always, for context)
        logger.info("Extracting from email body with Claude...")
        claude_extraction = extract_tasks_with_claude(email_data)
        
        # Add source_model to Claude tasks
        for task in claude_extraction.get('tasks', []):
            task['source_model'] = 'claude-sonnet-4-20250514'
        for item in claude_extraction.get('calendar_items', []):
            item['source_model'] = 'claude-sonnet-4-20250514'
        
        all_tasks.extend(claude_extraction.get('tasks', []))
        all_calendar_items.extend(claude_extraction.get('calendar_items', []))
        
        logger.info(f"Total extracted: {len(all_tasks)} tasks, {len(all_calendar_items)} calendar items")
        
        # =================================================================
        # MATTER MATCHING
        # =================================================================
        
        # Step 6: Get index number and caption for matter matching
        # Prefer Gemini (from document) over Claude (from email body)
        index_number = gemini_index_number or claude_extraction.get('index_number', {}).get('value')
        caption = gemini_caption or claude_extraction.get('caption', {}).get('value')
        
        # Step 7: Search for matching matter, create stub if not found
        matter_id = None
        stub_created = False
        
        # Extract case name from caption (e.g., "Smith" from "Smith v. Anderson")
        case_name = extract_first_plaintiff(caption) if caption else None
        
        # Primary match: Try case name first (most common scenario)
        if case_name:
            matter_id = find_matter_by_case_name(case_name)
            if matter_id:
                logger.info(f"Found matching matter by case name '{case_name}': {matter_id}")
        
        # Secondary match: Try index number if case name didn't work
        if not matter_id and index_number:
            matter_id = find_matter_by_index_number(index_number)
            if not matter_id:
                matter_id = search_case_by_index(index_number)  # Fallback search
            if matter_id:
                logger.info(f"Found matching matter by index '{index_number}': {matter_id}")
        
        # If still no matter found, create stub
        if not matter_id and (case_name or caption):
            # Before creating stub, double-check index doesn't exist (dedup)
            if index_number:
                existing = find_matter_by_index_number(index_number) or search_case_by_index(index_number)
                if existing:
                    matter_id = existing
                    logger.info(f"Found existing matter by index during stub check: {matter_id}")
            
            if not matter_id:
                # Create stub - index_number may be None, that's OK
                matter_id = create_stub_matter(index_number, caption)
                if matter_id:
                    stub_created = True
                    logger.info(f"Created stub matter for '{case_name or caption}': {matter_id}")
        
        # =================================================================
        # CREATE NOTION ITEMS
        # =================================================================
        
        # Step 8: Create tasks in Notion
        created_tasks = []
        for task in all_tasks:
            # Get source model for this task
            llm_model = task.get('source_model', 'claude-sonnet-4-20250514')
            
            status = determine_status(task, matter_id, email_data['has_attachment'], attachment_processed)
            notion_id = create_notion_task(task, email_data, fingerprint, matter_id, status, llm_model)
            
            if notion_id:
                task_title = task.get('title', {}).get('value')
                task_due_date = task.get('due_date', {}).get('value')
                
                created_tasks.append({
                    "title": task_title,
                    "notion_id": notion_id,
                    "status": status,
                    "llm_model": llm_model
                })
                
                # Log task event
                log_task_event(
                    fingerprint,
                    task_title,
                    "proposed",
                    notion_id
                )
                
                # Store content fingerprint for semantic deduplication
                # Prevents duplicates if user also creates same task manually in Todoist
                content_fp = compute_task_content_fingerprint(task_title, task_due_date, matter_id)
                store_task_content_fingerprint(content_fp, task_title, matter_id, notion_id, "flow1")
        
        # Step 9: Handle calendar items (create in Calendar Items database)
        for event in all_calendar_items:
            llm_model = event.get('source_model', 'claude-sonnet-4-20250514')
            status = "Proposed" if matter_id else "Needs Review"
            notion_id = create_notion_calendar_item(event, email_data, fingerprint, matter_id, status, llm_model)
            
            if notion_id:
                created_tasks.append({
                    "title": event.get('title', {}).get('value'),
                    "notion_id": notion_id,
                    "status": status,
                    "type": "calendar",
                    "llm_model": llm_model
                })
        
        logger.info(f"Processing complete. Created {len(created_tasks)} items.")
        
        return jsonify({
            "status": "processed",
            "fingerprint": fingerprint,
            "tasks_created": len(created_tasks),
            "tasks": created_tasks,
            "has_attachment": email_data['has_attachment'],
            "attachment_processed": attachment_processed,
            "matter_id": matter_id,
            "stub_created": stub_created,
            "index_number_found": index_number
        })
        
    except Exception as e:
        logger.error(f"Webhook processing failed: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "service": "anais-intake"})


# =============================================================================
# FLOW 2: PROMOTION WEBHOOK ENDPOINT
# =============================================================================

@app.route('/promotion-webhook', methods=['POST'])
def promotion_webhook():
    """
    Flow 2: Promotion to Todoist
    Triggered by Notion automation when task status changes to "Approved"
    
    Expected payload from Notion automation:
    {
        "page_id": "notion-page-uuid",
        "properties": { ... full page properties ... }
    }
    
    Or direct Notion webhook format with full page data.
    """
    try:
        payload = request.json
        logger.info(f"Promotion webhook received")
        
        # Handle Notion automation webhook format
        # Notion sends the page data in 'data' object
        if 'data' in payload:
            page_data = payload['data']
        else:
            page_data = payload
        
        # Extract page ID
        notion_page_id = page_data.get('id') or page_data.get('page_id')
        if not notion_page_id:
            return jsonify({"status": "error", "message": "No page ID in payload"}), 400
        
        logger.info(f"Processing promotion for page: {notion_page_id}")
        
        # Extract properties - if not provided, fetch from Notion
        properties = page_data.get('properties', {})
        
        if not properties:
            # Fetch page from Notion API
            logger.info(f"No properties in payload, fetching from Notion...")
            fetch_url = f"https://api.notion.com/v1/pages/{notion_page_id}"
            fetch_headers = {
                "Authorization": f"Bearer {NOTION_API_KEY}",
                "Notion-Version": "2022-06-28"
            }
            fetch_response = requests.get(fetch_url, headers=fetch_headers)
            if fetch_response.status_code != 200:
                logger.error(f"Failed to fetch page: {fetch_response.text}")
                return jsonify({"status": "error", "message": "Failed to fetch page from Notion"}), 400
            page_data = fetch_response.json()
            properties = page_data.get('properties', {})
        
        # Get task title
        title_prop = properties.get('Name', {}) or properties.get('Title', {})
        task_title = ""
        if title_prop.get('title'):
            task_title = title_prop['title'][0]['plain_text'] if title_prop['title'] else ""
        
        # Get due date
        due_date_prop = properties.get('Due Date', {}) or properties.get('Due', {})
        due_date = None
        if due_date_prop.get('date'):
            due_date = due_date_prop['date'].get('start')
        
        # Get priority
        priority_prop = properties.get('Priority', {})
        priority = "P2"  # Default
        if priority_prop.get('select'):
            priority = priority_prop['select'].get('name', 'P2')
        
        # Get matter relation
        matter_prop = properties.get('Matter', {})
        matter_id = None
        if matter_prop.get('relation'):
            relations = matter_prop['relation']
            if relations:
                matter_id = relations[0].get('id')
        
        # Get status to verify it's actually "Approved"
        status_prop = properties.get('Status', {})
        status = None
        if status_prop.get('status'):
            status = status_prop['status'].get('name')
        
        if status != "Approved":
            logger.info(f"Task status is '{status}', not 'Approved'. Skipping promotion.")
            return jsonify({"status": "skipped", "reason": f"Status is {status}, not Approved"})
        
        # Check if already has Todoist ID (already promoted)
        todoist_id_prop = properties.get('Todoist Task ID', {})
        existing_todoist_id = None
        if todoist_id_prop.get('rich_text'):
            texts = todoist_id_prop['rich_text']
            if texts:
                existing_todoist_id = texts[0].get('plain_text')
        
        if existing_todoist_id:
            logger.info(f"Task already has Todoist ID: {existing_todoist_id}. Skipping.")
            return jsonify({"status": "skipped", "reason": "Already promoted", "todoist_id": existing_todoist_id})
        
        # Get Original Sender for Todoist description
        sender_prop = properties.get('Original Sender', {})
        original_sender = ""
        if sender_prop.get('email'):
            original_sender = sender_prop['email']
        
        # Get subtasks_json if present
        subtasks_json_prop = properties.get('Subtasks JSON', {})
        subtasks = []
        if subtasks_json_prop.get('rich_text'):
            texts = subtasks_json_prop['rich_text']
            if texts:
                try:
                    subtasks = json.loads(texts[0].get('plain_text', '[]'))
                except json.JSONDecodeError:
                    logger.warning("Failed to parse subtasks_json")
                    subtasks = []
        
        # Get or create Todoist project for this matter (auto-creates if needed)
        todoist_project_id, mapping_id, project_was_created = get_or_create_todoist_project_for_matter(matter_id)
        
        if project_was_created:
            logger.info(f"Auto-created Todoist project for matter {matter_id}: {todoist_project_id}")
        elif todoist_project_id:
            logger.info(f"Using existing Todoist project for matter {matter_id}: {todoist_project_id}")
        else:
            logger.warning(f"No Todoist project for matter {matter_id}. Task will go to inbox.")
        
        # Compute promotion key for idempotency
        promotion_key = compute_promotion_key(notion_page_id, task_title, due_date, todoist_project_id)
        logger.info(f"Promotion key: {promotion_key[:16]}...")
        
        # Check idempotency - has this exact promotion happened before?
        if check_promotion_exists(promotion_key):
            logger.info(f"Promotion key already exists. Idempotency check passed - skipping.")
            return jsonify({"status": "skipped", "reason": "Already promoted (idempotency)"})
        
        # Build rich description for Todoist (matches Make.com blueprint)
        notion_link = f"https://notion.so/{notion_page_id.replace('-', '')}"
        description_parts = []
        if original_sender:
            description_parts.append(f"From: {original_sender}")
        description_parts.append(f"Notion: {notion_link}")
        todoist_description = "\n".join(description_parts)
        
        # Create Todoist task
        todoist_task_id, todoist_url = create_todoist_task(
            title=task_title,
            due_date=due_date,
            priority=priority,
            project_id=todoist_project_id,
            description=todoist_description
        )
        
        if not todoist_task_id:
            logger.error("Failed to create Todoist task")
            return jsonify({"status": "error", "message": "Todoist task creation failed"}), 500
        
        # Create subtasks in Todoist if present
        created_subtasks = []
        if subtasks:
            logger.info(f"Creating {len(subtasks)} subtasks...")
            for subtask in subtasks:
                subtask_title = subtask.get('title', '')
                offset_days = subtask.get('offset_days', 0)
                
                # Calculate subtask due date
                subtask_due = calculate_subtask_due_date(due_date, offset_days) if due_date else None
                
                subtask_id = create_todoist_subtask(
                    title=subtask_title,
                    parent_id=todoist_task_id,
                    due_date=subtask_due,
                    project_id=todoist_project_id
                )
                
                if subtask_id:
                    created_subtasks.append({
                        "title": subtask_title,
                        "todoist_id": subtask_id,
                        "due_date": subtask_due
                    })
            
            logger.info(f"Created {len(created_subtasks)} subtasks")
        
        # Update Notion with Todoist task ID
        update_notion_task_with_todoist(notion_page_id, todoist_task_id)
        
        # Log promotion event to audit trail
        log_promotion_event(notion_page_id, task_title, todoist_task_id, promotion_key)
        
        # Store content fingerprint for semantic deduplication
        # This prevents reverse sync from creating a duplicate if user also adds task manually in Todoist
        content_fingerprint = compute_task_content_fingerprint(task_title, due_date, matter_id)
        store_task_content_fingerprint(content_fingerprint, task_title, matter_id, notion_page_id, "promotion")
        
        # Create Matter Activity if we have a matter
        if matter_id:
            create_matter_activity(
                matter_id=matter_id,
                activity_type="Promoted",
                description=f"Promoted: {task_title}",
                related_task_id=notion_page_id,
                source="Todoist"
            )
        
        logger.info(f"Promotion complete: {task_title} → Todoist {todoist_task_id}")
        
        return jsonify({
            "status": "promoted",
            "notion_page_id": notion_page_id,
            "todoist_task_id": todoist_task_id,
            "todoist_url": todoist_url,
            "promotion_key": promotion_key,
            "subtasks_created": len(created_subtasks),
            "subtasks": created_subtasks
        })
        
    except Exception as e:
        logger.error(f"Promotion webhook failed: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


# =============================================================================
# FLOW 3: TODOIST WEBHOOK ENDPOINT (Completion + Reverse Sync)
# =============================================================================

@app.route('/todoist-webhook', methods=['POST'])
def todoist_webhook():
    """
    Todoist webhook handler for:
    1. item:completed - Sync completions back to Notion
    2. item:added - Reverse sync manual tasks from legal projects to Notion
    
    Todoist webhook payload:
    {
        "event_name": "item:completed" | "item:added",
        "user_id": "...",
        "event_data": {
            "id": "todoist-task-id",
            "content": "Task title",
            "project_id": "...",
            "due": {"date": "2025-02-15", ...},
            ...
        }
    }
    """
    try:
        payload = request.json
        event_name = payload.get('event_name', '')
        logger.info(f"Todoist webhook received: {event_name}")
        
        event_data = payload.get('event_data', {})
        todoist_task_id = str(event_data.get('id', ''))
        task_content = event_data.get('content', '')
        project_id = str(event_data.get('project_id', ''))
        
        if not todoist_task_id:
            return jsonify({"status": "error", "message": "No task ID in payload"}), 400
        
        # Route by event type
        if event_name in ['item:completed', 'item:complete']:
            return handle_todoist_completion(todoist_task_id, task_content)
        elif event_name in ['item:added', 'item:add']:
            return handle_todoist_item_added(todoist_task_id, task_content, project_id, event_data)
        else:
            logger.info(f"Ignoring event: {event_name}")
            return jsonify({"status": "ignored", "reason": f"Event {event_name} not handled"})
        
    except Exception as e:
        logger.error(f"Todoist webhook failed: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


def handle_todoist_completion(todoist_task_id, task_content):
    """Handle item:completed event - sync completion back to Notion."""
    logger.info(f"Processing completion for Todoist task: {todoist_task_id}")
    
    # Find corresponding Notion task
    notion_task = find_notion_task_by_todoist_id(todoist_task_id)
    
    if not notion_task:
        # Orphan task - completed in Todoist but not found in Notion
        # This is expected for personal tasks (Home, Inbox, etc.)
        logger.info(f"Orphan completion: Todoist task {todoist_task_id} not found in Notion")
        log_orphan_completion(todoist_task_id, task_content)
        return jsonify({
            "status": "orphan",
            "todoist_task_id": todoist_task_id,
            "message": "Task not found in Notion - logged as orphan"
        })
    
    notion_page_id = notion_task['id']
    task_title = notion_task['title']
    matter_id = notion_task.get('matter_id')
    
    # Mark task as completed in Notion
    success = mark_notion_task_completed(notion_page_id)
    
    if not success:
        logger.error(f"Failed to mark Notion task {notion_page_id} as completed")
        return jsonify({"status": "error", "message": "Failed to update Notion"}), 500
    
    # Log completion event
    log_completion_event(notion_page_id, task_title, todoist_task_id)
    
    # Create Matter Activity if we have a matter
    if matter_id:
        create_matter_activity(
            matter_id=matter_id,
            activity_type="Completed",
            description=f"Completed: {task_title}",
            related_task_id=notion_page_id,
            source="Todoist"
        )
    
    logger.info(f"Completion sync complete: {task_title}")
    
    return jsonify({
        "status": "completed",
        "notion_page_id": notion_page_id,
        "todoist_task_id": todoist_task_id,
        "task_title": task_title
    })


def handle_todoist_item_added(todoist_task_id, task_content, project_id, event_data):
    """
    Handle item:added event - reverse sync from Todoist to Notion.
    
    Only syncs tasks from LEGAL projects (those with mappings).
    Personal projects (Home, Inbox, etc.) are ignored.
    
    Deduplication:
    1. Check if Notion task exists with this Todoist ID (promoted task)
    2. Check content fingerprint (same title + due + matter = duplicate)
    """
    logger.info(f"Processing item:added for Todoist task: {todoist_task_id} in project {project_id}")
    
    # Step 1: Check if this project is mapped to a legal matter
    matter_id, case_name = get_matter_for_todoist_project(project_id)
    
    if not matter_id:
        # No mapping = personal project (Home, Inbox, Groceries, etc.)
        logger.info(f"Ignoring task in unmapped project {project_id} (personal)")
        return jsonify({
            "status": "ignored",
            "reason": "Personal project - not mapped to any legal matter",
            "todoist_task_id": todoist_task_id,
            "project_id": project_id
        })
    
    # Step 2: Check if Notion task already exists by Todoist ID
    # This catches tasks that were promoted FROM Notion
    if check_notion_task_exists_by_todoist_id(todoist_task_id):
        logger.info(f"Notion task already exists for Todoist {todoist_task_id} - skipping")
        return jsonify({
            "status": "exists",
            "reason": "Notion task already exists (by Todoist ID)",
            "todoist_task_id": todoist_task_id
        })
    
    # Step 3: Extract due date if present
    due_date = None
    due_info = event_data.get('due')
    if due_info:
        due_date = due_info.get('date')  # Format: "2025-02-15"
    
    # Step 4: Check content fingerprint for semantic duplicates
    # This catches: email created task A, user manually creates same task in Todoist
    content_fingerprint = compute_task_content_fingerprint(task_content, due_date, matter_id)
    if check_task_content_fingerprint_exists(content_fingerprint):
        logger.info(f"Content duplicate detected for '{task_content}' - skipping")
        return jsonify({
            "status": "duplicate",
            "reason": "Semantically identical task already exists (same title + due date + matter)",
            "todoist_task_id": todoist_task_id,
            "content_fingerprint": content_fingerprint[:16] + "..."
        })
    
    # Step 5: Create Notion task linked to the matter
    notion_page_id = create_notion_task_from_todoist(
        todoist_task_id=todoist_task_id,
        title=task_content,
        due_date=due_date,
        project_id=project_id,
        matter_id=matter_id
    )
    
    if not notion_page_id:
        return jsonify({"status": "error", "message": "Failed to create Notion task"}), 500
    
    # Step 6: Store content fingerprint for future deduplication
    store_task_content_fingerprint(content_fingerprint, task_content, matter_id, notion_page_id, "reverse_sync")
    
    # Step 7: Log the reverse sync event
    log_task_event(
        fingerprint=f"todoist-{todoist_task_id}",  # Use Todoist ID as pseudo-fingerprint
        task_title=task_content,
        event_type="reverse_sync",
        notion_page_id=notion_page_id
    )
    
    # Step 8: Create Matter Activity
    create_matter_activity(
        matter_id=matter_id,
        activity_type="Proposed",
        description=f"Synced from Todoist: {task_content}",
        related_task_id=notion_page_id,
        source="Todoist"
    )
    
    logger.info(f"Reverse sync complete: {task_content} → Notion {notion_page_id}")
    
    return jsonify({
        "status": "synced",
        "todoist_task_id": todoist_task_id,
        "notion_page_id": notion_page_id,
        "matter_id": matter_id,
        "case_name": case_name,
        "task_title": task_content
    })


# =============================================================================
# FLOW 4: CALENDAR SYNC (Placeholder - requires Google Calendar API)
# =============================================================================

@app.route('/calendar-sync', methods=['POST'])
def calendar_sync():
    """
    Flow 4: Calendar Sync
    Syncs approved Calendar Items from Notion to Google Calendar.
    
    This is a placeholder - full implementation requires Google Calendar API setup.
    Currently returns the items that would be synced.
    """
    try:
        # Query Notion for approved Calendar Items
        url = f"https://api.notion.com/v1/databases/{CALENDAR_DATABASE_ID}/query"
        headers = {
            "Authorization": f"Bearer {NOTION_API_KEY}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        
        data = {
            "filter": {
                "property": "Status",
                "status": {
                    "equals": "Approved"
                }
            }
        }
        
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        results = response.json().get('results', [])
        
        events_to_sync = []
        for item in results:
            props = item.get('properties', {})
            
            # Extract event details
            title_prop = props.get('Title', {}) or props.get('Name', {})
            title = ""
            if title_prop.get('title'):
                title = title_prop['title'][0]['plain_text'] if title_prop['title'] else ""
            
            event_date_prop = props.get('Event Date', {})
            event_date = None
            if event_date_prop.get('date'):
                event_date = event_date_prop['date'].get('start')
            
            # Check if already synced (has Google Event ID)
            gcal_id_prop = props.get('Google Event ID', {})
            already_synced = False
            if gcal_id_prop.get('rich_text'):
                texts = gcal_id_prop['rich_text']
                if texts and texts[0].get('plain_text'):
                    already_synced = True
            
            if not already_synced:
                events_to_sync.append({
                    "notion_id": item.get('id'),
                    "title": title,
                    "event_date": event_date
                })
        
        logger.info(f"Found {len(events_to_sync)} calendar items to sync")
        
        # TODO: When Google Calendar API is configured, actually create events here
        # For now, just return what would be synced
        
        return jsonify({
            "status": "preview",
            "message": "Calendar sync endpoint ready. Configure Google Calendar API to enable.",
            "events_pending": len(events_to_sync),
            "events": events_to_sync
        })
        
    except Exception as e:
        logger.error(f"Calendar sync failed: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


# =============================================================================
# FLOW 5: WEEKLY DIGEST
# =============================================================================

@app.route('/digest', methods=['GET', 'POST'])
def weekly_digest():
    """
    Flow 5: Weekly Digest
    Queries task_events from the last 7 days and returns summary data.
    
    Can be called:
    - GET /digest - Returns digest data as JSON (for Make.com to format and email)
    - POST /digest with {"days": 7} - Customizable time range
    
    The actual email sending remains in Make.com (Microsoft 365 module).
    """
    try:
        # Get time range
        days = 7
        if request.method == 'POST' and request.json:
            days = request.json.get('days', 7)
        
        # Calculate date range
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days)
        
        # Query Supabase for task_events in range
        url = f"{SUPABASE_URL}/rest/v1/task_events"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
        params = {
            "ts": f"gte.{start_date.isoformat()}",
            "order": "ts.desc",
            "limit": 500
        }
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        events = response.json()
        
        # Also query email_receipts
        email_url = f"{SUPABASE_URL}/rest/v1/email_receipts"
        email_params = {
            "created_at": f"gte.{start_date.isoformat()}",
            "order": "created_at.desc",
            "limit": 500
        }
        
        email_response = requests.get(email_url, headers=headers, params=email_params)
        email_response.raise_for_status()
        emails = email_response.json()
        
        # Aggregate by event type
        summary = {
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "days": days
            },
            "totals": {
                "emails_processed": len(emails),
                "tasks_proposed": sum(1 for e in events if e.get('event_type') == 'proposed'),
                "tasks_promoted": sum(1 for e in events if e.get('event_type') == 'Promoted'),
                "tasks_completed": sum(1 for e in events if e.get('event_type') == 'Completed'),
                "orphan_completions": sum(1 for e in events if e.get('event_type') == 'CompletedOrphan'),
            },
            "events": events,
            "emails": emails
        }
        
        # Group by matter if possible (extract from details_json)
        by_matter = {}
        for event in events:
            details = event.get('details_json')
            if details:
                try:
                    if isinstance(details, str):
                        details = json.loads(details)
                    task_title = details.get('task_title', 'Unknown')
                except:
                    task_title = 'Unknown'
            else:
                task_title = event.get('task_title', 'Unknown')
            
            notion_id = event.get('notion_row_id', 'unknown')
            if notion_id not in by_matter:
                by_matter[notion_id] = []
            by_matter[notion_id].append({
                "type": event.get('event_type'),
                "title": task_title,
                "ts": event.get('ts')
            })
        
        summary["by_task"] = by_matter
        
        logger.info(f"Digest generated: {summary['totals']}")
        
        return jsonify(summary)
        
    except Exception as e:
        logger.error(f"Digest generation failed: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


# =============================================================================
# TODOIST ADMIN ENDPOINT
# =============================================================================

@app.route('/todoist-admin', methods=['POST'])
def todoist_admin():
    """
    Admin endpoint for Todoist management.
    
    Actions:
    - {"action": "list_projects"} - List all Todoist projects
    - {"action": "create_project", "name": "Project Name"} - Create a project
    - {"action": "list_labels"} - List all labels
    - {"action": "create_label", "name": "Label Name"} - Create a label
    - {"action": "check_mapping", "matter_id": "..."} - Check if matter has mapping
    """
    try:
        payload = request.json or {}
        action = payload.get('action', '')
        
        if action == 'list_projects':
            projects = list_todoist_projects()
            return jsonify({
                "status": "success",
                "count": len(projects),
                "projects": projects
            })
        
        elif action == 'create_project':
            name = payload.get('name')
            if not name:
                return jsonify({"status": "error", "message": "Missing 'name' parameter"}), 400
            
            # Check for duplicate first
            existing = find_todoist_project_by_name(name)
            if existing:
                return jsonify({
                    "status": "exists",
                    "message": f"Project '{name}' already exists",
                    "project_id": existing
                })
            
            project_id = create_todoist_project(name)
            if project_id:
                return jsonify({
                    "status": "created",
                    "name": name,
                    "project_id": project_id
                })
            else:
                return jsonify({"status": "error", "message": "Failed to create project"}), 500
        
        elif action == 'list_labels':
            url = "https://api.todoist.com/rest/v2/labels"
            headers = {"Authorization": f"Bearer {TODOIST_API_KEY}"}
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            labels = response.json()
            return jsonify({
                "status": "success",
                "count": len(labels),
                "labels": [{"id": l.get("id"), "name": l.get("name")} for l in labels]
            })
        
        elif action == 'create_label':
            name = payload.get('name')
            if not name:
                return jsonify({"status": "error", "message": "Missing 'name' parameter"}), 400
            
            url = "https://api.todoist.com/rest/v2/labels"
            headers = {
                "Authorization": f"Bearer {TODOIST_API_KEY}",
                "Content-Type": "application/json"
            }
            response = requests.post(url, headers=headers, json={"name": name})
            response.raise_for_status()
            label = response.json()
            return jsonify({
                "status": "created",
                "name": name,
                "label_id": label.get("id")
            })
        
        elif action == 'check_mapping':
            matter_id = payload.get('matter_id')
            if not matter_id:
                return jsonify({"status": "error", "message": "Missing 'matter_id' parameter"}), 400
            
            project_id, mapping_id = get_todoist_project_for_matter(matter_id)
            case_name = get_case_name_from_notion(matter_id)
            
            return jsonify({
                "status": "success",
                "matter_id": matter_id,
                "case_name": case_name,
                "has_mapping": project_id is not None,
                "todoist_project_id": project_id,
                "mapping_id": mapping_id
            })
        
        else:
            return jsonify({
                "status": "error",
                "message": f"Unknown action: {action}",
                "available_actions": ["list_projects", "create_project", "list_labels", "create_label", "check_mapping"]
            }), 400
            
    except Exception as e:
        logger.error(f"Todoist admin failed: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/sync-matters', methods=['POST'])
def sync_matters():
    """
    Bulk sync: Create Todoist projects for all Legal Cases that don't have mappings.
    
    Options:
    - {"dry_run": true} - Preview what would be created without making changes
    - {"dry_run": false} - Actually create projects and mappings
    """
    try:
        payload = request.json or {}
        dry_run = payload.get('dry_run', True)  # Default to dry run for safety
        
        # Query all Legal Cases from Notion
        url = f"https://api.notion.com/v1/databases/{CASES_DATABASE_ID}/query"
        headers = {
            "Authorization": f"Bearer {NOTION_API_KEY}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        
        response = requests.post(url, headers=headers, json={})
        response.raise_for_status()
        cases = response.json().get('results', [])
        
        # Check each case for mapping
        results = {
            "total_cases": len(cases),
            "already_mapped": [],
            "needs_mapping": [],
            "created": [],
            "errors": []
        }
        
        for case in cases:
            case_id = case.get('id')
            props = case.get('properties', {})
            
            # Get case name
            case_name = None
            for prop_name in ['Name', 'Title', 'Caption', 'Case Name']:
                prop = props.get(prop_name, {})
                if prop.get('title'):
                    titles = prop['title']
                    if titles:
                        case_name = titles[0].get('plain_text', '')
                        break
            
            if not case_name:
                results["errors"].append({
                    "case_id": case_id,
                    "error": "Could not determine case name"
                })
                continue
            
            # Check if mapping exists
            existing_project_id, mapping_id = get_todoist_project_for_matter(case_id)
            
            if existing_project_id:
                results["already_mapped"].append({
                    "case_id": case_id,
                    "case_name": case_name,
                    "todoist_project_id": existing_project_id
                })
            else:
                if dry_run:
                    # Just record what would be created
                    # Also check if Todoist project exists by name
                    existing_todoist = find_todoist_project_by_name(case_name)
                    results["needs_mapping"].append({
                        "case_id": case_id,
                        "case_name": case_name,
                        "todoist_project_exists": existing_todoist is not None,
                        "existing_todoist_id": existing_todoist
                    })
                else:
                    # Actually create
                    project_id, new_mapping_id, was_created = get_or_create_todoist_project_for_matter(case_id)
                    
                    if project_id:
                        results["created"].append({
                            "case_id": case_id,
                            "case_name": case_name,
                            "todoist_project_id": project_id,
                            "mapping_id": new_mapping_id,
                            "project_was_new": was_created
                        })
                    else:
                        results["errors"].append({
                            "case_id": case_id,
                            "case_name": case_name,
                            "error": "Failed to create project or mapping"
                        })
        
        return jsonify({
            "status": "preview" if dry_run else "completed",
            "dry_run": dry_run,
            "results": results
        })
        
    except Exception as e:
        logger.error(f"Sync matters failed: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/', methods=['GET'])
def root():
    """Root endpoint with service info."""
    return jsonify({
        "service": "Anaïs Legal Task Automation",
        "version": "2.0.0",
        "endpoints": {
            "/webhook": "POST - Flow 1: Email intake",
            "/promotion-webhook": "POST - Flow 2: Notion → Todoist promotion (auto-creates projects)",
            "/todoist-webhook": "POST - Flow 3: Todoist completion sync",
            "/calendar-sync": "POST - Flow 4: Calendar sync (preview)",
            "/digest": "GET/POST - Flow 5: Weekly digest data",
            "/todoist-admin": "POST - Todoist management (list/create projects, labels)",
            "/sync-matters": "POST - Bulk sync Legal Cases to Todoist projects",
            "/health": "GET - Health check"
        }
    })


if __name__ == '__main__':
    # Verify required environment variables
    required_vars = ['NOTION_API_KEY', 'SUPABASE_URL', 'SUPABASE_KEY', 'ANTHROPIC_API_KEY', 'TODOIST_API_KEY']
    missing = [v for v in required_vars if not os.environ.get(v)]
    if missing:
        logger.error(f"Missing required environment variables: {missing}")
        exit(1)
    
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
