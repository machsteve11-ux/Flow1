"""
Anaïs Email Intake Service
Replaces Make.com Flow 1: Email intake, AI extraction, Notion task creation

This service:
1. Receives forwarded emails via webhook
2. Parses email headers and body
3. Computes fingerprint for deduplication
4. Checks Supabase for duplicates
5. Extracts tasks using Claude API
6. Creates tasks in Notion
7. Logs to Supabase audit trail
"""

import os
import re
import json
import hashlib
import logging
from datetime import datetime, timedelta
from urllib.parse import quote

from flask import Flask, request, jsonify
import anthropic
import requests

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

# Notion Database IDs
TASKS_DATABASE_ID = "2aee43055e06803dbf90d54231711e61"
CASES_DATABASE_ID = "2aee43055e0681ad8e43e6e67825f9dd"
CALENDAR_DATABASE_ID = "2cbe43055e06806d88bafb4b136061b5"
MAPPINGS_DATABASE_ID = "2aee43055e06802782abfee92f846d88"
MATTER_ACTIVITY_DATABASE_ID = "2aee43055e06800fb4860009681c5fe"  # Will need to verify this ID

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
# EMAIL PARSING (Same regex patterns as Make.com Modules 44-50)
# =============================================================================

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
        "notion_page_id": notion_page_id,
        "created_at": datetime.utcnow().isoformat()
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to log task event: {e}")


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


def determine_status(task, matter_id, has_attachment):
    """
    Determine task status based on confidence, due date, and matter match.
    Replicates Module 68 status logic.
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
    if has_attachment:
        return "Needs Review"  # Always review attachments
    elif force_review:
        return "Needs Review"
    elif not matter_id:
        return "Needs Review"  # No matter linked - needs manual review
    elif confidence >= 0.8:
        return "Proposed"
    else:
        return "Needs Review"


def create_notion_task(task, email_data, fingerprint, matter_id, status):
    """
    Create task in Notion Tasks (Proposed) database.
    Replicates Module 92: Notion Create Database Item.
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
            "rich_text": [{"text": {"content": "claude-sonnet-4-20250514"}}]
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


def create_notion_calendar_item(event, email_data, fingerprint, matter_id, status):
    """
    Create calendar item in Notion Calendar Items (Proposed) database.
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


def get_todoist_project_for_matter(matter_id):
    """
    Query Mappings database to find Todoist project ID for a matter.
    Returns default project if no mapping found.
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


def create_todoist_task(title, due_date, priority, project_id, description=None):
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


def update_notion_task_with_todoist(notion_page_id, todoist_task_id, status="Approved"):
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
    
    properties = {
        "Name": {
            "title": [{"text": {"content": description}}]
        },
        "Type": {
            "select": {"name": activity_type}
        },
        "Source": {
            "select": {"name": source}
        },
        "Case": {
            "relation": [{"id": matter_id}]
        }
    }
    
    if related_task_id:
        properties["Related Task"] = {
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
    Replicates entire Flow 1 logic.
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
        
        # Step 5: Extract tasks with Claude
        logger.info("Extracting tasks with Claude...")
        extraction = extract_tasks_with_claude(email_data)
        
        # Step 6: Get index number and caption for matter matching
        index_number = extraction.get('index_number', {}).get('value')
        caption = extraction.get('caption', {}).get('value')
        
        # Step 7: Search for matching matter, create stub if not found
        matter_id = None
        stub_created = False
        if index_number:
            matter_id = search_case_by_index(index_number)
            if matter_id:
                logger.info(f"Found matching matter: {matter_id}")
            else:
                logger.info(f"No matter found for index: {index_number}")
                # Create stub matter if we have caption
                if caption:
                    matter_id = create_stub_matter(index_number, caption)
                    if matter_id:
                        stub_created = True
                        logger.info(f"Created stub matter: {matter_id}")
        
        # Step 8: Create tasks in Notion
        created_tasks = []
        for task in extraction.get('tasks', []):
            status = determine_status(task, matter_id, email_data['has_attachment'])
            notion_id = create_notion_task(task, email_data, fingerprint, matter_id, status)
            
            if notion_id:
                created_tasks.append({
                    "title": task.get('title', {}).get('value'),
                    "notion_id": notion_id,
                    "status": status
                })
                
                # Log task event
                log_task_event(
                    fingerprint,
                    task.get('title', {}).get('value'),
                    "proposed",
                    notion_id
                )
        
        # Step 9: Handle calendar items (create in Calendar Items database)
        for event in extraction.get('calendar_items', []):
            status = "Proposed" if matter_id else "Needs Review"
            notion_id = create_notion_calendar_item(event, email_data, fingerprint, matter_id, status)
            
            if notion_id:
                created_tasks.append({
                    "title": event.get('title', {}).get('value'),
                    "notion_id": notion_id,
                    "status": status,
                    "type": "calendar"
                })
        
        logger.info(f"Processing complete. Created {len(created_tasks)} tasks.")
        
        return jsonify({
            "status": "processed",
            "fingerprint": fingerprint,
            "tasks_created": len(created_tasks),
            "tasks": created_tasks,
            "has_attachment": email_data['has_attachment'],
            "matter_id": matter_id,
            "stub_created": stub_created
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
        
        # Extract properties
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
        
        # Get Todoist project for this matter
        todoist_project_id, mapping_id = get_todoist_project_for_matter(matter_id)
        logger.info(f"Todoist project for matter {matter_id}: {todoist_project_id}")
        
        # Compute promotion key for idempotency
        promotion_key = compute_promotion_key(notion_page_id, task_title, due_date, todoist_project_id)
        logger.info(f"Promotion key: {promotion_key[:16]}...")
        
        # Check idempotency - has this exact promotion happened before?
        if check_promotion_exists(promotion_key):
            logger.info(f"Promotion key already exists. Idempotency check passed - skipping.")
            return jsonify({"status": "skipped", "reason": "Already promoted (idempotency)"})
        
        # Create Todoist task
        todoist_task_id, todoist_url = create_todoist_task(
            title=task_title,
            due_date=due_date,
            priority=priority,
            project_id=todoist_project_id,
            description=f"From Notion: {notion_page_id}"
        )
        
        if not todoist_task_id:
            logger.error("Failed to create Todoist task")
            return jsonify({"status": "error", "message": "Todoist task creation failed"}), 500
        
        # Update Notion with Todoist task ID
        update_notion_task_with_todoist(notion_page_id, todoist_task_id)
        
        # Log promotion event to audit trail
        log_promotion_event(notion_page_id, task_title, todoist_task_id, promotion_key)
        
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
            "promotion_key": promotion_key
        })
        
    except Exception as e:
        logger.error(f"Promotion webhook failed: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


# =============================================================================
# FLOW 3: TODOIST COMPLETION WEBHOOK ENDPOINT
# =============================================================================

@app.route('/todoist-webhook', methods=['POST'])
def todoist_webhook():
    """
    Flow 3: Completion Sync
    Triggered by Todoist webhook when task is completed.
    
    Todoist webhook payload:
    {
        "event_name": "item:completed",
        "user_id": "...",
        "event_data": {
            "id": "todoist-task-id",
            "content": "Task title",
            "project_id": "...",
            ...
        }
    }
    """
    try:
        payload = request.json
        logger.info(f"Todoist webhook received: {payload.get('event_name', 'unknown event')}")
        
        event_name = payload.get('event_name', '')
        
        # Only process completion events
        if event_name not in ['item:completed', 'item:complete']:
            logger.info(f"Ignoring event: {event_name}")
            return jsonify({"status": "ignored", "reason": f"Event {event_name} not handled"})
        
        event_data = payload.get('event_data', {})
        todoist_task_id = str(event_data.get('id', ''))
        task_content = event_data.get('content', '')
        
        if not todoist_task_id:
            return jsonify({"status": "error", "message": "No task ID in payload"}), 400
        
        logger.info(f"Processing completion for Todoist task: {todoist_task_id}")
        
        # Find corresponding Notion task
        notion_task = find_notion_task_by_todoist_id(todoist_task_id)
        
        if not notion_task:
            # Orphan task - completed in Todoist but not found in Notion
            # This is expected for manually created Todoist tasks
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
        
    except Exception as e:
        logger.error(f"Todoist webhook failed: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/', methods=['GET'])
def root():
    """Root endpoint with service info."""
    return jsonify({
        "service": "Anaïs Legal Task Automation",
        "version": "2.0.0",
        "endpoints": {
            "/webhook": "POST - Flow 1: Email intake",
            "/promotion-webhook": "POST - Flow 2: Notion → Todoist promotion",
            "/todoist-webhook": "POST - Flow 3: Todoist completion sync",
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
