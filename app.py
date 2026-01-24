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

# Notion Database IDs
TASKS_DATABASE_ID = "2aee43055e06803dbf90d54231711e61"
CASES_DATABASE_ID = "2aee43055e0681ad8e43e6e67825f9dd"

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
        return "Needs Matter"
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
        
        # Step 6: Get index number for matter matching
        index_number = extraction.get('index_number', {}).get('value')
        
        # Step 7: Search for matching matter
        matter_id = None
        if index_number:
            matter_id = search_case_by_index(index_number)
            if matter_id:
                logger.info(f"Found matching matter: {matter_id}")
            else:
                logger.info(f"No matter found for index: {index_number}")
        
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
        
        # Step 9: Handle calendar items (create as tasks for now)
        for event in extraction.get('calendar_items', []):
            # Convert calendar item to task format
            task = {
                "title": {"value": f"[CALENDAR] {event.get('title', {}).get('value', 'Event')}", "confidence": event.get('title', {}).get('confidence', 0.8)},
                "due_date": {"value": event.get('event_date', {}).get('value'), "confidence": 0.9},
                "relative_deadline": {"value": None, "confidence": 1.0},
                "priority": {"value": "P1", "confidence": 0.9},
                "extraction_rationale": event.get('extraction_rationale', ''),
                "applicable_rule": None,
                "subtasks": []
            }
            status = determine_status(task, matter_id, email_data['has_attachment'])
            notion_id = create_notion_task(task, email_data, fingerprint, matter_id, status)
            
            if notion_id:
                created_tasks.append({
                    "title": task.get('title', {}).get('value'),
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
            "has_attachment": email_data['has_attachment']
        })
        
    except Exception as e:
        logger.error(f"Webhook processing failed: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "service": "anais-intake"})


@app.route('/', methods=['GET'])
def root():
    """Root endpoint with service info."""
    return jsonify({
        "service": "Anaïs Email Intake",
        "version": "1.0.0",
        "endpoints": {
            "/webhook": "POST - Email intake webhook",
            "/health": "GET - Health check"
        }
    })


if __name__ == '__main__':
    # Verify required environment variables
    required_vars = ['NOTION_API_KEY', 'SUPABASE_URL', 'SUPABASE_KEY', 'ANTHROPIC_API_KEY']
    missing = [v for v in required_vars if not os.environ.get(v)]
    if missing:
        logger.error(f"Missing required environment variables: {missing}")
        exit(1)
    
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
