# Anaïs Email Intake Service

This Python service replaces Make.com Flow 1 for email intake, task extraction, and Notion task creation.

## What It Does

1. **Receives forwarded emails** via webhook
2. **Parses email headers** (sender, date, subject, body) using the same regex patterns as your Make.com flow
3. **Computes fingerprint** for deduplication (SHA256 of message_id + sender + timestamp)
4. **Checks Supabase** for duplicates
5. **Extracts tasks** using Claude API (same prompt structure as Gemini)
6. **Matches matters** by searching Notion Cases database for index number
7. **Creates tasks** in Notion Tasks (Proposed) database
8. **Logs to Supabase** audit trail

## Attachment Handling

**Current behavior:** Emails with attachments are processed normally, but the task is marked "Needs Review" with a note listing the attachment filenames. You review the original email in Outlook to see the attachments.

**Future enhancement:** Could add PDF processing via Claude's vision capability or document parsing.

---

## Deployment to Railway

### Step 1: Create Railway Account

1. Go to [railway.app](https://railway.app)
2. Click "Login" → Sign up with GitHub (easiest)
3. Verify your email

### Step 2: Create New Project

1. Click **"New Project"** button
2. Select **"Empty Project"**
3. Click **"Add a Service"** → **"Empty Service"**

### Step 3: Upload Code

**Option A: GitHub (Recommended)**
1. Create a new GitHub repository
2. Upload these files to the repository
3. In Railway, click your service → **"Connect Repo"**
4. Select your repository
5. Railway will auto-deploy on every push

**Option B: Direct Upload**
1. Install Railway CLI: `npm install -g @railway/cli`
2. Login: `railway login`
3. Link project: `railway link`
4. Deploy: `railway up`

### Step 4: Add Environment Variables

1. In Railway, click your service
2. Go to **"Variables"** tab
3. Add these variables:

| Variable | Value |
|----------|-------|
| `NOTION_API_KEY` | `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` |
| `SUPABASE_URL` | `https://mfuaknjpgcbjgvtxjdzi.supabase.co` |
| `SUPABASE_KEY` | `eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1mdWFrbmpwZ2Niamd2dHhqZHppIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MzMzNTk0MCwiZXhwIjoyMDc4OTExOTQwfQ.i-t80g0q0iHmubQC47jIx_pSfiJBhJqgz-ANWqcwgHI` |
| `ANTHROPIC_API_KEY` | `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` |

### Step 5: Configure Build Settings

Railway should auto-detect Python, but if not:

1. Click **"Settings"** tab
2. Under **"Build"**, ensure:
   - Builder: Nixpacks
   - Start command: `gunicorn app:app`

### Step 6: Get Your URL

1. Go to **"Settings"** tab
2. Under **"Networking"**, click **"Generate Domain"**
3. You'll get a URL like: `https://anais-intake-production-xxxx.up.railway.app`

### Step 7: Test the Service

1. Open your URL in a browser - you should see:
   ```json
   {
     "service": "Anaïs Email Intake",
     "version": "1.0.0",
     "endpoints": {...}
   }
   ```

2. Check health: `https://your-url.railway.app/health`

### Step 8: Update Email Forwarding

1. Open Outlook
2. Edit your Quick Step (or create new one)
3. Change the forward address from:
   - Old: `4gqazrktcq50s7psg4a12ykk6vuwk0fe@hook.us2.make.com`
   - New: `https://your-url.railway.app/webhook`

**Wait - that won't work!** Quick Steps forward to email addresses, not webhooks.

### Alternative: Keep Mailhook, Change Destination

Your current setup uses Mailhook to receive emails and convert to webhooks. You have two options:

**Option A: Keep using Make.com's Mailhook (Simplest)**
1. In Make.com, create a minimal scenario:
   - Trigger: Your existing Mailhook
   - Action: HTTP → Make a Request
     - URL: `https://your-url.railway.app/webhook`
     - Method: POST
     - Body: `{{toJSON(1)}}`
2. This forwards the raw email to your new service
3. ~$0/month (within free tier for simple webhook relay)

**Option B: Use a dedicated email-to-webhook service**
- [Mailhook.app](https://mailhook.app) - Dedicated service
- [Postmark Inbound](https://postmarkapp.com/developer/webhooks/inbound-webhook) - Professional option
- These give you an email address that POSTs to your webhook

**Option C: Set up your own email receiver**
- More complex, requires email server configuration
- Not recommended for solo practice

**Recommendation:** Use Option A for now. It's one simple Make.com scenario (2 modules) that just relays emails to your service. You can eliminate this later if desired.

---

## Testing

### Send a Test Email

1. Forward a test email to your intake address
2. Check Railway logs: **"Deployments"** tab → **"View Logs"**
3. Verify task appears in Notion Tasks (Proposed) database

### Expected Log Output

```
INFO: Received email: Fw: Martinez v. ABC Corp - Discovery Demands
INFO: Parsed email from: jsmith@smithjoneslegal.com
INFO: Fingerprint: a1b2c3d4e5f6...
INFO: Extracting tasks with Claude...
INFO: Found matching matter: 2aee4305-5e06-...
INFO: Created Notion task: Respond to discovery demands
INFO: Processing complete. Created 1 tasks.
```

---

## Troubleshooting

### "Missing required environment variables"
- Check Railway Variables tab
- Ensure all 4 variables are set
- No quotes around values

### "Notion API error"
- Verify your Notion integration has access to both databases:
  - Tasks (Proposed)
  - Legal Cases
- Go to each database → Share → Invite your integration

### "Supabase error"
- Verify the service_role key (not anon key)
- Check that `email_receipts` and `task_events` tables exist

### "Claude extraction failed"
- Check Anthropic API key is valid
- Check you have API credits
- View full error in Railway logs

---

## File Structure

```
anais-intake/
├── app.py              # Main application (all logic here)
├── requirements.txt    # Python dependencies
├── .env.example        # Environment variable template
└── README.md           # This file
```

---

## What's Different From Make.com

| Aspect | Make.com | This Service |
|--------|----------|--------------|
| Attachment processing | Gemini processes PDFs | Flags for manual review |
| Visual editor | Yes | No |
| Where it runs | Make.com servers | Railway (~$5/month) |
| Changes | Edit in UI | Ask Claude to modify code |
| Debugging | Click through modules | Railway logs |

---

## Future Enhancements

1. **Add Flows 2-5** to this same service
2. **PDF processing** via Claude vision
3. **Better calendar item handling** (create in Calendar Items database)
4. **Email notifications** on errors

---

## Support

If something breaks:
1. Check Railway logs
2. Copy the error message
3. Ask Claude to diagnose and fix

The code is straightforward Python - any changes are quick to make and redeploy.
