# Module for sending messages to a Teams Channel
# This needs to be setup on the Teams side with a [Workflows webhook](https://support.microsoft.com/en-us/office/create-incoming-webhooks-with-workflows-for-microsoft-teams-8ae491c7-0394-4861-ba59-055e33f75498)
import os
import requests
TEAMS_WEBHOOK = os.getenv("TEAMS_WEBHOOK")

# Underlying function for sending cards
def send_card(body, entities = [], summary = ""):
    if entities:
        body.append({
            "type": "TextBlock",
            "text": f"Ping {', '.join([e['text'] for e in entities])}"
        })
    res = requests.post(TEAMS_WEBHOOK, json = {
        "type": "message",
        "summary": summary,
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "type": "AdaptiveCard",
                "body": body,
                "msteams": { "width": "Full", "entities": entities }
            }
        }]
    })
    try:
        res.raise_for_status()
    except:
        print(f"\033[1;31m{res.text}\033[0m")
        raise
    return res

# Send a simple message
def send_msg(msg, entities = [], summary = []):
    send_card([{ "type": "TextBlock", "text": msg }], entities, summary)

# Create a summary card for a list of tasks (doesn't send, only creates the card body)
def tasks_summary_card(run_name, tasks):
    # Determine overall status
    if all(b.log["load_status"] == "success" for b in tasks):
        status = "success"
        color = "good" # good/warning/attention
    else:
        status = "ERROR"
        color = "attention" # good/warning/attention

    # Generate factset from tasks
    facts = []
    for b in tasks:
        t = b.log["task_name"]
        if b.log["load_status"] == "success":
            v = f"{b.log['row_count']} rows loaded"
        elif b.log["load_status"] == "error":
            v = str(b.load_error)
        else:
            v = b.log["load_status"]
        facts.append({ "title": t, "value": v })

    # Create card
    return [{
        "type": "Container",
        "style": "accent",
        "bleed": True,
        "items": [{
            "type": "TextBlock",
            "size": "small",
            "weight": "bolder",
            "text": run_name
        }, {
            "type": "TextBlock",
            "size": "large",
            "weight": "bolder",
            "spacing": "none",
            "color": color,
            "text": status
        }, {
            "type":"FactSet",
            "facts": facts
        }]
    }]
