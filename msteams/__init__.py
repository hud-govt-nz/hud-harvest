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

def make_base_card(task_name, status):
    # Determine overall status
    if status == "success":
        status = "success"
        color = "good" # good/warning/attention
    else:
        status = status or "error"
        color = "attention" # good/warning/attention
    # Create card
    return [{
        "type": "Container",
        "style": color,
        "bleed": True,
        "items": [{
            "type": "TextBlock",
            "size": "small",
            "weight": "bolder",
            "text": task_name
        }, {
            "type": "TextBlock",
            "size": "large",
            "weight": "bolder",
            "spacing": "none",
            "color": color,
            "text": status.upper()
        }]
    }]
