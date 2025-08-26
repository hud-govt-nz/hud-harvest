# Module for sending messages to a Teams Channel
# This needs to be setup on the Teams side with a [Workflows webhook](https://support.microsoft.com/en-us/office/create-incoming-webhooks-with-workflows-for-microsoft-teams-8ae491c7-0394-4861-ba59-055e33f75498)
import os
import requests
TEAMS_WEBHOOK = os.getenv("TEAMS_WEBHOOK")

# Underlying function for sending cards
def send_card(body, ping = [], summary = ""):
    # Deal with pings
    entities = []
    for e in ping:
        entities.append({
            "type": "mention",
            "text": f"<at>{e['name']}</at>",
            "mentioned": e
        })
    if ping:
        body.append({
            "type": "TextBlock",
            "text": f"Ping {', '.join([e['text'] for e in entities])}"
        })
    # Create payload
    payload = {
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
    }
    # Send
    res = requests.post(TEAMS_WEBHOOK, json = payload)
    try:
        res.raise_for_status()
        return res
    except:
        print(f"\033[1;31m{res.text}\033[0m")
        raise

# Send a simple message
def send_msg(msg, ping = [], summary = []):
    send_card([{ "type": "TextBlock", "text": msg }], ping, summary)

# Creates message and card wrappers around the content of a card
def make_base_card(task_name, status, items = []):
    # Determine overall status
    status = status or "error"
    color = "good" if status == "success" else "attention"
    # Create card
    payload = [{
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
    # Add items
    payload[0]["items"] += items
    return payload
