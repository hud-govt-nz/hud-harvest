import os
import pymsteams
TEAMS_WEBHOOK = os.getenv("TEAMS_WEBHOOK")

def send_msg(msg):
    card = pymsteams.connectorcard(TEAMS_WEBHOOK)
    card.text(msg)
    card.send()

def send_card(body, entities = []):
    card = pymsteams.connectorcard(TEAMS_WEBHOOK)
    card.payload = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "type": "AdaptiveCard",
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "version": "1.5",
                "body": body,
                "msteams": {
                    "width": "Full",
                    "entities": entities
                }
            }
        }]
    }
    card.send()
