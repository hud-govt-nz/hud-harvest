import os
import pymsteams
TEAMS_WEBHOOK = os.getenv("TEAMS_WEBHOOK")

def send_msg(msg):
    card = pymsteams.connectorcard(TEAMS_WEBHOOK)
    card.text(msg)
    card.send()

def send_card(body, entities = [], summary = ""):
    card = pymsteams.connectorcard(TEAMS_WEBHOOK)
    card.payload = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "summary": summary,
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
    # Teams API doesn't raise a bad status code so we have to read the content
    if card.last_http_response.text[:43] == "Webhook message delivery failed with error:":
        raise(Exception(card.last_http_response.text))

