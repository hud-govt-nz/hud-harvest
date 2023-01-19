import chatter
TEAMS_WEBHOOK = os.getenv("TEAMS_WEBHOOK")

chatter.send_card([{
    "type": "Container",
    "bleed": True,
    "items": [{
        "type": "TextBlock",
        "size": "small",
        "weight": "bolder",
        "text": "Test Task 1"
    }, {
        "type": "TextBlock",
        "size": "large",
        "weight": "bolder",
        "spacing": "none",
        "color": "good",
        "text": "SUCCESS"
    }, {
        "type":"FactSet",
        "size": "small",
        "facts":[{
            "title": "Table name",
            "value": "placeholder"
        }, {
            "title": "Source URL",
            "value": "placeholder"
        }, {
            "title": "File type",
            "value": "placeholder"
        }, {
            "title": "Size",
            "value": "placeholder"
        }, {
            "title": "Row count",
            "value": "placeholder"
        }, {
            "title": "Data start",
            "value": "placeholder"
        }, {
            "title": "Data end",
            "value": "placeholder"
        }, {
            "title": "Store status",
            "value": "placeholder"
        }, {
            "title": "Load status",
            "value": "placeholder"
        }, {
            "title": "Stored at",
            "value": "placeholder"
        }, {
            "title": "Loaded at",
            "value": "placeholder"
        }]
    }]
}])
