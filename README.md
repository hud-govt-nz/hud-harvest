# HUD harvesting tools
**CAUTION: This repo is public. Do not include sensitive data or key materials.**
Python-only toolkit for data harvesting. Intended as a shared library for bots.


## Installation
Install prerequisite libraries:
```
sudo apt install xvfb libfontconfig1-dev libharfbuzz-dev libfribidi-dev libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev unixodbc-dev
```

Selenium requires Chrome to run. If Chrome is not already installed:
```
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb
sudo apt install xvfb
```

Clone the repo then `pip install -e` to the local path. The `-e` will keep the file editable. Note you need to install `hud-keep` first.
```
pip install -e ~/hud-keep
pip install -e ~/hud-harvest
```
OR:
```
pipenv install -e ~/hud-keep
pipenv install -e ~/hud-harvest
```


## Usage
### Setting secrets
When using `hud-harvest`, you need to put secrets in a `.env` **where the project is run from**. For example, if you're using `hud-harvest` in `hmu-bot`, you need a `hmu-bot/.env` file that looks like:
```
DB_CONN="Driver={ODBC Driver 18 for SQL Server};Server=property.database.windows.net;uid=[USERNAME];pwd=[PASSWORD];"
CONTAINER_URL="https://dlprojectsdataprod.blob.core.windows.net/projects"
TEAMS_WEBHOOK="https://mhud.webhook.office.com/webhookb2/NOT-THE-REAL-WEBHOOK"
```

* `DB_CONN`: Your database connection string. If you're connecting from Windows you do not need to provide uid/pwd.
* `TEAMS_WEBHOOK`: The [incoming Teams webhook](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook), this allows the `chatter` module to send Teams messages.
* `CONTAINER_URL`: Location of the Azure storage container where `hud-keep` will store blobs.

### Running
```python
from sqltools import run_query
```
