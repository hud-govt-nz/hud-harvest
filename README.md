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

Install directly from Github. The `-e` will keep the file editable. Note you need to install `hud-keep` first.
```
pip install -e git+https://github.com/hud-govt-nz/hud-keep.git@main#egg=hudkeep
pip install -e git+https://github.com/hud-govt-nz/hud-harvest.git@main#egg=hudharvest
```
OR:
```
pipenv install -e git+https://github.com/hud-govt-nz/hud-keep.git@main#egg=hudkeep
pipenv install -e git+https://github.com/hud-govt-nz/hud-harvest.git@main#egg=hudharvest
```


## Usage
### Setting secrets
When using `hud-harvest`, you need to put secrets in a `.env` **where the project is run from**. For example, if you're using `hud-harvest` in `hmu-bot`, you need a `hmu-bot/.env` file that looks like:
```
CONTAINER_URL="https://dlprojectsdataprod.blob.core.windows.net/projects"
TEAMS_WEBHOOK="https://mhud.webhook.office.com/webhookb2/NOT-THE-REAL-WEBHOOK"
```

* `TEAMS_WEBHOOK`: The [incoming Teams webhook](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook), this allows the `chatter` module to send Teams messages.
* `CONTAINER_URL`: Location of the Azure storage container where `hud-keep` will store blobs.

### Running
```python
from sqltools import run_query
```
