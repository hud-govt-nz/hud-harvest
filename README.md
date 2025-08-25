# HUD harvesting tools
**CAUTION: This repo is public. Do not include sensitive data or key materials.**

Python-only toolkit for data harvesting. Intended as a shared library for bots.


## Installation
Install prerequisite libraries:
```
sudo apt install xvfb libfontconfig1-dev libharfbuzz-dev libfribidi-dev libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev unixodbc-dev
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

### Playwright
Playwright is now the preferred method for browser automation.
```
pipenv install playwright
playwright install
sudo apt-get install libwoff1 libopus0 libenchant-2-2 libsecret-1-0 libhyphen0 libegl1 libevdev2 libgles2 gstreamer1.0-libav
```


## Usage
### Setting secrets
When using `hud-harvest`, you need to put secrets in a `.env` **where the project is run from**. For example, if you're using `hud-harvest` in `hmu-bot`, you need a `hmu-bot/.env` file that looks like:
```
CONTAINER_URL="https://dlprojectsdataprod.blob.core.windows.net/projects"
TEAMS_WEBHOOK="https://mhud.webhook.office.com/webhookb2/NOT-THE-REAL-WEBHOOK"
```

* `TEAMS_WEBHOOK`: The [incoming Teams webhook](https://support.microsoft.com/en-us/office/create-incoming-webhooks-with-workflows-for-microsoft-teams-8ae491c7-0394-4861-ba59-055e33f75498), this allows the `msteams` module to send Teams messages.
* `CONTAINER_URL`: Location of the Azure storage container where `hud-keep` will store blobs.

### Running
```python
from sqltools import run_query
```
