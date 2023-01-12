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
```python
import tasker
```
