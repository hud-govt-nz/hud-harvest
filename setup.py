from setuptools import setup, find_packages
setup(
    name="hudharvest",
    version="0.5.0",
    description="HUD harvest/store/tracking functions",
    url="https://github.com/hud-govt-nz/hud-harvest",
    author="Keith Ng",
    author_email="keith.ng@hud.govt.nz",
    packages=[
        "scraper", "seleniumscraper",
        "sqltools", "mongotools",
        "dbloader", "taskmaster"
    ],
    include_package_data=True,
    install_requires=[
        "hudkeep",
        "pandas", "numpy", "bs4",
        "sqlalchemy", "pyodbc", "pymongo", "ijson",
        "requests", "xlrd", "openpyxl", "pyxlsb",
        "selenium", "webdriver-manager", "pyvirtualdisplay"
    ]
)
