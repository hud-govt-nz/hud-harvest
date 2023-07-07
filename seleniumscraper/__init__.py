import shutil, time
from pathlib import Path
from pyvirtualdisplay import Display
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Update Chrome on load, so that parallel runs won't try to install on top of each other
ChromeDriverManager().install()

# Download using a Selenium browser (i.e. Automated bot browser)
def selenium_download(src_url, dst_fn, max_wait = 300):
    display = Display(visible=0, size=(800, 600)) # Display into the void, so we can run without a display
    display.start()
    options = Options()
    options.experimental_options["prefs"] = { "download.default_directory": "/tmp" }
    service = Service(ChromeDriverManager().install())
    driver = WebDriver(service = service, options = options)
    driver.get(src_url)
    tmp_fn = f"/tmp/{src_url.split('?')[0].split('/')[-1]}"
    tmp = Path(tmp_fn)
    # Check that file has downloaded
    for i in range(0, max_wait):
        if not tmp.exists():
            time.sleep(1)
        elif tmp.stat().st_size == 0:
            raise Exception("File is empty!")
        else:
            shutil.move(tmp_fn, dst_fn)
            break
    else:
        raise Exception("Could not download file!")
    driver.close()

def selenium_get_page(page_url):
    display = Display(visible=0, size=(800, 600))
    display.start()
    service = Service(ChromeDriverManager().install())
    driver = WebDriver(service = service)
    driver.get(page_url)
    page = driver.page_source
    driver.close()
    return page
