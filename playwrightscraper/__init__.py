import time, random
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

# Download using a Selenium browser (i.e. Automated bot browser)
def playwright_download(file_url, dst_fn):
    with sync_playwright() as p:
        browser_list = [p.firefox, p.webkit, p.chromium]
        random.shuffle(browser_list)
        for browser_type in browser_list:
            browser = browser_type.launch()
            page = browser.new_page()
            stealth_sync(page)
            with page.expect_download() as download_info:
                try: page.goto(file_url) # Direct downloads trigger an error, which is unhelpful
                except: pass
            if download_info.is_done() and download_info.value:
                download = download_info.value
                download.save_as(dst_fn)
                browser.close()
                return(dst_fn)
            else:
                browser.close()
                time.sleep(5)
        else:
            raise Exception("All attempts to download file failed!")

# Use Playwright to fetch content of a page
# Try all three browsers before declaring failure - sometimes the antibot measures stop one but not others
def playwright_get_page(page_url, debug_path = None):
    with sync_playwright() as p:
        browser_list = [p.firefox, p.webkit, p.chromium]
        random.shuffle(browser_list)
        for browser_type in browser_list:
            browser = browser_type.launch()
            page = browser.new_page()
            stealth_sync(page)
            res = page.goto(page_url)
            if res.status in [200]:
                content = page.content()
                browser.close()
                return content
            else:
                if debug_path:
                    page.screenshot(path = f'{debug_path}/{browser_type.name}-failed.png')
                print(f"Got code {res.status} with {browser_type.name}!")
                browser.close()
                time.sleep(5)
        else:
            raise Exception("All attempts to fetch page failed!")
