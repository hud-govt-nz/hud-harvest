import time
import asyncio
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from playwright._impl import _errors as PlaywrightErrors


# Download using Playwright
# Try all three browsers before declaring failure - sometimes the antibot measures stop one but not others
async def playwright_async_download(file_url, dst_fn, debug_path = None):
    async with Stealth().use_async(async_playwright()) as p:
        browser_list = [p.firefox, p.webkit, p.chromium] # Firefox is the most reliable for getting past antibot
        for browser_type in browser_list:
            browser = await browser_type.launch()
            page = await browser.new_page()
            async with page.expect_download(timeout = 5000) as download_info:
                # Downloads should trigger an error (yes this is a weird way for it to work)
                try:
                    await page.goto(file_url)
                # If download does not occur, timeout will trigger instead
                except PlaywrightErrors.TimeoutError:
                    if debug_path:
                        await page.screenshot(path = f'{debug_path}/{browser_type.name}-failed.png')
                    print(f"Timed out with {browser_type.name}!")
                    await browser.close()
                # If download triggers, save the file
                except PlaywrightErrors.Error as e:
                    if "Download is starting" in e.message:
                        download = await download_info.value
                        await download.save_as(dst_fn)
                        await browser.close()
                        return dst_fn
                    else:
                        raise
        else:
            raise Exception("All attempts to download file failed!")

# Sync version
def playwright_download(file_url, dst_fn, debug_path = None):
    return asyncio.run(playwright_async_download(file_url, dst_fn, debug_path))


# Use Playwright to fetch content of a page
# Try all three browsers before declaring failure - sometimes the antibot measures stop one but not others
async def playwright_async_get_page(page_url, debug_path = None):
    async with Stealth().use_async(async_playwright()) as p:
        browser_list = [p.firefox, p.webkit, p.chromium] # Firefox is the most reliable for getting past antibot
        for browser_type in browser_list:
            browser = await browser_type.launch()
            page = await browser.new_page()
            res = await page.goto(page_url)
            if res.status in [200]:
                content = await page.content()
                await browser.close()
                return content
            else:
                if debug_path:
                    await page.screenshot(path = f'{debug_path}/{browser_type.name}-failed.png')
                print(f"Got code {res.status} with {browser_type.name}!")
                await browser.close()
                time.sleep(5)
        else:
            raise Exception("All attempts to fetch page failed!")

def playwright_get_page(page_url, debug_path = None):
    return asyncio.run(playwright_async_get_page(page_url, debug_path))