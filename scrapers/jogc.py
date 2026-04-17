from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import time
import os
this_dir = os.path.dirname(__file__)
repo_dir = os.path.join(this_dir, '..')
scratch_space = os.path.join(repo_dir, 'jogc-scratch')
os.makedirs(scratch_space, exist_ok=True)

driver = webdriver.Edge()

driver.get("https://onlinelibrary.wiley.com/toc/15733599/2024/33/5")
input("Press enter when ready")
page_content = driver.page_source
with open('../jogc-scratch/2024-33-5.html', 'w') as f:
    f.write(driver.page_source)


# # Make it look like we are browsing the journal
# url = "https://onlinelibrary.wiley.com/loi/15733599"
# driver.get(url)
# WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, 'a')))
# 

# links = driver.find_elements(By.XPATH, "//a[starts-with(@href, '/loi/15733599/year/')]")
# page_content = driver.page_source
# landing_page = os.path.join(scratch_space, 'landing-page.html')

# for link in links:
#     print(link)
#     link.click()
#     # gotta figure out the year
#     input("Press enter when ready")
#     time.sleep(3)
#     driver.back()
#     time.sleep(3)
#     if True:
#         continue
#     year_file = os.path.join(scratch_space, f'{year}.html')
#     if os.path.exists(year_file):
#         continue
#     driver.get(f'https://onlinelibrary.wiley.com/loi/15733599/year/{year}')
#     time.sleep(3)
#     with open(year_file, 'w') as f:
#         f.write(driver.page_source)

# # Next look for
# #https://onlinelibrary.wiley.com/toc/15733599/2024/33/5
