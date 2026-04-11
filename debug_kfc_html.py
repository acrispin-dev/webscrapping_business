import requests
from bs4 import BeautifulSoup
import re

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
url = 'https://www.kfc.com.pe/carta/twister-xl'

print("Fetching KFC Twister XL page...")
r = requests.get(url, headers=headers, timeout=30)
soup = BeautifulSoup(r.text, 'html.parser')

# Look for product elements
print('=== Looking for product elements ===')
print(f'Total HTML length: {len(r.text)} chars')

# Find all links
all_links = soup.find_all('a')
print(f'Total links found: {len(all_links)}')

# Find links with twister
twister_links = [link for link in all_links if 'twister' in (link.get('href') or '').lower()]
print(f'Twister links found: {len(twister_links)}')

# Show first few
for i, link in enumerate(twister_links[:3]):
    href = link.get('href', '')
    text = link.get_text()[:100]
    print(f'{i+1}. href={href}, text={text}')

# Look for prices
print('\n=== Looking for prices ===')
text = soup.get_text()
prices = re.findall(r'S/\s*[\d.]+', text)
print(f'Found {len(prices)} prices')
if prices:
    print('Sample prices:', prices[:10])

# Look for specific h3 tags
print('\n=== H3 tags ===')
h3s = soup.find_all('h3')
print(f'H3 tags found: {len(h3s)}')
if h3s:
    for h3 in h3s[:3]:
        print(f'  - {h3.get_text()[:50]}')

# Look for specific divs
print('\n=== Looking for data attributes ===')
data_divs = soup.find_all(attrs={'data-testid': True})
print(f'Data-testids found: {len(data_divs)}')
for div in data_divs[:3]:
    print(f'  - {div.name} ({div.get("data-testid", "")[:50]})')
