"""Debug script to analyze KFC HTML response"""
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9",
    "Referer": "https://www.kfc.com.pe/",
}

url = "https://www.kfc.com.pe/carta/twister-xl"

print(f"Fetching: {url}")
r = requests.get(url, headers=HEADERS, timeout=30)

print(f"Status: {r.status_code}")
print(f"Content-Type: {r.headers.get('content-type')}")
print(f"Content length: {len(r.text)}")

soup = BeautifulSoup(r.text, "html.parser")

# Look for links
all_links = soup.find_all('a')
print(f"\nTotal <a> tags: {len(all_links)}")

# Look for any price matches
price_pattern = "S/"
prices = r.text.count(price_pattern)
print(f"'S/' occurrences: {prices}")

# Look for any product-like divs
product_divs = soup.find_all('div', class_=lambda x: x and 'product' in x.lower() if x else False)
print(f"Divs with 'product' in class: {len(product_divs)}")

# Print first 2000 chars of HTML to see structure
print(f"\n--- First 2000 chars of HTML ---")
print(r.text[:2000])

# Look for any h3 or h4 tags that might contain product names
h3_tags = soup.find_all('h3')
h4_tags = soup.find_all('h4')
print(f"\n<h3> tags: {len(h3_tags)}")
print(f"<h4> tags: {len(h4_tags)}")

# Print first 5 links to see what they contain
print(f"\n--- First 5 <a> tags ---")
for i, link in enumerate(all_links[:5]):
    text = link.get_text().strip()[:100]
    href = link.get('href', '')
    print(f"  {i+1}. href={href[:50]}... text={text}...")
