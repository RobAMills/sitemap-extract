import os
import xml.etree.ElementTree as ET
import gzip
from concurrent.futures import ThreadPoolExecutor
import logging
import argparse
import cloudscraper
import random
import glob
from datetime import datetime
import sys

# Setup logging
logging.basicConfig(filename='sitemap_processing.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    # Add more user agents as necessary
]

def print_status(message):
    """Print status message with timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")
    sys.stdout.flush()

def create_scraper(use_cloudscraper=True, use_proxy=False):
    if use_cloudscraper:
        scraper = cloudscraper.create_scraper()
    else:
        import requests
        scraper = requests.Session()
    
    if use_proxy:
        proxy = "http://your-proxy-server:port"
        scraper.proxies.update({
            'http': proxy,
            'https': proxy,
        })
    
    return scraper

def fetch_xml(url, use_cloudscraper=True, use_proxy=False):
    print_status(f"Fetching XML from {url}")
    scraper = create_scraper(use_cloudscraper, use_proxy)
    scraper.headers['User-Agent'] = random.choice(USER_AGENTS)
    response = scraper.get(url)
    if response.status_code == 200:
        print_status(f"Successfully fetched {url}")
        return ET.fromstring(response.content)
    logging.error(f"Failed to fetch URL {url}: HTTP {response.status_code}")
    print_status(f"Failed to fetch {url} (HTTP {response.status_code})")
    return None

def decompress_gz(url, use_cloudscraper=True, use_proxy=False):
    print_status(f"Fetching and decompressing {url}")
    scraper = create_scraper(use_cloudscraper, use_proxy)
    scraper.headers['User-Agent'] = random.choice(USER_AGENTS)
    response = scraper.get(url, stream=True)
    if response.status_code == 200:
        with gzip.open(response.raw, 'rb') as f:
            print_status(f"Successfully decompressed {url}")
            return ET.fromstring(f.read())
    logging.error(f"Failed to decompress URL {url}: HTTP {response.status_code}")
    print_status(f"Failed to decompress {url} (HTTP {response.status_code})")
    return None

def save_urls(url, urls):
    filename = url.split('/')[-1].split('.')[0]
    filename = f"{filename}.txt" if filename else "sitemap_urls.txt"
    with open(filename, 'w') as f:
        f.write(f"Source URL: {url}\n")
        for url in urls:
            f.write(f"{url}\n")
    print_status(f"Saved {len(urls)} URLs to {filename}")
    logging.info(f"URLs saved to {filename} with {len(urls)} URLs.")

def read_urls_from_file(file_path):
    print_status(f"Reading URLs from {file_path}")
    with open(file_path, 'r') as file:
        urls = [line.strip() for line in file if line.strip()]
    print_status(f"Found {len(urls)} URLs in {file_path}")
    return urls

def find_xml_files_in_directory(directory):
    print_status(f"Scanning directory {directory} for XML files")
    xml_files = glob.glob(os.path.join(directory, '*.xml')) + glob.glob(os.path.join(directory, '*.xml.gz'))
    print_status(f"Found {len(xml_files)} XML files in {directory}")
    return xml_files

def process_sitemap(url, is_compressed=False, use_cloudscraper=True, use_proxy=False):
    root = decompress_gz(url, use_cloudscraper, use_proxy) if is_compressed else fetch_xml(url, use_cloudscraper, use_proxy)
    if not root:
        return [], []

    sitemap_urls = []
    page_urls = []
    namespace = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    
    # Process sitemap URLs
    for sitemap in root.findall('.//sm:sitemap', namespace):
        loc = sitemap.find('sm:loc', namespace)
        if loc is not None:
            sitemap_urls.append(loc.text)
    
    # Process page URLs
    for page in root.findall('.//sm:url', namespace):
        loc = page.find('sm:loc', namespace)
        if loc is not None:
            page_urls.append(loc.text)

    print_status(f"Found {len(sitemap_urls)} sitemaps and {len(page_urls)} pages in {url}")
    save_urls(url, page_urls)
    return sitemap_urls, page_urls

def process_all_sitemaps(start_urls, use_cloudscraper=True, use_proxy=False):
    all_sitemap_urls = set()
    all_page_urls = set()
    queue = start_urls[:]
    processed_count = 0
    total_sitemaps = len(queue)

    print_status(f"Starting processing of {total_sitemaps} initial sitemaps")
    
    with ThreadPoolExecutor() as executor:
        while queue:
            current_url = queue.pop(0)
            processed_count += 1
            
            print_status(f"Processing sitemap {processed_count}/{total_sitemaps}: {current_url}")
            
            future = executor.submit(process_sitemap, current_url, current_url.endswith('.xml.gz'), use_cloudscraper, use_proxy)
            sitemap_urls, page_urls = future.result()
            
            # Update counts and queue
            new_sitemaps = [url for url in sitemap_urls if url not in all_sitemap_urls]
            all_sitemap_urls.update(sitemap_urls)
            all_page_urls.update(page_urls)
            queue.extend(new_sitemaps)
            total_sitemaps += len(new_sitemaps)
            
            print_status(f"Progress: {processed_count}/{total_sitemaps} sitemaps processed")
            print_status(f"Total URLs found so far: {len(all_page_urls)}")

    save_urls("sitemap_index", all_sitemap_urls)
    return all_sitemap_urls, all_page_urls

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process XML sitemaps.')
    parser.add_argument('--url', type=str, help='Direct URL of the sitemap index file.')
    parser.add_argument('--file', type=str, help='File containing list of URLs.')
    parser.add_argument('--directory', type=str, help='Directory containing XML and XML.GZ files.')
    parser.add_argument('--no-cloudscraper', action='store_true', help='Disable Cloudscraper and use standard requests.')
    parser.add_argument('--proxy', action='store_true', help='Enable proxy support.')
    args = parser.parse_args()

    print_status("Starting sitemap extraction")
    
    urls_to_process = []
    if args.url:
        print_status(f"Processing URL: {args.url}")
        urls_to_process.append(args.url)
    if args.file:
        urls_to_process.extend(read_urls_from_file(args.file))
    if args.directory:
        urls_to_process.extend(find_xml_files_in_directory(args.directory))

    if urls_to_process:
        print_status(f"Starting to process {len(urls_to_process)} sitemaps")
        all_sitemap_urls, all_page_urls = process_all_sitemaps(urls_to_process, not args.no_cloudscraper, args.proxy)
        print_status(f"Completed processing")
        print_status(f"Total sitemaps processed: {len(all_sitemap_urls)}")
        print_status(f"Total URLs extracted: {len(all_page_urls)}")
    else:
        print_status("Error: No URLs provided to process")
        logging.error("No URLs provided to process.")
