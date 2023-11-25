import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
import json
import os
import fitz  # PyMuPDF library
import numpy as np  # NumPy library


def lambda_handler(event, context):

    API_URL_FORMATTED_DATA = os.environ.get('API_URL_FORMATTED_DATA')
    API_URL_BLOG = os.environ.get('API_URL_BLOG')
    API_URL_PODCAST = os.environ.get('API_URL_PODCAST')
    API_URL_VIDEO = os.environ.get('API_URL_VIDEO')

    API_URL_SIMPLECAST = os.environ.get('API_URL_SIMPLECAST')

    API_KEY = os.environ.get('API_KEY')

    API_KEY_SIMPLECAST = os.environ.get('API_KEY_SIMPLECAST')

    API_KEY_ACCEL = os.environ.get('API_KEY_ACCEL')

    re_upload_document_url = 'https://api.llmate.ai/v1/integrate/datasource/653142b2ce4ab714d64af2b3/re-upload/'

    update_status_url = 'https://api.llmate.ai/v1/integrate/datasource/653142b2ce4ab714d64af2b3/update-upload-status/'

    LIMIT = 100  # Number of episodes to fetch per request
    OFFSET = 0   # Starting offset, change this to fetch different sets of episodes

    params = {'limit': LIMIT, 'offset': OFFSET}

    headers = {'Authorization': f'{API_KEY}'}
    headers_episodes = {'Authorization': f'Bearer {API_KEY_SIMPLECAST}'}

    response_formatted_data = requests.get(
        API_URL_FORMATTED_DATA, headers=headers)
    response_blog = requests.get(API_URL_BLOG, headers=headers)
    response_podcast = requests.get(API_URL_PODCAST, headers=headers)
    response_video = requests.get(API_URL_VIDEO, headers=headers)

    response_simplecast_episodes = requests.get(
        API_URL_SIMPLECAST, headers=headers_episodes, params=params)

    # Check if the request was successful (status code 200)
    if response_blog.status_code == 200 and response_formatted_data.status_code == 200 and response_podcast.status_code == 200 and response_video.status_code == 200 and response_simplecast_episodes.status_code == 200:

        # Parse the response content (JSON in this example)
        formatted_data = response_formatted_data.json()
        blog_data = response_blog.json()
        podcast_data = response_podcast.json()
        video_data = response_video.json()

        simplecast_episodes_data = response_simplecast_episodes.json()

        df = pd.DataFrame(columns=['Name', 'Content Link', 'Content', 'Short Description', 'Domain', 'Business Function',
                                   'Published Date', 'Author ID', 'Read Time', 'Meta Title', 'Meta Description', 'AI Summary', 'Episode No', 'Youtube Video URL', 'Guests', 'Hosts',
                                   'Content Type', 'Spotify Link', 'Simplecast Link', 'Audio Link'])

        guest_dict = {}
        guest_designation_dict = {}

        for fdata in formatted_data['hits']:
            if fdata['contentType'] != 'Blog':
                guests = fdata.get('guests', [])

                guests_names = []
                for guest in guests:
                    guest_dict[guest.get("_id")] = guest.get("name")
                    guest_designation_dict[guest.get(
                        "_id")] = guest.get("designation")

        host_dict = {}
        host_designation_dict = {}

        for fdata in formatted_data['hits']:
            if fdata['contentType'] != 'Blog':
                hosts = fdata.get('hosts', [])
                for host in hosts:
                    host_dict[host.get("_id")] = host.get("name")
                    host_designation_dict[host.get(
                        "_id")] = host.get("designation")

        episode_dict = {}
        count = 0

        for episode in simplecast_episodes_data['collection']:

            episode_dict["https://insightspodcast.in/episodes/" +
                         episode.get('slug', "")] = episode.get('enclosure_url', "")
            count += 1

        print(episode_dict)
        print(count)

        for blog in blog_data['hits']:

            row = {
                'Name': blog.get('json', {}).get('name', ""),
                'Content Link': "https://seedtoscale.com/blog/" + blog.get('json', {}).get('slug', ""),
                'Content': blog.get('json', {}).get('content-2', ""),
                'Short Description': blog.get('json', {}).get('short-description-2', ""),
                'Domain': blog.get('json', {}).get('domain-3', ""),
                'Business Function': blog.get('json', {}).get('business-function-3', ""),
                'Published Date': blog.get('json', {}).get('custom-published-date', ""),
                'Author ID': blog.get('json', {}).get('author', ""),
                'Read Time': blog.get('json', {}).get('read-time', ""),
                'Meta Title': blog.get('json', {}).get('meta-title', ""),
                'Meta Description': blog.get('json', {}).get('meta-description', ""),
                'AI Summary': blog.get('json', {}).get('ai-summary', "")
            }

            df.loc[len(df)] = row

        df['Clean AI Summary'] = df['AI Summary'].apply(html_to_markdown)

        for podcast in podcast_data['hits']:

            row = {
                'Name': podcast.get('json', {}).get('name', ""),
                'Content Link': "https://seedtoscale.com/podcast/" + podcast.get('json', {}).get('slug', ""),
                'Content': pdf_to_markdown(podcast.get('json', {}).get('file-transcript', {}).get('url', "")),
                'Short Description': podcast.get('json', {}).get('short-description', ""),
                'Domain': podcast.get('json', {}).get('domain', ""),
                'Business Function': podcast.get('json', {}).get('business-function', ""),
                'Published Date': podcast.get('json', {}).get('custom-published-date', ""),
                'Meta Title': podcast.get('json', {}).get('meta-title', ""),
                'Meta Description': podcast.get('json', {}).get('meta-description', ""),
                'Episode No': podcast.get('json', {}).get('episode-no', ""),
                'Youtube Video URL': podcast.get('json', {}).get('youtube-video-url', {}).get('url', ""),
                'Hosts': ";; ".join([host_dict.get(gid, '') + f"({host_designation_dict.get(gid, '')})" for gid in podcast.get('json', {}).get('hosts', [])]),
                'Guests': ";; ".join([guest_dict.get(gid, '') + f"({guest_designation_dict.get(gid, '')})" for gid in podcast.get('json', {}).get('guests-2', [])]),
                'Spotify Link': podcast.get('json', {}).get('spotify-url', ""),
                'Simplecast Link': podcast.get('json', {}).get('simplecast-url', "")

            }

            df.loc[len(df)] = row

        for video in video_data['hits']:

            row = {
                'Name': video.get('json', {}).get('name', ""),
                'Content Link': "https://seedtoscale.com/video/" + video.get('json', {}).get('slug', ""),
                'Content': video.get('json', {}).get('content', ""),
                'Short Description': video.get('json', {}).get('short-description', ""),
                'Domain': video.get('json', {}).get('domain', ""),
                'Business Function': video.get('json', {}).get('business-function', ""),
                'Published Date': video.get('json', {}).get('custom-published-date', ""),
                'Meta Title': video.get('json', {}).get('meta-title', ""),
                'Meta Description': video.get('json', {}).get('meta-description', ""),
                'Youtube Video URL': video.get('json', {}).get('video-link', {}).get('url', ""),
                'Hosts': host_dict.get(video.get('json', {}).get('host', ""), "") + f"({host_designation_dict.get(video.get('json', {}).get('host', ''),'')})",
                'Guests': ";; ".join([guest_dict.get(gid, '') + f"({guest_designation_dict.get(gid, '')})" for gid in video.get('json', {}).get('guests', [])])
            }

            df.loc[len(df)] = row

        # Step 2: Apply the html_to_markdown function to the "Content" column
        # and store the result in a new "Clean Content" column
        df['Clean Content'] = df['Content'].apply(html_to_markdown)

        # Step 3: Extract all weblinks from the "Clean Content" column for each row
        # and store them in a new "Related Blog Links" column in a numbered list format

        # Regular expression pattern to match URLs in the content
        url_pattern = re.compile(r'\[.*?\]\((https?://[^\)]+)\)')

        def extract_links(markdown_text):
            """
            Extracts all weblinks from the Markdown-like text and
            formats them as a numbered list.
            """
            links = url_pattern.findall(markdown_text)
            numbered_links = '\n'.join(
                [f'{i+1}. {link}' for i, link in enumerate(links)])
            return numbered_links

        # Apply the extract_links function to the "Clean Content" column
        df['Related Blog Links'] = df['Clean Content'].apply(extract_links)

        # Display the first few rows of the updated DataFrame to check the results
        df.head()

        # Step 4: Delete the original "Content" column from the DataFrame
        df.drop(columns=['Content'], inplace=True)
        df.drop(columns=['AI Summary'], inplace=True)

        df.rename(columns={'Clean Content': 'Content'}, inplace=True)
        df.rename(columns={'Clean AI Summary': 'Summary'}, inplace=True)

        content_dict = {}

        for fdata in formatted_data['hits']:
            content_type = fdata.get('contentType', "")
            content_name = fdata.get('name', "")
            content_dict[content_name] = content_type

        df['Content Type'] = df['Name'].map(content_dict)

        author_dict = {}

        for fdata in formatted_data['hits']:
            if fdata['contentType'] == 'Blog':
                authors = fdata.get('author', [])
                for author in authors:
                    author_id = author.get('_id', "")
                    author_name = author.get('name', "")
                    author_dict[author_id] = author_name

        df['Author'] = df['Author ID'].map(author_dict)

        df['Audio Link'] = df['Simplecast Link'].map(episode_dict)
        print(df['Audio Link'])

        file_name = 'accel_data_complete.csv'
        file_type = 'text/csv'
        headers_accel = {'Authorization': f'{API_KEY_ACCEL}'}

        # Step 1: Get the presigned URL
        presigned_url = get_presigned_url(
            re_upload_document_url, file_name, file_type, headers_accel)

        # Step 2: Upload the file to the presigned URL
        temp_file_path = '/tmp/accel_data_complete.csv'
        df.to_csv(temp_file_path)  # Assuming 'df' is your pandas DataFrame
        upload_file_to_s3(presigned_url, temp_file_path)

        # Step 3: Update the upload status
        update_upload_status(update_status_url, headers_accel)

    else:
        print(f"Request failed with status code: {response_blog.status_code}")

    # Return a response
    return {"statusCode": 200, "body": json.dumps("File uploaded successfully.")}


# Function to convert HTML to Markdown-like format
def html_to_markdown(html):
    """
    Convert HTML to a simplified Markdown-like format.
    This function doesn't cover all edge cases and might not work perfectly for very complex HTML.
    """
    # Initialize BeautifulSoup object
    soup = BeautifulSoup(html, 'html.parser')

    # Replace <a> tags with Markdown-style links, but filter out the specified domains
    for a_tag in soup.find_all('a'):
        href = a_tag.get('href', '')
        # List of domains to ignore
        ignore_domains = ['linkedin.com', 'accel.com', 'unsplash.com',
                          'instagram.com', 'uploads-ssl.webflow.com', 'typeform.com']
        if not any(domain in href for domain in ignore_domains):
            a_tag.replace_with(f'[{a_tag.text}]({href})')
        else:
            a_tag.replace_with(a_tag.text)

    # Replace <strong> and <b> tags with Markdown-style bold
    for tag in soup.find_all(['strong', 'b']):
        tag.replace_with(f'**{tag.text}**')

    # Replace <em> and <i> tags with Markdown-style italic
    for tag in soup.find_all(['em', 'i']):
        tag.replace_with(f'*{tag.text}*')

    # Replace <h1>, <h2>, ... <h6> with Markdown-style headers
    for i in range(1, 7):
        for tag in soup.find_all(f'h{i}'):
            tag.replace_with(f'{"#" * i} {tag.text}')

    # Replace <p> tags with just the text content, assuming a new line after each paragraph
    for tag in soup.find_all('p'):
        tag.replace_with(f'{tag.text}\n')

    # Replace <ul> and <ol> tags with Markdown-style lists
    for ul_tag in soup.find_all('ul'):
        items = ''
        for li_tag in ul_tag.find_all('li'):
            items += f'* {li_tag.text}\n'
        ul_tag.replace_with(f'{items}\n')

    for ol_tag in soup.find_all('ol'):
        items = ''
        for i, li_tag in enumerate(ol_tag.find_all('li'), start=1):
            items += f'{i}. {li_tag.text}\n'
        ol_tag.replace_with(f'{items}\n')

    # Get the final "Markdown-like" text
    markdown_text = soup.get_text()

    return markdown_text

# Function to convert PDF to markdown like format


def pdf_to_markdown(url):

    if url == "":
        return ""
    markdown_text = ""

    content = requests.get(url).content

    # Open the PDF file
    pdf_document = fitz.open(stream=content, filetype="pdf")

    for page_num in range(pdf_document.page_count):
        # Get the page
        page = pdf_document[page_num]

        # Extract text from the page
        page_text = page.get_text()

        # Append extracted text to the Markdown string
        markdown_text += page_text + "\n\n"

    # Close the PDF document
    pdf_document.close()

    return markdown_text


def get_presigned_url(api_url, file_name, file_type, headers):
    payload = {
        "file_name": file_name,
        "file_type": file_type
    }
    response = requests.post(api_url, json=payload, headers=headers)
    if response.status_code == 200:
        # Assuming the response contains the presigned URL
        return response.json().get('url')
    else:
        raise Exception(f"Failed to get presigned URL: {response.text}")


def upload_file_to_s3(presigned_url, file_path):
    with open(file_path, 'rb') as f:
        files = {'file': (file_path, f)}
        response = requests.put(presigned_url, files=files)
        if response.status_code != 200:
            raise Exception(f"Failed to upload file to S3: {response.text}")


def update_upload_status(update_api_url, headers):
    payload = {
        "success": True
    }
    response = requests.post(update_api_url, json=payload, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to update upload status: {response.text}")
