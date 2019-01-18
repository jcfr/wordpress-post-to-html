#!/usr/bin/env python

import json
import requests

from bs4 import BeautifulSoup
from joblib import Parallel, delayed
from time import sleep

BLOG_URL = "https://blog.kitware.com"


def extract_slug(url):
    """Extract `this-is-the-slug` given an url for the form `https://blog.kitware.com/this-is-the-slug/`
    or `https://blog.kitware.com/this-is-the-slug`
    """
    if url.endswith("/"):
        url = url[:-1]
    return url.split("/")[-1]


def post_content_from_soup(soup_object):
    soup_entry_content = soup_object.find("div", class_="entry-content")

    # remove social media sharing section
    sharing = soup_entry_content.find("div", class_="sd-sharing-enabled")
    if sharing is not None:
        sharing.extract()
    # remove empty divs
    for div in soup_entry_content.find_all("div"):
        if div.text == "":
            div.extract()
    # update img source to include blog host
    for img in soup_entry_content.find_all("img"):
        img["src"] = BLOG_URL + img["src"] if img["src"].startswith("/") else img["src"]
    # strip style tag
    for styled in soup_entry_content.select("[style]"):
        styled["style"] = styled["style"].replace("text-align: left;", "")
        if styled["style"] == "":
            del styled["style"]
    # remove url host from 
    #html.select('a[href*="https://blog.kitware.com/"]')

    return soup_entry_content


#def soup_has_post_content(soup_object):
#    return soup_object.find("div", class_="entry-content") is not None


def post_metadata_from_soup(soup_article):
    metadata = {}
    # id
    if soup_article.has_attr("id"):
        metadata["id"] = soup_article['id'].split("-")[1]

    soup_header = soup_article.find("header")
    soup_entry_title = soup_header.find(class_="entry-title")

    # title
    metadata["title"] =soup_entry_title.string
    # slug
    if soup_entry_title.find("a") is not None:
        metadata["slug"] = extract_slug(soup_entry_title.a['href'])
    # date
    metadata["date"] = soup_header.find("span", class_="date").text.strip()
    # authors
    metadata["authors"] = [
        {"name": vcard.a.text, "slug": extract_slug(vcard.a["href"])} 
        for vcard in soup_header.find_all("span", class_="vcard")
    ]
    # tags
    soup_tags = soup_header.find("div", class_="tagscontainer")
    if soup_tags is not None:
        metadata["tags"] = [
            {"slug": extract_slug(tag["href"]), "name": tag.text} 
            for tag in soup_tags.find_all("a")
        ]

    return metadata


def posts_from_page_listing(page_number, verbose=0):
    page_url = BLOG_URL + '/page/%s/' % page_number
    if verbose >= 1:
        print("Fetching listing from %s" % page_url)
    soup = BeautifulSoup(requests.get(page_url).text, 'html.parser')
    return [post_metadata_from_soup(article) for article in soup.find(id="contentcontainer").find_all("article")]


def fetch_post_as_soup(post_id):
    post_url = BLOG_URL + '/?p=%s' % post_id
    return BeautifulSoup(requests.get(post_url).text, 'html.parser')


def load_metadata(filename):
    with open(filename) as metadata_file:
        return json.load(metadata_file)


def save_metadata(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, sort_keys=True, indent=4, ensure_ascii=False)

        
def save_content(html, filename):
    with open(filename, 'w') as f:
        f.write('<meta charset="utf-8">\n' + html)


def page_range(last_page=1, first_page=1):
    return range(last_page, first_page - 1, -1)


def save_posts(last_page=1, first_page=1, data_dir=".", verbose=0):
    for page_number in page_range(last_page=last_page, first_page=first_page):
        posts = posts_from_page_listing(page_number, verbose=verbose)
        for metadata in posts:
            basename = "{id}_{slug}".format(**metadata)
            
            soup = fetch_post_as_soup(metadata["id"]).find("article")

            metadata_filename = data_dir + "/%s.json" % basename
            if verbose >= 2:
                print("  %s" % metadata_filename)
            additional_metadata = post_metadata_from_soup(soup)
            metadata = {**metadata, **additional_metadata}
            save_metadata(metadata, metadata_filename)
            
            content_filename = data_dir + "/%s.html" % basename
            if verbose >= 2:
                print("  %s" % content_filename)
            save_content(post_content_from_soup(soup).prettify(), content_filename)


def save_posts_fake(last_page=1, first_page=1, data_dir=".", verbose=0):
    for page_number in page_range(last_page=last_page, first_page=first_page):
        if verbose >= 1:
            print("Fetching listing from page %s" % page_number)
        for index in range(10):
            metadata = {"id": index, "last_page": last_page, "first_page": first_page}
            basename = "{last_page}_{id}".format(**metadata)
            metadata_filename = data_dir + "/%s.json" % basename
            if verbose >= 2:
                print("  %s" % metadata_filename)
            save_metadata(metadata, metadata_filename)
            sleep(0.3)


def parallel_save_post(n_jobs=10, last_page=1, first_page=1, data_dir=".", verbose=0, test_mode=False):
    save_posts_func = save_posts
    if test_mode:
        save_posts_func = save_posts_fake
    r = Parallel(n_jobs=10, verbose=10)(
            delayed(save_posts_func)(
                last_page=page_number,
                first_page=page_number,
                data_dir="posts",
                verbose=verbose
            )
            for page_number in page_range(last_page=last_page, first_page=first_page)
        )


if __name__ == "__main__":
    # save_posts(last_page=190, first_page=190, data_dir="posts")
    # save_posts_fake(last_page=190, first_page=190, data_dir="posts")

    #parallel_save_post(last_page=190, data_dir="posts", verbose=1)
    #soup = fetch_post_as_soup(13304)
    #soup_article = soup.find("article")
    #html = post_content_from_soup(soup_article).prettify()
    #save_content(html, "test3.html")

    from joblib import Parallel, delayed
    last_page=195

    r = Parallel(n_jobs=10, verbose=10)(
        delayed(save_posts)(
            last_page=page_number,
            first_page=page_number,
            data_dir="posts"
        )
        for page_number in page_range(last_page=last_page)
    )

