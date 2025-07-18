"""Methods for working with urllib scraping web pages"""

import logging
import tomllib
from urllib import request, error
import xml.etree.ElementTree as ET

with open("pyproject.toml", "rb") as f:
    pyproject = tomllib.load(f)

version = pyproject["project"]["version"]
cdippy_lib = f"CDIPpy/{version}"

_headers = {"User-Agent": cdippy_lib}
logger = logging.getLogger(__name__)


def _make_cdippy_request(url):
    req = request.Request(url, headers=_headers)
    try:
        return request.urlopen(req)
    except error.URLError as e:
        logger.exception(f"URL error: {e.reason}")
    except error.HTTPError as e:
        logger.exception(f"HTTP error: request to {url} returned {e.code} - {e.reason}")
    except Exception as e:
        logger.exception(e)
    return None


def rfindta(el, r, tag, attr):
    """Recursively find tags with value tag and attribute attr and append to list r"""
    if len(el) > 0:
        for c in el:
            rfindta(c, r, tag, attr)
    if el.tag.find(tag) >= 0:
        for child in el.attrib:
            if child.find(attr) >= 0:
                r.append(el.attrib[child])


def rfindt(el, r, tag):
    """Recursively find tags with value tag append to list r"""
    if len(el) > 0:
        for c in el:
            rfindt(c, r, tag)
    if el.tag.find(tag) >= 0:
        r.append(el.text)


def read_url(url):
    response = _make_cdippy_request(url)
    return response.read().decode("UTF-8") if response else None


def load_et_root(url):
    response = _make_cdippy_request(url)
    return ET.fromstring(response.read()) if response else None
