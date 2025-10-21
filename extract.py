#!/usr/bin/env python3
"""
extract.py: Text extraction utilities for WARC records.
"""
from __future__ import annotations

import logging
import re
from bs4 import BeautifulSoup, Comment, FeatureNotFound, XMLParsedAsHTMLWarning

class TextExtractor:
    """Extracts clean text from HTML content."""
    @staticmethod
    def extract_clean_text(html_content_bytes: bytes, logger: logging.Logger, min_text_length: int) -> str:
        html_str = ""
        encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1', 'windows-1252']
        for enc in encodings_to_try:
            try:
                html_str = html_content_bytes.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        else:
            html_str = html_content_bytes.decode('ascii', errors='replace')
            logger.debug("Decoded HTML content as ASCII with replacements after common encodings failed.")

        soup = None
        parsers = [('lxml', {}), ('html.parser', {})]
        for parser_name, options in parsers:
            try:
                soup = BeautifulSoup(html_str, parser_name, **options)
                break
            except FeatureNotFound:
                logger.debug(f"BeautifulSoup parser '{parser_name}' not found. Trying next.")
            except XMLParsedAsHTMLWarning:
                 logger.debug(f"BeautifulSoup (lxml) issued XMLParsedAsHTMLWarning, but proceeding.")
                 break
            except Exception as e_parse:
                logger.debug(f"BeautifulSoup with '{parser_name}' failed: {e_parse}. Trying next.")

        if soup is None:
            logger.warning("All BeautifulSoup parsers failed to parse the HTML content.")
            return ""

        tags_to_decompose = [
            'script', 'style', 'noscript', 'meta', 'link', 'header',
            'footer', 'nav', 'aside', 'form', 'button', 'input',
            'select', 'textarea', 'iframe', 'embed', 'object',
            'applet', 'figure', 'figcaption', 'canvas', 'svg', 'path', 'video', 'audio'
        ]
        for tag_name in tags_to_decompose:
            for tag_element in soup.find_all(tag_name):
                tag_element.decompose()

        for comment_element in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment_element.extract()

        text_segments = []
        main_content_selectors = [
            'article', 'main', '[role="main"]',
            '.content', '#content', '.main-content', '#main-content',
            '.post-body', '.entry-content', '.article-body'
        ]

        found_main_content = False
        for selector in main_content_selectors:
            elements = soup.select(selector)
            for element in elements:
                segment = element.get_text(separator=' ', strip=True)
                if segment:
                    text_segments.append(segment)
                    found_main_content = True
            if found_main_content:
                break

        current_text_length = sum(len(s) for s in text_segments)
        if not found_main_content or current_text_length < min_text_length * 0.5:
            if not found_main_content :
                p_elements = soup.find_all('p')
                for p_tag in p_elements:
                    segment = p_tag.get_text(separator=' ', strip=True)
                    if segment:
                        text_segments.append(segment)

            if not text_segments or sum(len(s) for s in text_segments) < min_text_length * 0.25 :
                target_element = soup.body if soup.body else soup
                if target_element:
                    segment = target_element.get_text(separator=' ', strip=True)
                    if segment:
                         text_segments.append(segment)

        full_text = " ".join(filter(None, text_segments))
        full_text = re.sub(r'\s+', ' ', full_text).strip()

        return full_text