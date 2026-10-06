"""LiveSQLBench's official Soft-EX helpers -- VENDORED VERBATIM. Do not edit.

Six functions copied byte for byte from bird-bench/livesqlbench,
evaluation/src/test_utils.py at commit 5aab9623d6ce58d32e252f8c307f08fbcbdf4a70. The
upstream module also imports a PostgreSQL driver and its own database
helpers, which grading on DuckDB does not use, so only these functions and
the standard-library imports they need are kept. tests/test_vendored_lsb.py
re-asserts each function's sha256.

Upstream licence follows.

    MIT License

    Copyright (c) 2024 bird_sql

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
"""

import json
import logging
import re
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal


def process_decimals_recursive(item, decimal_places):
    """
    Recursively process decimals in any data structure (list, dict, tuple).
    Returns a new structure with all decimals rounded to specified places.
    """
    quantizer = Decimal(1).scaleb(-decimal_places)
    
    if isinstance(item, Decimal):
        return float(item.quantize(quantizer, rounding=ROUND_HALF_UP))
    elif isinstance(item, float):
        return float(Decimal(str(item)).quantize(quantizer, rounding=ROUND_HALF_UP))
    elif isinstance(item, (list, tuple)):
        return type(item)(process_decimals_recursive(x, decimal_places) for x in item)
    elif isinstance(item, dict):
        return {k: process_decimals_recursive(v, decimal_places) for k, v in item.items()}
    else:
        return item


def preprocess_results(results, decimal_places=2):
    """
    Process the result set:
    - Replace dates with normalized string: YYYY-MM-DD
    - Convert tuples to lists for JSON serializability
    - Convert any unhashable types (dicts, lists) to their string representation for comparison
    - Process decimals recursively in all nested structures
    """
    processed = []
    for result in results:
        processed_result = []
        for item in result:
            if isinstance(item, (date, datetime)):
                processed_result.append(item.strftime('%Y-%m-%d'))
            else:
                # Process decimals recursively first
                processed_item = process_decimals_recursive(item, decimal_places)
                if isinstance(processed_item, (dict, list)):
                    # Convert unhashable types to their string representation with sorted keys
                    processed_result.append(json.dumps(processed_item, sort_keys=True))
                else:
                    processed_result.append(processed_item)
        processed.append(tuple(processed_result))
    return processed


def remove_distinct(sql_list):
    """
    Remove all occurrences of the DISTINCT keyword (in any case form)
    from a single list of SQL query strings, but preserve DISTINCT ON clauses.
    
    This function uses regex to:
    - Remove standalone DISTINCT keywords
    - Preserve DISTINCT ON (column_list) clauses
    - Handle case-insensitive matching

    Parameters:
    -----------
    sql_list : list of str
        A list of SQL queries (strings).

    Returns:
    --------
    list of str
        A new list of SQL queries with standalone 'DISTINCT' keywords removed.
    """

    cleaned_queries = []
    for query in sql_list:
        # Pattern to match DISTINCT but not DISTINCT ON
        # \b ensures word boundaries, so "DISTINCT" matches but "DISTINCTON" doesn't
        # (?![^()]*\bON\b) negative lookahead ensures we don't match if ON follows
        # This handles cases like "DISTINCT ON (col1, col2)" - we keep the whole thing
        pattern = r'\bDISTINCT\b(?![^()]*\bON\b)'
        cleaned_query = re.sub(pattern, '', query, flags=re.IGNORECASE)
        
        # Clean up any extra whitespace that might be left
        cleaned_query = re.sub(r'\s+', ' ', cleaned_query).strip()
        cleaned_queries.append(cleaned_query)

    return cleaned_queries


def remove_comments(sql_list): 
    """
    Remove all SQL comments from each query string in the list.
    - Block comments: /* ... */
    - Line comments: -- ... (to end of line)
    Also collapses multiple blank lines into one, and strips leading/trailing whitespace.
    """
    cleaned = []
    for sql in sql_list:
        # remove block comments
        no_block = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        # remove line comments, keep newline
        no_line  = re.sub(r'--.*?(\r\n|\r|\n)', r'\1', no_block)
        # collapse extra blank lines
        no_blank = re.sub(r'\n\s*\n+', '\n', no_line)
        cleaned.append(no_blank.strip())
    return cleaned


def remove_round_functions(sql_string):
    """
    Remove all ROUND() function calls from a SQL string, including nested ones.
    This regex properly handles nested functions with commas.
    """
    
    def find_matching_paren(text, start_pos):
        """Find the position of the matching closing parenthesis."""
        paren_count = 0
        for i in range(start_pos, len(text)):
            if text[i] == '(':
                paren_count += 1
            elif text[i] == ')':
                paren_count -= 1
                if paren_count == 0:
                    return i
        return -1
    
    def find_first_arg_end(text, start_pos):
        """Find the end of the first argument, accounting for nested parentheses."""
        paren_count = 0
        for i in range(start_pos, len(text)):
            if text[i] == '(':
                paren_count += 1
            elif text[i] == ')':
                if paren_count == 0:
                    return i  # End of ROUND function
                paren_count -= 1
            elif text[i] == ',' and paren_count == 0:
                return i  # End of first argument
        return len(text)
    
    result = sql_string
    
    while True:
        # Find ROUND function (case insensitive)
        pattern = re.compile(r'ROUND\s*\(', re.IGNORECASE)
        match = pattern.search(result)
        
        if not match:
            break
            
        start_pos = match.start()
        open_paren_pos = match.end() - 1
        
        # Find the end of the first argument
        first_arg_end = find_first_arg_end(result, open_paren_pos + 1)
        
        # Find the matching closing parenthesis
        close_paren_pos = find_matching_paren(result, open_paren_pos)
        
        if close_paren_pos == -1:
            break  # Malformed SQL, can't find closing paren
        
        # Extract the first argument
        first_arg = result[open_paren_pos + 1:first_arg_end].strip()
        
        # Replace ROUND(...) with just the first argument
        result = result[:start_pos] + first_arg + result[close_paren_pos + 1:]
    
    return result


def remove_round(sql_list):
    """
    Remove ROUND function calls while preserving the inner expression.
    For example: 
    - ROUND(column, 2) -> column
    - ROUND(ROUND(price, 2), 1) -> ROUND(price, 2) -> price (handles nested ROUNDs)
    """
    cleaned = []
    for sql in sql_list:
        result = sql
        result = remove_round_functions(result)
        cleaned.append(result)
        if "ROUND" in result:
            logging.warning(f"ROUND found in {result}")
    return cleaned
