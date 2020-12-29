from cpe import CPE
from collections import namedtuple
from functools import lru_cache
from typing import List
import re

Match = namedtuple('Match', ['vulnerable', 'cpes'])

# Empty/wildcard CPE parts:
EMPTY_WILDCARD_CPE_SET = ('*', '-', '')


def is_vulnerable(candidate_cpes, configurations):
    '''
    This is the main entry point for this library.  External code should only
    need to worry about calling this function, and none of the others.

    Inputs:
    - an iterable of cpe strings
    - a list of dictionaries as contained within the "configurations" > "nodes"
      sections of the NVD CVE JSON data feed.
    '''
    vulnerable = False
    match_cpes = set()
    for node in configurations:
        match = handle_node(candidate_cpes, node)
        if match.vulnerable:
            vulnerable = True
            for match_cpe in match.cpes:
                match_cpes.add(match_cpe)

    result = Match(vulnerable=vulnerable, cpes=list(match_cpes))
    return result


def and_func(matches):
    '''
    This function takes an iterable of match results and checks to make sure
    all of the match results are "truthy," combining and returning the results
    if so.
    '''
    all_vulnerable = all(m.vulnerable for m in matches)
    match_cpes = set()

    if all_vulnerable:
        for match in matches:
            for cpe in match.cpes:
                match_cpes.add(cpe)

    result = Match(vulnerable=all_vulnerable, cpes=list(match_cpes))
    return result


def or_func(matches):
    '''
    This function takes an iterable of match results and checks to make sure
    at least one of the match results is "truthy," returning **all** of the
    matching results if so (not just the first one to match).
    '''
    any_vulnerable = False
    match_cpes = set()

    for match in matches:
        if match.vulnerable:
            any_vulnerable = True
            for cpe in match.cpes:
                match_cpes.add(cpe)

    result = Match(vulnerable=any_vulnerable, cpes=list(match_cpes))
    return result


def not_op(match):
    '''
    This function returns a cpe match result with no cpes, and a truthiness
    value opposite that of its input.
    '''
    if match.vulnerable:
        return Match(vulnerable=False, cpes=set())
    else:
        return Match(vulnerable=True, cpes=set())


def handle_node(candidate_cpes, node):
    '''
    This function handles checking a set of device CPEs against a CVE
    configuration node, including recursion and boolean operations.
    '''
    debug = []
    if node['operator'] == 'AND':
        and_or_op = and_func
        debug.insert(0, 'AND')
    elif node['operator'] == 'OR':
        and_or_op = or_func
        debug.insert(0, 'OR')
    else:
        raise NotImplementedError(node)

    if node.get('negate', False):
        # TODO: HANDLE NEGATION
        final_op = lambda x: not_op(and_or_op(x))  # noqa: E731
        debug.insert(0, 'NOT')
    else:
        final_op = and_or_op

    child_results = [handle_node(candidate_cpes, child) for child in node.get('children', [])]

    versionRangeAttrs = ['versionstartincluding', 'versionstartexcluding', 'versionendincluding', 'versionendexcluding']

    # Filter out any cpe match entries that rely on version ranges, just in
    # case. These should have all been replaced in the data by the NVD Updater
    # before ever reaching this point, but if they snuck through somehow, they
    # would wreak havoc and cause a ton of false positives, so it's better to
    # be defensive.
    match_results = [
        handle_match(candidate_cpes, cpe_match) for cpe_match in node.get('cpe_match', [])
        if not any(attr in cpe_match for attr in versionRangeAttrs)
    ]

    result = final_op(child_results + match_results)
    return result


@lru_cache(maxsize=20000)
def cpe_to_regex(cpe):
    '''
    This function converts a CPE to a regular expression, attempting to
    compensate for the weird escaping nuances of both CPEs and regular
    expressions.
    '''
    s = re.escape(cpe)
    s = s.replace('\\?', '[^:]?')
    s = s.replace('\\*', '[^:]*')
    return re.compile(s)


def handle_match(candidate_cpes, match):
    '''
    This function handles the lowest-level details of matching two sets of CPEs
    against each other.
    '''
    vulnerable = False
    match_cpes = set()

    cpe_str = match.get('cpe23uri', match.get('cpe23Uri'))

    # This conditional statement excludes CPEs that accept all versions.
    # This regrettable hack is necessitated by overly-broad wildcarding within
    # dumpsterfires such as https://nvd.nist.gov/vuln/detail/CVE-2017-8244
    cpe_version = cpe_str.split(':')[5]
    if cpe_version != '*':
        cve_cpe_regex = cpe_to_regex(cpe_str)
        for candidate_cpe in candidate_cpes:
            if cve_cpe_regex.match(candidate_cpe):
                vulnerable = True
                match_cpes.add(candidate_cpe)

    match = Match(vulnerable=vulnerable, cpes=list(match_cpes))
    return match


def filter_generic_cpes(cpe_list: List[str]) -> List[str]:
    '''
    This function takes in a list of CPE strings and filters out any
    CPEs with any specific information past the version number (edition, lang, etc).

    Returns a new list of CPE strings.
    '''
    filtered_cpes = []
    for cpe in cpe_list:
        c = CPE(cpe)
        # yapf: disable
        if (c.get_update()[0] in EMPTY_WILDCARD_CPE_SET and
                c.get_edition()[0] in EMPTY_WILDCARD_CPE_SET and
                c.get_language()[0] in EMPTY_WILDCARD_CPE_SET and
                c.get_software_edition()[0] in EMPTY_WILDCARD_CPE_SET and
                c.get_target_software()[0] in EMPTY_WILDCARD_CPE_SET and
                c.get_target_hardware()[0] in EMPTY_WILDCARD_CPE_SET and
                c.get_other()[0] in EMPTY_WILDCARD_CPE_SET):
            # yapf: enable
            filtered_cpes.append(cpe)
    return filtered_cpes
