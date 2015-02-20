
""" 
Download and parse latest list of partner distributions and search plugins.
Script expects 4 command-line arguments containing 4 output CSV file names:
  - table of full partner build info
  - table of partner build distribution IDs
  - table of partner build search plugins
  - table of Firefox official search plugins
provided in that order.
"""

import urllib2
from HTMLParser import HTMLParser
import ConfigParser
import codecs
import json
import sys
from operator import itemgetter


# Base dir containing partner repack config files.
partner_packs_url = 'https://hg.mozilla.org/build/partner-repacks/file/tip/'
# Localization config containing search plugins. 
spv_url = 'http://l10n.mozilla-community.org/~flod/p12n/searchplugins.json'

# Script extracts partner info from packing configs.
# Create separate lists for current and expired partner builds. 
partner_dirs = {
    'current': 'partners',
    'expired': 'inactive-configs'
}

# Headers for output CSVs.
# Filenames will be read from command-line args and added for each entry
# with the key 'file'.
output = {
    # Table containing partner package information - 
    # distribution IDs and search default. 
    'full_package_info': {
        'headers': [
            'type',
            'pack_name',
            'partner',
            'distrib_id',
            'repack_distrib_id',
            'search_default'
        ]
    },
    # List of partner distrib ID strings by partner 
    # (set of unique IDs based on both config files)
    'distrib_ids': {
        'headers': [
            'type',
            'partner',
            'distrib_id'
        ]
    },
    # List of partner search defaults.
    'distrib_search': {
        'headers': [
            'type',
            'partner',
            'search_name'
        ]
    },
    # Table of localized partner search strings.
    'all_search': {
        'headers': [
            'plugin_id',
            'search_name'
        ]
    }
}


class PartnerListParser(HTMLParser):
    """ Parser implementation to extract package subdir URLs 
        from package dir page.
    """
    def get_partner_links(self, dirname):
        """ Parse HTML directory listing of partner build packages.
            Page is a table of build names linking to build configs dir.
            Pass in the subdir name containing partner build dirs of interest.
            Returns a dict mapping partner build names to config dir URLs.
        """
        self.dirname = '/' + dirname + '/'
        # Maintain cache of current href attribute from 'a' tags 
        # to know when to store data.
        self.current_link = None
        # Output mapping.
        self.partner_links = {}
        
        html = urllib2.urlopen(partner_packs_url + dirname).read()
        self.feed(html)
        
        return self.partner_links
    
    def handle_starttag(self, tag, attrs):
        """ Look for anchor tags pointing to package subdirs.
            Cache URL when such a tag is encountered. """
        if tag == 'a':
            # Find href URLs containing self.dirname.
            href = [att[1] for att in attrs if att[0] == 'href'][0]
            if self.dirname in href:
                self.current_link = href
    
    def handle_endtag(self, tag):
        """ Clear current URL on tag close. """
        if tag == 'a' and self.current_link is not None:
            self.current_link = None
    
    def handle_data(self, data):
        """ Store package dir name as the non-trivial text content 
            of the 'a' tag. """
        if self.current_link is not None:
            if data is not None and len(data) > 0 and data != 'files':
                if data not in self.partner_links:
                    self.partner_links[data] = self.current_link


class FakeSecHead(object):
    """ Add fake section header for repack.cfg, 
        so that these files play well with ConfigParser.
        Adds section header 'FakeSection' to beginning of file on reading.
        Taken from 'http://stackoverflow.com/questions/2819696/
            parsing-properties-file-in-python'
    """
    def __init__(self, fp):
        self.fp = fp
        self.first_line = True
    def readline(self):
        if self.first_line:
            self.first_line = False
            return u'[FakeSection]\n'
        return self.fp.readline()


class UnicodeUrl:
    """ Ensure files downloaded from remote URLs are read as UTF-8. 
        Necessary for correctly reading search plugin names. 
    """
    def __init__(self, url):
        self.fp = urllib2.urlopen(url)
    def readline(self):
        return codecs.decode(self.fp.readline(), 'utf8', 'replace')


# Fields to look for in config files: (<section>, <option>)
config_options = { 
    'distrib_id': ('Global', 'id'),
    'partner': ('Preferences', 'mozilla.partner.id'),
    'repack_distrib_id': ('FakeSection', 'dist_id')
}
search_option = ('LocalizablePreferences', 'browser.search.defaultenginename')


# Encode and write list to a file as CSV row.
def write_row(fp, row):
    fp.write(','.join(row).encode('utf8') + '\n')


def generate_partner_tables():
    """ Download and parse necessary information from partner build configs.
        Organize information into tables and print to output files.
    """
    html_parser = PartnerListParser()
    partner_info = []
    
    for partner_type in partner_dirs:
        # For each group of partners, 
        # get list of partner IDs and default search plugins.
        config_urls = html_parser.get_partner_links(partner_dirs[partner_type])
        
        for name in sorted(config_urls.keys()):
            link = config_urls[name].replace('/file/', '/raw-file/')
            link = 'https://hg.mozilla.org' + link
            vals = { 'type': partner_type, 'pack_name': name }
            
            # Parse config files.
            configs = ConfigParser.RawConfigParser()
            configs.readfp(UnicodeUrl(link + '/distribution/distribution.ini'))
            configs.readfp(FakeSecHead(UnicodeUrl(link + '/repack.cfg')))
            
            # Read partner distribution ID strings.
            for co in config_options:
                try:
                    vals[co] = configs.get(*config_options[co]).strip('"')
                except:
                    pass
            
            # Fill in missing mozilla partner IDs using package name.
            # Common partner ID is part before underscore.
            if 'partner' not in vals:
                vals['partner'] = name.split('_')[0]
            
            # Special case for MSN - partner ID is different 
            # for each distribution (localized). Set it explicitly.
            if name.startswith('msn'):
                vals['partner'] = 'msn'
            
            # Search default could be in any LocalizablePreferences section -
            # add entry for each.
            searchdefs = []
            locprefs = [section for section in configs.sections() 
                                    if section.startswith(search_option[0])]
            for locprefs_sec in locprefs:
                try:
                    searchdefs.append(configs.get(locprefs_sec, 
                                                  search_option[1]))
                except:
                    pass
            
            if len(searchdefs) == 0:
                # No default search plugins. Add previously collected vals.
                partner_info.append(vals)
            else:
                # Add row entry copying previous vals for each search default.
                for sd in searchdefs:
                    vals['search_default'] = sd
                    partner_info.append(dict(vals))
    
    # Separate out distrib IDs and search plugins by partner name.
    distrib_ids = {}
    search_names = {}
    
    for entry in partner_info:
        key = (entry['type'], entry['partner'])
        if key not in distrib_ids:
            distrib_ids[key] = set()
        # Add distribution IDs from both sources.
        distrib_ids[key].add(entry['distrib_id'])
        distrib_ids[key].add(entry['repack_distrib_id'])
        if 'search_default' in entry:
            if key not in search_names:
                search_names[key] = set()
            search_names[key].add(entry['search_default'])
    
    # Output to CSV files.
    # Full table.
    with open(output['full_package_info']['file'], 'w') as f:
        write_row(f, output['full_package_info']['headers'])
        for row in partner_info:
            row = [row.get(key, '') 
                        for key in output['full_package_info']['headers']]
            write_row(f, row)
        f.close()
    
    # Distribution IDs by partner.
    with open(output['distrib_ids']['file'], 'w') as f:
        write_row(f, output['distrib_ids']['headers'])
        for p in sorted(distrib_ids.keys()):
            ids = list(distrib_ids[p])
            ids.sort()
            for id in ids:
                write_row(f, [p[0], p[1], id])
        f.close()
    
    # Search plugins by partner.
    with open(output['distrib_search']['file'], 'w') as f:
        write_row(f, output['distrib_search']['headers'])
        for p in sorted(search_names.keys()):
            ids = list(search_names[p])
            ids.sort()
            for id in ids:
                write_row(f, [p[0], p[1], id])
        f.close()


def generate_plugin_list():
    """ Download and parse necessary information on search plugins from
        localization JSON. Organize information into a table and 
        print to output file.
    """
    spv_json = json.load(urllib2.urlopen(spv_url))
    plugins = set()
    
    # Extract search plugin IDs and full names.
    # Search plugins are nested down multiple levels.
    for loc in spv_json['locales']:
        for app in spv_json['locales'][loc]:
            for channel in spv_json['locales'][loc][app]:
                spv_list = spv_json['locales'][loc][app][channel]
                spv_list = spv_list['searchplugins']
                for plugin_id in spv_list:
                    plugins.add((plugin_id, 
                                 '"%s"' % spv_list[plugin_id]['name']))
    
    plugins = list(plugins)
    plugins.sort(key = itemgetter(0,1))
    
    with open(output['all_search']['file'], 'w') as f:
        write_row(f, output['all_search']['headers'])
        for p in plugins:
            write_row(f, p)
        f.close()


def main(args):
    """ Read file names from command-line args and generate tables.
    """
    args = zip([
        'full_package_info',
        'distrib_ids',
        'distrib_search',
        'all_search'
    ], args)
    for a in args:
        output[a[0]]['file'] = a[1]
        
    generate_partner_tables()
    generate_plugin_list()

if __name__ == '__main__':
    # Check that all output files are specified.
    if len(sys.argv) < 5:
        sys.exit('Usage requires 4 command-line args' +
                 ' that are output file names')
    main(sys.argv[1:])

