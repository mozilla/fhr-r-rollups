# Download and parse latest list of partner distributions and search plugins.

import urllib2
from HTMLParser import HTMLParser
import ConfigParser
import codecs
import json
from operator import itemgetter


# Extract parnter info from packing configs.
# Create separate lists for current and expired partner builds. 
partner_dirs = {
    'current': 'partners',
    'expired': 'inactive-configs'
}

# Base dir containing partner repack config files.
partner_packs_url  = 'https://hg.mozilla.org/build/partner-repacks/file/tip/'
# Localization config containing search plugins. 
spv_url = 'http://l10n.mozilla-community.org/~flod/p12n/searchplugins.json'

# Table containing partner package information - 
# distribution IDs and search default. 
output_package_info = 'full_partner_info.csv'
# List of partner distrib ID strings by partner (set of unique IDs
# based on both config files)
output_distrib_ids = 'partner_distrib_ids.csv'
# List of partner search defaults.
output_distrib_search = 'partner_distrib_search.csv'
# Table of localized partner search strings.
output_all_search = 'official_search_plugins.csv'
    
# Parse HTML from repo package dir page. 
class PartnerListParser(HTMLParser):
    
    # Map package names to their subdir URLs.
    # Pass in dirname containing partner build dirs.
    def get_partner_links(self, html, dirname):
        # Maintain current href attribute from 'a' tags to know
        # when to store data.
        self.dirname = '/' + dirname + '/'
        self.current_link = None
        self.partner_links = {}
        self.feed(html)
        return self.partner_links
    
    # Look for anchor tags pointing to package subdirs.
    # Cache URL when such a tag is encountered.
    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            # Find href tags pointing to ".../partners/..." URLs.
            href = [att[1] for att in attrs if att[0] == 'href'][0]
            if self.dirname in href:
                self.current_link = href
    
    # Clear current URL on tag close.
    def handle_endtag(self, tag):
        if tag == 'a' and self.current_link is not None:
            self.current_link = None
    
    # Store package dir name as the non-trivial text content of the 'a' tag. 
    def handle_data(self, data):
        if self.current_link is not None:
            if data is not None and len(data) > 0 and data != 'files':
                if data not in self.partner_links:
                    self.partner_links[data] = self.current_link

# Add fake section header for repack.cfg, 
# so that these files play well with ConfigParser.
# Adds section header 'FakeSection' to beginning of file on reading.
# http://stackoverflow.com/questions/2819696/parsing-properties-file-in-python
class FakeSecHead(object):
    def __init__(self, fp):
        self.fp = fp
        self.first_line = True
    def readline(self):
        if self.first_line:
                self.first_line = False
                return u'[FakeSection]\n'
        return self.fp.readline()

# Ensure files are read as UTF-8 
# Necessary for correctly reading search plugin names.
class UnicodeUrl:
    def __init__(self, url):
        self.fp = urllib2.urlopen(url)
    def readline(self):
        return codecs.decode(self.fp.readline(), 'utf8', 'replace')
#-----------------------------------------

# Get list of partner IDs and search plugins for each group of partners.
        
html_parser = PartnerListParser()
# Fields to look for in config files: (<section>, <option>)
config_options = { 
    'distrib_id': ('Global', 'id'),
    'moz_partner_id': ('Preferences', 'mozilla.partner.id'),
    'repack_distrib_id': ('FakeSection', 'dist_id')
}
search_option = ('LocalizablePreferences', 'browser.search.defaultenginename')

partner_info = []
for partner_type in partner_dirs:
    html_dirlist = urllib2.urlopen(
        partner_packs_url + partner_dirs[partner_type]).read()
    # Distribution package names mapping to subdir URL.
    config_urls = html_parser.get_partner_links(html_dirlist, 
        partner_dirs[partner_type])
    
    for name in sorted(config_urls.keys()):
        link = config_urls[name].replace('/file/', '/raw-file/')
        link = 'https://hg.mozilla.org' + link
        vals = { 'type': partner_type, 'pack_name': name }
            
        configs = ConfigParser.RawConfigParser()
        configs.readfp(UnicodeUrl(link + '/distribution/distribution.ini'))
        configs.readfp(FakeSecHead(UnicodeUrl(link + '/repack.cfg')))
            
        # Read partner ID strings.
        for prop in config_options:
            try:
                vals[prop] = configs.get(config_options[prop][0], 
                    config_options[prop][1])
            except:
                pass
        
        # Fill in missing mozilla partner IDs using package name.
        # Common partner ID is part before underscore.
        if 'moz_partner_id' not in vals:
            vals['moz_partner_id'] = name.split('_')[0]
            
        # Special case for MSN - partner ID is different 
        # for each distribution (localized). Set it explicitly.
        if name.startswith('msn'):
            vals['moz_partner_id'] = 'msn'
        
        # Search default could be in any LocalizablePreferences section -
        # add entry for each.
        searchdefs = []
        locprefs = [section for section in configs.sections() 
            if section.startswith(search_option[0])]
        for lp_sec in locprefs:
            try:
                searchdefs.append(configs.get(lp_sec, search_option[1]))
            except:
                pass
        
        if len(searchdefs) == 0:
            partner_info.append(vals)
        else: 
            for sd in searchdefs:
                vals['search_def'] = sd
                partner_info.append(dict(vals))

# Separate out distrib IDs and search plugins by partner name.
distrib_ids = {}
search_names = {}

for entry in partner_info:
    key = (entry['type'], entry['moz_partner_id'].strip('"'))
    if key not in distrib_ids:
        distrib_ids[key] = set()
    distrib_ids[key].add(entry['distrib_id'])
    distrib_ids[key].add(entry['repack_distrib_id'].strip('"'))
    if 'search_def' in entry:
        if key not in search_names:
            search_names[key] = set()
        search_names[key].add(entry['search_def'])

# Output to CSV files.
                
headers = [
    'type',
    'pack_name', 
    'distrib_id', 
    'repack_distrib_id', 
    'moz_partner_id',
    'search_def'
]

# Encode and write list as CSV row.
def write_row(fp, row):
    fp.write(','.join(row).encode('utf8') + '\n')

# Full table.
with open(output_package_info, 'w') as f:
    f.write(','.join(headers) + '\n')
    for row in partner_info:
        row = [row.get(key, '') for key in headers]
        write_row(f, row)
    f.close()
   
# Distribution IDs by partner.
with open(output_distrib_ids, 'w') as f:
    f.write('type,partner,distrib_id\n')
    for p in sorted(distrib_ids.keys()):
        ids = list(distrib_ids[p])
        ids.sort()
        for id in ids:
            write_row(f, [p[0], p[1], id])
    f.close()

# Search plugins by partner.
with open(output_distrib_search, 'w') as f:
    f.write('type,partner,search_name\n')
    for p in sorted(search_names.keys()):
        ids = list(search_names[p])
        ids.sort()
        for id in ids:
            write_row(f, [p[0], p[1], id])
    f.close()
            
# Parse JSON with search plugins.
# Extract unique combinations of plugin ID string and search provider name.
spv_json = json.load(urllib2.urlopen(spv_url))
plugins = set()

# Search plugins are nested down multiple levels.
for loc in spv_json['locales']:
    for app in spv_json['locales'][loc]:
        for channel in spv_json['locales'][loc][app]:
            spv_list = spv_json['locales'][loc][app][channel]['searchplugins']
            for plugin_id in spv_list:
                plugins.add((plugin_id, '"' + spv_list[plugin_id]['name'] + '"'))

plugins = list(plugins)
plugins.sort(key = itemgetter(0,1))

with open(output_all_search, 'w') as f:
    f.write('plugin_id,search_name\n')
    for p in plugins:
        write_row(f, p)
    f.close()

