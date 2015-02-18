# Download and parse latest list of partner distributions and search plugins.

import urllib2
from HTMLParser import HTMLParser
import ConfigParser
import codecs
import json
from operator import itemgetter


# Extract parnter info from packing configs.
partner_packs_url  = (
    'https://hg.mozilla.org/build/partner-repacks/file/tip/partners')
# Localization config containing search plugins. 
spv_url = 'http://l10n.mozilla-community.org/~flod/p12n/searchplugins.json'

# Table containing partner package information - 
# distribution IDs and search default. 
output_package_info = 'partner_id_strings.csv'
# List of partner distrib ID strings by partner (set of unique IDs
# based on both config files)
output_distrib_list = 'partner_distrib_ids.csv'
# List of partner search defaults.
output_distrib_search = 'partner_distrib_search.csv'
# Table of localized partner search strings.
output_partner_search = 'official_search_partners.csv'
    
# Parse HTML from repo package dir page. 
class PartnerListParser(HTMLParser):
    
    # Map package names to their subdir URLs.
    def get_partner_links(self, html):
        # Maintain current href attribute from 'a' tags to know
        # when to store data.
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
            if '/partners/' in href:
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

partner_list_html = urllib2.urlopen(partner_packs_url).read()
parser = PartnerListParser()
partner_links = parser.get_partner_links(partner_list_html)

# Now look up distrib IDs and default search plugin name from 
# package config files.

headers = [
    'pack_name', 
    'distrib_id', 
    'repack_distrib_id', 
    'moz_partner_id',
    'search_def'
]
opts = { 
    'distrib_id': ('Global', 'id'),
    'moz_partner_id': ('Preferences', 'mozilla.partner.id'),
    'repack_distrib_id': ('FakeSection', 'dist_id')
}
search_def = ('LocalizablePreferences', 'browser.search.defaultenginename')

# Add fake section header for repack.cfg.
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
        return codecs.decode(self.fp.readline(), 'utf8')

partner_info = []   

for name in sorted(partner_links.keys()):
    link = partner_links[name].replace('/file/', '/raw-file/')
    link = 'https://hg.mozilla.org' + link
    vals = { 'pack_name': name }
    
    cp = ConfigParser.RawConfigParser()
    cp.readfp(UnicodeUrl(link + '/distribution/distribution.ini'))
    cp.readfp(FakeSecHead(UnicodeUrl(link + '/repack.cfg')))
    
    for optname in opts:
        try:
            vals[optname] = cp.get(opts[optname][0], opts[optname][1])
        except:
            pass
    
    # Add overall partner ID for aol (missing).
    if name.startswith('aol'):
        vals['moz_partner_id'] = '"aol"'
    
    # Search default could be in any LocalizablePreferences section -
    # add entry for each.
    searchdefs = []
    locprefs = [sec for sec in cp.sections() if sec.startswith(search_def[0])]
    for lp_sec in locprefs:
        try:
            searchdefs.append(cp.get(lp_sec, search_def[1]))
        except:
            pass
    
    if len(searchdefs) == 0:
        partner_info.append(vals)
    else: 
        for sd in searchdefs:
            vals['search_def'] = sd
            partner_info.append(dict(vals))

# Encode and write output to files.

with open(output_package_info, 'w') as f:
    f.write(','.join(headers) + '\n')
    for row in partner_info:
        row = [row.get(key, '') for key in headers]
        row = ','.join(row).encode('utf8') + '\n'    
        f.write(row)
    f.close()

    
# List distrib IDs and search plugins by partner name.

distrib_ids = {}
search_names = {}

for entry in partner_info:
    partner_name = entry['moz_partner_id'].strip('"')
    if partner_name not in distrib_ids:
        distrib_ids[partner_name] = set()
    if partner_name not in search_names:
        search_names[partner_name] = set()
    distrib_ids[partner_name].add(entry['distrib_id'])
    distrib_ids[partner_name].add(entry['repack_distrib_id'].strip('"'))
    search_names[partner_name].add(entry['search_def'].strip('"'))

with open(output_distrib_list, 'w') as f:
    f.write('partner,distrib_id\n')
    for p in sorted(distrib_ids.keys()):
        ids = list(distrib_ids[p])
        ids.sort()
        for id in ids:
            row = p + ',' + id + '\n'
            f.write(row.encode('utf8'))
    f.close()

with open(output_distrib_search, 'w') as f:
    f.write('partner,search_name\n')
    for p in sorted(search_names.keys()):
        ids = list(search_names[p])
        ids.sort()
        for id in ids:
            row = p + ',' + id + '\n'
            f.write(row.encode('utf8'))
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
                plugins.add((plugin_id, spv_list[plugin_id]['name']))

plugins = list(plugins)
plugins.sort(key = itemgetter(0,1))

with open(output_partner_search, 'w') as f:
    f.write('plugin_id,search_name\n')
    for p in plugins:
        row = ','.join(p) + '\n'
        f.write(row.encode('utf8'))
    f.close()

