
# Download and parse latest list of search plugins.

import urllib2
import json
from operator import itemgetter

spv_url = 'http://l10n.mozilla-community.org/~flod/p12n/searchplugins.json'
output_file = 'officialsearchpartners.csv'

json_file = urllib2.urlopen(spv_url)
spv_list = json.load(json_file)

# Extract search plugin info - short name, full name, description.
spv_set = set()

for loc in spv_list['locales']:
    for app in spv_list['locales'][loc]:
        for channel in spv_list['locales'][loc][app]:
            for channel in spv_list['locales'][loc][app]:
                current_list = spv_list['locales'][loc][app][channel]
                current_list = current_list['searchplugins']
                for spv in current_list:
                    spv_set.add((spv, 
                        current_list[spv]['name'],
                        current_list[spv]['description']))

# Order by columns.
spv_set = list(spv_set)
spv_set.sort(key = itemgetter(0,1,2))

# Write to output file.
spv_set = ['\t'.join(x).encode('utf8') + '\n' for x in spv_set]
with open(output_file, 'w') as f:
    f.write(u'shortname\tfullname\tdescription\n')
    f.writelines(spv_set)
    f.close()

        