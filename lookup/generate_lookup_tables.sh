#!/bin/bash

#######################################################################
##
##  This script generates lookup tables of partner distribution IDs 
##  and search plugin names to be used in processing FHR data.
##  
##  In particular, these tables are useful in identifying partner
##  builds from distribution ID strings, and identifying searches
##  through a particular search provider. 
##  
##  Processing steps are as follows:
##  - Parse out necessary information from online resources and 
##    summarize it in a few tables stored as CSV files. 
##  - Copy CSV files to Mozilla web server (app1) so that they 
##    can be viewed within Mozilla.
##  - Generate RData containing the lookup information reformatted
##    appropriately for use in the R lookup functions.
##  - Copy RData to HDFS location accessible by FHR rollups job.
##  - Make RData accessible to other hala users for adhoc 
##    FHR processing (via /usr/local/share/). 
##  
#######################################################################

. ~/.bash_profile

# Base dir for the script output.
OUTPUT_DIR="$HOME/fhr-lookup"
[[ -d $OUTPUT_DIR ]] || mkdir $OUTPUT_DIR
# Log file.
OUTPUT_LOG="$OUTPUT_DIR/generate_lookup_tables.log"

exec > $OUTPUT_LOG 2>&1

# CSV files to contain lookup tables generated by the Python script:
# Full table of relevant info about partner builds
# extracted from the repack configs.
FULL_DISTRIB_INFO="$OUTPUT_DIR/full_partner_distrib_info.csv"

# Table of partner names and corresponding distribution IDs 
# used by that partner.
DISTRIB_IDS_TABLE="$OUTPUT_DIR/partner_distrib_ids.csv"

# Table of partner names and default search plugins
# used across that partner's custom distributions.
DISTRIB_SEARCH_TABLE="$OUTPUT_DIR/partner_distrib_search.csv"

# Table of all official search plugins included across all Mozilla builds.
# Lists shortname and full descriptive name for each.
ALL_SEARCH_PLUGINS="$OUTPUT_DIR/official_search_plugins.csv"

## RData file name to contain lookup objects.
LOOKUP_RDATA_BASENAME="partner-search-lookup.RData"
LOOKUP_RDATA="$OUTPUT_DIR/$LOOKUP_RDATA_BASENAME"

# Shared locations:
# Web - location is pulled from env variable in local profile.
SHARED_WEB_LOCATION="$APP1_REF"
# HDFS
SHARED_HDFS_LOCATION="${HOME/home/user}/shared"

echo "Running lookup table generation:  `date`"

## Generate CSV files.
echo "Generating CSV files..."
python partners.py \
    $FULL_DISTRIB_INFO \
    $DISTRIB_IDS_TABLE \
    $DISTRIB_SEARCH_TABLE \
    $ALL_SEARCH_PLUGINS
if [[ $? != 0 ]]; then
    echo "There was an error generating CSV files. Exiting..."
    exit 1
fi
echo "Done."

## Generate RData.
echo "Generating RData..."
Rscript --vanilla package-lookups.R \
    $DISTRIB_IDS_TABLE \
    $DISTRIB_SEARCH_TABLE \
    $ALL_SEARCH_PLUGINS \
    $LOOKUP_RDATA
if [[ $? != 0 ]]; then
    echo "There was an error generating RData. Exiting..."
    exit 1
fi
echo "Done."

## Copy to app1.
chmod 644 $OUTPUT_DIR/*.csv
[[ -z $SHARED_WEB_LOCATION ]] || \
    (scp $OUTPUT_DIR/*.csv $SHARED_WEB_LOCATION && \
    echo "Copied CSVs to app1.")

## Copy to HDFS.
chmod 755 $LOOKUP_RDATA
hadoop dfs -copyFromLocal $LOOKUP_RDATA \
    "$SHARED_HDFS_LOCATION/$LOOKUP_RDATA_BASENAME" && \
    echo "Copied RData to HDFS."
    
echo "Lookup table generation completed: `date`"
echo "Exiting..."
exit 0



