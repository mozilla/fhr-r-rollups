
## Helper functions for processing FHR or ADI profile info values.

##----------------------------------------------------------------

### Standardization functions expect a scalar argument and
### return a standardized value (useful for grouping).
### Missing or invalid values are expected to be represented
### by either NA or "missing".
### Currently NULL values are not handled (should not be passed).s

## Is the record considered a standard Firefox profile?
## (ie should it be included in the set of "all Firefox profiles").
## Checks based on vendor name and app name strings.
## Returns boolean.
standardProfileValue <- function(vendor, appname) {
    identical(vendor, "Mozilla") && identical(appname, "Firefox")
}


## Converts a channel name string to its standard channel name, 
## if the channel string is considered to belong to one of the 
## 4 main channels (release/beta/aurora/nightly) or esr.  
## Otherwise returns "other".
## Missing or invalid values are expected to be either NA or "missing".
standardChannelValue <- function(channel) {
    if(channel %in% c(NA, "missing")) return("other")
    ## Standard channels are identified by prefix-matching
    ## on the standard name.
    ## Some esr channels have the version number appended.
    stdchannel <- regmatches(channel, regexec(
        "^(nightly|aurora|beta|release|esr(\\d{2})?)(-.+)?$", channel))[[1]]
    if(length(stdchannel) == 0) return("other")
    stdchannel <- stdchannel[[2]]
    if(grepl("esr", stdchannel, fixed = TRUE)) return("esr")
    stdchannel
}

## Converts a OS name string to a standardized name corresponding to 
## its major OS group (Windows/Mac/Linux), or "other".
## Some *nixes may get assigned to "other", but they will be 
## a small proportion of OSs overall.
standardOSValue <- function(os) {
    if(os %in% c(NA, "missing")) return("other")
    if(identical(os, "WINNT")) return("Windows")
    if(identical(os, "Darwin")) return("Mac")
    if(grepl("Linux|BSD|SunOS", os)) return("Linux")
    "other"
}

## Resolves a distribution ID string as a major distribution type:
## - "mozilla" for stock/other Mozilla distributions
## - <partnername> for partner builds belonging to a particular partner
##      (eg. "yahoo" or "yandex").
##      Builds from expired partnerships that are still in use are 
##      suffixed by "|expired", eg. "google|expired".
## - "other" for non-Mozilla and non-partner distributions.       
majorDistribValue <- function(distrib) {
    if(distrib %in% c(NA, "missing")) return("other")
    ## Stock distributions have "" as their ID string.
    ## Some other specific distrib names qualify as Mozilla.
    if(!nzchar(distrib) || identical(distrib, "euballot") ||
        grepl("mozilla", distrib, ignore.case = TRUE)) return("mozilla")
    ## To identify partner builds, look up distribution ID in table.
    if(distrib %in% names(partner.list)) return(partner.list[[distrib]])
    "other"
}
## Package in lookup table.
if(!exists("partner.list")) 
    load("/usr/local/share/partner-search-lookup.RData")
f.env <- new.env(parent = globalenv())
assign("partner.list", partner.list, envir = f.env)
environment(majorDistribValue) <- f.env

## Converts a version string to major version number.
## Returns "missing" if version number was missing or malformed.
majorVersion <- function(ver) {
    if(ver %in% c(NA, "missing")) return("missing")
    vermatch <- regmatches(ver, regexec("^(\\d+)\\.", ver))[[1]]
    if(length(vermatch) == 0) return("missing")
    vermatch[[2]]
}


##----------------------------------------------------------------

### Convenience functions to be applied to full FHR records. 

## Checks whether the profile is considered a standard Firefox profile.
## Returns boolean.
is.standard.profile <- function(r) {
    standardProfileValue(isn(r$geckoAppInfo$vendor), isn(r$geckoAppInfo$name))
}

## Returns the standard channel name for a profile, 
## or "other" if it is not on a standard channel.
get.standardized.channel <- function(r) {
    standardChannelValue(isn(r$geckoAppInfo$updateChannel))
}

## Returns the standard (major) OS name for a profile, or "other".
get.standardized.os <- function(r) {
    standardOSValue(isn(r$geckoAppInfo$os))
}

## Returns the major distribution type for a profile 
## ("mozilla" or partner name), or "other".
get.distribution.type <- function(r) {
    majorDistribValue(isn(  
        r$data$last$org.mozilla.appInfo.appinfo$distributionID))
}

## Returns locale string for a profile (returns "missing" if invalid).
get.locale <- function(r) {
    loc <- isn(r$data$last$org.mozilla.appInfo.appinfo$locale)
    if(is.na(loc)) "missing" else loc
}

## Returns current major version number (as a string).
get.current.version <- function(r) {
    majorVersion(isn(r$geckoAppInfo$version))
}
