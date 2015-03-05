#######################################################################
###  
###  ## Need to source partner-search-lookup.RData first! ##
###  
###  These are functions for computing search counts for FHR profiles,
###  by search provider name and SAP name, by day or overall for the
###  profile.
###  
###  Search counts should be computed using dailySearchCounts() or
###  totalSearchCounts(). They can return overall sums, or else search 
###  counts over groups of search provider or SAP. Groupings can be 
###  specified in a variety of ways, as explained in the function doc.
###  These functions are applied to the data$days list in the FHR 
###  payload.
###  
###  Each of these functions require applying the function allSearches() 
###  to the data$days list as a preprocessing step. However, it is 
###  usually not necessary to do this explicitly. Passing 
###  'preprocess = TRUE' (the default) in the SearchCounts functions 
###  will apply the preprocessing to the data$days list automatically. 
###  If this is to be applied multiple times to the same active days,
###  it may be better to cache the output of allSearches() first and 
###  use 'preprocess = FALSE'.
###  
###  The second section gives some convenience functions for finding
###  search provider names that correspond to official partner searches
###  and major search engines. These can be used to group searches
###  in the SearchCounts functions.    
###  
#######################################################################


## Extracts all search count information from the supplied FHR days list 
## (data$days or a subset, even empty), and packages it 
## into a convenient form.
## 
## This is intended as a preprocessing step for computing search counts
## by search provider and SAP. It parses out provider and SAP strings, 
## and removes values that are considered invalid. 
## 
## Returns a list with an entry for each day that has valid search counts
## (searches.counts entry), or NULL if none are found in the supplied days list.
## Each entry is itself a list, with entries of the form:
## list(provider = character(...), sap = character(...), count = numeric(...)).
## Element i of the vectors give the search provider, SAP, and count 
## corresponding to that day's i-th search count respectively.
allSearches <- function(days) {
    if(length(days) == 0) return(NULL)
    ## Process search info on each day. 
    ## Result will contain NULL entries for each day with no searches.
    sc <- lapply(days, function(d) {
        ## Handle any errors due to bad data on a per-day basis.
        tryCatch({
            s <- d$org.mozilla.searches.counts
            if(length(s) == 0) return(NULL)
            
            ## Preprocess.
            ## Remove version field if present.
            s[["_v"]] <- NULL
            ## Convert to numeric vector and remove any invalid entries.
            s <- unlist(s)
            if(length(s) == 0) return(NULL)
            s <- setNames(as.numeric(s), names(s))
            bad <- is.na(s) | s <= 0
            if(all(bad)) return(NULL)
            if(any(bad)) s <- s[!bad]
            
            ## Now parse the search name strings and restructure.
            ## Name format should be <searchengine>.<SAP>.
            ## Don't match SAP names exactly (in case they change),
            ## but check that they consist of [a-z] characters.
            ## Also trim whitespace around search provider name. 
            ## (In particular, some Bing searches are recorded as "Bing ".)
            parsed <- regmatches(names(s),
                regexec("^\\s*(\\S.*?)\\s*\\.([a-z]+)$", names(s)))
            ## Remove entries with invalid name strings.
            bad <- unlist(lapply(parsed, length)) == 0
            if(all(bad)) return(NULL)
            if(any(bad)) parsed <- parsed[!bad]
            
            list(provider = unlist(lapply(parsed, "[[", 2)),
                sap = unlist(lapply(parsed, "[[", 3)),
                count = unname(s[unlist(lapply(parsed, "[[", 1))]))
        }, error = function(e) { NULL })
    })
    ## Remove NULL entries in days list, indicating days with no valid searches.
    sc.null <- unlist(lapply(sc, is.null))
    if(all(sc.null)) return(NULL)    
    if(any(sc.null)) return(sc[!sc.null])
    sc
}

## Computes the search counts for a single day's searches (a single entry
## in the list returned by allSearches()), totaled by
## search provider and SAP.
##
## The provider and SAP identifier strings can optionally be grouped, and the
## search counts will be summed over these groups instead of over the raw 
## identifiers. To do this, supply functions to the arguments provider.grouping 
## and sap.grouping. The functions should map a vector of identifier strings to 
## the corresponding group labels that they should be counted under. The default
## grouping is to use all original identifiers.
##
## Either of the provider or SAP dimensions can be ignored entirely in summing,
## ie. all its identifiers are lumped into a single, trivial group, and the 
## results will not include it. To do this, set the appropriate grouping
## argument to NULL. Seting both to NULL means computing overall total searches.
##
## If either of the grouping functions can return NA, the counts for groups
## labelled NA can be removed from the output by setting removeNA = TRUE.
## This can be used to restrict counts to a subset of interest: make the 
## grouping functions return NA for other searches and drop them from the
## output. If there is grouping along both dimensions, this will remove counts
## for which the group labels for either provider or sap is NA. By default,
## NA groups are not removed and are treated the same as the other groups.
##
## The retun value depends on which of the provider and SAP dimensions are used
## for counting. If both grouping arguments are NULL, the result is a scalar 
## count of overall total searches for that day. If one grouping argument is 
## NULL, the result is a named vector mapping group names to search counts for 
## that group. If both grouping dimensions are used, the result is a 
## two-dimensional array, with provider groups as row names and SAP groups as 
## column names. If removeNA caused all search counts to be removed, the result
## is NULL.
searchCountValues <- function(searchday, provider.grouping = identity, 
                                    sap.grouping = identity, removeNA = FALSE) {
    ## If both grouping arguments are NULL, just return the total count.
    if(is.null(provider.grouping) && is.null(sap.grouping))
        return(sum(searchday$count))
    ## Otherwise apply grouping and use tapply.
    groupby <- list()
    if(!is.null(provider.grouping))
        groupby[[length(groupby) + 1]] <- provider.grouping(searchday$provider)
    if(!is.null(sap.grouping))
        groupby[[length(groupby) + 1]] <- sap.grouping(searchday$sap)
    ## Convert grouping variables to factor explicity to retain NAs.
    groupby <- lapply(groupby, factor, exclude = NULL)
    searchcounts <- tapply(searchday$count, groupby, sum, simplify = TRUE)
    if(removeNA) {
        tokeep <- lapply(dimnames(searchcounts), function(n) { !is.na(n) })
        searchcounts <- do.call("[", c(list(searchcounts), tokeep, drop = FALSE))
        if(length(searchcounts) == 0) return(NULL)
    }
    ## In the 2-d case, fill in NA entries with zeros (for missing combinations).
    if(length(dim(searchcounts)) == 2)
        searchcounts[is.na(searchcounts)] <- 0
    searchcounts
}

## Computes daily search counts by search provider and SAP.
##
## Input is either the data$days list (default), or the list returned 
## by allSearches(). The input type should be flagged by setting 'preprocess' 
## to TRUE for the former and FALSE for the latter. 
##
## Search counts can be returned either as overall daily totals, or grouped
## or subsetted by either search provider name or SAP name. This behaviour
## is controlled using the 'provider' and 'sap' arguments. 
##
## In grouping, the search counts will be assigned to groups based on the 
## search providers or SAP string identifiers. The method for assigning groups
## can be specified in a variety of ways, which apply to both 'provider' and 
## 'sap' arguments: 
##  - TRUE, meaning that there will be a separate count for each raw identifier
##    (no grouping). This is the default.
##  - FALSE or NULL, meaning that counts will not be split across this dimension
##    (ie. all values belong to a single, trivial group)
##  - A function which maps a vector of identifiers to group names (or NA)
##  - A list whose names are the group names, and whose elements are
##    either functions or character vectors used to determine membership in
##    the named group. For vectors, searches are assigned to the group if the
##    identifier is listed in the vector. Functions should map a vector of 
##    identifiers to a vector of booleans indicating whether each should be 
##    assigned to that group. 
##    Identifiers that are not assigned to any group by any of these functions 
##    will be assigned to a group named NA. To specify another name for this 
##    "unassigned" group, the list should contain an NA entry whose name is the
##    desired group name. An example of this form is: 
##      list(yahoo = function(v) { grepl("^yahoo", v) }, google = "google", 
##          other = NA).
##  An input in any different form generates an error.
## 
## By default, any searches assigned to a group labelled NA will be treated
## the same as any other group. However, setting removeNA = TRUE will cause
## the NA group to be removed from the output. This can be used for subsetting
## searches. To only count searches belonging to a certain subset of interest, 
## assign the others to a group labelled NA (either explicitly in the function
## form or automatically using the list form), and use removeNA = TRUE. If both
## the provider and sap dimensions have NAs, this will drop counts for which 
## either dimension is NA.
## 
## Returns a list with an entry for each day that has searches (as returned by
## allSearches()), except for days for which all searches were removed by 
## setting removeNA = TRUE. If there are no remaining days, returns NULL.
## Each entry is of the form returned by searchCountValues(), either a scalar
## or a one- or two-dimensional array.
dailySearchCounts <- function(days, provider = TRUE, sap = TRUE, 
                                        removeNA = FALSE, preprocess = TRUE) {
    if(preprocess) days <- allSearches(days)
    if(length(days) == 0) return(NULL)
    searchcounts <- lapply(days, searchCountValues, groupingFunction(provider),
        groupingFunction(sap), removeNA)
    ## Remove any NULLs that were introduced by subsetting.
    if(removeNA) {
        sc.null <- unlist(lapply(searchcounts, is.null))
        if(all(sc.null)) return(NULL)    
        if(any(sc.null)) return(searchcounts[!sc.null])
    }
    searchcounts
}

## Computes total search counts by search provider and SAP across all days
## in the time period that have searches. 
##
## Arguments are as described for dailySearchCounts().
##
## The return value is the same as that for searchCountValues().
totalSearchCounts <- function(days, provider = TRUE, sap = TRUE, 
                                        removeNA = FALSE, preprocess = TRUE) {
    if(preprocess) days <- allSearches(days)
    if(length(days) == 0) return(NULL)
    searches <- list(provider = unlist(lapply(days, "[[", "provider")),
        sap = unlist(lapply(days, "[[", "sap")),
        count = unlist(lapply(days, "[[", "count")))
    searchCountValues(searches, groupingFunction(provider), 
        groupingFunction(sap), removeNA)
}


## Generates a function to apply for grouping in searchCountValues()
## based on the various input types accepted by the SearchCounts functions.
groupingFunction <- function(grouping) {
    if(isTRUE(grouping)) return(identity)
    if(is.null(grouping) || identical(grouping, FALSE)) return(NULL)
    if(is.function(grouping)) return(grouping)
    if(is.list(grouping)) {
        if(!all(unlist(lapply(grouping, function(g) { 
            identical(g, NA) || is.function(g) || is.character(g) 
        }))))
            stop("Invalid grouping argument.")
        ## First check if we have a label for the NA group. 
        nagroup <- is.na(grouping)
        nagroupname <- if(any(nagroup)) {
            n <- names(grouping)[nagroup][[1]] 
            grouping <- grouping[!nagroup]
            n
        } else { NA }
        ## Convert character vector inputs to functions for checking membership.
        grouping <- lapply(grouping, function(g) {
            if(is.character(g)) { 
                function(vals) { vals %in% g } 
            } else { g }
        })
        return((function(groupmembers) {
            eval(bquote(function(vals) {
                ## Assign values to each group as appropriate.
                ## Keep track of unassigned values.
                unassigned <- rep_len(TRUE, length(vals))
                for(groupname in names(groupmembers)) {
                    incurrentgroup <- groupmembers[[groupname]](vals)
                    if(!any(incurrentgroup)) next
                    vals[incurrentgroup] <- groupname
                    unassigned <- unassigned & !incurrentgroup
                    if(!any(unassigned)) break
                }
                if(any(unassigned)) vals[unassigned] <- .(unassignedgroupname)
                vals
            }, list(unassignedgroupname = nagroupname)))
        })(grouping))
    }
    ## At this point, the input doesn't match any of the valid forms.
    stop("Invalid grouping argument.")
}

##----------------------------------------------------------------

## These functions identify commonly queried search types (ie. searches 
## through major partner search engines), subject to our scheme for
## mapping search provider strings to actual search engines. 
## 
## These can be used to create groups for the *SearchCounts() functions above.
## The argument 'distribtype' in the following functions should be supplied
## the major distribution type or partner as returned by majorDistribValue().


## Returns the list of search provider name strings representing official
## search plugins, optionally restricted to those related to a major search 
## engine.
## 
## Official search plugins consist of all the plugins included across all 
## stock builds, and may also include certain "other"-prefixed search names 
## on certain partner distributions. Hence, this list should be queried 
## separately for each profile, based on its distribution. 
##
## Official plugins related to major search providers are generally identified
## by full name or prefix which generally matches the partner name (suffixes
## generally indicate localizations). Official plugins related to search partners
## should generally include the prefixed stock plugins, as well as certain
## "other"-prefixed plugins on that partner's builds, if any.
##
## - To find the overall list of official stock plugins, use no arguments.
## - To find the list of plugins considered official for a specific profile,
##   specify the major distribution type (ie. as returned by majorDistribValue())
##   as 'distribtype'. The result will be the full list of stock plugins, 
##   together with any distribution-specific "other"-prefixed plugins that need
##   to be included.
## - To restrict plugins to specific search engines, supply the prefix names
##   for the search engines as 'pluginprefix' (eg. pluginprefix = "yahoo").
##   The prefix will also be checked against the distribtype, if one is supplied.
##   If the distribtype has the same prefix, its plugin names will also be 
##   included. 
searchNamesOfficialAndPartner <- function(distribtype = NULL, prefix = NULL) {
    ## First find the list of official stock plugins, optionally restricted
    ## by prefix.
    searchnames <- if(length(prefix) == 0) { 
        official.plugins 
    } else {
        patterns <- sprintf("^%s", prefix)
        unlist(lapply(patterns, grep, official.plugins, value = TRUE))
    }
    if(length(distribtype) == 0) return(searchnames)
    
    ## Then add plugins coming from relevant partner distributions.
    ## Add any official plugins corresponding to distribtype.
    ## If we are using the prefix restriction, only include plugins from 
    ## distributions related to that prefix.
    if(length(prefix) > 0) {
        patterns <- sprintf("^%s", prefix)
        distribtype <- unlist(lapply(patterns, grep, distribtype, value = TRUE))
    }
    partnersearch <- unlist(partner.plugins[distribtype], use.names = FALSE)
    append(searchnames, sprintf("other-%s", partnersearch))
}

## List of all search provider names considered official plugins for 
## the given distribution type, returned as a character vector.
## Consists of all official stock plugin names and any "other"-prefixed
## names to be included for the specified distribution.
official.searchnames <- function(distribtype) {
    searchNamesOfficialAndPartner(distribtype)
}

## List of all search provider names associated with official Yahoo searches
## for the specified distribution, returned as a character vector.
yahoo.searchnames <- function(distribtype) {
    searchNamesOfficialAndPartner(distribtype, "yahoo")
}

## List of all search provider names associated with official Bing searches
## for the specified distribution, returned as a character vector.
bing.searchnames <- function(distribtype) {
    searchNamesOfficialAndPartner(distribtype, "bing")
}

## List of all search provider names associated with official Google searches
## for the specified distribution, returned as a character vector.
google.searchnames <- function(distribtype) {
    searchNamesOfficialAndPartner(distribtype, "google")
}

