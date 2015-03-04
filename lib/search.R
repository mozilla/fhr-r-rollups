#######################################################################
###  
###  Functions for computing search statistics for FHR profiles.
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
## The retun value depends on which of the provider and SAP dimensions are used
## for counting. If both grouping arguments are NULL, the result is a scalar 
## count of overall total searches for that day. If one grouping argument is 
## NULL, the result is a named vector mapping group names to search counts for 
## that group. If both grouping dimensions are used, the result is a 
## two-dimensional array, with provider groups as row names and SAP groups as 
## column names.
searchCountValues <- function(searchday, provider.grouping = identity, 
                                                sap.grouping = identity) {
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
    tapply(searchday$count, groupby, sum, simplify = TRUE)
}

