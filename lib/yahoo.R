#######################################################################
###  
###  Functions for working with profiles that got the default change
###  to Yahoo.
###  
#######################################################################


## Is the profile considered targeted by the change to Yahoo?
## This grouping is based on distribution and locale, and optionally 
## on geo and current version.
##
## All of these profiles should have gotten the default change on updating
## to v34+, if they updated, unless their default search engine was set to 
## something other than Google. Profiles not belonging to this group should 
## not have gotten the default change, and are generally not of interest 
## in our analysis.
##
## Targeted profile are on en-US locale, and "mozilla" or "yahoo" distribution
## type (as returned by majorDistribValue). They should also be in the physical
## US (checked if 'geo' is supplied) and have a current version of 34.0 or 
## later (checked if 'version' is supplied).
## Arguments are the locale string, major distribution identifier, two-letter
## geoCountry code, and current browser version.
isTargetedY <- function(locale, distribtype, geo = NULL, version = NULL) {
    targeted <- identical(locale, "en-US") && 
        distribtype %in% c("mozilla", "yahoo")
    if(!is.null(geo)) targeted <- targeted && identical(geo, "US")
    if(!is.null(version)) {
        targeted <- targeted && grepl("^\\d{2,}", version) && version > "34"
    }
    targeted
}

## Collects information around updates to v34+, and change of default to Yahoo, 
## if any. 
## 
## Versions 34+ saw the launch of the new search UI (landed in different 
## versions, depending on locale), and a change of search default to a new 
## partner for some profiles (eg. Yahoo for en-US/geo US).
## 
## This function helps identify profiles that updated to v34+ or were newly
## created on one of those versions, as well as profiles that got a change
## of search default. 
##
## For profiles that are targeted by the change to Yahoo (profile is from 
## the physical US and isTargetedY() is TRUE), the function first looks for 
## UITour measurements. This is considered the most reliable source of 
## information. If it is not present, information is inferred by looking for 
## FHR version updates, and the profile is considered to have gotten the 
## change of search default if the profile has been on the new version and 
## was targeted. 
##
## While this is primarily intended for gathering information about the change
## to Yahoo, it should be useable for determining other changes on v34+, 
## such as profiles that got the new UI, or the change to Yandex in the
## "ru" locale, by specifying which profiles are considered targeted 
## as appropriate.
## 
## Arguments are:
## - the data$days list as 'days' containing at least the days from 2014-12-01 
##   onwards (the launch date of v34)
## - the profile creation date as 'pcd' (expected as yyyy-mm-dd and never NA)
## - whether the profile is considered targeted as 'targeted' (for the Yahoo
##   change in the US, this should be the output of 
##      isTargetedY(locale, distribtype, geo))
## 
## Tuning arguments give control over:
## - the minimum version to look for ('.minver', default is "34")
## - the release date of the minimum version ('.minverdate', 
##   default is "2014-12-01"); this is used to determine profiles newly created
##   on the version of interest
## - whether or not to look for UITour information ('.useuit', default is TRUE);
##   should only be used for the change to Yahoo in the US
## - the search engines that will get changed if they are the previous default
##   ('.prevdef', default is "google", NULL means not to check defaults).
## 
## Returns a list containing the following information:
## - has.ver: whether or not the profile has been on a version of interest
##           (>= .minver) at some point (boolean)
## - ver.status: whether the profile updated to the version of interest from an 
##              older version ("updated"), was newly created on the version 
##              ("new"), or has never been on a version of interest ("older").
## - ver.date: the first date on a version of interest (date of update, or 
##            profile creation date for new profiles), or else NA if has.ver
##            is FALSE.
## - new.sdef: whether a new search default was set on ver.date because of the 
##             use of the version of interest, either on updating or on first 
##             use of a new profile? 
## - reason: a string indicating which step gave the results
##
## If UITour information is present, 'new.sdef' is read from there. Otherwise, 
## it is TRUE iff has.ver is TRUE and the profile was considered targeted, and
## if the profile updated from an older version, the previous search default 
## (as well as can be inferred) is in .prevdef.
## 
## Returns NULL if there are no days.
v34UpdateInfo <- function(days, pcd, targeted, 
                                    .minver = "34", .minverdate = "2014-12-01", 
                                    .useuit = TRUE, .prevdef = "google") {
    if(length(days) == 0) return(NULL)
    newverdates <- areDatesInPeriod(names(days), from = .minverdate)
    ## If the profile was not active Dec 1 2014 or later, it will not have
    ## been on a version of interest.
    if(!any(newverdates)) {
        return(list(
            has.ver = FALSE,
            ver.status = "older",
            ver.date = NA,
            new.sdef = FALSE,
            reason = "nodays"
        ))
    }
    newverdays <- days[newverdates]
    
    ## First look at UITour measurement, if required, for targeted profiles.
    if(targeted && .useuit) {
        uit <- uitourInfo(newverdays)
        if(!is.null(uit)) {
            ## Whether the profile got the change to Yahoo depends on the
            ## treatment branch.
            forcedbranches <- c("firstrun_yahooDefault",
                                    sprintf("whatsnew_1%s", c("A", "B", "C")))
            sneakpeekbranches <- sprintf("whatsnew_2%s", c("A", "B", "C"))
            ## The profile is considered new if it was on a "firstrun" branch.
            onfirstrun <- grepl("^firstrun", uit$treatment)
            return(list(
                has.ver = TRUE,
                ver.status = if(onfirstrun) "new" else "updated",
                ver.date = uit$date,
                ## The profile got the change if it was on either the Yahoo
                ## firstrun branch or one of the forced (1) whatsnew branches, 
                ## or it if was on one of the sneak peek (2) branches and had 
                ## an opt-in ("Switch" action).
                new.sdef = 
                    (uit$treatment %in% forcedbranches ||
                        (uit$treatment %in% sneakpeekbranches && 
                                                    "Switch" %in% uit$action)),
                reason = "uitour"
            ))
        }
    }
    
    ## Otherwise, infer the required information from other profile fields.
    
    ## If the profile was newly created on a version of interest, it may not
    ## have an update recorded for that initial version. 
    ## Handle this case separately.
    if(pcd >= .minverdate) {
        ## If it was created after the version of interest was released, 
        ## assume it was created on the new version.
        return(list(
            has.ver = TRUE,
            ver.status = "new",
            ## The initial date is the earliest FHR active date.
            ## This should generally be the same as the profile creation date,
            ## but guarantees that ver.date is a date in the days list.
            ver.date = min(names(days)),
            ## Assume it got the new default if it was targeted.
            new.sdef = targeted,
            reason = "pcd"
        ))
    }
    
    ## Otherwise, the profile was not newly created on a version of interest.
    ## Infer information from updates. 
    updates <- updateValues(newverdays)
    newverupdates <- updates >= .minver
    if(any(newverupdates)) {
        info <- list(
            has.ver = TRUE,
            ver.status = "updated",
            ver.date = min(names(updates)[newverupdates]),
            new.sdef = targeted,
            reason = "updates"
        )
        ## Check previous search default if necessary.
        if(info$new.sdef && length(.prevdef) > 0) {
            ## Only look at dates prior to the date of update.
            sdates <- areDatesInPeriod(names(days), 
                                to = as.character(as.Date(info$ver.date) - 1))
            if(any(sdates)) {
                sdef <- inferDefaultSearchEngine(days[sdates], lastonly = TRUE)
                ## Only check the previous search default if one was found.
                ## Otherwise, think of it as missing information:
                ## The previous value of new.sdef is the best guess.
                if(!is.na(sdef)) {
                    info$new.sdef <- sdef %in% .prevdef
                    info$reason = "updates,prevdef"
                }
            }
        }
        return(info)
    }
    
    ## No updates to a version of interest were found. 
    ## Conclude that the profile is still on an older version.
    list(
        has.ver = FALSE,
        ver.status = "older",
        ver.date = NA,
        new.sdef = FALSE,
        reason = "noupdates"
    )
}

## Extract UITour info from the active days on or after 2014-12-01, if any.
## Only intended to be applied to targeted profiles (as defined in 
## isTargetedY()). The output may not be accurate or make sense for other
## profiles.
## 
## Input is the list of FHR days in which to look for UITour measurements. 
## 
## If no valid UITour measurements are found, returns NULL. 
## Otherwise, returns a list containing the following:
## - date: the date of the first valid UITour measurement (which should be 
##         considered the date of update or change of search default.
## - treatment: the treatment branch the profile was assigned to. 
##              It will be one of the following:
##      > firstrun_yahooDefault (newly created on v34+, with Yahoo default)
##      > firstrun_otherDefault (newly created on v34+, with other default)
##      > whatsnew_Default (updated from older version, no change of default)
##      > whatsnew_1[ABC] (updated from older version, default changed to Yahoo)
##      > whatsnew_2[ABC] (updated from older version, no change of default
##          unless action sequence contains "Switch"
## - action: a vector of actions that were recorded in response to the treatment
uitourInfo <- function(days) {
    ## Extract UITour measurements.
    uit <- lapply(days, "[[", "org.mozilla.uitour.treatment")
    nouit <- unlist(lapply(uit, is.null))
    if(all(nouit)) return(NULL)
    if(any(nouit)) uit <- uit[!nouit]
    ## Order by date.
    uit <- uit[order(names(uit))]
    
    ## The profile's treatment is taken to be the first out of the sequence of 
    ## possibly multiple or repeated treatment strings across all UITour days.
    treatments <- unlist(lapply(uit, "[[", "srch-chg-treatment"))
    ## If no treatment strings are found, consider the UITour info invalid.
    if(length(treatments) == 0) return(NULL)
    thetmt <- treatments[[1]]
    
    ## Look for actions corresponding to this treatment as follows:
    ## - keep all the actions for the first day
    ## - if multiple consecutive UITour days have the same treatment string, 
    ##   append the actions for those days.
    actiondays <- if(length(uit) > 1) {
        ## First see if any days other than the first have the same treatment.
        sametmtdays <- unlist(lapply(uit[-1], function(ud) {
            ## Is this day's treatment a length-1 vector with the same value?
            identical(ud[["srch-chg-treatment"]], thetmt)
        }))
        ## If other days have the same treatment, record indices of 
        ## the ones consecutive to the first.    
        if(all(sametmtdays)) { 1:length(uit) } else {
            if(any(sametmtdays)) { 1:min(which(!sametmtdays)) } else 1
        }
    } else { 1 }
    actions <- unlist(lapply(uit[actiondays], "[[", "srch-chg-action"))
    
    list(date = names(uit)[[1]], treatment = thetmt, action = unique(actions))
}

## Returns a function that generates a list similar to that returned by
## v34UpdateInfo() for any given date, indicating whether the profile 
## had updated to a version of interest or gotten a new default by that date.
##
## This is intended to provide update information for data tables collected
## at the daily level rather than the profile level.
## 
## Input is the profile's v34UpdateInfo() value.
## The list returned by the created function will have the following elements:
## - date: the current date on which the function is called
## - on.ver: whether the profile was on a version of interest by this date
## - ver.status: same as the supplied value for the profile
## - new.sdef: same as the supplied value for the profile
## - reason: same as the supplied value for the profile
dailyUpdateInfoFunction <- function(updateinfo) {
    ## If the profile never updated to a version of interest, 
    ## the output is trivial.
    if(!updateinfo$has.ver) return(
        function(currentdate) {
            list(
                date = currentdate, 
                on.ver = FALSE,
                ver.status = updateinfo$ver.status,
                new.sdef = updateinfo$new.sdef,
                reason = updateinfo$reason
            )
        }
    )
    ## Otherwise need to check current date against update date.
    function(currentdate) {
        list(
            date = currentdate, 
            on.ver = currentdate >= updateinfo$ver.date,
            ver.status = updateinfo$ver.status,
            new.sdef = updateinfo$new.sdef,
            reason = updateinfo$reason
        )
    }
}




