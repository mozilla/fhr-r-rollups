#######################################################################
###  
###  Functions for working with other parts of the FHR payload, 
###  not covered in other lib files.
###  
###  The first part of this script concerns processing version numbers
###  and updates for active days.
###  
#######################################################################


## Figure the browser version a profile was on for each day in its history.
##
## Returns a named vector mapping dates from the data$days history to the 
## version in effect on each active day:
## - for days on which updates occurred, the version of the latest update
## - for other days, the version of the most recent previous day that saw an 
##   update
## - for days on which the version could not be determined (ie. days before the 
##   first day with an update), NA.
## If no updates were recorded in the profile history, the version for each day 
## in the history will be set to the current version, or NA if it could not be 
## determined. If the profile had no active days, returns NULL.
##
## If 'majorversions' is TRUE, only major version numbers will be returned.
version.on.dates <- function(r, majorversions = FALSE) {
    days <- r$data$days
    if(length(days) == 0) return(NULL)
    ## The daily version sequence with NAs on days with no update.
    vers <- allUpdates(days)
    vers <- vers[order(names(vers))]
    ## If there were no updates in the profile history, map each date to the 
    ## current version.
    if(all(is.na(vers))) {
        currentver <- validateVersion(isn(r$geckoAppInfo$version))
        ## If a valid current version is not available, nothing more to do.
        if(is.na(currentver)) return(vers)
        if(majorversions) currentver <- majorVersion(currentver)
        vers[] <- currentver
        return(vers)
    }
    ## Otherwise, convert to major versions if necessary, fill in missing
    ## values.
    if(majorversions) vers <- majorVersion(vers)
    fillWithLastObserved(vers)
}


## Retrieve the sequence of version updates from the supplied list of FHR 
## active days.
## 
## Returns a vector mapping dates that saw version updates to the version
## number updated to on that day, ordered chronologically. If there were 
## multiple updates on the day, this is the last update that occurred on that 
## day. If no valid updates were found in the input days, returns NULL.
##
## If 'majorversions' is TRUE, only major version numbers will be returned.
updateValues <- function(days, majorversions = FALSE) {
    updates <- allUpdates(days)
    if(length(updates) == 0) return(NULL)
    ## Days that had updates are the non-NA values.
    updonday <- !is.na(updates)
    if(!any(updonday)) return(NULL)
    if(!all(updonday)) updates <- updates[updonday]
    if(majorversions) updates <- majorVersion(updates)
    updates[order(names(updates))]
}


## Fill in NA entries in a vector with the closest previous non-NA value, if 
## any. In other words, if entry i is NA, and there is a non-NA entry with 
## index 1 <= j < i, entry i will get populated with the value at the largest
## such j.
fillWithLastObserved <- function(v) {
    isobs <- !is.na(v)
    newv <- c(NA, v[isobs])[cumsum(isobs) + 1]
    if(!is.null(names(v))) names(newv) <- names(v)
    newv
}


## Extract the major version numbers from a vector of Firefox version numbers.
## The major version number is given by the digits prior to the first ".".
majorVersion <- function(versions) {
    sub("\\..*$", "", versions)
}


## Apply a simple validity check to Firefox version numbers. If the version 
## number is invalid, return NA. Otherwise return the version number itself.
validateVersion <- function(versions) {
    ## Valid versions should start with 1 or more digits followed by ".".
    invalidvers <- !is.na(versions) & !grepl("^[0-9]+\\.", versions)
    if(any(invalidvers)) versions[invalidvers] <- NA
    versions
}


## Retrieve the version updates observed on each day in the supplied list of 
## FHR profile days.
##
## Returns a named vector mapping dates from the input list to the latest 
## version update observed on each day, or NA if there were no updates on that
## day. Note that the returned vector is not necessarily ordered by date.
allUpdates <- function(days) {
    if(length(days) == 0) return(NULL)
    updates <- lapply(days, function(d) {
        vu <- d$org.mozilla.appInfo.versions
        ## Handle different field versions. 
        vu <- if(identical(vu[["_v"]], 1)) vu$version else vu$appVersion
        if(length(vu) == 0) return(NA)
        ## If there are multiple versions, use the latest one. 
        if(length(vu) > 1) return(vu[[length(vu)]])
        vu
    })
    # vupdates <- as.character(updates)
    ## as.character(list(NA)) returns "NA" - use unlist instead.
    vupdates <- unlist(updates, use.names = FALSE)
    ## Simple validity check.
    vupdates <- validateVersion(vupdates)
    names(vupdates) <- names(updates)
    vupdates
}


## Retrieve the sequence of valid version updates from the supplied FHR 
## days list, and figure the version in effect on each active day. 
## 
## Returns a vector mapping dates to the version number the profile was on for
## that date. If 'updatesonly' is TRUE, the result will only contain entries
## for the days which saw version updates, or none
##that saw version updates to the full version
## number updated to on that day, ordered chronologically. 
## If there were multiple updates on the day, this is the last update on the day. 
## 
## If no valid updates were found across the input days, returns NULL.
# versionOnDates <- function(days, updatesonly = FALSE) {
    # if(length(days) == 0) return(NULL)
    # ## Look up the latest version update on each date, returning NA if none.
    # updates <- lapply(days, function(d) {
        # vu <- d$org.mozilla.appInfo.versions
        # ## Handle different field versions. 
        # vu <- if(identical(vu[["_v"]], 1)) vu$version else vu$appVersion
        # if(length(vu) == 0) return(NA)
        # ## If there are multiple versions, use the latest one. 
        # if(length(vu) > 1) vu <- vu[[length(vu)]]
        # ## Simple validity check.
        # if(!grepl("^[0-9]+\\.", vu)) return(NA)
        # vu
    # })
    # vupdates <- as.character(updates)
    # names(vupdates) <- names(updates)
    # if(updatesonly) {
        # vna <- is.na(vupdates)
        # if(all(vna)) return(NULL)
        # if(any(vna)) return(vupdates[!vna])
    # }
    # fillWithLastObserved(vupdates)
# }


## Retrieves the sequence of valid version updates from the supplied FHR 
## days list.
## 
## Returns a vector mapping dates that saw version updates to the full version
## number updated to on that day, ordered chronologically. 
## If there were multiple updates on the day, this is the last update on the day. 
## 
## If no valid updates were found across the input days, returns NULL.
# updateValues <- function(days) {
    # if(length(days) == 0) return(NULL)
    # updates <- lapply(days, function(d) {
        # vu <- d$org.mozilla.appInfo.versions
        # ## Handle different field versions. 
        # vu <- if(identical(vu[["_v"]], 1)) vu$version else vu$appVersion
        # if(length(vu) == 0) return(NULL)
        # ## If there are multiple versions, use the latest one. 
        # if(length(vu) > 1) vu <- vu[[length(vu)]]
        # ## Simple validity check.
        # if(is.na(vu) || !grepl("^[0-9]+\\.", vu)) return(NULL)
        # vu
    # })
    # u.null <- as.logical(lapply(updates, is.null))
    # if(all(u.null)) return(NULL)
    # if(any(u.null)) updates <- updates[!u.null]
    # updates.v <- as.character(updates)
    # names(updates.v) <- names(updates)
    # updates.v[order(names(updates.v))]
# }


#######################################################################


## Find the dates of a specific weekday closest to a vector of dates.
## For example, for a set of dates, find the closest Sunday before each date. 
##
## This can be useful for splitting dates into weeks (tagging them by the 
## start date of the week containing them) or for constraining date ranges 
## to be bounded by specific weekdays.
##
## Input dates should be represented in the standard format ("yyyy-mm-dd").
## Output is a vector of date strings in the same format.
## Only a single weekday to use can be specified, by name (full or abbreviated) 
## or by number using the POSIX wday scheme (0 = Sunday, 1 = Monday,..., 
## 6 = Saturday).
## The default is to return the closest Sunday before the input dates.
## The function will return the closest weekday after the input dates by 
## setting after.date = TRUE.
closest.weekday <- function(dates, weekday = "sunday", after.date = FALSE) {
    if(length(weekday) > 1) {
        warning(paste("'weekday' argument should be a single scalar value -",
            "only the first element will be used"))
        weekday <- weekday[[1]]
    }
    ## If weekday is a numbered day of week, use as is.
    ## Otherwise, assume it is a character string naming the day.
    if(!(is.numeric(weekday) && any(weekday == 0:6))) {
        weekday <- tolower(as.character(weekday))
        ## Find the day-of-week index corresponding to the specified weekday.
        ## This will be the same as the POSIX wday value.
        ## In the case of no match, default to Sunday.
        weekday <- pmatch(weekday, 
            c("sunday", "monday", "tuesday", "wednesday", "thursday", 
                "friday", "saturday"), 
            nomatch = 1) - 1
    }
    dates <- as.Date(dates, format = "%Y-%m-%d")
    ## Days of week are numbered starting at 0 with Sunday.
    ## This gives the offset between the date and the previous Sunday.
    dayoffsets <- as.POSIXlt(dates)$wday
    ## Compute offsets relative to desired weekday.
    dayoffsets <- dayoffsets - weekday
    if(after.date) dayoffsets <- 7 - dayoffsets
    dayoffsets <- dayoffsets %% 7
    ## Shift dates by offset.
    dates <- if(after.date) dates + dayoffsets else dates - dayoffsets
    format(dates, "%Y-%m-%d")
}

