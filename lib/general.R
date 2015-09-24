#######################################################################
###  
###  Functions for working with other parts of the FHR payload, 
###  not covered in other lib files.
###  
#######################################################################


## Retrieves the sequence of valid version updates from the supplied FHR 
## days list.
## 
## Returns a vector mapping dates that saw version updates to the full version
## number updated to on that day, ordered chronologically. 
## If there were multiple updates on the day, this is the last update on the day. 
## 
## If no valid updates were found across the input days, returns NULL.
updateValues <- function(days) {
    if(length(days) == 0) return(NULL)
    updates <- lapply(days, function(d) {
        vu <- d$org.mozilla.appInfo.versions
        ## Handle different field versions. 
        vu <- if(identical(vu[["_v"]], 1)) vu$version else vu$appVersion
        if(length(vu) == 0) return(NULL)
        ## If there are multiple versions, use the latest one. 
        if(length(vu) > 1) vu <- vu[[length(vu)]]
        ## Simple validity check.
        if(is.na(vu) || !grepl("^[0-9]+\\.", vu)) return(NULL)
        vu
    })
    u.null <- as.logical(lapply(updates, is.null))
    if(all(u.null)) return(NULL)
    if(any(u.null)) updates <- updates[!u.null]
    updates.v <- as.character(updates)
    names(updates.v) <- names(updates)
    updates.v[order(names(updates.v))]
}

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

