#######################################################################
###  
###  These are functions for computing browser activity for FHR 
###  profiles, such as active days and sessions counts and times.
###  
###  Functions in the second section below are convenience functions
###  for working at the profile level. They take as arguments the full
###  parsed payload, and optional start and end dates for bounding 
###  a time period of interest. There are functions for checking for 
###  activity and counting active days and total activity statistics 
###  during the period. However, if the data$days object will be 
###  used for other things in the script, the best thing is to extract
###  it directly using get.active.days().
### 
###  Functions in the first section are lower-level and are applied to
###  subsets of the data$days list in the FHR payload (such as would
###  be returned by get.active.days()). The functions dailyActivity()
###  and totalActivity() compute session-based activity statistics 
###  either per active day or in aggregate over all the active days.
###  Session-based days are different from active days, since not 
###  every active day need have a session started on that day (and a 
###  corresponding appSessions.previous entry in the payload). 
###  
###  Each of these functions require applying the function allActivity() 
###  to the data$days list as a preprocessing step. However, it is 
###  usually not necessary to do this explicitly. Passing 
###  'preprocess = TRUE' (the default) in the Activity functions will
###  apply the preprocessing to the data$days list automatically. 
###  If this is to be applied multiple times to the same active days,
###  it may be better to cache the output of allActivity() first and 
###  use 'preprocess = FALSE'.
###  
###  The third sections consists of functions for mapping dates to 
###  months (first or last day of the month containing the date),
###  and checking whether a day is a weekday. Dates can be passed as
###  either character strings or Date objects.
###  
#######################################################################


## Extracts all session activity from the supplied FHR days list 
## (data$days or a subset, even empty), and packages it 
## into a convenient form.
## 
## This is intended as a preprocessing step for computing session-based
## statistics: total times, active times, and session counts. 
## It removes values that are considered invalid. 
## 
## This processing is not related to computing active days, 
## as that is based on the top-level data$days list entries and not on 
## session activity.
## 
## Returns a list with an entry for each day that has session information
## (appSessions.previous entry), or NULL if none are found in the supplied list.
## Note that the returned list may not have an entry for every active day, 
## or may be NULL even if the record has active days, in cases where sessions
## were started on few or none of the active days.
## Each entry is of the form: 
## list(totalsec = numeric(...), activesec = numeric(...)). 
## The two vectors give the number of total and active seconds 
## for each session started on that day, including both clean and 
## aborted sessions.
allActivity <- function(days) {
    if(length(days) == 0) return(NULL)
    ## Process session info on each day. 
    ## Result will contain NULL entries for each day with no valid
    ## session info.
    act <- lapply(days, function(d) {
        d <- d$org.mozilla.appSessions.previous
        if(length(d) == 0) return(NULL)
        
        ## Combine clean and aborted session times.
        tt <- as.numeric(c(d$cleanTotalTime, d$abortedTotalTime))
        at <- as.numeric(c(d$cleanActiveTicks, d$abortedActiveTicks))
        ## Each vector must have the same number of sessions. 
        if(!identical(length(tt), length(at))) return(NULL)
        ## Make sure the record for this day is non-null.
        if(length(tt) == 0) return(NULL)
        
        ## Check for NA, negative times, or unreasonably huge times
        ## (indicating errors). 
        ## Cutoff for "unreasonably huge" is currently 180 days.
        ## In checking for positive times, active ticks could be 0.
        bad <- is.na(tt) | is.na(at) | tt <= 0 | at < 0 |
            tt > 15552000 | at > 3110400
        if(all(bad)) return(NULL)
        if(any(bad)) {
            tt <- tt[!bad]
            at <- at[!bad]
        }
        
        list(totalsec = tt, activesec = at * 5)
    })
    ## Remove NULL entries before returning. 
    act.null <- unlist(lapply(act, is.null))
    if(all(act.null)) return(NULL)
    if(any(act.null)) return(act[!act.null])
    act
}

## Computes daily total session activity statistics (number of sessions,
## total session seconds, total active seconds) for a single day's 
## session activity (ie. a single entry of the list returned by allActivity()).
##
## Returns a list of the form: list(nsessions, totalsec, activesec), 
## giving the number of sessions started on the day, and the total overall 
## and active seconds for these sessions.
## If the input contains no activity, each value of the result will be 0.
dailyActivityValues <- function(activityday) {
    list(nsessions = length(activityday$totalsec), 
            totalsec = sum(activityday$totalsec),
            activesec = sum(activityday$activesec))
}

## Computes daily total session activity statistics (number of sessions,
## total session seconds, total active seconds) for days on which 
## sessions started. 
## 
## Input is either the data$days list (default), or the list returned 
## by allActivity(). This should be specified by setting 'preprocess' to TRUE
## for the former and FALSE for the latter. 
##
## Returns a list with the same names as the input, or NULL if the input 
## has zero length. Each entry is of the form:
## list(nsessions, totalsec, activesec), giving the number of sessions 
## started on that day, and the total overall and active seconds 
## for these sessions.
dailyActivity <- function(days, preprocess = TRUE) {
    if(preprocess) days <- allActivity(days)
    if(length(days) == 0) return(NULL)
    lapply(days, dailyActivityValues)
}

## Computes total session activity statistics (number of sessions,
## total session seconds, total active seconds) across all sessions
## started during an entire time period.
## 
## Input is either the data$days list (default), or the list returned 
## by allActivity(). This should be specified by setting 'preprocess' to TRUE
## for the former and FALSE for the latter. 
##
## Returns a list of the form: list(nsessions, totalsec, activesec), 
## listing the number of sessions started during the period, 
## and the total overall and active seconds for these sessions.
## If the input contains no activity, each value of the result will be 0.
totalActivity <- function(days, preprocess = TRUE) {
    if(preprocess) days <- allActivity(days)
    if(length(days) == 0) 
        return(list(nsessions = 0, totalsec = 0, activesec = 0))
    activity <- list(totalsec = unlist(lapply(days, "[[", "totalsec")),
        activesec = unlist(lapply(days, "[[", "activesec")))
    dailyActivityValues(activity)
}

## Checks whether a vector of dates (Date or character) fall within 
## a specified time period, bounded inclusively by 'from' and 'to'. 
## If either is missing, that end of the time period is considered unbounded.
## The arguments 'from' and 'to' are expected to be scalars.
##
## Returns a vector of booleans the same length as the vector of dates, 
## indicating for each whether it falls in the period.
areDatesInPeriod <- function(dates, from = NULL, to = NULL) {
    ## Ignore FHR dates earlier than April 2013.
    if(length(from) > 1 || length(to) > 1) 
        stop("'from' and 'to' are expected to be single (scalar) dates.")
    # mindate <- "2013-04-01"
    # maxdate <- Sys.Date()
    # if(length(from) == 0 || from < mindate) from <- mindate
    ## Latest valid date is today.
    # if(length(to) == 0 || to > maxdate) to <- maxdate
    inperiod <- rep_len(TRUE, length(dates))
    if(length(from) > 0) inperiod <- inperiod & dates >= from
    if(length(to) > 0) inperiod <- inperiod & dates <= to
    inperiod
}


##----------------------------------------------------------------

## The following functions involve working with the list of FHR days
## (data$days). Each function takes optional arguments 'from' and 'to'
## that can be used to restrict the list of days to a date range.
## If either is supplied, it will be considered an inclusive bound 
## on the range. If either is missing, there will be no bound on that end
## of the range.
## 
## The argument 'r' should be a full FHR record.


## Returns a boolean indicating whether the profile was active 
## (ie. has active days recorded) during the specified date range.
is.active <- function(r, from = NULL, to = NULL) {
    ## Not active if no FHR days are present.
    if(length(r$data$days) == 0) return(FALSE)
    any(areDatesInPeriod(names(r$data$days), from, to))
}

## Returns a boolean indicating whether the profile was active during
## the month represented by a date string. 
## The month to check for is the year/month to which the specified date
## belongs.
is.active.in.month <- function(r, month) {
    is.active(r, from = to.month(month), to = last.month.day(month))
}

## Returns the list of FHR days (data$days) from the profile, 
## optionally restricted to a date range, or NULL if the profile has
## no such active days.
get.active.days <- function(r, from = NULL, to = NULL) {
    days <- r$data$days
    if(length(days) == 0) return(NULL)
    inperiod <- areDatesInPeriod(names(days), from, to)
    if(!any(inperiod)) return(NULL)
    if(!all(inperiod)) return(days[inperiod])
    days
}

## Returns the number of active days (data$days entries) recorded 
## during the date range, or 0 if none.
count.active.days <- function(r, from = NULL, to = NULL) {
    if(length(r$data$days) == 0) return(0)
    sum(areDatesInPeriod(names(r$data$days), from, to))
}

## Computes the total number of active days and session activity totals
## for days and sessions started during the specified time period.
## 
## Returns a list of the form: 
## list(nactivedays, nsessions, totalsec, activesec), 
## giving the number of active days during the period, 
## the number of sessions started during the period, 
## and the total overall and active seconds for these sessions.
## If the input contains no activity, values of the result will be 0.
get.total.activity <- function(r, from = NULL, to = NULL) {
    days <- get.active.days(r, from, to)
    c(nactivedays = length(days), totalActivity(days))
}


##----------------------------------------------------------------

## Convenience functions for working with dates.

## Converts date strings to months (represented as the date string 
## for the first day in the month, ie. "yyyy-mm-01").
## Returns a vector of character strings the same length as the input vector.
to.month <- function(dates) {
    dates <- as.POSIXlt(dates, format = "%Y-%m-%d")
    ## Set the day of the month to 1.
    dates$mday <- 1
    format(dates, "%Y-%m-%d")
}
 
## Computes the last day in the month for each date represented in a vector
## of character strings. 
## For each input date string, finds the year/month to which that date belongs.
## Returns a vector of date strings representing the last date 
## of each year/month.
last.month.day <- function(dates) {
    dates <- as.POSIXlt(dates, format = "%Y-%m-%d")
    ## Set dates to the first day of their dates, to ensure that 
    ## advancing dates works properly.
    dates$mday <- 1
    ## Advance year/month by 1.
    dates$mon <- dates$mon + 1
    ## Reverse to the last day of the previous month (ie the original month).
    dates$mday <- 0
    format(dates, "%Y-%m-%d")
}

## Returns booleans for each date in the input vector indicating whether
## the date is a weekday.
is.weekday <- function(dates) {
    dates <- as.POSIXlt(dates, format = "%Y-%m-%d")
    dates$wday %in% 1:5
}





