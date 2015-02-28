#######################################################################
###  
###  Functions for computing browser activity for FHR profiles.
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
## Each entry is of the form 
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
        tt <- c(d$cleanTotalTime, d$abortedTotalTime)
        at <- c(d$cleanActiveTicks, d$abortedActiveTicks)
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
## Returns a list of the form list(nsessions, totalsec, activesec), 
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
## Input is the list returned by allActivity(). 
## Returns a list with the same names as the input, or NULL if the input 
## has zero length. Each entry is of the form 
## list(nsessions, totalsec, activesec), giving the number of sessions 
## started on that day, and the total overall and active seconds 
## for these sessions.
dailyActivity <- function(activity) {
    if(length(activity) == 0) return(NULL)
    lapply(activity, dailyActivityValues)
}

## Computes total session activity statistics (number of sessions,
## total session seconds, total active seconds) across all sessions
## started during an entire time period.
## 
## Input is the list returned by allActivity(). 
## Returns a list of the form list(nsessions, totalsec, activesec), 
## listing the number of sessions started during the period, 
## and the total overall and active seconds for these sessions.
## If the input contains no activity, each value of the result will be 0.
totalActivity <- function(activity) {
    if(length(activity) == 0) 
        return(list(nsessions = 0, totalsec = 0, activesec = 0))
    activity <- list(totalsec = unlist(lapply(activity, "[[", "totalsec")),
        activesec = unlist(lapply(activity, "[[", "activesec")))
    dailyActivityValues(activity)
}

## Checks whether a vector of dates (Date or character) fall within 
## a specified time period, bounded inclusively by 'from' and 'to'. 
## If either is missing, its bound is replaced by the earliest or latest
## date respectively that is considered valid for FHR dates.
##
## Returns a vector of booleans the same length as vector of dates, 
## indicating for each whether it falls in the period.
areDatesInPeriod <- function(dates, from = NULL, to = NULL) {
    ## Ignore FHR dates earlier than April 2013.
    if(length(from) == 0) from <- "2013-04-01"
    ## Latest valid date is today.
    if(length(to) == 0) to <- Sys.Date()
    dates >= from & dates <= to
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
## Returns a list of the form 
## list(nactivedays, nsessions, totalsec, activesec), 
## giving the number of active days during the period, 
## the number of sessions started during the period, 
## and the total overall and active seconds for these sessions.
## If the input contains no activity, values of the result will be 0.
get.total.activity <- function(r, from = NULL, to = NULL) {
    days <- get.active.days(r, from, to)
    totalact <- totalActivity(allActivity(days))
    c(nactivedays = length(days), totalact)
}


##----------------------------------------------------------------

## Convenience functions for working with dates.


to.month <- function(dates) {
    
}


last.month.day <- function(month) {

}







