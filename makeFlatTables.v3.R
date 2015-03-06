library(zoo)
dayTimeChunk <- function(fr, to){
    lapply(strftime(seq(from=as.Date(fr),to=as.Date(to),by=1),"%Y-%m-%d"),function(s){ c(start=s, end=s)})
}

### Time chunking ###
weekTimeChunk <- function(fr,to){
    ## Sunday is the first day of a week For example, 25 Dec 2013 was a Wednesday and hence belonged to the week
    ## starting Sunday 21st if 'fr' is not a sunday it will be rewinded to the Sunday it belongs if 'to' is not a
    ## Saturday, it will be rewinded to the previous Saturday(end of week)
    sundaystart <- as.Date(fr)-as.POSIXlt(as.Date(fr))$wday
    weeks <- seq(from=sundaystart,to=as.Date(to), by="weeks") 
    m <-lapply(weeks,function(s) c(start=strftime(s,"%Y-%m-%d"), end=strftime(s+7-1,"%Y-%m-%d")))
    names(m) <- NULL;m
}

monthTimeChunk <- function(fr,to){
    ## round 'fr' down to beginning of month and
    ## round 'to' to beginning of the month
    fr1 <- as.Date(as.yearmon(fr, "%Y-%m-%d"), frac = 0)
    to1 <- as.Date(as.yearmon(to, "%Y-%m-%d"), frac = 0)
    lapply(seq(fr1,to1, by='month'),function(s){
        c(start = strftime(s,"%Y-%m-%d"), end = strftime(as.Date(as.yearmon(s, "%Y-%m-%d"), frac = 1),"%Y-%m-%d"))
    })
}

quarterTimeChunk <- function(years){
    ## ReWrite
    a <- list()
    unlist(lapply(years,function(year){
        list(c(start=sprintf("%s-01-01",year),end=sprintf("%s-03-31",year)),
             c(start=sprintf("%s-04-01",year),end=sprintf("%s-06-30",year)),
             c(start=sprintf("%s-07-01",year),end=sprintf("%s-09-30",year)),
             c(start=sprintf("%s-10-01",year),end=sprintf("%s-12-31",year)))
    }),rec=FALSE)
}
 

#---------------------------------------

### Profile info dimensions ### 

## Raw values for profile info dimensions.
getDimensions <- function(b){
    vendor   <- isn(b$geckoAppInfo$vendor, "missing")
    name     <- isn(b$geckoAppInfo$name, "missing")
    channel  <- isn(b$geckoAppInfo$updateChannel, "missing")
    os       <- isn(b$geckoAppInfo$os, "missing")
    osdetail <- local({
        if(os=="WINNT"){
            getWindowsVersionAsString(b$data$last$org.mozilla.sysinfo.sysinfo$version)
        }else if(os=="Darwin"){
            getOSXVersionAsString(b$data$last$org.mozilla.sysinfo.sysinfo$version)
        } else os
    })
    distribution <- isn(b$data$last$org.mozilla.appInfo.appinfo$distributionID,
        "missing")
    locale       <- isn(b$data$last$org.mozilla.appInfo.appinfo$locale, "missing")
    geo          <- isn(b$geo)
    version      <- isn(b$geckoAppInfo$version, "missing")
    list(vendor=vendor, name=name, channel=channel, os=os, osdetail=osdetail, 
        distribution=distribution, locale=locale, geo=geo, version=version)
}

## Summary or standardized values for profile info (for convenience). 
## Pass in output of getDimensions.
getStandardizedDimensions <- function(dims) {
    isstdprofile <- as.character(standardProfileValue(dims$vendor, dims$name))
    stdchannel <- standardChannelValue(dims$channel)
    stdos <- standardOSValue(dims$os)
    ## Identifier for the distribution group.
    ## "mozilla" for standard Moz distributions,
    ## <partnerID> for partner builds,
    ## otherwise "other".
    distribtype <- majorDistribValue(dims$distribution)
    
    list(isstdprofile=isstdprofile, stdchannel=stdchannel, stdos=stdos,
        distribtype=distribtype)
}

#---------------------------------------

### Activity stats ###

## Whether profile was active in time chunk.
computeActives          <- function(days)    if(length(days)>0) 1 else 0

## Whether profile was created prior to time chunk.
computeExistingProfiles <- function(profileCrDate,timeChunk) if(profileCrDate < timeChunk['start']) 1 else 0

## Whether profile was created during time chunk.
computeNewProfiles      <- function(profileCrDate,timeChunk) if(profileCrDate >= timeChunk['start'] && profileCrDate <= timeChunk['end']) 1 else 0

## Whether profile existed during time chunk.
computeTotalProfiles    <- function(profileCrDate,timeChunk) if(profileCrDate <= timeChunk['end']) 1 else 0

## Number of active days in the time chunk.
computeActiveDays <- function(days) length(days)

## Total time in seconds for sessions started during time chunk.
computeTotalSeconds <- function(activity) activity$totalsec 

## Total active time in seconds for sessions started during time chunk.
computeActiveSeconds <- function(activity) activity$activesec 

## Total # sessions started during time chunk.
computeNumSessions <- function(activity) activity$nsessions


#---------------------------------------

### Search counts ###

## Total # SAP searches for specified groups.
computeSearchCounts <- function(searchcounts, groups = NULL) {
    if(length(searchcounts) == 0) return(0)
    if(length(groups) > 0) {
        searchcounts <- searchcounts[names(searchcounts) %in% groups]
    }
    sum(searchcounts)
}

# computeTotalSearches <- function(searchcounts) sum(searchcounts)

# ## Searches by provider (through official search plugins).
# computeYahooSearches <- function(searchcounts) {
    # if("yahoo" %in% names(searchcounts)) searchcounts[["yahoo"]] else 0
# } 

# computeGoogleSearches <- function(searchcounts) {
    # if("google" %in% names(searchcounts)) searchcounts[["google"]] else 0
# }

# computeBingSearches <- function(searchcounts) {
    # if("bing" %in% names(searchcounts)) searchcounts[["bing"]] else 0
# }

# ## Searches through any official plugins.
# computeOfficialSearches <- function(searchcounts) {
    # if(length(searchcounts) == 0) return(0)
    
# }

#---------------------------------------

### Other stats ###

## Whether Fx was considered the default browser across the time chunk.
## If profile was inactive during the timechunk find their last status
## if it's still missing despite that, oh well - consider it as '0'
computeIsDefault        <- function(days,alldays,timeChunk){
    if( length(days) == 0){
        previousDays <- as.numeric(unlist(lapply(alldays[ names(alldays) < timeChunk['start'] ],function(s) s$org.mozilla.appInfo.appinfo$isDefaultBrowser)))
        if(length(previousDays)==0) return(0)
        tail(previousDays,1)
    }else{
        1*(sum(unlist(lapply(days,function(s) s$org.mozilla.appInfo.appinfo$isDefaultBrowser))) > 0.5*length(days))
    }
}

##  Computes if the active profile is default
computeIsActiveProfileDefault    <- function(days){
        1*(sum(unlist(lapply(days,function(s) s$org.mozilla.appInfo.appinfo$isDefaultBrowser))) > 0.5*length(days))
}

## Whether the profile was active on 5 out of the last 7 days.
compute5outOf7          <- function(days,alldays,granularity,timeChunk){
    if(granularity=="day"){
        beg <- strftime(as.Date(timeChunk['start'])-6,"%Y-%m-%d")
        last7days <- alldays [ names(alldays)>=beg  & names(alldays)<=timeChunk['start']]
        1*(length(last7days)>=5)
    }else if (granularity=="week"){
        last7days <- days
        1*(length(last7days)>=5)
    }
    else 0
}
## Whether the profle was peviously active and since inactive.
computeChurn1           <- function(alldays,timeChunk){
    ## Active in the 4-3 weks before the timeStart, but inactive
    ## in 1-2 weeks before timeStart
    ## This hardly means those profiles have gone away, many do come back.
    st1 <- strftime(as.Date(timeChunk['start'])-14,"%Y-%m-%d")
    ed1 <- timeChunk['start']
    wasActiveLast14Days <- any(names(alldays)>= st1 & names(alldays)<ed1)
    st1 <- strftime(as.Date(timeChunk['start'])-30,"%Y-%m-%d")
    ed1 <- strftime(as.Date(timeChunk['start'])-15,"%Y-%m-%d")
    wasActivePrev14Days <- any(names(alldays)>= st1 & names(alldays)<ed1)
    1*(wasActiveLast14Days==FALSE && wasActivePrev14Days==TRUE)
}
computeChurn            <- function(alldays,timeChunk) computeChurn1(alldays, timeChunk)    

## Was the profile active in the 28 days before the beginning of the window and has UP?
computeIfProfileHasUp <- function(alldays, timeChunk,b){
    upname    <- "firefox.interest.dashboard@up.mozilla"
    x <- if( (!is.null(b$data$last$org.mozilla.addons.addons) && upname %in% names(b$data$last$org.mozilla.addons.addons))
            || (!is.null(b$data$last$org.mozilla.addons.active) && upname %in% names(b$data$last$org.mozilla.addons.active)))
        1 else 0
    st1       <- strftime(as.Date(timeChunk['start'])-28,"%Y-%m-%d")
    ed1       <- strftime(as.Date(timeChunk['start']),"%Y-%m-%d")
    wasActive <- any(names(alldays) >= st1 & names(alldays) <= ed1)
    return(1*(wasActive && x))
}

## Total # crashes.
computeTotalCrashes     <- function(days)  sum(unlist(lapply(days,function(dc) c(dc$org.mozilla.crashes.crashes[c("main-crash", "content-crash")]))))

## Collect all activity stats. 
## Each of these stats will be added up within segments. 
computeAllStats <- function(days,control){
    c(
        tTotalProfiles          = isn(computeTotalProfiles(control$profileCrDate, control$timeChunk),0),
        tExistingProfiles       = isn(computeExistingProfiles(control$profileCrDate, control$timeChunk),0),
        tNewProfiles            = isn(computeNewProfiles(control$profileCrDate, control$timeChunk),0),
        tActiveProfiles         = isn(computeActives(days),0),
        tActiveDays             = isn(computeActiveDays(days),0),
        tTotalSeconds           = computeTotalSeconds(control$activity),
        tActiveSeconds          = computeActiveSeconds(control$activity),
        tNumSessions            = computeNumSessions(control$activity),
        tCrashes                = isn(computeTotalCrashes(days),0),
        tTotalSearch            = computeSearchCounts(control$searchcounts),
        tGoogleSearch           = computeSearchCounts(control$searchcounts, "google"),
        tYahooSearch            = computeSearchCounts(control$searchcounts, "yahoo"),
        tBingSearch             = computeSearchCounts(control$searchcounts, "bing"),
        tOfficialSearch         = computeSearchCounts(control$searchcounts,
                                    c("google", "yahoo", "bing", "otherofficial")),
        tIsDefault              = isn(computeIsDefault(days,alldays=control$jsObject$data$days,timeChunk = control$timeChunk),0),
        tIsActiveProfileDefault = isn(computeIsActiveProfileDefault(days),0),
        t5outOf7                = isn(compute5outOf7(days, 
                                    alldays     = control$jsObject$data$days,
                                    granularity = control$granularity,
                                    timeChunk   = control$timeChunk),0),
        tChurned                = isn(computeChurn(alldays   = control$jsObject$data$days, 
                                    timeChunk = control$timeChunk),0),
        tHasUP                  = isn(computeIfProfileHasUp(alldays   = control$jsObject$data$days,
                                     timeChunk = control$timeChunk,
                                     b         = control$jsObject),0)
    )
}
    
#---------------------------------------

### Job map function ###
    
summaries <- function(a,b){
    if(PARAM$needstobetagged){
        b <- fromJSON(b)
    }
    bdim              <- getDimensions(b)
    bdim              <- append(bdim, getStandardizedDimensions(bdim))
    bdim$snapshot     <- PARAM$whichdate
    bdim$granularity  <- PARAM$granularity
    profileCrDate     <- getProfileCreationDate(b)
    if(is.null(profileCrDate)) return()
    lapply(PARAM$listOfTimeChunks,function(timeChunk){
        days           <- get.active.days(b, timeChunk['start'], timeChunk['end'])
        bdim$timeStart <- as.character(timeChunk['start'])
        bdim$timeEnd   <- as.character(timeChunk['end'])
        ## Activity measures aggregated over time chunk.
        activity       <- totalActivity(days)
        ## Search counts over time chunk by provivder.
        grouping <- list(yahoo = yahoo.searchnames(bdim$distribtype),
                        google = google.searchnames(bdim$distribtype),
                        bing = bing.searchnames(bdim$distribtype))
        ## Add a group for searches that use official plugins but none of the above.
        officialsn <- official.searchnames(bdim$distribtype)
        grouping[["otherofficial"]] <- 
            officialsn[!(officialsn %in% unlist(grouping, use.names = FALSE))]
        grouping[["other"]] <- NA
        searchcounts <- totalSearchCounts(days, provider = grouping, sap = FALSE)
        
        ## Your custom code can be here (in statcomputer):
        mystats        <- PARAM$statcomputer(days, control=list(
            jsObject      = b,
            profileCrDate = profileCrDate, 
            granularity   = PARAM$granularity, 
            timeChunk     = timeChunk,
            activity      = activity,
            searchcounts  = searchcounts)
        )
        if(PARAM$usedt){
            rhcollect(sample(1:1000,1), 
                cbind(as.data.table(bdim), as.data.table(as.list(mystats)))) 
        }else{
            rhcollect(bdim,mystats)
        }
    })
}

shared.files <- "/user/dzeber/shared/partner-search-lookup.RData"
setup <- expression(map={
    suppressPackageStartupMessages(library(data.table))
    library(rjson)
    load("partner-search-lookup.RData")
})


     



