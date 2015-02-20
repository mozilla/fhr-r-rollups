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
    require(zoo)
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
    vendor   <- isn(b$gecko$vendor, "missing")
    name     <- isn(b$gecko$name, "missing")
    channel  <- isn(b$geckoAppInfo$updateChannel, "missing")
    os       <- isn(b$geckoAppInfo$os, "missing")
    osdetail <- local({
        if(os=="WINNT"){
            WNVer(b$data$last$org.mozilla.sysinfo.sysinfo$version)
        }else os
    })
    distribution <- isn(b$data$last$org.mozilla.appInfo.appinfo$distributionID,
        "missing")
    locale       <- isn(b$data$last$org.mozilla.appInfo.appinfo$locale, "missing")
    geo          <- isn(b$geo)
    version      <- isn(b$geckoAppInfo$version, "missing")
    list(vendor=vendor, name=name, channel=channel, os=os, osdetail=osdetail, 
        distribution=distribution, locale=locale, geo=geo, version=version)
}

## Is the record considered a standard Firefox profile
## (ie should be included in the set of "all Firefox profiles".
isStandardProfile <- function(vendor, appname) {
    as.character(identical(vendor, "Mozilla") && identical(appname, "Firefox"))
}

## Standard channel name, if the channel string is considered
## to belong to one of the 4 main channels or "esr", 
## otherwise "other".
getStandardChannel <- function(channel) {
    if(identical(channel, "missing")) return("other")
    stdchannel <- regmatches(channel, regexec(
        "^(nightly|aurora|beta|release|esr(\\d{2})?)(-.+)?$", channel))[[1]]
    if(length(stdchannel) == 0) return("other")
    stdchannel <- stdchannel[[2]]
    if(grepl("esr", stdchannel, fixed = TRUE)) return("esr")
    stdchannel
}

## Standard OS name - for convenience in reports.
## "Linux" bucket includes Unix-like.
getStandardOS <- function(os) {
    if(identical(os, "missing")) return("other")
    if(identical(os, "WINNT")) return("Windows")
    if(identical(os, "Darwin")) return("Mac")
    if(grepl("Linux|BSD|SunOS", os)) return("Linux")
    "other"
}
 
## Is the distribution considered a standard Mozilla distribution?
## Includes stock and few other specific cases. 
isMozillaDistrib <- function(distrib) {
    if(identical(distrib, "missing")) return(FALSE)
    ## Stock has distributionID == "" (empty string).
    !nzchar(distrib) || identical(distrib, "euballot") ||
        grepl("mozilla", distrib, ignore.case = TRUE)
}

## If the distribution is considered a partner build, find the partner name.
## Distributions from expired partnerships have the suffix "|expired" appended.
## Otherwise, return "other".
## partner.list is a lookup table mapping distribution IDs
## to corresponding partner name, including both current and expired partners. 
getPartnerName <- function(distrib) {
    if(distrib %in% names(partner.list)) 
        return(partner.list[[distrib]])
    "other"
}

## Summary or standardized values for profile info (for convenience). 
## Pass in output of getDimensions.
getStandardizedDimensions <- function(dims) {
    isstdprofile <- isStandardProfile(dims$vendor, dims$name)
    stdchannel <- getStandardChannel(dims$channel)
    stdos <- getStandardOS(dims$os)
    ## Identifier for the distribution group.
    ## "mozilla" for standard Moz distributions,
    ## <partnerID> for partner builds,
    ## otherwise "other".
    distribtype <- if(isMozillaDistrib(dims$distribution)) { 
        "mozilla" 
    } else { 
        getPartnerName(dims$distribution) 
    }
    
    list(isstdprofile=isstdprofile, stdchannel=stdchannel, stdos=stdos,
        distribtype=distribtype)
}

#---------------------------------------

### Activity stats ###

## Extract all session activity over the time chunk.
## Returns a list with summary stats for each day.
getAllActivity <- function(days) {
    if(length(days) == 0) return(NULL)
    act <- lapply(days, function(d) {
        d <- d$org.mozilla.appSessions.previous
        if(length(d) == 0) return(NULL)
        
        tt <- c(d$cleanTotalTime, d$abortedTotalTime)
        at <- c(d$cleanActiveTicks, d$abortedActiveTicks)
        ## Each vector must have the same number of sessions. 
        if(!identical(length(tt), length(at))) return(NULL)
        ## Make sure the record for this day is non-null.
        if(length(tt) == 0) return(NULL)
        
        ## Check for NA, negative times, or unreasonably huge times
        ## (indicating errors). 
        ## In checking for positive times, active ticks could be 0.
        bad <- is.na(tt) | is.na(at) | tt <= 0 | at < 0 |
            tt > 15552000 | at > 3110400
        if(all(bad)) return(NULL)
        if(any(bad)) {
            tt <- tt[!bad]
            at <- at[!bad]
        }
        
        list(nsessions = length(tt), totalSeconds = sum(tt), 
            activeSeconds = sum(at) * 5)
    })
    act[!unlist(lapply(act, is.null))]
}

## Whether profile was active in time chunk.
computeActives          <- function(days)    if(length(days)>0) 1 else 0

## Whether profile was created prior to time chunk.
computeExistingProfiles <- function(profileCrDate,timeChunk) if(profileCrDate < timeChunk['start']) 1 else 0

## Whether profile was created during time chunk.
computeNewProfiles      <- function(profileCrDate,timeChunk) if(profileCrDate >= timeChunk['start'] && profileCrDate <= timeChunk['end']) 1 else 0

## Whether profile existed during time chunk.
computeTotalProfiles    <- function(profileCrDate,timeChunk) if(profileCrDate <= timeChunk['end']) 1 else 0

## Total time in seconds for sessions started during time chunk.
computeTotalSeconds <- function(activity) {
    sum(unlist(lapply(activity, "[[", "totalSeconds")))
}
## Total active time in seconds for sessions started during time chunk.
computeActiveSeconds <- function(activity) {
    sum(unlist(lapply(activity, "[[", "activeSeconds")))
}
## Total # sessions started during time chunk.
computeNumSessions <- function(activity) {
    sum(unlist(lapply(activity, "[[", "nsessions")))
}


#---------------------------------------

### Search counts ###

## Extract all SAP search counts over the time chunk, 
## named by their (raw) search provider name.
## Returns NULL if no searches 
## (so summing the output in this case should give 0).
getAllSearches <- function(days) { 
    if(length(days) == 0) return(NULL)
    names(days) <- NULL
    sc <- unlist(lapply(days, function(d) {
        s <- d$org.mozilla.searches.counts
        if(length(s) == 0) return(NULL)
        ## Preprocess.
        ## Remove version field if present.
        s[["_v"]] <- NULL
        ## Sanity check on count values.
        ss <- as.numeric(unlist(s))
        to.remove <- is.na(ss) | ss <= 0
        if(any(to.remove)) s <- s[!to.remove]
        if(length(s) == 0) return(NULL)
        
        ## Extract search provider names.
        spv <- names(s)
        ## Format should be <searchengine>.<SAP>.
        ## Don't check for exact SAP names (in case they change),
        ## but search count string should end with [a-z] characters.
        spv[!grepl("^.+\\.[a-z]+$", spv)] <- NA
        ## Remove SAP string.
        spv <- sub("\\.[^.]+$", "", spv)
        ## Trim search provider name. 
        ## In particular, a bunch of Bing searches are recorded as "Bing ".
        spv <- gsub("^\\s+|\\s+$", "", spv)
        spv[!nzchar(spv)] <- NA
        
        s <- setNames(s, spv)
        if(any(is.na(spv))) s <- s[!is.na(spv)]
        s
    }))
    if(length(sc) == 0) return(NULL)
    tapply(sc, names(sc), sum)
}

## Count searches whose provider names appear in the specified whitelist.
countSearches <- function(searches, namestoinclude) {
    sum(searches[names(searches) %in% namestoinclude])
}

## Total # SAP searches.
computeTotalSearches <- function(searches) {
    sum(searches)
}

## Search providers to include for paid searches.
# searchNamesPaid <- function(distribtype) {
    # None on non-standard/non-partner distributions. 
    # switch(distribtype, other = NULL, mozilla = paid.plugins,
        # Default case is partner build.
        # Also count other-prefixed plugins relevant to that partner.
        # append(paid.plugins, sprintf("other-%s", partner.plugins[[distribtype]]))
# }

## Plugin names to include for counting all searches 
## through a given search provider.
## Includes all relevant official plugin names, and any relevant 
## other-prefixed shortnames on corresponding partner builds. 
##
## Arguments are the distribution type string for the current record,
## an identifier for the official search plugins, 
## and an identifier for partner builds.
## The plugin identifier is considered a prefix to all relevant 
## official plugin names (NULL means include all official plugins).
## The partner identifier is the partner build whose other-prefixed
## search names should be included (defaults to same as pluginprefix).
searchNamesOfficialAndPartner <- function(distribtype, pluginprefix = NULL, 
                                            partnername = pluginprefix) {
    searchnames <- if(length(pluginprefix) == 0) { 
        official.plugins 
    } else {
        patterns <- sprintf("^%s", pluginprefix)
        unlist(lapply(patterns, grepl, official.plugins))
    }
    
    if(length(partnername) > 0 && distribtype %in% partnername) {
        searchnames <- append(searchnames, 
            sprintf("other-%s", partner.plugins[[distribtype]]))
    }
    searchnames
}

## Plugin names to count all searches through official default plugins
## and partner build defaults.
searchNamesOfficial <- function(distribtype) {
    searchNamesOfficialAndPartner(distribtype)
}

## Plugin names to include for Google searches through official
## default plugins and expired partner build defaults.
searchNamesGoogle <- function(distribtype) {
    searchNamesOfficialAndPartner(distribtype, "google", "google|expired")
}

## Plugin names to include for Yahoo searches through official default
## plugins.
searchNamesYahoo <- function(distribtype) {
    searchNamesOfficialAndPartner(distribtype, "yahoo")
}

## Plugin names to include for Bing searches through official default
## plugins.
searchNamesBing <- function(distribtype) {
    searchNamesOfficialAndPartner(distribtype, "bing")
}

#---------------------------------------

### Other stats ###

## Whether Fx was considered the default browser across the time chunk.
## If profile was inactive during the timechunk find their last status
## if it's still missing despite that, oh well - consider it as '0'
computeIsDefault        <- function(days,alldays){
    if(length(days) == 0){
        previousDays <- as.numeric(unlist(lapply(alldays[names(alldays)<min(names(days))],function(s) s$org.mozilla.appInfo.appinfo$isDefaultBrowser)))
        if(length(previousDays)==0) return(0)
        tail(na.locf(previousDays),1)
    }else{
        1*(sum(unlist(lapply(days,function(s) s$org.mozilla.appInfo.appinfo$isDefaultBrowser))) > 0.5*length(days))
    }
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
    else NA
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

getProfileCreationDate  <- function(b){
    profileCrDate     <- strftime(  as.Date(b$data$last$org.mozilla.profile.age$profileCreation,"1970-01-01"), "%Y-%m-%d")
    if(is.null(profileCrDate)) {
        profileCrDate <- if(length(b$data$days) > 0) min(names(b$data$days)) else b$thisPingDate
    }
    if(is.null(profileCrDate) || is.na(profileCrDate)) return(NULL)
    return(profileCrDate)
}


        ## Was the profile active in the 28 days before the beginning of the window and has UP?
computeIfProfileHasUp <- function(alldays, timeChunk,b){
    upname    <- "firefox.interest.dashboard@up.mozilla"
    x <- if( (!is.null(b$data$last$org.mozilla.addons.addons) && upname %in% names(b$data$last$org.mozilla.addons.addons))
            || (!is.null(b$data$last$org.mozilla.addons.active) && upname %in% names(b$data$last$org.mozilla.addons.active)))
        1 else 0
    st1       <- strftime(as.Date(timeChunk['start'])-28,"%Y-%m-%d")
    ed1       <- strftime(as.Date(timeChunk['start'])-1,"%Y-%m-%d")
    wasActive <- any(names(alldays)>= st1 & names(alldays)<ed1)
    return(1*(wasActive && x))
}

## Total # crashes.
computeTotalCrashes     <- function(days)  sum(unlist(lapply(days,function(dc) c(dc$org.mozilla.crashes.crashes[c("main-crash", "content-crash")]))))

## Collect all activity stats. 
## Each of these stats will be added up within segments. 
computeAllStats <- function(days,control){
    c(
        tTotalProfiles    = isn(computeTotalProfiles(control$profileCrDate,
                                control$timeChunk),0),
        tExistingProfiles = isn(computeExistingProfiles(control$profileCrDate,
                                control$timeChunk),0),
        tNewProfiles      = isn(computeNewProfiles(control$profileCrDate,
                                control$timeChunk),0),
        tActives          = isn(computeActives(days),0),
        tTotalSeconds     = computeTotalSeconds(control$activity),
        tActiveSeconds    = computeActiveSeconds(control$activity),
        tNumSessions      = computeNumSessions(control$activity),
        tCrashes          = isn(computeTotalCrashes(days),0),
        tTotalSearch      = computeTotalSearches(control$searchcounts),
        tGoogleSearch     = countSearches(control$searchcounts,
                                searchNamesGoogle(control$distribtype)),
        tYahooSearch      = countSearches(control$searchcounts,
                                searchNamesYahoo(control$distribtype)),
        tBingSearch       = countSearches(control$searchcounts,
                                searchNamesBing(control$distribtype)),
        tOfficialSearch   = countSearches(control$searchcounts,
                                searchNamesOfficial(control$distribtype)),
        tIsDefault        = isn(computeIsDefault(days,alldays=control$jsObject$data$days),0),
        t5outOf7          = isn(compute5outOf7(days, 
                                alldays = control$jsObject$data$days,
                                granularity =control$granularity,
                                timeChunk = control$timeChunk),0),
        tChurned          = isn(computeChurn(
                                alldays = control$jsObject$data$days, 
                                timeChunk=control$timeChunk),0),
        tHasUP            = isn(computeIfProfileHasUp(
                                alldays=control$jsObject$data$days,
                                timeChunk=control$timeChunk,
                                b=control$jsObject),0)
    )
}
    
#---------------------------------------

### Job map function ###
    
summaries <- function(a,b){
    if(PARAM$needstobetagged){
        b <- fromJSON(b)
        b$data$days <- tagDaysByBuildVersion(b)
    }
    bdim              <- getDimensions(b)
    bdim              <- append(bdim, getStandardizedDimensions(bdim))
    bdim$snapshot     <- PARAM$whichDate
    bdim$granularity  <- PARAM$granularity
    profileCrDate     <- getProfileCreationDate(b)
    if(is.null(profileCrDate)) return()
    lapply(PARAM$listOfTimeChunks,function(timeChunk){
        days           <- b$data$days [names(b$data$days)>=timeChunk['start']  & names(b$data$days)<= timeChunk['end']]
        bdim$timeStart <- timeChunk['start']
        bdim$timeEnd   <- timeChunk['end']
        ## Your custome code can be here (in statcomputer)
        activity       <- getAllActivity(days)
        searchcounts   <- getAllSearches(days)
        mystats        <- PARAM$statcomputer(days, control=list(
            jsObject      = b,
            profileCrDate = profileCrDate, 
            granularity   = PARAM$granularity, 
            timeChunk     = timeChunk,
            activity      = activity,
            searchcounts  = searchcounts,
            distribtype   = bdim$distribtype)
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
    # load("partner-search-lookup.RData")
    load(shared.files)
})


     



