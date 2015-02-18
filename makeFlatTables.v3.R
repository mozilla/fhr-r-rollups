
### Time chunking ###

dayTimeChunk <- function(fr, to){
    lapply(strftime(seq(from=as.Date(fr),to=as.Date(to),by=1),"%Y-%m-%d"),function(s){ c(start=s, end=s)})
}
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
    vendor <- isn(b$gecko$vendor, "missing")
    name <- isn(b$gecko$name, "missing")
    channel <- isn(b$geckoAppInfo$updateChannel, "missing")
    os <- isn(b$geckoAppInfo$os, "missing")
    osdetail <- local({
        if(os=="WINNT"){
            WNVer(b$data$last$org.mozilla.sysinfo.sysinfo$version)
        }else "none"
    })
    distribution <- isn(r$data$last$org.mozilla.appInfo.appinfo$distributionID,
        "missing")
    locale <- isn(b$data$last$org.mozilla.appInfo.appinfo$locale, "missing")
    geo <- isn(b$geo)
    version <- isn(b$geckoAppInfo$version, "missing")
    
    list(vendor=vendor, name=name, channel=channel, os=os, osdetail=osdetail, 
        distribution=distribution, locale=locale, geo=geo, version=version)
}

## Summary or standardized values for profile info (for convenience). 
## Pass in output of getDimensions.
getStandardizedDimensions <- function(dims) {
    isstdprofile <- isStandardProfile(dims$vendor, dims$name)
    stdchannel <- getStandardChannel(dims$channel)
    stdos <- getStandardOS(dims$os)
    ismozdistrib <- isMozillaDistrib(dims$distribution)
    distribpartner <-getPartnerName(dims$distribution)
    
    list(isstdprofile=isstdprofile, stdchannel=stdchannel, stdos=stdos,
        ismozdistrib=ismozdistrib, distribpartner=distribpartner)
}

## Is the record considered a standard Firefox profile
## (ie should be included in the set of "all Firefox profiles".
isStandardProfile <- function(vendor, appname) {
    identical(vendor, "Mozilla") && identical(appname, "Firefox")
}

## Standard channel name, if the channel string is considered
## to belong to one of the 4 main channels, or "other".
getStandardChannel <- function(channel) {
    if(identical(channel, "missing")) return("other")
    ## Release includes "release" and "esr".
    if(grepl("^(release|esr(\\d{2})?)(-.+)?$", channel) return("release")
    ## Prerelease.
    chmatch <- regexpr("^(nightly|aurora|beta)(-.+)?$", channel)
    if(chmatch < 0) "other" else regmatches(channel, chmatch)
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
## Otherwise, return "none".
## partner.list is a lookup table mapping distribution IDs
## to corresponding partner name.
getPartnerName <- function(distrib) {
    if(!(distrib %in% names(partner.list))) return("none")
    partner.list[[distrib]]
}

getProfileCreationDate <- function(b){
    profileCrDate <- strftime(as.Date(b$data$last$org.mozilla.profile.age$profileCreation,"1970-01-01"), "%Y-%m-%d")
    if(is.null(profileCrDate)) {
        profileCrDate <- if(length(b$data$days) > 0) min(names(b$data$days)) else b$thisPingDate
    }
    if(is.null(profileCrDate) || is.na(profileCrDate)) return(NULL)
    return(profileCrDate)
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
## Total time in seconds for sessions started during time chunk.
computeTotalSeconds     <- function(days)
{
    sum(unlist(Filter(function(s) s>=0,lapply(days, function(dc){ c(dc$org.mozilla.appSessions.previous$cleanTotalTime,dc$org.mozilla.appSessions.previous$abortedTotalTime)}))))
}
## Total active time in seconds for sessions started during time chunk.
computeActiveSeconds    <- function(days)
{
    sum(unlist(Filter(function(s) s>=0,lapply(days, function(dc){ c(dc$org.mozilla.appSessions.previous$cleanActiveTicks,dc$org.mozilla.appSessions.previous$abortedActiveTicks)}))))*5
}
## Total # sessions started during time chunk.
computeNumSessions    <- function(days)
{
    length(unlist(Filter(function(s) s>=0,lapply(days, function(dc){ c(dc$org.mozilla.appSessions.previous$main) }))))
}
## Total # crashes.
computeTotalCrashes     <- function(days)  sum(unlist(lapply(days,function(dc) c(dc$org.mozilla.crashes.crashes[c("main-crash", "content-crash")]))))

## Extract all SAP search counts over the time chunk, 
## named by their (raw) search provider name.
getAllSearches <- function(days) { 
    names(days) <- NULL
    sc <- unlist(lapply(days, function(d) {
        s <- d$org.mozilla.searches.counts
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
    tapply(sc, names(sc), sum)
}

## Total # SAP searches.
computeTotalSearches <- function(searches) {
    sum(searches)
}

## Total # paid SAP searches.
computeTotalPaidSearches <- function(searches) {
    
}

## Total Google searches through official plugins.
computeTotalGoogleSearches <- function(days){
    computeTotalSearches(days,regex="google")
}
## Total Yahoo searches through official plugins.
computeTotalYahooSearches <- function(days){
    computeTotalSearches(days,regex="yahoo")
}
## Total Bing searches through official plugins.
computeTotalBingSearches <- function(days){
    computeTotalSearches(days,regex="bing")
}
## Total SAP searches that are neither of official Google, Bing, Yahoo. 
computeTotalOthersSearches <- function(days){
    computeTotalSearches(days,regex="(google|yahoo|bing)",negate=TRUE)
}
## Whether Fx was considered the default browser across the time chunk.
computeIsDefault        <- function(days) 1*(sum(unlist(lapply(days,function(s) s$org.mozilla.appInfo.appinfo$isDefaultBrowser))) > 0.5*length(days))
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
    wasActiveLast14Days <- any(names(alldays$data$days)>= st1 & names(alldays$data$days)<ed1)
    st1 <- strftime(as.Date(timeChunk['start'])-30,"%Y-%m-%d")
    ed1 <- strftime(as.Date(timeChunk['start'])-15,"%Y-%m-%d")
    wasActivePrev14Days <- any(names(alldays$data$days)>= st1 & names(alldays$data$days)<ed1)
    1*(wasActiveLast14Days==FALSE && wasActivePrev14Days==TRUE)
}
computeChurn            <- function(alldays,timeChunk) computeChurn1(alldays, timeChunk)    

## Collect all activity stats. 
## Each of these stats will be added up within segments. 
computeAllStats <- function(days,control){
    c(
        tActives          = isn(computeActives(days),0),
        tTotalProfiles    = isn(computeTotalProfiles(control$profileCrDate,control$timeChunk),0),
        tExistingProfiles = isn(computeExistingProfiles(control$profileCrDate,control$timeChunk),0),
        tNewProfiles      = isn(computeNewProfiles(control$profileCrDate,control$timeChunk),0),
        tTotalHours       = isn(computeTotalSeconds(days),0),
        tActiveHours      = isn(computeActiveSeconds(days),0),
        tNumSessions      = isn(computeNumSessions(days),0),
        tCrashes          = isn(computeTotalCrashes(days),0),
        tGoogleSearch     = isn(computeTotalGoogleSearches(days),0),
        tYahooSearch      = isn(computeTotalYahooSearches(days),0),
        tBingSearch       = isn(computeTotalBingSearches(days),0),
        tOthersSearch     = isn(computeTotalOthersSearches(days),0),
        tIsDefault        = isn(computeIsDefault(days),0),
        t5outOf7          = isn(compute5outOf7(days, alldays = control$alldays,granularity =control$granularity,timeChunk = control$timeChunk),0)
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
    bdim              <- c(bdim, getStandardizedDimensions(bdim))
    bdim$snapshot     <- PARAM$whichDate
    bdim$granularity  <- PARAM$granularity
    profileCrDate     <- getProfileCreationDate(b)
    if(is.null(profileCrDate)) return()
    lapply(PARAM$listOfTimeChunks,function(timeChunk){
        days           <- b$data$days [ names(b$data$days)>=timeChunk['start']  & names(b$data$days)<= timeChunk['end']]
        bdim$timeStart <- timeChunk['start']
        bdim$timeEnd   <- timeChunk['end']
        searchcounts   <- getAllSearches(days)
        ## Your custome code can be here (in statcomputer)
        mystats        <- PARAM$statcomputer(days, control=list(
            alldays = b$data$days,
            profileCrDate=profileCrDate, 
            granularity = PARAM$granularity, 
            timeChunk=timeChunk)
        )
        if(PARAM$usedt){
            rhcollect(sample(1:1000,1), cbind(as.data.table(bdim), as.data.table(as.list(mystats)))) 
        }else{
            rhcollect(bdim,mystats)
        }
    })
}

################################################################################
## Examples
################################################################################

## PARAM <- list(needstobetagged=FALSE,whichdate=strftime(Sys.Date(),"%Y%m%d"), granularity='week'
##               ,listOfTimeChunks = weekTimeChunk(timeperiod$start, timeperiod$end),statcomputer=computeAllStats,usedt=FALSE)

## z <- rhwatch(map=summaries, reduce=rhoptions()$temp$colsummer, input="/user/sguha/fhr/samples/output/fromjson1pct"
##              ,debug='collect'
##              ,output='testnew'
##              ,setup=expression({suppressPackageStartupMessages(library(data.table))})
##              ,param=list(PARAM=PARAM))

## z2 <- make.dt(z,c(names(z[[1]][[1]]),names(z[[1]][[2]])))



