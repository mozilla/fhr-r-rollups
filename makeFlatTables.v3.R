                                   
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
    


getDimensions <- function(b){
    channel = isn(b$geckoAppInfo$updateChannel, "missing")
    os      = isn(b$geckoAppInfo$os, "missing")
    osdetail= local({
        if(os=="WINNT"){
            WNVer((b$data$last$ org.mozilla.sysinfo.sysinfo$version))
        }else os
    })
    locale  = isn(b$data$last$org.mozilla.appInfo.appinfo$locale, "missing")
    geo     = isn(b$geo)
    version = isn(b$geckoAppInfo$version,"missing")
    list(vendor=isn(b$gecko$vendor,'missing'),name = isn(b$gecko$name,"missing"),channel=channel, os=os, osdetail=osdetail, locale=locale, geo=geo,version=version)
}

computeActives          <- function(days)    if(length(days)>0) 1 else 0
computeExistingProfiles <- function(profileCrDate,timeChunk) if(profileCrDate < timeChunk['start']) 1 else 0
computeNewProfiles      <- function(profileCrDate,timeChunk) if(profileCrDate >= timeChunk['start'] && profileCrDate <= timeChunk['end']) 1 else 0
computeTotalProfiles    <- function(profileCrDate,timeChunk) if(profileCrDate <= timeChunk['end']) 1 else 0
computeTotalSeconds     <- function(days)
{
    sum(unlist(Filter(function(s) s>=0,lapply(days, function(dc){ c(dc$org.mozilla.appSessions.previous$cleanTotalTime,dc$org.mozilla.appSessions.previous$abortedTotalTime)}))))
}
computeActiveSeconds    <- function(days)
{
    sum(unlist(Filter(function(s) s>=0,lapply(days, function(dc){ c(dc$org.mozilla.appSessions.previous$cleanActiveTicks,dc$org.mozilla.appSessions.previous$abortedActiveTicks)}))))*5
}
computeNumSessions    <- function(days)
{
    length(unlist(Filter(function(s) s>=0,lapply(days, function(dc){ c(dc$org.mozilla.appSessions.previous$main) }))))
}

computeTotalCrashes     <- function(days)  sum(unlist(lapply(days,function(dc) c(dc$org.mozilla.crashes.crashes[c("main-crash", "content-crash")]))))
computeTotalSearches    <- function(days,regex,negate=FALSE) {
    sum(unlist(lapply(days,function(dc){
        x <- dc$org.mozilla.searches.counts;
        x[["_v"]] <- NULL
        whichnames <- grepl(regex,names(x))
        whichnames <- if(negate) !whichnames else whichnames
        unlist(x [ whichnames ])
    })))
}
computeTotalGoogleSearches <- function(days){
    computeTotalSearches(days,regex="google")
}
computeTotalYahooSearches <- function(days){
    computeTotalSearches(days,regex="yahoo")
}
computeTotalBingSearches <- function(days){
    computeTotalSearches(days,regex="bing")
}
computeTotalOthersSearches <- function(days){
    computeTotalSearches(days,regex="(google|yahoo|bing)",negate=TRUE)
}

computeIsDefault        <- function(days) 1*(sum(unlist(lapply(days,function(s) s$org.mozilla.appInfo.appinfo$isDefaultBrowser))) > 0.5*length(days))
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

getProfileCreationDate <- function(b){
    profileCrDate     <- strftime(  as.Date(b$data$last$org.mozilla.profile.age$profileCreation,"1970-01-01"), "%Y-%m-%d")
    if(is.null(profileCrDate)) {
        profileCrDate <- if(length(b$data$days) > 0) min(names(b$data$days)) else b$thisPingDate
    }
    if(is.null(profileCrDate) || is.na(profileCrDate)) return(NULL)
    return(profileCrDate)
}
    

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
    
    
summaries <- function(a,b){
        if(PARAM$needstobetagged){
            b <- fromJSON(b)
            b$data$days <- tagDaysByBuildVersion(b)
        }
        bdim              <- getDimensions(b)
        bdim$snapshot     <- PARAM$whichDate
        bdim$granularity  <- PARAM$granularity
        profileCrDate     <- getProfileCreationDate(b)
        if(is.null(profileCrDate)) return()
        lapply(PARAM$listOfTimeChunks,function(timeChunk){
            days           <- b$data$days [ names(b$data$days)>=timeChunk['start']  & names(b$data$days)<= timeChunk['end']]
            bdim$timeStart <- timeChunk['start']
            bdim$timeEnd   <- timeChunk['end']
            ## Your custome code can be here (in statcomputer)
            mystats        <- PARAM$statcomputer(days, control=list(alldays = b$data$days,profileCrDate=profileCrDate
                                                           , granularity = PARAM$granularity, timeChunk=timeChunk))
            if(PARAM$usedt){
                rhcollect(sample(1:1000,1), cbind( as.data.table( bdim), as.data.table(as.list(mystats)))) 
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



