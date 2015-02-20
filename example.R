
## Examples

setwd("~/fhr-r-rollups")
source("lib/sguha.functions.R",keep.source=FALSE)
source("makeFlatTables.v3.R",keep.source=FALSE)

timeperiod <- list(start = strftime(Sys.Date()-30,"%Y-%m-%d"), end   = strftime(Sys.Date()-21,"%Y-%m-%d"))
PARAM <- list(needstobetagged=FALSE,whichdate=strftime(Sys.Date(),"%Y%m%d"), granularity='week'
              ,listOfTimeChunks = monthTimeChunk(timeperiod$start, timeperiod$end),statcomputer=computeAllStats,usedt=FALSE)

z <- rhwatch(map     = summaries, reduce=rhoptions()$temp$colsummer, input="/user/sguha/fhr/samples/output/fromjson1pct"
             ,debug  ='collect'
             ,output ='/user/sguha/tmp/testnew'
             ,setup  = setup
             ,param  = list(PARAM=PARAM)
             ,handler = jobHandler(pctcut=0.0001)
             ,shared = shared.files,read=FALSE)
z2 <- make.dt(rhread(z),c(names(z[[1]][[1]]),names(z[[1]][[2]])))

(z3 <- z2[name=='Firefox', list(tActives=sum(tActiveProfiles), tTotalProfiles = sum(tTotalProfiles), tExistingProfiles=sum(tExistingProfiles),
       tNewProfiles=sum(tNewProfiles), tTotalSeconds=sum(tTotalSeconds), tActiveSeconds=sum(tActiveSeconds), tNumSessions=sum(tNumSessions),
       tCrashes=sum(tCrashes), tTotalSearch=sum(tTotalSearch), tIsDefault=sum(tIsDefault), t5outOf7=sum(t5outOf7), tChurned=sum(tChurned), tHasUP=sum(tHasUP)),by=timeStart][order(timeStart),])


whichTimes <- z3$timeStart
z4 <- rhwatch(map=function(a,b){
    if(a$prodname == 'Firefox' && a$prodversion=='ANY' && a$timeStart %in%  whichTimes) {
        rhcollect(list(timestart = a$timeStart),b[c('tActiveProfiles','tTotalProfiles','tExistingProfiles','tNewProfiles','tTotalSec','tActiveSec','tSessions','tCrashMain','tSrch','tDefault','twasActive5','tInactiveLast30Days')])
    }
}, reduce=rhoptions()$temp$colsummer, input='/user/sguha/rollups/2015-02-16-1pct/rweek',debug='collect')
z4 <- make.dt(z4,c(names(z4[[1]][[1]]),names(z4[[1]][[2]])))[order(timestart),]
