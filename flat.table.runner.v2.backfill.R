setwd("~/fhr-r-rollups")

source("lib/sguha.functions.R",keep.source=FALSE)
source("makeFlatTables.v3.R",keep.source=FALSE)

store.path <- "/user/sguha/fhrrollup"


extract.date <- function(s){
    a <- tail(strsplit(s,"/",fixed=TRUE)[[1]],1)
    list(name=a, date=as.Date(a))
}

waitForJobs <- function(s,i,outof,L){
    resus <- lapply(L, function(ajob){
        rhstatus(ajob,mon.sec=Inf)
    })
    resusText <- (lapply(resus, function(ajob){
        tryCatch({
            jobname <- ajob$jobname
            rerrors <- isn(ajob$counters$R_UNTRAPPED_ERRORS)
            totalerrors <- sum(rerrors[,1])
            mapinput <- ajob$counters$"Map-Reduce Framework"["Map input records",]
            data.table(name = jobname, pct = round(100*rerrors[,1]/mapinput,1), name = rownames(rerrors))
        },error=function(e){ data.table(name=sprintf("could not get errors for %s",ajob$jobname), pct=0, name="MISS")})
    }))
    embody <- paste(c(hdfs.getwd(),"\n",capture.output(print(rbindlist(resusText)))), collapse="\n")
    email(subj=sprintf("BACKFILL FOR %s [%s/%s] done",s,i,outof), body=embody)
    list(TRUE,embody)
}
    

## Recreate the data
pathnames <- sort(rhls("/user/sguha/fhr/samples/backup")$file)

BACK <- 170


for( pi in rev(seq_along(pathnames))){
    p <- pathnames[pi]
    dt <- extract.date(p)
    fileOriginDate <- dt$date
    MS <- 0
    rhmkdir(sprintf("%s/%s", store.path,dt$name))
    hdfs.setwd(sprintf("%s/%s", store.path,dt$name))

    print(sprintf("Processing for %s", dt$name))
    timeperiod <- list(start = strftime(fileOriginDate-BACK,"%Y-%m-%d"), end   = strftime(fileOriginDate-7,"%Y-%m-%d"))
    PARAM <- list(needstobetagged=TRUE,whichdate=strftime(dt$date,"%Y%m%d"),statcomputer=computeAllStats,usedt=FALSE)
    
    input.path <- sqtxt(sprintf("%s/1pct/",p))
    ## Weekly Summary
    timeChunksWk <- weekTimeChunk(timeperiod$start, timeperiod$end)
    zweek <- rhwatch(map=summaries, reduce=rhoptions()$temp$colsummer, input=input.path
                     ,debug='collect'
                     ,output='rweek'
                     ,jobname=sprintf("Weekly [ %s ]",dt$name)
                     ,mon.sec=MS
                     ,setup=expression({ library(rjson) })
                     ,read=FALSE
                     ,param=list(PARAM=append(PARAM, list(granularity='week' ,listOfTimeChunks = timeChunksWk))))

    ## Monthly Summary
    timeChunksMonth <- monthTimeChunk(timeperiod$start, timeperiod$end)
    zmonth <- rhwatch(map=summaries, reduce=rhoptions()$temp$colsummer, input=input.path
                      ,debug='collect'
                      ,output='rmonth'
                      ,mon.sec=MS
                      ,jobname=sprintf("Monthly [ %s ]",dt$name)
                      ,setup=expression({ library(rjson) })
                      ,read=FALSE
                      ,param=list(PARAM=append(PARAM, list(granularity='month' ,listOfTimeChunks = timeChunksMonth))))

    ## Daily Summary
    ## timeChunksDay <- dayTimeChunk(timeperiod$start, timeperiod$end)
    ## zday <- rhwatch(map=summaries, reduce=rhoptions()$temp$colsummer, input=input.path
    ##                 ,debug='collect'
    ##                 ,output='rday'
    ##                 ,mon.sec=MS
    ##                 ,jobname=sprintf("Daily [ %s ]",dt$name)
    ##                 ,setup=expression({ library(rjson) })
    ##                 ,read=FALSE
    ##                 ,param=list(PARAM=append(PARAM, list(granularity='day' ,listOfTimeChunks = timeChunksDay))))
    print(waitForJobs(p,pi,length(pathnames),list(zweek, zmonth)))
}
email("ALL BACKFILLS DONE")
