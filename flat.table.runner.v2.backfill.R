setwd("~/fhr-r-rollups")

source("lib/sguha.functions.R",keep.source=FALSE)
source("makeFlatTables.v3.R",keep.source=FALSE)

store.path <- "/user/sguha/fhrrollup"

PCTMAX <- 1
MONSEC <- 15
extract.date <- function(s){
    a <- tail(strsplit(s,"/",fixed=TRUE)[[1]],1)
    list(name=a, date=as.Date(a))
}

waitForJobs <- function(s,i,outof,L){
    resus <- lapply(L, function(ajob){
        rhstatus(ajob,mon.sec=MONSEC,handler  = jobHandler(pctcut=PCTMAX))
    })
    resusText <- (lapply(resus, function(ajob){
        tryCatch({
            jobname <- ajob$jobname
            status  <- ajob$state
            rerrors <- isn(ajob$counters$R_UNTRAPPED_ERRORS)
            totalerrors <- sum(rerrors[,1])
            mapinput <- ajob$counters$"Map-Reduce Framework"["Map input records",]
            data.table(name = jobname, state=status, pct = round(100*rerrors[,1]/mapinput,1), name = rownames(rerrors))
        },error=function(e){
            email(sub='Issue With Return Values', body=capture.output(print(list(data.table(name=sprintf("could not get errors for %s",ajob$jobname), pct=0, name="MISS"), ajob))))
            NULL
        })
    }))
    RT <- rbindlist(resusText)
    if(nrow(RT)>0){
        majorError <- c("no major errors", "MAJOR ERRORS!!")[ any(grepl("UserKill",RT$state))+1 ]
        embody <- paste(c(hdfs.getwd(),"\n%s\n",majorError,capture.output(print(RT))), collapse="\n")
        email(subj=sprintf("BACKFILL FOR %s [%s/%s] done(%s)",s,i,outof,majorError), body=embody)
        list(TRUE,embody)
    }else{
        resusText
    }
}
    

## Recreate the data
pathnames <- sort(rhls("/user/sguha/fhr/samples/backup")$file)

BACK <- 170

getCorrectedDates <- function(Chunker){
    date_max <- NA
    rbindlist(lapply(rev(seq_along(pathnames)),function(pi){
        p               <- pathnames[pi]
        dt              <- extract.date(p)
        fileOriginDate  <- dt$date
        timeperiod      <- list(start = strftime(fileOriginDate-BACK,"%Y-%m-%d"), end   = strftime(fileOriginDate-7,"%Y-%m-%d"))
        timeChunksWk    <- do.call(rbind,Y <- Chunker(timeperiod$start, timeperiod$end))
        X <- data.table(i=pi, p=p,dt=dt$date,B = NA, E = NA,rightmost=NA)
        if(pi==length(pathnames)){
            X <- data.table(i=pi, p=p,dt=dt$date,B = min(timeChunksWk[,'start']), E = max(timeChunksWk[,'start']),rightmost=TRUE)
            date_max <<-  min(timeChunksWk[,'start'])
        }else{
            if(min(timeChunksWk[,'start']) < date_max){
                X <- data.table(i=pi, p=p,dt=dt$date,B = min(timeChunksWk[,'start']), E = date_max,rightmost=FALSE)
                date_max <<-  min(timeChunksWk[,'start'])
            }
        }
        return(X)
    }))
}


whichWks    <- getCorrectedDates(weekTimeChunk)
whichMonths <- getCorrectedDates(monthTimeChunk)
whichDays   <- getCorrectedDates(dayTimeChunk)

for( pi in rev(seq_along(pathnames))){
    p               <- pathnames[pi]
    dt              <- extract.date(p)
    fileOriginDate  <- dt$date
    rhmkdir(sprintf("%s/%s", store.path,dt$name))
    hdfs.setwd(sprintf("%s/%s", store.path,dt$name))

    print(sprintf("Processing for %s", dt$name))
    timeperiod      <- list(start = strftime(fileOriginDate-BACK,"%Y-%m-%d"), end   = strftime(fileOriginDate-7,"%Y-%m-%d"))
    PARAM           <- list(needstobetagged=TRUE,whichdate=strftime(dt$date,"%Y%m%d"),statcomputer=computeAllStats,usedt=FALSE)
    
    input.path      <- sqtxt(sprintf("%s/1pct/",p))
    ## Weekly Summary
    timeChunksWk    <- weekTimeChunk(timeperiod$start, timeperiod$end)
    W <- whichWks[i==pi,]
    timeChunksWk <- Filter(function(s) if(!is.na(W[,rightmost]) && s['start'] >= W[,B]
                          && (   (W[,rightmost]==TRUE &&  s['start'] <=W[,E])
                              || (W[,rightmost]==FALSE &&  s['start'] < W[,E]))) TRUE else FALSE, timeChunksWk)
    zweek           <- rhwatch(map       = summaries, reduce=rhoptions()$temp$colsummer, input=input.path
                               ,debug    = 'collect'
                               ,output   = 'rweek'
                               ,jobname  = sprintf("Weekly [ %s, %s/%s ]",dt$name,pi, length(pathnames))
                               ,mon.sec  = 0
                               ,setup    = setup
                               ,shared   = shared.files                               
                               ,read     = FALSE
                               ,param    = list(PARAM=append(PARAM, list(granularity='week' ,listOfTimeChunks = timeChunksWk))))

    ## Monthly Summary
    timeChunksMonth <- monthTimeChunk(timeperiod$start, timeperiod$end)
    W <- whichMonths[i==pi,]
    timeChunksMonth <- Filter(function(s) if(!is.na(W[,rightmost]) && s['start'] >= W[,B]
                          && (   (W[,rightmost]==TRUE &&  s['start'] <=W[,E])
                              || (W[,rightmost]==FALSE &&  s['start'] < W[,E]))) TRUE else FALSE, timeChunksMonth)
    zmonth          <- rhwatch(map      = summaries, reduce=rhoptions()$temp$colsummer, input=input.path
                               ,debug   = 'collect'
                               ,output  = 'rmonth'
                               ,mon.sec = 0
                               ,jobname = sprintf("Monthly [ %s %s/%s ]",dt$name,pi, length(pathnames))
                               ,setup   = setup
                               ,shared  = shared.files
                               ,read    = FALSE
                               ,param   = list(PARAM=append(PARAM, list(granularity='month' ,listOfTimeChunks = timeChunksMonth))))

    ## Daily Summary
    ## timeChunksDay <- dayTimeChunk(timeperiod$start, timeperiod$end)
    ## W <- whichDays[i==pi,]
    ## timeChunksDay <- Filter(function(s) if(!is.na(W[,rightmost]) && s['start'] >= W[,B]
    ##                       && (   (W[,rightmost]==TRUE &&  s['start'] <=W[,E])
    ##                           || (W[,rightmost]==FALSE &&  s['start'] < W[,E]))) TRUE else FALSE, timeChunksDay)
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
