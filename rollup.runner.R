PCTMAX <- 1
MONSEC <- 15
extract.date <- function(s){
    a <- tail(strsplit(s,"/",fixed=TRUE)[[1]],1)
    list(name=a, date=as.Date(a))
}

waitForJobs <- function(L){
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
        email(subj=sprintf("[FHR Rollups]: done(%s)",majorError), body=embody)
        list(TRUE,embody)
    }else{
        resusText
    }
}

################################################################################
## convert files to text
################################################################################
toText <- function(i,o){
    y <- rhwatch(map=function(a,b){    rhcollect(NULL, c(a,b))    },reduce=0, input=i
                 ,output=rhfmt(type='text', folder=o,writeKey=FALSE,field.sep="\t",stringquote=""),read=FALSE)
    a <- rhls(o)$file
    rhdel(a[!grepl("part-",a)])
    rhchmod(o,"777")
    o
}
    

#########################################################################################
## Running
#########################################################################################
setwd("~/fhr-r-rollups")
source("lib/sguha.functions.R",keep.source=FALSE)
source("makeFlatTables.v3.R",keep.source=FALSE)

I <- list(name="/user/sguha/fhr/samples/output/fromjson1pct",tag=FALSE)

## Get the snapshot date from the sample creation time
rhread("/user/sguha/fhr/samples/output/createdTime.txt",type='text')
fileOrigin     <-  strsplit(rhread("/user/sguha/fhr/samples/output/createdTime.txt",type='text'),"\t")[[2]]
ll             <- regexec("[0-9]{4}-[0-9]{2}-[0-9]{2}",fileOrigin)
fileOrigin     <- substr(fileOrigin, ll[[1]],ll[[1]]+attributes(ll[[1]])$"match.length"-1)
fileOriginDate <- as.Date(fileOrigin); fileOrigin <- strftime(fileOriginDate,"%Y%m%d")
rm(ll);

## Make the directory into which the rollups will be placed
rhmkdir(sprintf("/user/sguha/fhrrollup/%s", strftime(fileOriginDate,"%Y-%m-%d")))
hdfs.setwd(sprintf("/user/sguha/fhrrollup/%s/",strftime(fileOriginDate,"%Y-%m-%d")))

BACK <- 90
timeperiod <- list(start = strftime(fileOriginDate-BACK,"%Y-%m-%d"),
                   end   = strftime(fileOriginDate-7,"%Y-%m-%d"))
PARAM      <- list(needstobetagged=I$tag,whichdate=fileOrigin,statcomputer=computeAllStats,usedt=FALSE)

timeChunksWk    <- weekTimeChunk(timeperiod$start, timeperiod$end)
uweek           <- rhwatch(map       = summaries, reduce=rhoptions()$temp$colsummer
                           ,input    = I$name
                           ,debug    = 'collect'
                           ,output   = 'rweek'
                           ,jobname  = sprintf("Weekly Rollup")
                           ,mon.sec  = 0
                           ,setup    = setup
                           ,shared   = shared.files                               
                           ,read     = FALSE
                           ,param    = list(PARAM=append(PARAM, list(granularity='week' ,listOfTimeChunks = timeChunksWk))))

timeChunksMonth <- monthTimeChunk(timeperiod$start, timeperiod$end)
umonth          <- rhwatch(map       = summaries, reduce=rhoptions()$temp$colsummer
                           ,input    = I$name
                           ,debug    = 'collect'
                           ,output   = 'rmonth'
                           ,jobname  = sprintf("Monthly Rollup")
                           ,mon.sec  = 0
                           ,setup    = setup
                           ,shared   = shared.files                               
                           ,read     = FALSE
                           ,param    = list(PARAM=append(PARAM, list(granularity='month' ,listOfTimeChunks = timeChunksMonth))))

timeChunksDay   <- dayTimeChunk(timeperiod$start, timeperiod$end)
uday            <- rhwatch(map       = summaries, reduce=rhoptions()$temp$colsummer
                           ,input    = I$name
                           ,debug    = 'collect'
                           ,output   = 'rday'
                           ,jobname  = sprintf("Daily Rollup")
                           ,mon.sec  = 0
                           ,setup    = setup
                           ,shared   = shared.files                               
                           ,read     = FALSE
                           ,param    = list(PARAM=append(PARAM, list(granularity='day' ,listOfTimeChunks = timeChunksDay))))

print(waitForJobs(list(uday,umonth, uweek)))

toText("rweek"  ,o="tweek")
toText("rday"   ,o="tday")
toText("rmonth" ,o="tmonth")



