PCTMAX <- 1
MONSEC <- 15
library(sendmailR)
email <- function(subj="blank subject", body="blank body",to="<sguha@mozilla.com>"){
    tryCatch({
        bodyWithAttachment <- list(body)
        sendmail(from="<sguha@mozilla.com>",to=to,subject=subj
               ,msg=bodyWithAttachment
                 ,control=list(smtpServer='smtp.mozilla.org'))
        list(TRUE,NA)
    },error=function(e) list(FALSE,e))
}
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

    

#########################################################################################
## Running
#########################################################################################
source("/etc/mozilla.cluster.conf/other_configs/rhipe.mozilla.setup.R")
sqtxt <- function (folders) rhfmt(type = "sequence", folder = folders, recordsAsText = TRUE)

setwd("~/fhr-r-rollups")
source("lib/search.R",keep.source=FALSE)
source("lib/profileinfo.R",keep.source=FALSE)
source("lib/activity.R",keep.source=FALSE)
source("lib/sguha.functions.R",keep.source=FALSE)
source("makeFlatTables.v3.R",keep.source=FALSE)

I <- list(name=sqtxt("/user/sguha/fhr/samples/output/1pct"),tag=TRUE) ## tag if need to apply fromJSON

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
                   end   = strftime(fileOriginDate-1,"%Y-%m-%d"))
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
                           ,mapred   = list(mapred.task.timeout=0)
                           ,param    = list(PARAM=append(PARAM, list(granularity='day' ,listOfTimeChunks = timeChunksDay))))

print(waitForJobs(list(uday,umonth, uweek)))

toText("rweek"  ,o="tweek")
toText("rmonth" ,o="tmonth")
toText("rday"   ,o="tday")



################################################################################
## Now merge into vertica
################################################################################

if(exists("NotUsingCron") && NotUsingCron==TRUE){
email(subj="Verica Import Begins", body="empty body",to="<joy@mozilla.com>")

d <- odbc(user='fhr_rollup_rw', pass='WIATOoTv5qc4Macl')
exceptionsFile = "fhr_tables_import_exceptions.txt"
badDataFile =  "fhr_tables_import_baddata.txt"
Sys.setenv(HADOOP_HOME="")

G <- function(s) if(s %in% c("week","month")) sprintf("%sly",s) else "daily"
for(x in c( "day","week",'month')){
    ## We need to copy the data
    inputLocation = sprintf("%s/t%s", hdfs.getwd(),x)
    t <- tempfile()
    sys_cmd = sprintf("hadoop dfs -text %s/part* > %s", inputLocation, t)
    print("---------" )
    print(sys_cmd)
    result <- system(sys_cmd,intern=TRUE)
    print(result)
    print(sprintf("Copied %s MB", j <- round( as.numeric(file.info(t)['size']/1024^2))))

    ## Now delete this data frokm the table if it exists (just in case
    ## we want tor run this merge again, we do not want a duplicate copy
    dbSendUpdate(d$con,sprintf( "DELETE FROM fhr_rollups_%s_base  where snapshot = '%s'", G(x), fileOrigin))

    ## Delete data from previous rollup run to remove duplicates
    maxPreviousSnapshot <- as.character(d$q(sprintf("select max(snapshot) from fhr_rollups_%s_base", G(x))))
    ## Keep only data before this date from previous snapshot
    (   X <- data.frame(data.table(d$q(sprintf("select snapshot, min(timeStart) as ms, max(timeStart) as mas from fhr_rollups_%s_base  group by snapshot order by snapshot,ms",G(x))))))
    (    data.frame(data.table(d$q(sprintf("select distinct(timeStart) as v from fhr_rollups_%s_base where snapshot='%s'",G(x),maxPreviousSnapshot)))[order(v),]))
    (keepdates <- range(unlist(list(day=timeChunksDay, week=timeChunksWk,month=timeChunksMonth)[[x]])))
    (sql <- sprintf("delete from fhr_rollups_%s_base where snapshot='%s' and timeStart >= '%s'", G(x),maxPreviousSnapshot, keepdates[1]))
    dbSendUpdate(d$con,sql)
    
    system(sprintf("rm -rf /tmp/%s-%s",exceptionsFile,x))
    sql <- sprintf("COPY fhr_rollups_%s_base  FROM LOCAL '%s' delimiter '\t' EXCEPTIONS '/tmp/%s-%s' REJECTED DATA '/tmp/%s-%s' ;", G(x) , t, exceptionsFile, x,badDataFile,x)
    dbSendUpdate(d$con, sql)
    err <- if(file.exists(sprintf("/tmp/%s-%s",exceptionsFile,x))) tryCatch(strsplit(system(sprintf("wc -l  /tmp/%s-%s",exceptionsFile,x),intern=TRUE)," ")[[1]][[1]],error=function(e) 0) else err <- 0
    if(err>0) stop("Errors parsing")
}

.s <- d$q("select max(timeStart), min(timeStart) from fhr_rollups_daily_base")
email(subj=sprintf("FHR Rollups V2: Completed for %s", strftime(as.Date(fileOrigin,"%Y%m%d"),"%Y-%m-%d")),
      body=sprintf("Rolllups have been created with a min date of %s and a max date of %s\n%s",  as.character(.s[2]), as.character(.s[1]),fort()),
      to="<metrics@mozilla.com>")

}
## Testing, do not delete, this is useful
## h <- odbc()

## (v1 <- data.table(h$q("select timeStart,sum(tTotalProfiles) as t,sum(tNewProfiles) as n,sum(tExistingProfiles) as e, sum(tDefault) as d, sum(tInactiveLast30Days) as ch, sum(twasActive5) as t57,sum(tSrch) as sr, sum(tSrchGoogle) as srg, sum(tSrchYahoo) as sry from fhr_rollups_day_view where prodname='Firefox'and prodVersion='ANY' group by timeStart order by timeStart")))

## (v2 <- data.table(d$q("select timeStart,sum(tTotalProfiles) as t,sum(tNewProfiles) as n,sum(tExistingProfiles) as e, sum(tIsDefault) as d, sum(tChurned) as ch, sum(t5outOf7) as t57,sum(tTotalSearch) as sr, sum(tGoogleSearch) as srg, sum(tYahooSearch) as sry from fhr_rollups_daily_base where name='Firefox' group by timeStart order by timeStart")))

## (s1 <- v1[timeStart %between% c("2015-02-01","2015-02-06"),])
## (s2 <- v2[timeStart %between% c("2015-02-01","2015-02-06"),])


## local({
##     a <- as.matrix(s1[, 2:10, with=FALSE])
##     b <- as.matrix(s2[, 2:10, with=FALSE])
##     round(100*abs(a-b)/a,1)
## })


email(subj="Rollups Completed Successfully", body=sprintf("The newest snapshot is %s", fileOrigin), to="<joy@mozilla.com>")
