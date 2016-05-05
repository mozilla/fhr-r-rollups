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


srchStats  <- function(days,control){
    MULTIPLIER <- control$MULTIPLIER
    c(
        tActiveProfiles   = MULTIPLIER * isn(computeActives(days),0),
        tActiveSeconds    = MULTIPLIER * computeActiveSeconds(control$activity),
        tTotalSearch      = MULTIPLIER * computeSearchCounts(control$searchcounts),
        tGoogleSearch     = MULTIPLIER * computeSearchCounts(control$searchcounts, "google"),
        tYahooSearch      = MULTIPLIER * computeSearchCounts(control$searchcounts, "yahoo"),
        tBingSearch       = MULTIPLIER * computeSearchCounts(control$searchcounts, "bing"),
        tAmazonSearch     = MULTIPLIER * computeSearchCounts(control$searchcounts, "amazon"),
        tYandexSearch     = MULTIPLIER * computeSearchCounts(control$searchcounts, "yandex"),
        tDDGSearch        = MULTIPLIER * computeSearchCounts(control$searchcounts, "duckduckgo"),
        tEbaySearch       = MULTIPLIER * computeSearchCounts(control$searchcounts, "ebay"),
        tSeznamSearch     = MULTIPLIER * computeSearchCounts(control$searchcounts, "seznam"),
        tAOLSearch        = MULTIPLIER * computeSearchCounts(control$searchcounts, "aol"),
        tYahooJapanSearch = MULTIPLIER * computeSearchCounts(control$searchcounts, "yahooJapan"),
        tOfficialSearch   = MULTIPLIER * computeSearchCounts(control$searchcounts,
                                                             c("google", "yahoo", "bing", "otherofficial"))
    )
}

searchSummarizer  <- function(a,b){
    if(PARAM$needstobetagged){
        b <- fromJSON(b)
    }
    bdim              <- getDimensions(b)
    bdim              <- append(bdim, getStandardizedDimensions(bdim))
    bdim$snapshot     <- PARAM$whichdate
    bdim$granularity  <- PARAM$granularity
    if(bdim$isstdprofile!="TRUE") return()
    distribtype <- bdim$distribtype
    bdim$distribtype <- bdim$isstdprofile <- bdim$vendor <- bdim$name <- bdim$channel <- bdim$os  <- bdim$version <- NULL
    lapply(PARAM$listOfTimeChunks,function(timeChunk){
        days           <- get.active.days(b, timeChunk['start'], timeChunk['end'])
        bdim$timeStart <- as.character(timeChunk['start'])
        bdim$timeEnd   <- as.character(timeChunk['end'])
        activity       <- totalActivity(days)
        ## Search counts over time chunk by provivder.
        grouping <- list(yahoo = yahoo.searchnames(distribtype),
                         google = google.searchnames(distribtype),
                         bing = bing.searchnames(distribtype))
        ## Add a group for searches that use official plugins but none of the above.
        officialsn <- official.searchnames(distribtype)
        grouping[["otherofficial"]] <-
            officialsn[!(officialsn %in% unlist(grouping, use.names = FALSE))]
        grouping[["yahooJapan"]] <- c("yahoo-jp-auctions","yahoo-jp")
        grouping[["amazon"]] <- amazon.searchnames(distribtype)
        grouping[["yandex"]] <- yandex.searchnames(distribtype)
        grouping[["duckduckgo"]] <- duckduckgo.searchnames(distribtype)
        grouping[["ebay"]] <- ebay.searchnames(distribtype)
        grouping[['seznam']] <- c("seznam-cz","other-Seznam")
        grouping[['aol']] <- aol.searchnames(distribtype)        
        grouping[["other"]] <- NA
        searchcounts <- totalSearchCounts(days, provider = grouping, sap = FALSE)

        ## Your custom code can be here (in statcomputer):
        mystats        <- PARAM$statcomputer(days, control=list(MULTIPLIER    = isn(PARAM$sampleMultiplier,1),
                                                                activity = activity,
                                                                searchcounts  = searchcounts)
                                             )
        rhcollect(bdim,mystats)
    })
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

I <- list(name=sqtxt("/user/sguha/fhr/samples/output/1pct"),tag=TRUE, sampleMultiplier = 100) 

## Get the snapshot date from the sample creation time
rhread("/user/sguha/fhr/samples/output/createdTime.txt",type='text')
fileOrigin     <-  strsplit(rhread("/user/sguha/fhr/samples/output/createdTime.txt",type='text'),"\t")[[2]]
ll             <- regexec("[0-9]{4}-[0-9]{2}-[0-9]{2}",fileOrigin)
fileOrigin     <- substr(fileOrigin, ll[[1]],ll[[1]]+attributes(ll[[1]])$"match.length"-1)
fileOriginDate <- as.Date(fileOrigin); fileOrigin <- strftime(fileOriginDate,"%Y%m%d")
rm(ll);

## Make the directory into which the rollups will be placed
rhmkdir(sprintf("/user/sguha/srchrollup/%s", strftime(fileOriginDate,"%Y-%m-%d")))
hdfs.setwd(sprintf("/user/sguha/srchrollup/%s/",strftime(fileOriginDate,"%Y-%m-%d")))

PARAM      <- list(needstobetagged=I$tag,whichdate=fileOrigin,statcomputer=srchStats,usedt=FALSE,sampleMultiplier=I$sampleMultiplier)


BACK <- 45
timeperiod <- list(start = strftime(fileOriginDate-BACK,"%Y-%m-%d"),
                   end   = strftime(fileOriginDate-1,"%Y-%m-%d"))
timeChunksMonth <- monthTimeChunk(timeperiod$start, timeperiod$end)
umonth          <- rhwatch(map       = searchSummarizer, reduce=rhoptions()$temp$colsummer
                           ,input    = I$name
                           ,debug    = 'collect'
                           ,output   = 'rmonth'
                           ,jobname  = sprintf("Monthly Search Rollup")
                           ,mon.sec  = 0
                           ,setup    = setup
                           ,shared   = shared.files
                           ,read     = FALSE
                           ,param    = list(PARAM=append(PARAM, list(granularity='month' ,listOfTimeChunks = timeChunksMonth))))



BACK <- 175
timeperiod <- list(start = strftime(fileOriginDate-BACK,"%Y-%m-%d"),
                   end   = strftime(fileOriginDate-1,"%Y-%m-%d"))
timeChunksWk    <- weekTimeChunk(timeperiod$start, timeperiod$end)
uweek           <- rhwatch(map       = searchSummarizer, reduce=rhoptions()$temp$colsummer
                           ,input    = I$name
                           ,debug    = 'collect'
                           ,output   = 'rweek'
                           ,jobname  = sprintf("Weekly Search Rollup")
                           ,mon.sec  = 0
                           ,setup    = setup
                           ,shared   = shared.files
                           ,read     = FALSE
                           ,param    = list(PARAM=append(PARAM, list(granularity='week' ,listOfTimeChunks = timeChunksWk))))

print(waitForJobs(list(uday,umonth, uweek)))

toText("rmonth" ,o="tmonth",red=1)
toText("rweek"  ,o="tweek",red=1)

