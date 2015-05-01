## ################################################
## Stratified on:
## vendor
## appname
## version
## os
## channel
## buildid(1,8)
## arch
## dummySplit to make things faster (subsequently dropped)
## #################################################
Sys.setenv(MOZILLA_CLUSTER="peach")  
source("~/prefix.R")

## if(Sys.getenv("FHR_CLEAN_DATA")=="")
##     stop("What is FHR_CLEAN_DATA")
## print(Sys.getenv("FHR_CLEAN_DATA"))


setup <- expression(map={ library(rjson)})
code <- expression({
  beta.samples <- sample(b[[1]], length(map.values), replace=TRUE)
  release.samples <- sample(b[[2]], length(map.values), replace=TRUE)
  mapply(function(k,r,index){
    tryCatch({
      ## json <- fromJSON(r2c <- rawToChar(r[[1]]))
      
      json <- fromJSON(r2c <- r[[1]])
      ## I absolutely do not care about versions <2
      if(is.null(json) || json$version <2) return()
      
      gecko <- json$geckoAppInfo
      vendor      <- gecko$vendor
      appname     <- gecko$name
      version     <- gecko$version
      os          <- gecko$os
      channel     <- gecko$updateChannel
      buildID     <- substr(gecko$appBuildID,1,8)
      geo         <- isn(json$geoCountry)
      did         <- isn(json$data$last$org.mozilla.appInfo.appinfo$distributionID ," ")
      sys <- json$data$last$"org.mozilla.sysinfo.sysinfo"
      arch  <- sys$architecture
      
      dummysplit = tryCatch(if(grepl('beta',channel)) beta.samples[index]  else if(grepl('release',channel)) release.samples[index] , error=function(e) rhcounter("SUBSAMPLER",e,1))
      strat.key <- list(vendor, appname, version, os,channel, buildID, arch,geo,did,ds = dummysplit)
      rhcollect(strat.key,list(k,r2c))
    }, error=function(e) rhcounter("RHIPE_UNTRAPPED_ERRORS",as.character(e),1))
  },map.keys,map.values, 1:length(map.values))
})


sampler <- expression(
    reduce = {
      for(j in sampleTypes){
        idc <- j$chooseIndices(reduce.values,reduce.key)  
        for(i in idc){
          aj <- reduce.values[[i]]
          rhcollect(list(j$name,aj[[1]]), aj[[2]])
        }
      }
    })

hdfs.setwd("/user/sguha/fhr/samples/")
randomIndex <- function(avec, prop){
  which(runif(length(avec))< prop)
}
sampleTypes <- list(  list(name="5pct",fraction = 0.05,chooseIndices=function(a,b) randomIndex(a,0.05))
                  , list(name="10pct",fraction = 0.1,chooseIndices=function(a,b) randomIndex(a,0.1))
                  , list(name="1pct",fraction = 0.01,chooseIndices=function(a,b) randomIndex(a,0.01))
                  , list(name="0.01pct",fraction = 0.01/100,chooseIndices=function(a,b) randomIndex(a,0.01/100))
                    , list(name="fromjson1pct",fraction = 0.01,chooseIndices=function(a,b) randomIndex(a,0.01))
                    , list(name="nightly", chooseIndices = function(a,k)
                           tryCatch({
                             if(grepl("nightly",k[[5]]))
                               return(1:length(a)) else integer(0)},
                                    error=function(e){
                                      rhcounter("Errs","PreRelease",1)
                                      integer(0)
                                    }))
                    , list(name="aurora", chooseIndices = function(a,k)
                           tryCatch({
                             if(grepl("aurora",k[[5]]))
                               return(1:length(a)) else integer(0)},
                                    error=function(e){
                                      rhcounter("Errs","PreRelease",1)
                                      integer(0)
                                    }))
                    , list(name="beta", chooseIndices = function(a,k)
                           tryCatch({
                             if(grepl("beta",k[[5]]))
                               return(1:length(a)) else integer(0)},
                                    error=function(e){
                                      rhcounter("Errs","PreRelease",1)
                                      integer(0)
                                    }))
                    )


getLocation <- function(){
  if(Sys.getenv("FHR_CLEAN_DATA")!="") {
    return(Sys.getenv("FHR_CLEAN_DATA"))
  }else{
      extractPathAndTime <- function(s){
          w2 <- regexpr("[0-9]{4}-[0-9]{2}-[0-9]{2}",s)
          if(w2<0){
              return(NULL)
          }else{
              c(path=s, date=substr(s, w2, attr(w2,"match.length")+w2-1))
          }
      }
      names <- NULL
      deOrphanLocation <- "/user/bcolloran/deorphaned/"
      lastDeOrphanedSample <- extractPathAndTime(rhread("/user/sguha/fhr/samples/output/createdTime.txt",type='text')[2])
      allpaths <- do.call(rbind,lapply(rhls(deOrphanLocation)$file,function(s){
          extractPathAndTime(s)
      }))
      allpaths <- allpaths[order(allpaths[,'date']),]
      newIndex <- max(which(allpaths[,'date']> lastDeOrphanedSample['date']))
      if(newIndex <0) return(NULL)
      newPath <- sprintf("%s/v2",allpaths[newIndex,'path'])
      newDate <- allpaths[newIndex,'date']
      ## Is the job complete?
      if(any(grepl("_SUCCESS", rhls(newPath)$file)))
          names <- newPath
      }
  names
}

total.time.waited <- 0
while(total.time.waited<= 10*60*60){
    input.location <- getLocation()
    if(is.null(input.location)){
        total.time.waited <- total.time.waited+30*60
        message("waiting 30 mins")
        Sys.sleep(30*60)
    } else{
        break
    }
}
if(is.null(input.location)){
    email(subj="FHR Sample Error: Waited for 10 hrs, yet could not find a valid input.location")
    stop("No input.path")
}
foundPathToProcess <- !is.null(input.location)

email(subj="FHR Sample : Started")

if(foundPathToProcess){
    T1 <- rhwatch(map      = code
                  ,reduce  = 0
                  ,input   = sqtxt(input.location)
                  ,output  = 'ABC'
                  ,jobname = sprintf('T1 FHR Sampler: %s(%s)', input.location,fort())
                  ,setup   = setup
                  ,read    = FALSE
                  ,mapred  = list(mapred.reduce.tasks=0,mapred.job.priority="VERY_LOW",mapred.task.timeout=0,mapred.output.compress="true")
                  ,param   = list(sampleTypes=sampleTypes, randomIndex = randomIndex,SAMPLE.PCT=0.05,b=list(1:10000,1:100000)))


    T2 <- rhwatch(map      = expression({ mapply(function(k,v) rhcollect(k,v), map.keys, map.values)})
              ,reduce  = sampler
                 ,input   = 'ABC'
                 ,output  =  'sample-stage1'
                 ,jobname = sprintf('T2 FHR Sampler: %s(%s)', input.location,fort())
                 ,setup   = setup
                 ,read    = FALSE
                 ,mapred  = list(mapred.reduce.tasks=1500,mapred.job.priority="VERY_LOW",mapred.task.timeout=0,mapred.output.compress="true")
                 ,param   = list(sampleTypes=sampleTypes, randomIndex = randomIndex,SAMPLE.PCT=0.05,b=list(1:10000,1:100000)))


    stats <- T2
    
    if(stats[[1]]$state!="SUCCEEDED"){
        library(sendmailR)
        sendmail(from="joy@mozilla.com",to="joy@mozilla.com",subject="FHR Sample (ERROR)"
                 ,msg=list(sprintf("Error\n%s", capture.output(print(stats[[1]])))),control=list(smtpServer='localhost'))
        quit("no")
    }
    st <- paste(capture.output(print(stats[[1]])),collapse="\n")

    ## #####################################################
    ## Shuffle out the samples into smaller directories
    ## ####################################################
    success <- 0
    i <- 1
    saveJobInfo <- function(w, input.location,dest){
        library(rjson)
        yy <- list(mapred=structure(as.list(w$counters$"Map-Reduce Framework"),names=rownames(w$counters$"Map-Reduce Framework")))
        yy$samplerInfo <- list(  'samplerEndTime'=sprintf("%s",as.character(Sys.time())),"samplerInputPath"=input.location)
        writeLines(toJSON(yy),sprintf("/tmp/%s",dest))
        rhput(sprintf("/tmp/%s",dest),"output/")
        unlink(sprintf("/tmp/%s",dest))
    }

    for(atype in sampleTypes){
        map <- expression({
            w <- unlist(lapply(map.keys, function(a) a[[1]]==atype$name ))
            mapply(function(a,b) {
                tryCatch({
                    if(a[[1]] %in% c("fromjson1pct")){
                        b <- fromJSON(b)
                        b$data$days <- tagDaysByBuildVersion(b)
                    }
                    rhcollect(a[[2]],b)
                }, error=function(e) rhcounter("StatError",as.character(e),1))
            }, map.keys[w], map.values[w])
        })
        tryCatch(rhdel(sprintf("output/%s",atype$name)),error=function(e) print(e) )
        tryCatch({
            j <- rhwatch(map=map, reduce=100
                         ,input   = 'sample-stage1'
                         ,output  = if(atype$name=="fromjson1pct") sprintf("output/%s",atype$name) else rhfmt(type='sequence',folder=sprintf("output/%s",atype$name),recordsAsText=TRUE)
                         ,jobname = sprintf("Shuffling Sample %s %s/%s",atype$name,i,length(sampleTypes))
                         ,mapred  = list(mapred.job.priority="VERY_LOW",mapred.output.compress="true",mapred.task.timeout=0)
                         ,param   = list(atype=atype,tagDaysByBuildVersion=tagDaysByBuildVersion)
                         ,setup   = expression({library(rjson)})
                         ,mon.sec = Inf
                         ,read    = FALSE)
            i <- i+1
            saveJobInfo(j[[1]], input.location, sprintf("stats.%s.json",atype$name))
        },error=function(e) print(e))
    }

    rhdel("sample-stage1")
    rhdel('ABC')

    js <- paste(capture.output(print(j[[1]])),collapse="\n")

    st <- sprintf("Sampler:\n%s \nOne Shuffle:\n%s",st,js)
    library(sendmailR)
    bodyWithAttachment <- list(st)
    tryCatch({
        sendmail(from="joy@mozilla.com",to="joy@mozilla.com",subject="FHR Sample"
                 ,msg=bodyWithAttachment,control=list(smtpServer='localhost'))
    },error=function(e){
        sendmail(from="joy@mozilla.com",to="joy@mozilla.com",subject="FHR Sample"
                 ,msg=list(as.character(e)),control=list(smtpServer='localhost'))
    })
    writeLines(c(sprintf("%s",as.character(Sys.time())),input.location),"/tmp/createdTime.txt")
    rhput("/tmp/createdTime.txt","output/")

                                        # ########################################################
                                        # # Copy to backup
                                        # ####################################################
    ll <- regexec("[0-9]{4}-[0-9]{2}-[0-9]{2}",input.location)
    ll <- substr(input.location, ll[[1]],ll[[1]]+attributes(ll[[1]])$"match.length"-1)
    rhmkdir(backup.location <- sprintf("/user/sguha/fhr/samples/backup/%s/1pct",ll))
    backup.results <- rhwatch(map=function(a,b) rhcollect(a,b), input=sqtxt("/user/sguha/fhr/samples/output/1pct")
                              ,reduce=0 ,output=rhfmt(type='sequence',folder=sprintf("%s/",backup.location),recordsAsText=TRUE),read=FALSE)
    rm(ll)    
}

## #######################################################
## testing
## #######################################################
## sm <- function(x){  
##   m <- data.frame(channel = fn(x,"k",1), country = fn(x,"k",2), count = fn(x,"v",1))
##   m[order(m$channel,m$country),]
## }
## hdfs.setwd("/user/sguha/fhr/samples/tmp")
## f <- rhwatch(map=rhmap({
##   if(r$geckoAppInfo$updateChannel %in% c('nightly',"aurora","beta"))
##     rhcollect(list(isn(r$geckoAppInfo$updateChannel), r$geo),1)
## }), reduce=summer, input="output/1")
## h <- doFHR({
##   if(json$version>=2 && json$geckoAppInfo$updateChannel %in% c('nightly',"aurora","beta"))
##     rhcollect(list(isn(json$geckoAppInfo$updateChannel), json$geo),1)
## },reduce=summer,debug='count')

## f1 <- sm(f)
## h1 <- sm(h)


## source("~/prefix.R")
## map <- function(a,b){
##   rhcollect(runif(1),paste(sample(letters,10),collapse=";"))
## }
## z <- rhwatch(map=map, reduce=0, input=c(5,1), output=rhfmt(type='sequence',folder="/user/sguha/tmp/y",recordsAsText=TRUE))
