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



hdfs.setwd("/user/sguha/fhr/samples/")
sampleTypes <- list(  list(name="5pct",sample.method=function(k,v) if(runif(1)< 0.05) TRUE else FALSE)
                  , list(name="10pct",sample.method=function(k,v) if(runif(1)< 0.1) TRUE else FALSE)
                  , list(name="1pct",sample.method=function(k,v) if(runif(1)< 0.01) TRUE else FALSE)
                  , list(name="nightly",sample.method=function(k,v)
                      ## grepl('"updateChannel":".*nightly.*"', v))
                      isn(grepl("nightly",v$gecko$updateChannel),FALSE))
                  , list(name="aurora",sample.method=function(k,v)
                      isn(grepl("aurora",v$gecko$updateChannel),FALSE))
                  , list(name="beta",sample.method=function(k,v)
                      isn(grepl("beta",v$gecko$updateChannel),FALSE))
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
      newPathOther <- sprintf("%s/v3",allpaths[newIndex,'path'])
      newDate <- allpaths[newIndex,'date']
      ## Is the job complete?
      if(any(grepl("_SUCCESS", rhls(newPath)$file)) &&  any(grepl("_SUCCESS", rhls(newPathOther)$file))  )
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


setup <- expression(map={
                        library(rjson)
                        initPRNG()
                    })

if(foundPathToProcess){
    ## Each One is an Iteration, much simpler
    f <- rhwatch(
        map=function(k,v){
            tryCatch({
                v1 <- fromJSON(v)
                for(sType in sampleTypes){
                    if(sType$sample.method(k,v1)) rhcollect(sType$name, list(k,v))
                }
            }, error=function(e) rhcounter("MapError",as.character(e),1))
        }
       ,reduce  = 0
       ,jobname = sprintf(' Sampler: %s(%s)', input.location,fort())
       ,input   = sqtxt(input.location)
       ,output  = "ABC"
       ,setup   = setup
       ,read    = FALSE
       ,mapred  = list(mapred.reduce.tasks=0,mapred.job.priority="VERY_LOW",mapred.task.timeout=0,mapred.output.compress="true")
       ,param   = list(isn = function(s,subcode=NA) if(is.null(s) || length(s)==0) subcode else s,
            sampleTypes=sampleTypes, initPRNG = Rhipe:::initPRNG(10)))

    i <- 0
    for(atype in sampleTypes){
        i <- i+1
        tryCatch(rhdel(sprintf("output/%s",atype$name)),error=function(e) print(e) )
        rhwatch(map=function(a,b){
                    if(a==typeInQ) rhcollect(b[[1]], b[[2]])
                }
               ,reduce  = 200
               ,jobname = sprintf("Shuffling Sample %s %s/%s",atype$name,i,length(sampleTypes))
               ,mapred  = list(mapred.job.priority="VERY_LOW",mapred.output.compress="true",mapred.task.timeout=0)
               ,param   = list(typeInQ = atype$name)
               ,input   = "ABC"
               ,debug   = 'collect'
               ,read    = FALSE
               ,output  = rhfmt(type='sequence',folder=sprintf("output/%s",atype$name),recordsAsText=TRUE))
    }

    rhdel("ABC")

    writeLines(c(sprintf("%s",as.character(Sys.time())),input.location),"/tmp/createdTime.txt")
    ## rhput("/tmp/createdTime.txt","output/")
    system("hadoop dfs -rm /user/sguha/fhr/samples/output/createdTime.txt")
    system("hdfs dfs -put /tmp/createdTime.txt /user/sguha/fhr/samples/output/")
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
