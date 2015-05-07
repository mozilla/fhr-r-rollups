
isn <- function(s,subcode=NA) if(is.null(s) || length(s)==0 || is.na(s)) subcode else s
winVerCheck <- function(ver,keepverforothers=FALSE){
    ## http://www.msigeek.com/442/windows-os-version-numbers
    keepverforothers=eval(keepverforothers)
    mu <- sapply(c("win7"="6.1","winVista"="6.0",'winXP'="(5.1|5.2)",'win8/Server2012' = "6.2"
                   ,"win8.1"="6.3","win10"="10.0"
                   ,"win2K"="5.0","winMe"="4.9", "win98"="4.1","win95"="4.0","winNT"="3.5")
                 ,function(r) sprintf("^(%s)",r))
    mun <- names(mu)
    id <- 1:length(mu)
    return(function(s){
        if(is.na(s) || is.null(s) || length(s)==0) return(NA)
        for(i in id){
            if(grepl(mu[i],s)) return(mun[i])
        }
        if(keepverforothers) return(sprintf("winOther_%s",s)) else ("winOthers")
  })
}
getWindowsVersionAsString <- winVerCheck(keepverforothers=TRUE)

##' Takes bytes and converts to human readable format
##' @param bytes is the number of bytes
##' @return a string
bytesToString <- function(bytes,rnd=2){
  Round <- function(a,b){
    format(round(a,b),nsmall=2)
  }
  if(bytes<1024) sprintf("%s kb",bytes)
  else if(bytes<1024*1024) sprintf("%s KB",Round(bytes/1024,rnd))
  else if(bytes< 1024*1024*1024) sprintf("%s MB", Round(bytes/(1024*1024),rnd))
  else  sprintf("%s GB",Round(bytes/(1024*1024*1024),rnd))
}


##' Takes seconds and converts to human readable format
##' @param secs is the number of seconds
##' @return a string
secondsToString <- function(secs,rnd=2){
  Round <- function(a,b){
    format(round(a,b),nsmall=2)
  }
  if(secs<60) sprintf("%s seconds",secs)
  else if(secs<60*60) sprintf("%s minutes",Round(secs/60,rnd))
  else if(secs< 86400) sprintf("%s hours", Round(secs/(60*60),rnd))
  else if(secs< (86400*30)) sprintf("%s days",Round(secs/(86400),rnd))
  else if(secs< (86400*365)) sprintf("%s months",Round(secs/(86400*30),rnd))
  else  sprintf("%s years",Round(secs/(86400*365),rnd))
}


getOSXVersionAsString <- function(s){
    s <- isn(s)
    if(grepl("^(10)",s)) return("darwinSnowLeopard")
    if(grepl("^(11)",s)) return("darwinLion")
    if(grepl("^(12)",s)) return("darwinMountainLion")
    if(grepl("^(13)",s)) return("darwinMavericks")
    if(grepl("^(14)",s)) return("darwinYosemite")
    return(sprintf("darwinOthers_%s", s))
}
    
getProfileCreationDate <- function(b){
    profileCrDate <- strftime(as.Date(
        b$data$last$org.mozilla.profile.age$profileCreation,"1970-01-01"), 
        "%Y-%m-%d")
    if(is.null(profileCrDate)) {
        profileCrDate <- if(length(b$data$days) > 0) { 
            min(names(b$data$days)) 
        } else { b$thisPingDate }
    }
    if(is.null(profileCrDate) || is.na(profileCrDate)) return(NULL)
    return(profileCrDate)
}
           
## parseJobId In future version of RHIPE and Hadoop MR2 this will be
## removed, it is hack which isnt robust to Hadoop versions
parseJobIDFromTrackingURL <- function(job){
    ## replace with parseJobIDFromTracking
    x <- gregexpr("jobid=", job[[1]]$tracking)
    st <- x[[1]] + attr(x[[1]], "match.length")
    substring(job[[1]]$tracking, st, 1000000L)
}

## This code monitors errors during the map phase and if the % of
## errors is greater than pctcut aborts the job and saves the status
## counter in errorvar
jobHandler <- function(y,pctcut=2,errorVar="jobErrorVar"){
    pctcut <- eval(pctcut)
    return(function(y){
        mapinputrecords <- tryCatch(as.numeric(y$counters$"Map-Reduce Framework"["Map input records",]),error=function(e) NULL)
        rerrors <- y$counters$R_UNTRAPPED_ERRORS
        rvalue <- TRUE
        if(!is.null(mapinputrecords) && !is.null(rerrors) && nrow(rerrors)>0){
            z <- data.table(errors=rownames(rerrors), n=as.numeric(rerrors[,1]), pct = round(as.numeric(rerrors[,1]/mapinputrecords)*100,2))
            if(any(z$pct>pctcut)){
                J <- list(z,y)
                assign(errorVar,J,.GlobalEnv)
                message(sprintf("Handler is Killing %s",y$tracking))
                rhkill(parseJobIDFromTrackingURL(list(y)))
                rvalue <- FALSE
            }
        }
        rvalue
    })
}

## Converts the sequence files to text files for Vertica to import
toText <- function(i,o){
    y <- rhwatch(map=function(a,b){
        b <- formatC(b, format="f",digits=0)
        rhcollect(NULL, c(a,b))    },reduce=0, input=i
                 ,output=rhfmt(type='text', folder=o,writeKey=FALSE,field.sep="\t",stringquote=""),read=FALSE)
    a <- rhls(o)$file
    rhdel(a[!grepl("part-",a)])
    rhchmod(o,"777")
    o
}



hlmCategory <- function(days,totalDays=NULL,asNumber=FALSE,returnHrs=FALSE){
    ## needs dependen functinons in https://github.com/mozilla/fhr-r-rollups/blob/master/makeFlatTables.v3.R
    totalhrs <- totalActivity(days)
    totalhrs <- totalhrs$activesec/3600
    avg <- totalhrs / totalDays
    state=if(avg<10/60) "01La" else if(avg<=30/60) "02Oc" else if(avg<1) "03Li" else if(avg<3) "04Me" else if (avg<18) "05Hi" else "06Bo"
    res <- if(asNumber){
        if(state=="01La") 1 else if(state=="02Oc") 2 else if(state=="03Li") 3 else if(state=="04Me") 4 else if(state=="05Hi") 5 else 6
    }else state
    if(returnHrs) return( list(hrs=totalDays, state=res)) else return(res)
}


tagDaysByVersion <- function(d){
    ##  takes $data$days and returns a modified data$days
    ## with versioninfo attached as a field
    ## i've not done much error checking with this
    ## if you get errors notify me
    if(length(d$data$days)==0) return(d$data$days)
    days <- d$data$days [ order(names(d$data$days)) ]
    dates <- names(days)
    dversion <- rep(NA, length(dates))
    iversion <- NA
    verupdate <- rep(FALSE,length(dates))
    for(i in 1:length(dates)){
        vs <- days[[i]]$org.mozilla.appInfo.versions
        if(is.null(vs$appVersion)){
            dversion[i] <- iversion
        }else{
            iversion <- dversion[i] <- max(unlist(vs$appVersion)) ## there can be several on a day
            verupdate[i] <- TRUE
        }
    }
    ## If alll of dversion is NA, there was never an update
    ## Force them to be equal to gecko values
    if(all(is.na(dversion))) dversion <- rep(d$geckoAppInfo$platformVersion, length(dates))
    days <- mapply(function(dc, dv, vu){
        dc$versioninfo <- list(version=dv,vup=vu)
        dc
    }, days, dversion,verupdate, SIMPLIFY=FALSE)
    return(days)
}
