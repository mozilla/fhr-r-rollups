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
