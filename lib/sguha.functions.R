winVerCheck <- function(ver,keepverforothers=FALSE){
    ## http://www.msigeek.com/442/windows-os-version-numbers
    keepverforothers=eval(keepverforothers)
    mu <- sapply(c("win7"="6.1","winVista"="6.0",'winXP'="(5.1|5.2)",'win8/Server2012' = "6.2"
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
WNVer <- winVerCheck(keepverforothers=TRUE)
isn <- function(s,subcode=NA) if(is.null(s) || length(s)==0) subcode else s
                                   
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
