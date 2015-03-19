#######################################################################
###  
###  Functions for working with other parts of the FHR payload, 
###  not covered in other lib files.
###  
#######################################################################


## Retrieves the sequence of valid version updates from the supplied FHR 
## days list.
## 
## Returns a vector mapping dates that saw version updates to the full version
## number updated to on that day, ordered chronologically. 
## If there were multiple updates on the day, this is the last update on the day. 
## 
## If no valid updates were found across the input days, returns NULL.
updateValues <- function(days) {
    if(length(days) == 0) return(NULL)
    updates <- unlist(lapply(days, function(d) {
        vu <- d$org.mozilla.appInfo.versions
        if(length(vu) == 0) return(NULL)
        ## Handle different field versions. 
        vu <- if(identical(vu[["_v"]], 1)) vu$version else vu$appVersion
        ## If there are multiple versions, use the latest one. 
        if(length(vu) > 1) vu <- vu[[length(vu)]]
        ## Simple validity check.
        if(is.na(vu) || !grepl("^(\\d+)\\.", vu)) return(NULL)
        vu
    }))
    if(length(updates) == 0) return(NULL)
    updates[order(names(updates))]
}



