source("~/prefix.R")
################################################################################
## Import data into vertica
################################################################################
email(subj="Verica Import Begins", body="empty body",to="<joy@mozilla.com>")

d <- odbc(user='fhr_rollup_rw', pass='WIATOoTv5qc4Macl')
exceptionsFile = "fhr_tables_import_exceptions.txt"
badDataFile =  "fhr_tables_import_baddata.txt"
Sys.setenv(HADOOP_HOME="")

## dbSendUpdate(d$con,"DELETE FROM fhr_rollups_daily_base")
## dbSendUpdate(d$con,"DELETE FROM fhr_rollups_weekly_base")
## dbSendUpdate(d$con,"DELETE FROM fhr_rollups_monthly_base")

G <- function(s) if(s %in% c("week","month")) sprintf("%sly",s) else "daily"

for(x in c( "day","week",'month')){
    for(backfill in rhls("/user/sguha/fhrrollup/")$file){
        print(sprintf("Backfill for %s",backfill))
        fileOrigin <- strftime(as.Date(tail(strsplit(backfill,"/")[[1]],1)),"%Y%m%d")
        hdfs.setwd(backfill)
        print(sprintf("Backfill for %s %s",backfill,x))
        inputLocation = sprintf("%s/t%s", hdfs.getwd(),x)
        t <- tempfile()
        sys_cmd = sprintf("hadoop dfs -text %s/part* > %s", inputLocation, t)
        print("---------" )
        print(sys_cmd)
        result <- system(sys_cmd,intern=TRUE)
        print(result)
        print(sprintf("Copied %s MB", j <- round( as.numeric(file.info(t)['size']/1024^2))))
        ## Delete this table if it exists
        if(j>0){
           dbSendUpdate(d$con,sprintf( "DELETE FROM fhr_rollups_%s_base  where snapshot = '%s'", G(x), fileOrigin))
           ## Import this new data
           system(sprintf("rm -rf /tmp/%s-%s",exceptionsFile,x))
           sql <- sprintf("COPY fhr_rollups_%s_base  FROM LOCAL '%s' delimiter '\t' EXCEPTIONS '/tmp/%s-%s' REJECTED DATA '/tmp/%s-%s' ;", G(x) , t, exceptionsFile, x,badDataFile,x)
           print(sql)
           err <- if(file.exists(sprintf("/tmp/%s-%s",exceptionsFile,x))) tryCatch(strsplit(system(sprintf("wc -l  /tmp/%s-%s",exceptionsFile,x),intern=TRUE)," ")[[1]][[1]],error=function(e) 0) else err <- 0
           if(err>0) stop("Errors parsing")
           dbSendUpdate(d$con, sql)
        }
    }
}


###############
## one time
###############

email(subj="Verica Import Ended", body="empty body",to="joy@mozilla.com")


## because the way backfills were done, this table is not really necc
## before every table had 170 days of data
## but now it's smart
## so there are no dup dates
## so need for this

################################################################################
## Merge to Create Rollups
################################################################################

## debug <- FALSE
## d <- odbc(user='fhr_rollup_rw', pass='WIATOoTv5qc4Macl')
## email(subj="Merge Begins", body="empty body",to="<joy@mozilla.com>")


## for(x in list('day' 'week','month')){
##     tname <- sprintf("fhr_rollups_%s_base", G(x ))
##     vname <- sprintf("fhr_rollups_%s",G(x))
##     if(!debug){
##         ## dbSendUpdate(d$con, sprintf("TRUNCATE TABLE %s", vname))
##         tryCatch(dbSendUpdate(d$con,sprintf("delete from   %s",vname)),error=function(e) print(e))
##     }
##     sql <- sprintf("select snapshot,min(timeStart) as datemin, max(timeStart) as datemax from  %s  group by  snapshot order by snapshot desc", tname)
##     rows <- d$q(sql)
##     sqlq <- vector(mode='character', length=nrow(rows))
##     for(i in 1:nrow(rows)){
##         if(i==1){
##             insert_sql = sprintf("SELECT * FROM %s  WHERE %s.timeStart >= '%s' AND %s.timeStart <= '%s' AND %s.snapshot = '%s'",  tname, tname, rows$datemin[i], tname,rows$datemax[i] , tname,rows$snapshot[i])
##             date_max <- rows$datemin[i]
##         } else {
##             if ( rows$datemin[i] < date_max){
##                 insert_sql <- sprintf("SELECT * FROM %s  WHERE %s.timeStart >= '%s' AND %s.timeStart < '%s' AND %s.snapshot = '%s'",  tname, tname, rows$datemin[i], tname,date_max, tname,rows$snapshot[i])
##                 date_max =  rows$datemin[i]
##             }else{
##                 insert_sql <- "skip"
##             }
##         }
##         if(insert_sql!="skip") sqlq[i] <- insert_sql
##     }
##     sqlq <- unlist(Filter(function(s) s!="", sqlq))
##     ## finalq <- sprintf("CREATE TABLE  fhr_rollups_%s_view AS\n %s",x,paste(sqlq, collapse="\nUNION\n"))
##     finalq <- sprintf("INSERT INTO fhr_rollups_%s \n (%s)",G(x),paste(sqlq, collapse="\nUNION\n"))
##     cat(finalq)
##     print("--")
##     if(nchar(finalq)>0) dbSendUpdate(d$con,finalq)
## }

## email(subj="Merge Ended", body="empty body",to="<joy@mozilla.com>")


