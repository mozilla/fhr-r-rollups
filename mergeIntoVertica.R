source("~/prefix.R")
################################################################################
## Import data into vertica
################################################################################
email(subj="Verica Import Begins", body="empty body",to="joy@mozilla.com")

d <- odbc(user='fhr_rollup_rw', pass='WIATOoTv5qc4Macl')
exceptionsFile = "fhr_tables_import_exceptions.txt"
badDataFile =  "fhr_tables_import_baddata.txt"
Sys.setenv(HADOOP_HOME="")
for(x in list( 'week','month')){
    G <- function(s) if(s %in% c("week","month")) sprintf("%sly",s) else "daily"
    inputLocation = sprintf("%s/t%s", hdfs.getwd(),x)
    t <- tempfile()
    sys_cmd = sprintf("hadoop dfs -text %s/part* > %s", inputLocation, t)
    print("---------" )
    print(sys_cmd)
    result <- system(sys_cmd,intern=TRUE)
    print(result)

    ## Delete this table if it exists
    dbSendUpdate(d$con,sprintf( "DELETE FROM fhr_rollups_%s_base  where snapshot = '%s'", G(x), fileOrigin))

    ## Import this new data
    sql <- sprintf("COPY fhr_rollups_%s_base  FROM LOCAL '%s' delimiter '\t' EXCEPTIONS '/tmp/ %s-%s' REJECTED DATA '/tmp/ %s-%s' ;", G(x <- , t, exceptionsFile, x,badDataFile,x)
    print(sql)
    dbSendUpdate(d$con, sql)
}


###############
## one time
###############

email(subj="Verica Import Ended", body="empty body",to="joy@mozilla.com")


################################################################################
## Merge to Create Rollups
################################################################################

debug <- FALSE
d <- odbc(user='aphadke', pass='b3DrWmcn9Wbj')
email(subj="Merge Begins", body="empty body",to="joy@mozilla.com")


for(x in list( 'day','week','month','qtr')){
    tname <- sprintf("fhr_rollups_%s", x)
    vname <- sprintf("fhr_rollups_%s_view",x)
    if(!debug){
        ## dbSendUpdate(d$con, sprintf("TRUNCATE TABLE %s", vname))
        tryCatch(dbSendUpdate(d$con,sprintf("truncate  table %s",vname)),error=function(e) print(e))
    }
    sql <- sprintf("select snapshot_date,min(timeStart) as datemin, max(timeStart) as datemax from  %s  group by  snapshot_date order by snapshot_date desc", tname)
    rows <- d$q(sql)
    sqlq <- vector(mode='character', length=nrow(rows))
    for(i in 1:nrow(rows)){
        if(i==1){
            insert_sql = sprintf("SELECT * FROM %s  WHERE %s.timeStart >= '%s' AND %s.timeStart <= '%s' AND %s.snapshot_date = '%s'",  tname, tname, rows$datemin[i], tname,rows$datemax[i] , tname,rows$snapshot_date[i])
            date_max <- rows$datemin[i]
        } else {
            if ( rows$datemin[i] < date_max){
                insert_sql <- sprintf("SELECT * FROM %s  WHERE %s.timeStart >= '%s' AND %s.timeStart < '%s' AND %s.snapshot_date = '%s'",  tname, tname, rows$datemin[i], tname,date_max, tname,rows$snapshot_date[i])
                date_max =  rows$datemin[i]
            }else{
                insert_sql <- "skip"
            }
        }
        ## print(rows[i,])
        ## print(insert_sql)
        ## if(insert_sql != "skip") dbSendUpdate(d$con,insert_sql)
        if(insert_sql!="skip") sqlq[i] <- insert_sql
    }
    sqlq <- unlist(Filter(function(s) s!="", sqlq))
    ## finalq <- sprintf("CREATE TABLE  fhr_rollups_%s_view AS\n %s",x,paste(sqlq, collapse="\nUNION\n"))
    finalq <- sprintf("INSERT INTO   fhr_rollups_%s_view \n (%s)",x,paste(sqlq, collapse="\nUNION\n"))
    cat(finalq)
    print("--")
    if(nchar(finalq)>0) dbSendUpdate(d$con,finalq)
}

email(subj="Merge Ended", body="empty body",to="joy@mozilla.com")

