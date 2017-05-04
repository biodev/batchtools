#' @title ClusterFunctions for HTCondor Systems
#'
#' @description
#' Cluster functions for HTCondor (\url{https://research.cs.wisc.edu/htcondor/}).
#'
#' NOTE: The below is a slight modification of the text for \code{clusterFunctionsSGE}
#' Job files are created based on the brew template \code{template}. This
#' file is processed with brew and then submitted to the queue using the
#' \code{condor_submit} command. Jobs are killed using the \code{condor_rm} command and the
#' list of running jobs is retrieved using \code{condor_q}. The user must have
#' the appropriate privileges to submit, delete and list jobs on the cluster
#' (this is usually the case).
#'
#' The template file can access all resources passed to \code{\link{submitJobs}}
#' as well as all variables stored in the \code{\link{JobCollection}}.
#' It is the template file's job to choose a queue for the job and handle the desired resource
#' allocations.
#'
#' @note Need to supply \code{walltime}, \code{memory}, \code{cpus}, \code{universe}, \code{R.bin.path} and \code{out.dir} in a list to the resources argument of submitJobs etc.
#'
#' @templateVar cf.name condor
#' @template template
#' @inheritParams makeClusterFunctions
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
makeClusterFunctionsCondor = function(template = system.file("templates/condor-ohsu.tmpl",package="batchtools"), scheduler.latency = 1, fs.latency = 65) { # nocov start
  template = findTemplateFile(template)
  template = cfReadBrewTemplate(template)
  
  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")
    
    outfile = cfBrewTemplate(reg, template, jc)
    res = runOSCommand("condor_submit", c("-verbose", outfile))
    
    if (res$exit.code > 0L) {
      cfHandleUnknownSubmitError("condor_submit", res$exit.code, stri_flatten(res$output, collapse=" "))
    } else {
      
      possible.ids <- regmatches(res$output, regexec("Proc\\s+(\\d+\\.\\d+)", res$output))
      
      found.ids <- lengths(possible.ids) > 0
      
      if (sum(found.ids) != 1){
        cfHandleUnknownSubmitError("condor_submit", 5, paste("Too many possible ids:", stri_flatten(res$output, collapse=" ")))
      }
      
      batch.id = possible.ids[found.ids][[1]][2]
      makeSubmitJobResult(status = 0L, batch.id = batch.id)
    }
  }
  
  run.map <- c(
    "Idle",
    "Running",
    "Removed",
    "Completed",
    "Held",
    "Transferring Output"
  )
  
  listJobs = function(reg, cmd, status) {
    classads <- runOSCommand(cmd[1L], cmd[-1L])$output
    
    if ((length(classads) == 0) | (length(classads) == 1 && classads == "Error: Collector has no record of schedd/submitter")){
      return(character(0))
    }
    
    class.by.job <- strsplit(stri_flatten(classads, collapse="$$"), "\\$\\$\\$\\$")[[1]]
    
    found.ids <- lapply(class.by.job, function(x){
      split.x <- strsplit(x, "$$", fixed=T)[[1]]
      
      status <- strsplit(split.x[grep("^JobStatus", split.x)], "\\s+=\\s+")[[1]][2]
      cluster <- strsplit(split.x[grep("^ClusterId", split.x)], "\\s+=\\s+")[[1]][2]
      proc <- strsplit(split.x[grep("^ProcId", split.x)], "\\s+=\\s+")[[1]][2]
      
      list(batch.id=paste(cluster, proc, sep=".") ,status=status)
      
    })
    
    keep.ids <- run.map[as.integer(sapply(found.ids, "[[", "status"))] %in% status
    
    temp <- sapply(found.ids, "[[", "batch.id")[keep.ids]
    
    "!DEBUG [listJobs]: `paste(temp, collapse=',')`"
    
    return(temp)
  }
  
  #ClusterId and ProcId
  
  listJobsQueued = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    listJobs(reg, c("condor_q",  "-long", "-submitter $USER"), c("Idle", "Held"))
  }
  
  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    listJobs(reg, c("condor_q", "-long",  "-submitter $USER"), "Running")
  }
  
  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    cfKillJob(reg, "condor_rm", batch.id)
  }
  
  makeClusterFunctions(name = "Condor", submitJob = submitJob, killJob = killJob, listJobsQueued = listJobsQueued,
                       listJobsRunning = listJobsRunning, store.job = TRUE, scheduler.latency = scheduler.latency, fs.latency = fs.latency)
} # nocov end
