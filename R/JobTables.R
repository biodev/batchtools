#' @title Query Job Information
#'
#' @description
#' \code{getJobStatus} returns the internal table which stores information about the computational
#' status of jobs, \code{getJobPars} a table with the job parameters, \code{getJobResources} a table
#' with the resources which were set to submit the jobs, and \code{getJobTags} the tags of the jobs
#' (see \link{Tags}).
#'
#' \code{getJobTable} returns all these tables joined.
#'
#' @templateVar ids.default all
#' @template ids
#' @param flatten [\code{logical(1)}]\cr
#'   Transform the job parameters and/or resource specifications to data frame columns?
#'   Defaults to \code{TRUE} if all elements parameters or resources are atomics,
#'   \code{FALSE} otherwise where each row of the column will hold a named list.
#'   New columns will be named, depending on the value of \code{prefix}.
#' @param prefix [\code{logical(1)}]\cr
#'   If set to \code{TRUE}, the prefix \dQuote{par.} is used to name column names of parameters
#'   for a \code{\link{Registry}} and prefixes \dQuote{prob.par.} and \dQuote{algo.par.} are used to name
#'   the columns of a \code{\link{ExperimentRegistry}}. Resources are prefixed with \dQuote{res.}.
#' @template reg
#' @return [\code{\link{data.table}}] with the following columns (not necessarily in this order):
#'   \describe{
#'     \item{job.id}{Unique Job ID as integer.}
#'     \item{submitted}{Time the job was submitted to the batch system as \code{\link[base]{POSIXct}}.}
#'     \item{started}{Time the job was started on the batch system as \code{\link[base]{POSIXct}}.}
#'     \item{done}{Time the job terminated (successfully or with an error) as \code{\link[base]{POSIXct}}.}
#'     \item{error}{Either \code{NA} if the job terminated successfully or the error message.}
#'     \item{memory}{Estimate of the memory usage.}
#'     \item{batch.id}{Batch ID as reported by the scheduler.}
#'     \item{job.hash}{Unique string identifying the job or chunk.}
#'     \item{time.queued}{Time in seconds (as \code{\link[base]{difftime}}) the job was queued.}
#'     \item{time.running}{Time in seconds (as \code{\link[base]{difftime}}) the job was running.}
#'     \item{pars}{List of parameters/arguments for this job. Possibly expanded to separate columns (see \code{prefix}.}
#'     \item{resources}{List of computational resources set for this job. Possibly expanded to separate columns (see \code{prefix}.}
#'     \item{tags}{Tags as joined string, delimited by \dQuote{,}.}
#'     \item{problem}{Only for \code{\link{ExperimentRegistry}}: the problem identifier.}
#'     \item{algorithm}{Only for \code{\link{ExperimentRegistry}}: the algorithm identifier.}
#'   }
#'   See \code{\link{JoinTables}} for examples on working with job tables.
#' @export
#' @examples
#' tmp = makeRegistry(file.dir = NA, make.default = FALSE)
#' f = function(x) if (x < 0) stop("x must be > 0") else sqrt(x)
#' batchMap(f, x = c(-1, 0, 1), reg = tmp)
#' submitJobs(reg = tmp)
#' waitForJobs(reg = tmp)
#'
#' getJobTable(reg = tmp, flatten = FALSE)
#' getJobPars(reg = tmp)
#' addJobTags(1:2, "tag1", reg = tmp)
#' addJobTags(2, "tag2", reg = tmp)
#' getJobTags(reg = tmp)
getJobTable = function(ids = NULL, flatten = NULL, prefix = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  ids = convertIds(reg, ids)
  getJobStatus(ids, reg = reg)[getJobPars(ids, flatten = flatten, prefix = prefix, reg = reg)][getJobResources(ids = ids, flatten = flatten, reg = reg)][getJobTags(ids = ids, reg = reg)]
}

#' @export
#' @rdname getJobTable
getJobStatus = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg, sync = TRUE)
  submitted = started = done = NULL

  ids = convertIds(reg, ids)
  tab = inner_join(reg$status, ids)[, !c("def.id", "resource.id"), with = FALSE]
  tab[, "submitted" := as.POSIXct(submitted, origin = "1970-01-01")]
  tab[, "started" := as.POSIXct(started, origin = "1970-01-01")]
  tab[, "done" := as.POSIXct(done, origin = "1970-01-01")]
  tab[, "time.queued" := as.difftime(started - submitted, units = "secs")]
  tab[, "time.running" := as.difftime(done - started, units = "secs")]
  tab[]
}

#' @export
#' @rdname getJobTable
getJobResources = function(ids = NULL, flatten = NULL, prefix = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  if (!is.null(flatten))
    assertFlag(flatten)
  assertFlag(prefix)

  ids = convertIds(reg, ids)
  tab = merge(inner_join(reg$status, ids), reg$resources, all.x = TRUE, by = "resource.id")[, c("job.id", "resources"), with = FALSE]
  if (flatten %??% qtestr(tab$resources, c("v", "0"), depth = 2L)) {
    tab = rbindlist(.mapply(function(job.id, resources) c(list(job.id = job.id), resources), tab, list()), fill = TRUE)
    if (prefix && nrow(tab) >= 2L) {
      nn = setdiff(names(tab), "job.id")
      setnames(tab, nn, stri_join("res.", nn))
    }
  }
  setkeyv(tab, "job.id")[]
}


#' @export
#' @rdname getJobTable
getJobPars = function(ids = NULL, flatten = NULL, prefix = FALSE, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  assertFlag(flatten, null.ok = TRUE)
  assertFlag(prefix)
  UseMethod("getJobPars", object = reg)
}


#' @export
getJobPars.Registry = function(ids = NULL, flatten = NULL, prefix = FALSE, reg = getDefaultRegistry()) {
  ids = convertIds(reg, ids)
  tab = inner_join(reg$defs, inner_join(reg$status, ids))[, c("job.id", "pars"), with = FALSE]

  if (flatten %??% qtestr(tab$pars, c("v", "L"), depth = 2L)) {
    new.cols = rbindlist(tab$pars)
    if (ncol(new.cols) > 0L) {
      if (prefix)
        setnames(new.cols, names(new.cols), stri_join("par.", names(new.cols)))
      tab[, names(new.cols) := new.cols]
    }
    tab[, "pars" := NULL, with = FALSE]
  }

  setkeyv(tab, "job.id")[]
}


#' @export
getJobPars.ExperimentRegistry = function(ids = NULL, flatten = NULL, prefix = FALSE, reg = getDefaultRegistry()) {
  ids = convertIds(reg, ids)
  tab = inner_join(reg$defs, inner_join(reg$status, ids))[, c("job.id", "pars", "problem", "algorithm"), with = FALSE]

  if (flatten %??% qtestr(tab$pars, c("v", "L"), depth = 2L)) {
    new.cols = rbindlist(lapply(tab$pars, unlist, recursive = FALSE), fill = TRUE)
    if (ncol(new.cols) > 0L) {
      pattern = "^(prob|algo)\\.pars\\."
      replacement = if (prefix) "$1.par." else ""
      setnames(new.cols, names(new.cols), stri_replace_all_regex(names(new.cols), pattern, replacement))
      tab[, names(new.cols) := new.cols]
    }
    tab[, "pars" := NULL]
  }

  setkeyv(tab, "job.id")[]
}


#' @export
#' @rdname getJobTable
getJobTags = function(ids = NULL, reg = getDefaultRegistry()) {
  assertRegistry(reg)
  ids = convertIds(reg, ids, default = allids(reg))
  tag = NULL
  reg$tags[ids, on = "job.id"][, list(tags = stri_join(sort(tag, na.last = TRUE), collapse = ",")), by = "job.id"]
}