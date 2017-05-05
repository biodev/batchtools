# Biodev fork of batchools

**Note this is still experimental at this time**

## Installation

```
library(devtools)
install_github("biodev/batchtools")

```

## Basic Usage

```
library(batchtools)

reg = makeRegistry(file.dir = "registry", seed = 1)

reg$cluster.functions <- makeClusterFunctionsCondor()

#per the batchtools example

piApprox = function(n) {
  nums = matrix(runif(2 * n), ncol = 2)
  d = sqrt(nums[, 1]^2 + nums[, 2]^2)
  4 * mean(d <= 1)
}

dir.create("test")

#walltime here maps to the MaxExecutionTime parameter and is in seconds
#out.dir will contain the stdout and stderr values from the run while the condor logs will be in (for this example) registry/logs/*.log

cur.resources <- list(walltime = 3600, memory = "1G", cpus=1, universe="Vanilla", R.bin.path="/usr/bin/R", out.dir="test")

```
Either manage jobs manually
```

batchMap(fun = piApprox, n = rep(1e5, 10))
submitJobs(resources = cur.resources)
waitForJobs()
reduceResults(function(x, y) x + y) / 10

```
Or take the simpler route:

```
temp <- btlapply(rep(1e5, 10), piApprox, reg=reg, resources=cur.resources)

Reduce(function(x, y) x + y , temp) / 10

```


