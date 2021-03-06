context("addAlgorithm")

test_that("addAlgorithm", {
  reg = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) NULL)
  expect_is(algo, "Algorithm")
  expect_equal(algo$name, "a1")
  expect_function(algo$fun)
  expect_file(getAlgorithmURI(reg, algo$name))

  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data) nrow(data))
  algo = addAlgorithm(reg = reg, "a2", fun = function(...) NULL)
  ids = addExperiments(list(p1 = data.table()), algo.designs = list(a1 = data.table(), a2 = data.table()), repls = 2, reg = reg)
  expect_integer(ids$job.id, len = 4L)

  removeAlgorithms(reg = reg, "a1")
  expect_integer(reg$status$job.id, len = 2L)
  expect_set_equal(levels(reg$defs$algorithm), "a2")
  expect_set_equal(getAlgorithmIds(reg), "a2")
  expect_false(file.exists(getAlgorithmURI(reg, "a1")))
  expect_true(file.exists(getAlgorithmURI(reg, "a2")))
  expect_set_equal(as.character(getJobPars(reg = reg)$algorithm), "a2")
  checkTables(reg)
})


test_that("addAlgorithm overwrites old algo", {
  reg = makeExperimentRegistry(file.dir = NA, make.default = FALSE)
  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data) 2)
  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) instance * 2)
  ids = addExperiments(list(p1 = data.table()), list(a1 = data.table()), reg = reg)
  run = function(id) suppressAll(execJob(makeJob(id, reg = reg)))

  expect_equal(run(1), 4)

  prob = addProblem(reg = reg, "p1", data = iris, fun = function(job, data) 4)
  expect_equal(run(1), 8)

  algo = addAlgorithm(reg = reg, "a1", fun = function(job, data, instance, ...) instance * 8)
  expect_equal(run(1), 32)
})
