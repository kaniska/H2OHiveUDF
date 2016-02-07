library(h2o)
h2o.init(nthreads = -1)

# Specify the number of rows for your experiment
numrows <- 100000
numcols <- 10
fr <- h2o.createFrame(rows = numrows, cols = numcols,
                      categorical_fraction = 0.01, 
                      response_factors = 2, has_response = TRUE,
                      seed = 999)

gbm_m <- h2o.gbm(training_frame = fr, x = 2:(numcols+1), y = 1,
                 distribution = "bernoulli", model_id = "GBM_Model")

# Score in H2O to compare scoring as a POJO and scoring in Hive
pred <- h2o.predict(object = gbm_m, newdata = fr)

# Download the frame or at least a significant part of the frame to score in H2O
h2o.exportFile(data = fr, path = "fr", force = T)

# Download or export the predictions for comparison with predictions from hive scoring
h2o.exportFile(data = pred, path = "pred", force = T)

# Download the POJO and h2o-genmodel.jar
if (! file.exists("pojo")) {
  dir.create("pojo")
}
h2o.download_pojo(model = gbm_m, path = "pojo", getjar = T)