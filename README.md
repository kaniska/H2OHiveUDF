# H2OHiveUDF
### Predictive Intelligence using H2O inside Hive UDF

### Original Challenge 

https://github.com/h2oai/coding-challenges/tree/master/deployment/hive_udf

### Steps followed to verify H2O-R prediction matches with USF-Pojo prediction

#### Generate the POJO Model by running -  Rscript script.R

https://github.com/h2oai/coding-challenges/blob/master/deployment/hive_udf/script.R

** ensure h2o properly installed ** install.packages("h2o",repos="http://cran.cnr.berkeley.edu”)

** updated the script to predict on 10 columns for the ease of testing

#### Examine the content of frames generated by H2O-R script

head -2 fr

"response","C1","C2","C3","C4","C5","C6","C7","C8","C9","C10"

"0",0,79.00524687669414,1,62.34107756773965,94.72604293120712,-3.1683586213359094,37.19742854068111,-46.56359552425913,-62.62769638188415,-61

#### Look at the prediction data

head -2 pred

"predict", "p0", "p1"

"1", 0.48181779315157247, 0.5181822068484275

#### Now we create a table adult_data_set in Hive

i.  the H2O hrame csv data imported into the table using HDP-beeswax

#### Create the UDF to generate predictions from H2O Pojo

i. copy the generated pojo model jar into the UDF project's localjars folder

ii. copy the generated pojo model source into UDF project's localjars folder

iii. write the UDF to return the prediction values

iv. write test case to verify H2O prediction and UDF predictions are matching.

** In the testcase, the expected value derived from pred record as shown in #3

** Here we see the UDF-evaluted prediction is matching with expected values !

  Assert.assertEquals(1.0, ((Double)doublVals[0]).doubleValue());  - PASS
  
  Assert.assertEquals(0.4818177, doublVals[1],tolerance); - PASS
  
  Assert.assertEquals(0.51818, doublVals[2],tolerance); - PASS

#### Write Query to fetch predictions from UDF

select score(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) as preds FROM score_table

### Scope of Improvement

i. Did some RnD , but couldn't yet figure out how can I retirve the individual column values from the multi-valued list/array returned by the UDF.  Actually I need to display preds[0] , preds[1] , preds[3] and save into another table / csv
