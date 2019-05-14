Pre-Requisite:
Installation of SBT, Scala, Spark
sparkling-water-2.4.10

Overview:
This application runs a GBM model with parameters ntrees = 200 and rest default. It predict on a provided test data set and
print first 10 lines of the prediction file in the terminal.
This also contains gbm grid search on learn_rate = 0.1,0.01,0.001.

Command to run:
"sbt run" to run the code

first 10 lines of the prediction file in the terminal.
0.6925156155189716
0.7199719854866662
0.38573828968179713
0.5545662392656259
0.5235896627399943
0.6721743608449633
0.5756094674235901
0.5919115071102969
0.6026093725469654
0.5756094674235901

This application save the prediction file under directory "src/main/resources/prediction_result".
