
rm -Rf bycategory
rm -Rf bydistrict
rm -Rf star

bin/hadoop jar sfcrime.hadoop.mapreduce.jobs-0.0.1-SNAPSHOT.jar com.dynamicalsoftware.hadoop.mapreduce.SanFranciscoCrime sfcrime.csv bycategory bydistrict

bin/hadoop jar sfcrime.hadoop.mapreduce.jobs-0.0.1-SNAPSHOT.jar com.dynamicalsoftware.hadoop.mapreduce.SanFranciscoCrimePrepOlap bycategory/part-00000 bydistrict/part-00000 sfcrime.csv star

