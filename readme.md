1. Get into scripts folder run streamListener.py to stream Tweets as following
	python streamListener.py -d directoryName
	
	or if we want to stream Tweets filter by tags

	python streamListener.py -d directoryName -q hashtags

2. Process streamed raw tweets to creat docs for Spark App. Run the script auto.sh as following:
	./auto.sh file_Name_Of_Raw_Tweets
	it will create a file named doc containing processed strings.

3. In Spark Application there is a JAVA Netbeans project. find the Jar under dist folder under the TwitterTopicModelling folder.

4. Run the jar in the Spark Environment as following.
	./bin/spark-submit --class twittertopicmodelling.TwitterTopicModelling JarFileName NumOfTopics TermsPerTopic InputFileName BatchSize

5. This will create the output and store it in hadoop file system for each batch. There is also a variant of this application available for non-hadoop environment under non-hadoop folder.We can run that as same as above command it will create outputs under ldaData directory inside the output folder.
