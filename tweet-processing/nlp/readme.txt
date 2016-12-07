This folder contains a Gradle-managed Java project for lemmatizing
tweets and extracting named entities from them, taking advantage of
the HBase Java API and the Stanford NLP library. To build the
project, simply run 

'gradle build'

The result is a jar file that is necessary for the tweet
processing pipeline contained in the "pipeline" folder
in this project's parent.
