
# simple tool for running a splunk search

java -server -cp jars/splunk.jar:libs/farrago.jar com.splunk.search.Connection "$@"
