#!/usr/bin/env Rscript

# Calculate average departure delays by year and month for each airline in the
# airline data set (http://stat-computing.org/dataexpo/2009/the-data.html).
# Requires rmr package (https://github.com/RevolutionAnalytics/RHadoop/wiki).

DEBUG=F

library(rmr)

source('R/functions.R')

if (DEBUG)
{
	# use rmr's local backend (doesn't use Hadoop at all -- pretty cool feature!)
	rmr.options.set(backend='local')

	hdfs.input.path = 'data/asa-airline/20040325.csv'
	hdfs.output.root = 'out/debug/enroute-time'
	
} else {
	hdfs.input.path = 'asa-airline/data'
	hdfs.output.root = 'asa-airline/out'
}

mr.year.market.enroute_time = function (input, output) {
	mapreduce(input = input,
			  output = output,
			  input.format = asa.csvtextinputformat,
			  map = mapper.year.market.enroute_time,
			  reduce = reducer.year.market.enroute_time,
			  backend.parameters = list( 
			  				hadoop = list(D = "mapred.reduce.tasks=10") 
			  				),
			  verbose=T)
}

hdfs.output.path = file.path(hdfs.output.root, 'enroute-time')
results = mr.year.market.enroute_time(hdfs.input.path, hdfs.output.path)

results.df = from.dfs(results, to.data.frame=T)
colnames(results.df) = c('year', 'market', 'flights', 'scheduled', 'actual', 'in.air')

save(results.df, file="out/enroute.time.RData")
