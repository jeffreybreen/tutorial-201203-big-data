#!/usr/bin/env Rscript

# Calculate average departure delays by year and month for each airline in the
# airline data set (http://stat-computing.org/dataexpo/2009/the-data.html).
# Requires rmr package (https://github.com/RevolutionAnalytics/RHadoop/wiki).

library(rmr)

hdfs.input.path = 'data/airline-data/20040325.csv'
hdfs.output.path = '/tmp/enroute-time-local'

rmr.options.set(backend='local')

mapper.year.market.enroute_time = function(k, fields) {
	# Skip header lines, cancellations, and diversions:
	if (!(identical(fields$Year, "Year")) & 
		fields$Cancelled==0 & fields$Diverted==0 ) {		 	
		
		# We don't care about direction of travel, so convert LAX-JFK to JFK-LAX
		# sort alphabetically)		
		if (fields$Origin < fields$Dest)
			market = paste(fields$Origin, fields$Dest, sep='-')
		else
			market = paste(fields$Dest, fields$Origin, sep='-')
		
		# key consists of year, market
		k = c(fields$Year, market)
		
		keyval(k, fields$ActualElapsedTime )
		
	}
}

mapper.year.carrier.market.enroute_time = function(k, fields) {
	# the make.input.format() reader treats the first field as the key
	# whether you want that or not:
	fields$Year = k
	
	# Skip header lines, cancellations, and diversions:
	if (!(identical(fields$Year, "Year")) & 
			fields$Cancelled==0 & fields$Diverted==0 ) {		 	

		# We don't care about direction of travel, so convert LAX-JFK to JFK-LAX
		# sort alphabetically)		
		if (fields$Origin < fields$Dest)
			market = paste(fields$Origin, fields$Dest, sep='-')
		else
			market = paste(fields$Dest, fields$Origin, sep='-')
		
		# key consists of year, carrier, market
		k = c(fields$Year, fields$UniqueCarrier, market)

		keyval(k, c(fields$CRSElapsedTime, fields$ActualElapsedTime, fields$AirTime) )
#		keyval(k, fields$ActualElapsedTime )
		
		}
}
	
mapper.jseidmam  = function(k, fields) {
	# Skip header lines and bad records:
	if (!(identical(fields[[1]], "Year")) & length(fields) == 29) {
		deptDelay <- fields[[16]]
		# Skip records where departure dalay is "NA":
		if (!(identical(deptDelay, "NA"))) {
			# field[9] is carrier, field[1] is year, field[2] is month:
			keyval(c(fields[[9]], fields[[1]], fields[[2]]), deptDelay)
		}
	}
}

# incoming key = Year, UniqueCarrier, market
reducer.year.carrier.market.enroute_time = function(k, value.list) {
#	print("key:")
#	print(str(k))
#	print("value.list:")
#	print( str(value.list) )
#	keyval(key[[2]], c(keySplit[[3]], length(vv), keySplit[[1]], mean(as.numeric(vv))) )

	if ( require(plyr) )	
		value.df = ldply(value.list)
	else
		value.df = as.data.frame( value.list, as.data.frame )
	colnames(value.df) = c('actual','crs','air')
		
	keyval(k, c( nrow(value.df), mean(value.df$actual), mean(value.df$crs), mean(value.df$air) ))
}

reducer.year.market.enroute_time = function(keySplit, vv) {
	keyval(keySplit[[2]], c(length(vv), mean(as.numeric(vv))) )
}

reducer.jseidman = function(keySplit, vv) {
	keyval(keySplit[[2]], c(keySplit[[3]], length(vv), keySplit[[1]], mean(as.numeric(vv))) )
}


# from jseidman, wrapped in a list for rmr-1.2's new I/O model:
csvtextinputformat = list(mode = 'text', format = function(line) {
	keyval(NULL, unlist(strsplit(line, "\\,")))
}, streaming.format=NULL)

csv.input.format = make.input.format('csv', mode='text')

# make.input.format assumes first field is key -- any way to stop that?
col.names=c('Year','Month','DayofMonth','DayOfWeek','DepTime','CRSDepTime',
			'ArrTime','CRSArrTime','UniqueCarrier','FlightNum','TailNum',
			'ActualElapsedTime','CRSElapsedTime','AirTime','ArrDelay',
			'DepDelay','Origin','Dest','Distance','TaxiIn','TaxiOut',
			'Cancelled','CancellationCode','Diverted','CarrierDelay',
			'WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay')

asa.csv.input.format = make.input.format('csv', 'text', sep=',',
										  col.names=col.names,
										  stringsAsFactors=F)

# sample.line = '2004,3,25,4,1118,1125,1231,1236,UA,425,N840UA,73,71,54,-5,-7,DEN,ABQ,349,3,16,0,,0,0,0,0,0,0'
# l = asa.csv.input.format$format(sample.line)
# mapper.year.carrier.market.enroute_time(l$key, l$val)


mr.enroute_time = function (input, output) {
	mapreduce(input = input,
			  output = output,
			  input.format = asa.csv.input.format,
			  map = mapper.year.carrier.market.enroute_time,
			  reduce = reducer.year.carrier.market.enroute_time,
			  verbose=T)
}

df = from.dfs(mr.enroute_time(hdfs.input.path, hdfs.output.path), to.data.frame=T)
# print(df)
