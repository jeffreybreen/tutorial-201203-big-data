# count the number of lines of code it took jseidman to implement his monthly
# departure delay script using each Hadoop package for R
#
# see https://github.com/jseidman/hadoop-R for code
#
# by Jeffrey Breen <jeffrey@atmosgrp.com> because, yes, that's how I judge APIs
#                                                            (at least in part)
#

jseidman.path = 'github-projects/hadoop-R/airline/src/deptdelay_by_month/R'

reverse.grep = '/usr/bin/egrep -v'
count.lines = '/usr/bin/wc -l'

# match lines with nothing but whitespace or which begin with a comment char (#)
pattern = "'(^\\s*$)|(^#)'"

packages = c('streaming', 'rhipe', 'hive', 'rmr')
packages.src = list(streaming = c('map.R', 'reduce.R'),
					rhipe = 'rhipe.R',
					hive = 'hive.R',
					rmr = 'deptdelay-rmr.R')

api.lines = data.frame()

for (package in packages) {

	src.files = file.path(jseidman.path, package, packages.src[[package]])
	src.files = paste(src.files, collapse=' ')

	cmd.line = paste(reverse.grep, pattern, src.files, '|', count.lines)

	print(cmd.line)

	cmd.pipe = pipe(cmd.line)
	results = as.numeric(readLines(cmd.pipe))
	close(cmd.pipe)
	print(results)
	
	results.df = data.frame(package=package, lines=as.numeric(results))

	api.lines = rbind(api.lines, results.df)
	}

library(ggplot2)
g = ggplot(api.lines, aes(x=package, y=lines)) + geom_bar(fill='#4689cc') +
	geom_bar(data=subset(api.lines, lines==min(lines)), fill='#FF6600') + theme_bw()

print(g)
