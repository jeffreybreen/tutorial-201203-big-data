
library(reshape2)
library(ggplot2)
library(doBy)

print(load('out/enroute.time.RData'))

results.df$year = as.numeric(results.df$year) + 1986
head(results.df)

nrow(results.df)

yearly.mean = ddply(results.df, c('year'), summarise,
						scheduled = weighted.mean(scheduled, flights),
						actual = weighted.mean(actual, flights),
						in.air = weighted.mean(in.air, flights),
					.progress='text')

# yearly.mean$year = as.numeric(yearly.mean$year)
g = ggplot(yearly.mean) + geom_line(aes(x=year, y=scheduled), color='#CCCC33') +
	geom_line(aes(x=year, y=actual), color='#FF9900') + 
	geom_line(aes(x=year, y=in.air), color='#4689cc') + theme_bw() +
	ylim(c(60, 130)) + ylab('minutes')
print(g)
