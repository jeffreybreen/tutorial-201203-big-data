
library(reshape2)
library(ggplot2)
library(doBy)

print(load('out/enroute.times.every8.RData'))

actuals.df = dcast(enroute.times.every8, market~year, value.var='actual')
actuals.df = subset(actuals.df, !is.na(`1988`) & !is.na(`1998`) & !is.na(`2008`))

actuals.df$delta = with(actuals.df, `2008` - `1988`)
actuals.df$delta.pct = with(actuals.df, delta / `1988`)
actuals.df = orderBy(~-delta.pct, actuals.df)
