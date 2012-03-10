test.input = '2004,3,25,4,1445,1437,1820,1812,AA,399,N275AA,215,215,197,8,8,BOS,MIA,1258,6,12,0,,0,0,0,0,0,0'

print("Input:")
print(test.input)

test.formatted.jseidman = csvtextinputformat$format(test.input)
test.formatted = asa.csvtextinputformat$format(test.input)

print("Formatted:")
print(dput(test.formatted))

test.mapped = mapper.year.market.enroute_time(test.formatted$key, test.formatted$val)

print("Mapped:")
print(dput(test.mapped))

test.reducer.input.key = c("2004", "BOS-MIA")
test.reducer.input.values = list(c("215", "215", "197"), c("187", "195", "170"), 
								   c("198", "198", "168"), c("199", "199", "165"),
								   c("204", "182", "157"), c("219", "227", "176"), 
								   c("206", "178", "158"), c("216", "202", "180"), 
								   c("203", "203", "173"), c("207", "175", "161"), 
								   c("187", "193", "163"), c("194", "221", "196"))
test.reduced = reducer.year.market.enroute_time(test.reducer.input.key, test.reducer.input.values)

print("Reduced:")
print( dput(test.reduced) )
