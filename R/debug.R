# # debugging code to pase into mapper, reducer if you're really desperate:
# 
# DEBUG = 1
#
# # [...] 
#
# if (DEBUG) {
# 	log = file('out/debug/mapper-debug.txt', open='at')
# 	cat("key:", file=log, sep='\n')
# 	dput(k, file=log)
# 	cat("fields:", file=log, sep='\n')
# 	dput(fields, file=log)
# 	close(log)
# }
#
# # [...]
#
#
# if (DEBUG) {
# 	log = file('out/debug/reducer-debug.txt', open='at')
# 	cat("key:", file=log, sep='\n')
# 	dput(k, file=log)
# 	cat("value.list:", file=log, sep='\n')
# 	dput(value.list, file=log)
# 	close(log)
# }
# 
