filename ='out.txt'
with open(filename) as f:
	content = f.readlines()
	print len(content)
