filename = raw_input('Input file name:\n')
# like format like F:/usermodel/lsb/lsb.acct.20171127
infile = open(filename, 'r')
# record the job name
counter = 0
for line in infile:
    counter += 1
    if counter < 19540:
        continue
    else:
        print line
        input()
