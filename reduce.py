import sys

# REDUCE

hist = {}

for line in sys.stdin:
    ch = line.strip()
    hist[ch] = hist.get(ch, 0) + 1

for ch in hist:
    sys.stdout.write(str(hist[ch]) + '\t' + ch + '\n')
