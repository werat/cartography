import sys

# MAP

for line in sys.stdin:
    for letter in line.strip():
        sys.stdout.write(letter+'\t\n')

