import sys

if __name__ == '__main__':
    with open("json.ndjson") as f:
        for line in sys.stdin.readlines():
            f.write(line)