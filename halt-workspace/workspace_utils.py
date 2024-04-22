import sys

def print_header(title):
    print(f"\n{'='*len(title)}\n{title}\n{'='*len(title)}")


def check_errors(response):
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        sys.exit()
