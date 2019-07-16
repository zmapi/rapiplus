import tcache
import os
import subprocess
import argparse
import sys
import tempfile
from time import sleep

SCRIPT_PATH = os.path.realpath(__file__)
SCRIPT_DIR = os.path.basename(SCRIPT_PATH)
NUM_WRITERS = 5

def run_writer(args):
    cache_fn = args["cache_fn"]
    for i in range(100):
        with tcache.open(cache_fn) as db:
            db[str(i)] = i

def force_kill_child_processes(processes):
    for p in processes:
        p.kill()

def run_reader(cache_fn):
    last_num_keys = 0
    count_stagnant = 0
    while True:
        try:
            with tcache.open(cache_fn, "r") as db:
                num_keys = len(list(db.keys()))
                print("\r{}".format(num_keys), flush=True, end="")
                if num_keys == last_num_keys:
                    count_stagnant += 1
                else:
                    count_stagnant = 0
                last_num_keys = num_keys
        except FileNotFoundError:
            last_num_keys = 0
            count_stagnant += 1
        if count_stagnant >= 20:
            break
        sleep(0.1)
    print()

def run_master():
    cache_fn = tempfile.mktemp(prefix="tcache_test_", suffix=".db")
    print("Test db filename: {}".format(cache_fn))
    children = []
    for i in range(NUM_WRITERS):
        cmd = ["python3", SCRIPT_PATH, "writer", cache_fn]
        p = subprocess.Popen(cmd)
        children.append(p)
    print("Launched {} subprocesses".format(len(children)))
    try:
        run_reader(cache_fn)
    except KeyboardInterrupt:
        print("Interrupted")
        exit(1)
    print("Waiting for children to finish...")
    for child in children:
        try:
            child.wait()
        except KeyboardInterrupt:
            print("Interrupted, force kill children processes")
            force_kill_child_processes(children)
            exit(1)

def get_action():
    if len(sys.argv) == 1:
        return "master", {}
    if len(sys.argv) > 1:
        return sys.argv[1], {"cache_fn": sys.argv[2]}

def main():
    action, args = get_action()
    if not action:
        print("Invalid count of arguments", file=sys.stderr)
        sys.exit(1)
    if action == "master":
        run_master()
    elif action == "writer":
        run_writer(args)
    else:
        print("Invalid action: {}".format(action), file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
