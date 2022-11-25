import subprocess
from pathlib import Path
import os
import signal
import time
import shutil
import random

server_process = None

def test_error(msg):
    # Kill server process
    global server_process
    print("Test error, killing server process")
    os.killpg(os.getpgid(server_process.pid), signal.SIGTERM)
    raise Exception(msg)

def sync_folder(local_dir, wait=True):
    global server_process
    cmd_client = "exec go run cmd/SurfstoreClientExec/main.go -d localhost:8081 {} 4".format(local_dir)
    client_process = subprocess.Popen(cmd_client, stdout=subprocess.PIPE, shell=True)
    if wait:
        try:
            client_process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            client_process.kill()
            test_error(server_process, "Client command timeout")


if __name__ == '__main__':
    # Run rpc server
    cmd = "exec go run cmd/SurfstoreServerExec/main.go -s both -p 8081 -l localhost:8081"
    server_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, preexec_fn=os.setsid)

    # Wait for server to start
    print("Waiting for server to start...")
    time.sleep(3)

    for i in range(0, 10):
        print("Starting client {}".format(i))

        # Create local directory
        local_dir = "data{}/".format(i)
        local_file = local_dir + "file{}".format(i)
        if Path(local_dir).exists():
            shutil.rmtree(Path(local_dir))
        Path(local_dir).mkdir(parents=True, exist_ok=True)
        
        # Run client command
        sync_folder(local_dir=local_dir)
        # Create file
        local_file = open(local_file, "w+")
        content = "This is data from client {}\n".format(i)
        local_file.write(content)
        local_file.close()

        # Run client command again
        sync_folder(local_dir=local_dir)
    
    print("Start adding test")
    for i in range(0, 10):
        # Run client command
        # Read file and check if they are correct
        local_dir = "data{}/".format(i)
        sync_folder(local_dir=local_dir)
        
        for j in range(0, 10):
            local_file = local_dir + "file{}".format(j)
            if os.path.exists(local_file) == False:
                test_error("File {} does not exist".format(local_file))
            local_file_handler = open(local_file, "r")
            data = local_file_handler.read()
            content = "This is data from client {}\n".format(j)
            if data != content:
                test_error("Wrong content on {}".format(local_file))
    
    print("Start Updating test")
    updated = {}
    for i in range(0, 10):
        updated[i] = []
    
    for i in range(0, 10):
        local_dir = "data{}/".format(i)
        sync_folder(local_dir=local_dir)
        for j in range(0, 10):
            num = random.randint(0, 1)
            if num == 0:
                local_file = local_dir + "file{}".format(j)
                if os.path.exists(local_file) == False:
                    test_error("File {} does not exist".format(local_file))
                local_file_handler = open(local_file, "a+")
                content = "Update from client {}\n".format(i)
                local_file_handler.write(content)
                local_file_handler.close()
                updated[j].append(i)
        sync_folder(local_dir=local_dir)
    
    # Check if all updating has been completed
    for i in range(0, 10):
        local_dir = "data{}/".format(i)
        sync_folder(local_dir=local_dir)
        for j in range(0, 10):
            local_file = local_dir + "file{}".format(j)
            if os.path.exists(local_file) == False:
                test_error("File {} does not exist".format(local_file))
            local_file_handler = open(local_file, "r")
            data = local_file_handler.read()
            for k in updated[j]:
                content = "Update from client {}\n".format(k)
                if content not in data:
                    test_error("Update from {} doesn't in {}".format(k, local_file))
    

    print("Start conflicting test")
    conflicting_index = random.randint(0, 9)
    for i in range(0, 10):
        local_dir = "data{}/".format(i)
        conflict_file = local_dir + "file{}".format(conflicting_index)
        if os.path.exists(local_file) == False:
            test_error("File {} does not exist".format(local_file))
        conflict_file_handler = open(conflict_file, "a+")
        content = "Conflicting update from client {}\n".format(i)
        conflict_file_handler.write(content)
        conflict_file_handler.close()
    
    for i in range(0, 10):
        local_dir = "data{}/".format(i)
        sync_folder(local_dir=local_dir, wait=False)
    
    print("Waiting for all client finished sync")
    time.sleep(5)
    print("Checking all conflict file are same")
    same_line = None
    for i in range(0, 10):
        local_dir = "data{}/".format(i)
        conflict_file = local_dir + "file{}".format(conflicting_index)
        conflict_file_handler = open(conflict_file, "r")
        results = conflict_file_handler.readlines()
        found = False
        for line in results:
            if line.startswith("Conflicting update from client"):
                if same_line is None:
                    same_line = line
                elif same_line != line:
                    test_error("Inconsistent conflict line in {}".format(conflict_file))
                found = True
        if found == False:
            test_error("Cant find conflict line from file {}".format(conflict_file))
        conflict_file_handler.close()


    print("Start deleting test")
    for i in range(0, 10):
        # Remove a file and sync again
        local_dir = "data{}/".format(i)
        local_file = local_dir + "file{}".format(i)
        if os.path.exists(local_file) == False:
            test_error("File {} does not exist".format(local_file))
        os.remove(local_file)
        
        sync_folder(local_dir=local_dir)
        
        for j in range(0, i):
            local_file2 = local_dir + "file{}".format(j)
            if os.path.exists(local_file2):
                test_error("File {} exists".format(local_file2))



    # Kill server process
    print("Test finished successfully, killing server process")
    os.killpg(os.getpgid(server_process.pid), signal.SIGTERM)
