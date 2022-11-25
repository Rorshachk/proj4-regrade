import subprocess
from pathlib import Path
import os
import signal
import time
import shutil

def test_error(server_process, msg):
    # Kill server process
    print("Test error, killing server process")
    os.killpg(os.getpgid(server_process.pid), signal.SIGTERM)
    raise Exception(msg)

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
        cmd_client = "exec go run cmd/SurfstoreClientExec/main.go localhost:8081 {} 4".format(local_dir)
        client_process = subprocess.Popen(cmd_client, stdout=subprocess.PIPE, shell=True)
        try:
            client_process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            client_process.kill()

        # Create file
        local_file = open(local_file, "w+")
        content = "This is data from client {}".format(i)
        local_file.write(content)
        local_file.close()

        # Run client command again
        client_process = subprocess.Popen(cmd_client, stdout=subprocess.PIPE, shell=True)
        try:
            client_process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            client_process.kill()
    
    print("Start adding test")
    for i in range(0, 10):
        # Run client command
        # Read file and check if they are correct
        local_dir = "data{}/".format(i)
        cmd_client = "exec go run cmd/SurfstoreClientExec/main.go localhost:8081 {} 4".format(local_dir)
        client_process = subprocess.Popen(cmd_client, stdout=subprocess.PIPE, shell=True)
        try:
            client_process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            client_process.kill()
            test_error(server_process, "Client command timeout")
        
        for j in range(0, 10):
            local_file = local_dir + "file{}".format(j)
            if os.path.exists(local_file) == False:
                test_error(server_process, "File {} does not exist".format(local_file))
            local_file_handler = open(local_file, "r")
            data = local_file_handler.read()
            content = "This is data from client {}".format(j)
            if data != content:
                test_error(server_process, "Wrong content on {}".format(local_file))
        
    print("Start deleting test")
    for i in range(0, 10):
        # Remove a file and sync again
        local_dir = "data{}/".format(i)
        local_file = local_dir + "file{}".format(i)
        if os.path.exists(local_file) == False:
            test_error(server_process, "File {} does not exist".format(local_file))
        os.remove(local_file)
        
        cmd_client = "exec go run cmd/SurfstoreClientExec/main.go localhost:8081 {} 4".format(local_dir)
        client_process = subprocess.Popen(cmd_client, stdout=subprocess.PIPE, shell=True)
        try:
            client_process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            client_process.kill()
            test_error(server_process, "Client command timeout")
        
        for j in range(0, i):
            local_file2 = local_dir + "file{}".format(j)
            if os.path.exists(local_file2):
                test_error(server_process, "File {} exists".format(local_file2))

    print("Start conflicting test")


    # Kill server process
    print("Test finished successfully, killing server process")
    os.killpg(os.getpgid(server_process.pid), signal.SIGTERM)
