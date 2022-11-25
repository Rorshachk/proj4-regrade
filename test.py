import subprocess
from pathlib import Path
import os
import signal


if __name__ == '__main__':
    # Run rpc server
    cmd = "exec go run cmd/SurfstoreServerExec/main.go -s both -p 8081 -l localhost:8081"
    server_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, preexec_fn=os.setsid)

    for i in range(0, 10):
        print("Starting client {}".format(i))

        # Create local directory
        local_dir = "data{}/".format(i)
        local_file = local_dir + "file{}".format(i)
        Path(local_dir).mkdir(parents=True, exist_ok=True)

        # Create file
        local_file = open(local_file, "w+")
        content = "This is data from client {}".format(i)
        local_file.write(content)
        local_file.close()

        # Run client command
        cmd_client = "exec go run cmd/SurfstoreClientExec/main.go localhost:8081 {} 4".format(local_dir)
        client_process = subprocess.Popen(cmd_client, stdout=subprocess.PIPE, shell=True)
        try:
            client_process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            client_process.kill()
    
    for i in range(0, 10):
        # Run client command
        cmd_client = "exec go run cmd/SurfstoreClientExec/main.go localhost:8081 {} 4".format(local_dir)
        client_process = subprocess.Popen(cmd_client, stdout=subprocess.PIPE, shell=True)
        try:
            client_process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            client_process.kill()
        
        # Read file and check if they are correct
        local_dir = "data{}/".format(i)
        for j in range(0, 10):
            local_file = local_dir + "file{}".format(j)
            local_file_handler = open(local_file, "r")
            data = local_file_handler.read()
            content = "This is data from client {}".format(j)
            if data != content:
                raise Exception("Wrong content on {}".format(local_file))
        


    # Kill server process
    print("Test finished, killing server process")
    os.killpg(os.getpgid(server_process.pid), signal.SIGTERM)
