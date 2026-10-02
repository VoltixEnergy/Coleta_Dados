import subprocess

output = subprocess.check_output("docker ps", shell=True).decode("utf-8")
print(len(output.split("\n")) -2)