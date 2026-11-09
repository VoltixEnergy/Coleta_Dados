import subprocess

def get_containers_name():
  docker_ps_query = None

  try:
    docker_ps_query = subprocess.check_output(
        "docker ps -a",
        shell=True
    ).decode("utf-8")
  except:
    print("Erro ao executar: docker ps -a")
    return []

  containers_info = docker_ps_query.split("\n")
  containers_info = containers_info[1 : len(containers_info) -1]

  container_names = []
  for info in containers_info:
    info_splited = info.split(" ")
    container_name = info_splited[len(info_splited) -1]
    container_names.append(container_name)

  return container_names

def get_container_logs(names: list, since: str):
  container_logs = {}
  
  try:
    for name in names:
      query = subprocess.check_output(
          f"docker logs --since {since} {name}",
          shell=True
      ).decode("utf-8")
      query_splited = query.split("\n")
      log_lines = query_splited[1 : len(query_splited) -1]

      container_logs[name] = log_lines
  except:
    print("Erro ao executar: docker logs --since...")

  return container_logs

def transform_logs_to_csv_text(logs: dict):
  csv_output = "container_name;level;ts;scope;message;stacktrace\n"
  for container_name, logs in logs.items():
    for log in logs:
      log_splited = None

      try:
        log_splited = log.split(" | ")
      except:
        pass

      if log_splited[0] in ["INFO", "WARN"]:
        csv_output += "\n"
        log_level = log_splited[0] + ";"
        log_timestamp = log_splited[1] + ";"
        log_scope = log_splited[2] + ";"
        log_message = log_splited[3] + ";"
        csv_output += container_name + ";" + log_level + log_timestamp + log_scope + log_message
        continue
      if log_splited[0] == "ERROR":
        csv_output += "\n"
        log_level = log_splited[0] + ";"
        log_timestamp = log_splited[1] + ";"
        log_scope = log_splited[2] + ";"
        log_message = log_splited[3] + ";"
        csv_output += container_name + ";" + log_level + log_timestamp + log_scope + log_message
        continue
      
      csv_output += log
      
    csv_output += "\n"
  return csv_output

def main(since: str = "5m"):
  containers_name = get_containers_name()
  logs = get_container_logs(containers_name, since)
  csv_text = transform_logs_to_csv_text(logs)

  return csv_text