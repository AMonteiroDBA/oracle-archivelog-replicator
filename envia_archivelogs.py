#!/usr/bin/env python3

import os
import re
import subprocess
import logging
import datetime
import time
import sys
import fcntl
import signal
from concurrent.futures import ThreadPoolExecutor

# ===== Configuraes Principais =====
SISDBAHOME = os.path.expanduser("~alkdba")
os.makedirs(os.path.join(SISDBAHOME, "log"), exist_ok=True)
os.makedirs(os.path.join(SISDBAHOME, "tmp"), exist_ok=True)

SOURCEBASEDIR = "/u02/flash_recovery_area/DBPROD/archivelog"
DESTBASEDIR = "/u12/flash_recovery_area/DBPROD/archivelog"
DESTBASEDIR_U15 = "/u15/flash_recovery_area/DBPROD/archivelog"

DRSERVER = "PAMVS0003L"
SSHUSER = "oracle"

LOGFILE = os.path.join(SISDBAHOME, "log", "archivelog_replication.log")
LOCKFILE = os.path.join(SISDBAHOME, "tmp", "archivelog_replication.lock")

LOCKTIMEOUTMINUTES = 30
MAXWORKERS = 5

ARCHIVELOGPATTERN = re.compile(r"o1_mf_1_(\d+)_.*\.arc")
DATEDIRPATTERN = re.compile(r"\d{4}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])")

# ===== NOVO: Filtro opcional por faixa de sequncia =====
SEQ_FILTER_MIN = None  # Ex.: 198000 para comear daqui
SEQ_FILTER_MAX = None  # Ex.: 198099 para terminar aqui (None = sem limite)

TITLE = "--- Configuraes Principais ---"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=LOGFILE,
    filemode="a"
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)

def is_process_running(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False

def acquire_lock():
    current_pid = os.getpid()
    try:
        lock_fd = open(LOCKFILE, "a")
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.seek(0)
        content = lock_fd.read().strip()
        if content:
            try:
                old_pid, old_timestamp_str = content.split()
                old_pid = int(old_pid)
                old_timestamp = datetime.datetime.fromtimestamp(float(old_timestamp_str))
                logging.warning(f"Lock file encontrado (PID {old_pid}). Limpando...")
            except ValueError:
                logging.warning(f"Lock file corrompido. Limpando.")
        lock_fd.seek(0)
        lock_fd.truncate()
        lock_fd.write(f"{current_pid} {time.time()}")
        lock_fd.flush()
        os.fsync(lock_fd.fileno())
        logging.info(f"Lock adquirido. PID {current_pid}")
        return lock_fd
    except BlockingIOError:
        try:
            with open(LOCKFILE, "r") as f:
                content = f.read().strip()
            if not content:
                logging.error("Lock file vazio. Saindo.")
                return None
            old_pid, old_timestamp_str = content.split()
            old_pid = int(old_pid)
            old_timestamp = datetime.datetime.fromtimestamp(float(old_timestamp_str))
            if is_process_running(old_pid):
                time_elapsed = (datetime.datetime.now() - old_timestamp).total_seconds() / 60
                if time_elapsed < LOCKTIMEOUTMINUTES:
                    logging.info(f"Processo {old_pid} em execuo ({time_elapsed:.2f} min). Saindo.")
                    return None
                else:
                    logging.warning(f"Processo {old_pid} excedeu timeout. Tentando encerrar...")
                    try:
                        os.kill(old_pid, signal.SIGTERM)
                        time.sleep(5)
                        if is_process_running(old_pid):
                            os.kill(old_pid, signal.SIGKILL)
                            time.sleep(2)
                        if is_process_running(old_pid):
                            logging.error(f"No foi possvel encerrar {old_pid}. Saindo.")
                            return None
                        logging.info(f"Processo {old_pid} encerrado.")
                        os.remove(LOCKFILE)
                        return acquire_lock()
                    except (ProcessLookupError, Exception) as e:
                        logging.error(f"Erro ao encerrar: {e}")
                        return None
            else:
                logging.warning(f"Lock {old_pid} inativo. Removendo.")
                os.remove(LOCKFILE)
                return acquire_lock()
        except Exception as e:
            logging.error(f"Erro ao verificar lock: {e}")
            return None
    except Exception as e:
        logging.critical(f"Erro FATAL no lock: {e}", exc_info=True)
        return None

def release_lock(lock_fd):
    try:
        if lock_fd:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()
            os.remove(LOCKFILE)
            logging.info("Lock liberado.")
    except Exception as e:
        logging.error(f"Erro ao liberar lock: {e}")

def get_archivelog_directories(base_dir):
    directories = []
    try:
        logging.info(f"Buscando diretrios em {base_dir}...")
        for item in os.listdir(base_dir):
            full_path = os.path.join(base_dir, item)
            if os.path.isdir(full_path) and DATEDIRPATTERN.match(item):
                directories.append(full_path)
        directories.sort()
        logging.info(f"Diretrios encontrados: {len(directories)}")
        return directories
    except FileNotFoundError:
        logging.error(f"Caminho {base_dir} no encontrado.")
        return []
    except Exception as e:
        logging.error(f"Erro ao listar: {e}")
        return []

def get_archivelog_files(directory):
    files = []
    try:
        for filename in os.listdir(directory):
            match = ARCHIVELOGPATTERN.match(filename)
            if match:
                sequence_number = int(match.group(1))
                full_path = os.path.join(directory, filename)
                files.append((full_path, sequence_number))
        files.sort(key=lambda x: x[1])
        return files
    except Exception as e:
        logging.error(f"Erro ao listar {directory}: {e}")
        return []

def get_highest_applied_sequence_on_dr():
    logging.info(f"Consultando sequence no DR ({DRSERVER})...")
    sql_command = """source ~/.bashprofile; sqlplus -s / as sysdba <<EOF
set feed off term off head off pages 0 trim on trimspool on
SELECT LPAD(NVL(MAX(SEQUENCE#), 0), 10) FROM V\$LOG WHERE ARCHIVED='YES';
EOF
"""
    ssh_command = ["ssh", f"{SSHUSER}@{DRSERVER}", sql_command]
    try:
        result = subprocess.run(ssh_command, check=True, capture_output=True, text=True, timeout=60)
        output = result.stdout.strip()
        highest_sequence = int(output)
        logging.info(f"Maior sequence no DR: {highest_sequence}")
        return highest_sequence
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, ValueError) as e:
        logging.error(f"Erro ao consultar DR: {e}")
        return 0

def file_exists_on_dr(filename):
    """Verifica se o arquivo j existe em /u12 OU /u15 no DR."""
    ssh_command = [
        "ssh", f"{SSHUSER}@{DRSERVER}",
        f"[ -f {DESTBASEDIR}/{filename} ] || [ -f {DESTBASEDIR_U15}/{filename} ] && echo 'EXISTS' || echo 'NOT_EXISTS'"
    ]
    try:
        result = subprocess.run(ssh_command, check=False, capture_output=True, text=True, timeout=30)
        exists = result.stdout.strip() == "EXISTS"
        if exists:
            logging.debug(f"Arquivo {filename} j existe no DR.")
        return exists
    except Exception as e:
        logging.error(f"Erro ao verificar {filename} no DR: {e}")
        return False

def is_file_stable(file_path, check_interval_seconds=3, num_checks=2):
    """Verifica se o tamanho do arquivo est estvel."""
    if not os.path.exists(file_path):
        logging.warning(f"Arquivo no encontrado: {file_path}")
        return False
    try:
        initial_size = os.path.getsize(file_path)
        for i in range(num_checks - 1):
            time.sleep(check_interval_seconds)
            current_size = os.path.getsize(file_path)
            if current_size != initial_size:
                logging.info(f"{os.path.basename(file_path)} ainda est crescendo.")
                return False
            initial_size = current_size
        logging.debug(f"{os.path.basename(file_path)} estvel.")
        return True
    except (OSError, Exception) as e:
        logging.error(f"Erro ao verificar {file_path}: {e}")
        return False

def execute_rsync(source_file_path, dest_dir_remote, sequence_number):
    """Executa rsync com opes de retomada."""
    filename = os.path.basename(source_file_path)
    rsync_command = [
        "rsync",
        "-avz",
        "--partial",
        "--inplace",
        "--append-verify",
        "--timeout=300",
        source_file_path,
        f"{SSHUSER}@{DRSERVER}:{dest_dir_remote}/"
    ]
    logging.debug(f"rsync para {filename} (seq {sequence_number})")
    try:
        result = subprocess.run(rsync_command, check=True, capture_output=True, text=True, timeout=300)
        if "sent 0 bytes" in result.stdout or "up to date" in result.stdout:
            logging.info(f" {filename} (seq {sequence_number}) j existe ou sem alteraes.")
            return True, f"Pulado {filename}"
        else:
            logging.info(f" {filename} (seq {sequence_number}) replicado.")
            return True, f"Copiado {filename}"
    except subprocess.TimeoutExpired:
        logging.error(f" Timeout para {filename} (seq {sequence_number}). Parcial mantido.")
        return False, f"Timeout {filename}"
    except subprocess.CalledProcessError as e:
        logging.error(f" Falha em {filename}: {e.stderr.strip()}")
        return False, f"Falha {filename}"
    except Exception as e:
        logging.error(f" Erro em {filename}: {e}")
        return False, f"Erro {filename}"

def replicate_archivelogs(specific_filename=None):
    """Replica archivelogs do source para o DR."""
    logging.info("")
    logging.info("Iniciando replicao...")
    logging.info(f"Source: {SOURCEBASEDIR}")
    logging.info(f"Destino: {SSHUSER}@{DRSERVER}:{DESTBASEDIR}")
    logging.info(f"Workers: {MAXWORKERS}")
    
    if specific_filename:
        logging.info(f"Modo: Transferncia especfica de {specific_filename}")
    else:
        logging.info("Modo: Replicao padro de todos os logs no aplicados")
    
    dest_dir_remote = DESTBASEDIR
    mkdir_command = ["ssh", f"{SSHUSER}@{DRSERVER}", f"mkdir -p {dest_dir_remote}"]
    
    try:
        subprocess.run(mkdir_command, check=True, capture_output=True)
        logging.info(f"Diretrio remoto pronto: {dest_dir_remote}")
    except Exception as e:
        logging.critical(f"Erro ao criar diretrio remoto: {e}")
        return
    
    with ThreadPoolExecutor(max_workers=MAXWORKERS) as executor:
        futures = []
        
        if specific_filename:
            found_file_path = None
            source_dirs = get_archivelog_directories(SOURCEBASEDIR)
            for sdir in source_dirs:
                potential_path = os.path.join(sdir, specific_filename)
                if os.path.exists(potential_path):
                    found_file_path = potential_path
                    break
            
            if found_file_path:
                match = ARCHIVELOGPATTERN.match(specific_filename)
                sequence_number = int(match.group(1)) if match else 0
                if is_file_stable(found_file_path):
                    logging.info(f"Arquivo {specific_filename} encontrado e estvel.")
                    future = executor.submit(execute_rsync, found_file_path, dest_dir_remote, sequence_number)
                    futures.append(future)
                else:
                    logging.warning(f"Arquivo {specific_filename} no est estvel.")
            else:
                logging.error(f"Arquivo {specific_filename} no encontrado.")
        else:
            highest_applied_seq = get_highest_applied_sequence_on_dr()
            if highest_applied_seq == 0:
                logging.warning("No foi possvel determinar sequence no DR. Continuando...")
            
            source_dirs = get_archivelog_directories(SOURCEBASEDIR)
            if not source_dirs:
                logging.warning(f"Nenhum diretrio encontrado em {SOURCEBASEDIR}")
                return
            
            all_archivelog_files = []
            for source_dir in source_dirs:
                files = get_archivelog_files(source_dir)
                if files:
                    all_archivelog_files.extend(files)
            
            all_archivelog_files.sort(key=lambda x: x[1])
            
            for source_file_path, sequence_number in all_archivelog_files:
                filename = os.path.basename(source_file_path)
                
                if sequence_number <= highest_applied_seq:
                    logging.debug(f"{filename} (seq {sequence_number}) j aplicado. Pulando.")
                    continue
                
                if SEQ_FILTER_MIN is not None and sequence_number < SEQ_FILTER_MIN:
                    continue
                if SEQ_FILTER_MAX is not None and sequence_number > SEQ_FILTER_MAX:
                    continue
                
                if file_exists_on_dr(filename):
                    logging.info(f"{filename} (seq {sequence_number}) j existe no DR. Pulando.")
                    continue
                
                if is_file_stable(source_file_path):
                    future = executor.submit(execute_rsync, source_file_path, dest_dir_remote, sequence_number)
                    futures.append(future)
                else:
                    logging.info(f"{filename} (seq {sequence_number}) no est estvel. Pulando por agora.")
        
        for future in futures:
            try:
                success, message = future.result()
                if not success:
                    logging.error(f"Falha na transferncia: {message}")
            except Exception as e:
                logging.error(f"Erro ao obter resultado: {e}")
    
    logging.info("")
    logging.info("Processo de replicao concludo.")
    logging.info("")

if __name__ == "__main__":
    lock_fd_held = None
    specific_file = None
    
    if len(sys.argv) > 1:
        specific_file = sys.argv[1]
    
    try:
        lock_fd_held = acquire_lock()
        if lock_fd_held:
            replicate_archivelogs(specific_file)
        else:
            sys.exit(0)
    except Exception as e:
        logging.critical(f"Erro FATAL: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if lock_fd_held:
            release_lock(lock_fd_held)
