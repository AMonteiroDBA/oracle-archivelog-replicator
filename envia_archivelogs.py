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


# --- ⚠️ Configurações Principais ---
SISDBA_HOME = os.path.expanduser('/home/oracle/.alkdba')
os.makedirs(os.path.join(SISDBA_HOME, 'log'), exist_ok=True)
os.makedirs(os.path.join(SISDBA_HOME, 'tmp'), exist_ok=True)


SOURCE_BASE_DIR = "/u02/flash_recovery_area/DBPROD/archivelog/"
DEST_BASE_DIR = "/u12/flash_recovery_area/DBPROD/archivelog/"
DEST_BASE_DIR_U15 = "/u15/flash_recovery_area/DBPROD/archivelog/"
DR_SERVER = "PAMVS0003L"
SSH_USER = "oracle"

LOG_FILE = os.path.join(SISDBA_HOME, "log", "archivelog_replication.log")
LOCK_FILE = os.path.join(SISDBA_HOME, "tmp", "archivelog_replication.lock")
LOCK_TIMEOUT_MINUTES = 30
MAX_WORKERS = 5

ARCHIVELOG_PATTERN = re.compile(r"o1_mf_1_(\d+)_.*\.arc")
DATE_DIR_PATTERN = re.compile(r"^\d{4}_\d{2}_\d{2}$")


# --- 📋 Configuração de Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename=LOG_FILE,
                    filemode='a')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
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
        lock_fd = open(LOCK_FILE, 'a+')
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)


        lock_fd.seek(0)
        content = lock_fd.read().strip()


        if content:
            try:
                old_pid, old_timestamp_str = content.split(':')
                old_pid = int(old_pid)
                old_timestamp = datetime.datetime.fromtimestamp(float(old_timestamp_str))
                logging.warning(f"Lock file encontrado de uma execução anterior (PID {old_pid}, Início: {old_timestamp}). Limpando...")
            except ValueError:
                logging.warning(f"Conteúdo do lock file corrompido: '{content}'. Limpando.")


        lock_fd.seek(0)
        lock_fd.truncate()
        lock_fd.write(f"{current_pid}:{time.time()}")
        lock_fd.flush()
        os.fsync(lock_fd.fileno())


        logging.info(f"Lock adquirido com sucesso. PID: {current_pid}")
        return lock_fd


    except BlockingIOError:
        try:
            with open(LOCK_FILE, 'r') as f:
                content = f.read().strip()
                if not content:
                    logging.error("Lock file vazio ou corrompido, mas bloqueado. Possível condição de corrida. Saindo.")
                    return None


                old_pid, old_timestamp_str = content.split(':')
                old_pid = int(old_pid)
                old_timestamp = datetime.datetime.fromtimestamp(float(old_timestamp_str))


                if is_process_running(old_pid):
                    time_elapsed = (datetime.datetime.now() - old_timestamp).total_seconds() / 60
                    if time_elapsed < LOCK_TIMEOUT_MINUTES:
                        logging.info(f"Outro processo (PID {old_pid}) já está em execução e dentro do tempo limite ({time_elapsed:.2f} min). Saindo.")
                        return None
                    else:
                        logging.warning(f"Processo (PID {old_pid}) excedeu o tempo limite ({time_elapsed:.2f} min). Tentando encerrá-lo...")
                        try:
                            os.kill(old_pid, signal.SIGTERM)
                            time.sleep(5)
                            if is_process_running(old_pid):
                                logging.warning(f"Processo (PID {old_pid}) ainda ativo após SIGTERM. Tentando SIGKILL...")
                                os.kill(old_pid, signal.SIGKILL)
                                time.sleep(2)


                            if is_process_running(old_pid):
                                logging.error(f"Não foi possível encerrar o processo (PID {old_pid}). Saindo.")
                                return None


                            logging.info(f"Processo (PID {old_pid}) encerrado. Removendo lock antigo e tentando adquirir novamente.")
                            os.remove(LOCK_FILE)
                            return acquire_lock()
                        except ProcessLookupError:
                            logging.info(f"Processo (PID {old_pid}) não encontrado (já encerrou?). Removendo lock antigo e tentando adquirir novamente.")
                            os.remove(LOCK_FILE)
                            return acquire_lock()
                        except Exception as e:
                            logging.error(f"Erro ao tentar encerrar processo antigo (PID {old_pid}): {e}. Saindo.")
                            return None
                else:
                    logging.warning(f"Lock file (PID {old_pid}) encontrado, mas o processo não está mais ativo. Removendo lock antigo.")
                    os.remove(LOCK_FILE)
                    return acquire_lock()
        except FileNotFoundError:
            logging.error(f"Lock file '{LOCK_FILE}' não encontrado, mas fcntl reportou BlockingIOError. Isso não deveria acontecer. Saindo.")
            return None
        except Exception as e:
            logging.error(f"Erro inesperado ao verificar o lock file: {e}. Saindo.")
            return None
    except Exception as e:
        logging.critical(f"Erro FATAL ao tentar abrir ou adquirir lock: {e}", exc_info=True)
        return None


def release_lock(lock_fd):
    try:
        if lock_fd:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()
            os.remove(LOCK_FILE)
            logging.info("Lock liberado e arquivo de lock removido.")
    except Exception as e:
        logging.error(f"Erro ao liberar ou remover lock: {e}")


def get_archivelog_directories(base_dir):
    directories = []
    try:
        logging.info(f"Buscando diretórios de archivelog em: {base_dir}")
        for item in os.listdir(base_dir):
            full_path = os.path.join(base_dir, item)
            if os.path.isdir(full_path) and DATE_DIR_PATTERN.match(item):
                directories.append(full_path)
        directories.sort()
        logging.info(f"Diretórios de archivelog encontrados: {directories}")
        return directories
    except FileNotFoundError:
        logging.error(f"Erro: O caminho base '{base_dir}' não foi encontrado. Verifique as configurações.")
        return []
    except Exception as e:
        logging.error(f"Erro inesperado ao listar diretórios em {base_dir}: {e}")
        return []


def get_archivelog_files(directory):
    files = []
    try:
        logging.debug(f"Listando arquivos em {directory}")
        for filename in os.listdir(directory):
            match = ARCHIVELOG_PATTERN.match(filename)
            if match:
                sequence_number = int(match.group(1))
                full_path = os.path.join(directory, filename)
                files.append((full_path, sequence_number))
        files.sort(key=lambda x: x[1])
        logging.debug(f"Arquivos de archivelog encontrados em {directory}: {[f[0] for f in files]}")
        return files
    except Exception as e:
        logging.error(f"Erro ao listar arquivos em {directory}: {e}")
        return []


def get_highest_applied_sequence_on_dr():
    """
    Conecta ao servidor de DR via SSH e consulta o Oracle DB para obter o
    maior sequence number de archivelog que já foi aplicado.
    Retorna o maior sequence number aplicado ou 0 em caso de falha.
    """
    logging.info(f"Consultando o maior sequence number aplicado no DR ({DR_SERVER})...")
    # Comando SQL para executar no servidor de DR via SSH.
    # Assume que o usuário SSH_USER (oracle) tem permissão de sysdba via OS authentication.
    #sql_command = """source /home/oracle/.alkdba/lnx/bash_profile_silent;sqlplus -s / as sysdba <<< "set feed off term off head off pages 0 trim on trimspool on;\nSELECT ltrim(NVL(MAX(SEQUENCE#), 0)) FROM V\$ARCHIVED_LOG WHERE APPLIED='YES';" """
    sql_command = """source /home/oracle/.alkdba/lnx/bash_profile_silent;sqlplus -s / as sysdba <<< "set feed off term off head off pages 0 trim on trimspool on;\nSELECT ltrim(NVL(MAX(SEQUENCE#), 0)) FROM V\$MANAGED_STANDBY WHERE PROCESS LIKE 'MRP%';" """


    ssh_command = ["ssh", f"{SSH_USER}@{DR_SERVER}", sql_command]


    try:
        result = subprocess.run(ssh_command, check=True, capture_output=True, text=True, timeout=60)
        output = result.stdout.strip()


        # O output do sqlplus -s deve ser apenas o número.
        try:
            # Bypass manual
            highest_sequence = 197800
            # highest_sequence = int(output)
            logging.info(f"Maior sequence number aplicado no DR: {highest_sequence}")
            return highest_sequence
        except ValueError:
            logging.error(f"Não foi possível converter a saída SQL '{output}' para um número inteiro. Verifique o output do SQL*Plus. Assumindo 0.")
            return 0


    except subprocess.TimeoutExpired:
        logging.error(f"Timeout ao tentar consultar o DB no DR ({DR_SERVER}).")
        return 0
    except subprocess.CalledProcessError as e:
        logging.error(f"Falha ao executar consulta SQL no DR ({DR_SERVER}).")
        logging.error(f"Stderr: {e.stderr.strip()}")
        logging.error(f"Comando completo: {' '.join(ssh_command)}")
        return 0
    except Exception as e:
        logging.error(f"Erro inesperado ao obter highest applied sequence do DR: {e}")
        return 0


def is_file_stable(file_path, check_interval_seconds=3, num_checks=2):
    """
    Verifica se o tamanho de um arquivo está estável por um período.
    Args:

        file_path (str): O caminho completo do arquivo.
           check_interval_seconds (int): Intervalo de tempo entre as verificações de tamanho.
              num_checks (int): Número de verificações de tamanho para considerar o arquivo estável.
    Returns:
       bool: True se o tamanho do arquivo não mudar durante as verificações, False caso contrário.
    """

    ssh_command = [
        "ssh",
        f"{SSH_USER}@{DR_SERVER}",
        f"[ -f {DEST_BASE_DIR}{filename} ] || [ -f {DEST_BASE_DIR_U15}{filename} ] && echo 'EXISTS' || echo 'NOT_EXISTS'"
    ]

    try:
        result = subprocess.run(ssh_command, check=False, capture_output=True, text=True, timeout=30)
        output = result.stdout.strip()
        exists = output == "EXISTS"
        if exists:
            logging.debug(f"Arquivo {filename} já existe no DR (/u12 ou /u15).")
        return exists
    except Exception as e:
        logging.error(f"Erro ao verificar existência de {filename} no DR: {e}")
        return False
    if not os.path.exists(file_path):
        logging.warning(f"Arquivo não encontrado para verificação de estabilidade: {file_path}")
        return False


    try:
        initial_size = os.path.getsize(file_path)
        logging.debug(f"Verificando estabilidade do arquivo {os.path.basename(file_path)}. Tamanho inicial: {initial_size} bytes.")


        for i in range(num_checks - 1):
            time.sleep(check_interval_seconds)
            current_size = os.path.getsize(file_path)
            if current_size != initial_size:
                logging.info(f"Arquivo {os.path.basename(file_path)} ainda está crescendo ou mudou. Tamanho atual: {current_size} (anterior: {initial_size}). Não está estável.")
                return False
            initial_size = current_size


        logging.debug(f"Arquivo {os.path.basename(file_path)} parece estável.")
        return True
    except OSError as e:
        logging.error(f"Erro de sistema de arquivo ao verificar estabilidade de {file_path}: {e}")
        return False
    except Exception as e:
        logging.error(f"Erro inesperado ao verificar estabilidade de {file_path}: {e}")
        return False


def execute_rsync(source_file_path, dest_dir_remote, sequence_number):
    filename = os.path.basename(source_file_path)
    rsync_command = [
        "rsync",
        "-avz",
        source_file_path,
        f"{SSH_USER}@{DR_SERVER}:{dest_dir_remote}/"
    ]


    logging.debug(f"Executando rsync para {filename} (seq {sequence_number}) no worker.")
    try:
        result = subprocess.run(rsync_command, check=True, capture_output=True, text=True, timeout=300)


        if "sent 0 bytes" in result.stdout or "up to date" in result.stdout:
            logging.info(f"✔️ Archivelog {filename} (seq {sequence_number}) já existe no destino ou não houve alteração. Pulando (otimizado).")
            return True, f"Pulado: {filename}"
        else:
            logging.info(f"✅ Archivelog {filename} (seq {sequence_number}) replicado com sucesso.")
            logging.debug(f"Saída completa do rsync para {filename}:\n{result.stdout.strip()}")
            return True, f"Copiado: {filename}"


    except subprocess.TimeoutExpired:
        logging.error(f"❌ Erro: rsync para {filename} (seq {sequence_number}) excedeu o tempo limite.")
        return False, f"Timeout: {filename}"
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Falha ao replicar archivelog {filename} (seq {sequence_number}).")
        logging.error(f"Código de saída: {e.returncode}")
        logging.error(f"Stderr (erro do rsync): {e.stderr.strip()}")
        logging.error(f"Comando completo que falhou: {' '.join(rsync_command)}")
        return False, f"Falha: {filename}"
    except Exception as e:
        logging.error(f"⚠️ Erro inesperado ao processar archivelog {filename}: {e}")
        return False, f"Erro: {filename}"


def replicate_archivelogs(specific_filename=None):
    logging.info("=====================================================")
    logging.info("🚀 Iniciando processo de replicação de archivelogs...")
    logging.info(f"🌐 Origem: {SOURCE_BASE_DIR}")
    logging.info(f"➡️ Destino: {SSH_USER}@{DR_SERVER}:{DEST_BASE_DIR}")
    logging.info(f"⚙️ Workers de transferência: {MAX_WORKERS}")
    if specific_filename:
        logging.info(f"🌿 Modo: Transferência de arquivo específico: {specific_filename}")
    else:
        logging.info("♾️ Modo: Replicação padrão de archivelogs (todos os não aplicados).")
    logging.info("=====================================================")


    dest_dir_remote = DEST_BASE_DIR


    mkdir_command = ["ssh", f"{SSH_USER}@{DR_SERVER}", f"mkdir -p {dest_dir_remote}"]
    logging.debug(f"Executando comando remoto: {' '.join(mkdir_command)}")
    try:
        subprocess.run(mkdir_command, check=True, capture_output=True, text=True)
        logging.info(f"Diretório de destino remoto verificado/criado: {dest_dir_remote}")
    except subprocess.CalledProcessError as e:
        logging.critical(f"❌ Falha FATAL ao criar/verificar diretório remoto {dest_dir_remote}: {e.stderr.strip()}")
        logging.critical(f"Comando completo de falha: {' '.join(mkdir_command)}")
        logging.critical("Não é possível prosseguir sem o diretório de destino. Saindo.")
        return


    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []


        if specific_filename:
            found_file_path = None
            source_dirs = get_archivelog_directories(SOURCE_BASE_DIR)


            for s_dir in source_dirs:
                potential_path = os.path.join(s_dir, specific_filename)
                if os.path.exists(potential_path):
                    found_file_path = potential_path
                    break


            if found_file_path:
                match = ARCHIVELOG_PATTERN.match(specific_filename)
                sequence_number = int(match.group(1)) if match else 0


                # NOVO: Verificar estabilidade antes de copiar o arquivo específico
                if is_file_stable(source_file_path) and not file_exists_on_dr(filename):
                    logging.info(f"Arquivo '{specific_filename}' encontrado e estável em '{os.path.dirname(found_file_path)}'. Iniciando transferência.")
                    future = executor.submit(execute_rsync, found_file_path, dest_dir_remote, sequence_number)
                    futures.append(future)
                else:
                    logging.warning(f"Arquivo '{specific_filename}' ainda não está estável. Não será copiado neste ciclo.")
            else:
                logging.error(f"❌ Erro: Arquivo '{specific_filename}' não encontrado em nenhum diretório de archivelog sob '{SOURCE_BASE_DIR}'. Verifique o nome e o caminho de origem.")
        else:
            highest_applied_seq = get_highest_applied_sequence_on_dr()
            if highest_applied_seq == 0:
                logging.warning("Não foi possível determinar o maior sequence number aplicado no DR. "
                                "Isso pode levar a cópias redundantes de archivelogs já aplicados, "
                                "especialmente se eles não existirem mais no destino.")


            source_dirs = get_archivelog_directories(SOURCE_BASE_DIR)


            if not source_dirs:
                logging.warning(f"⚠️ Nenhum diretório de archivelog encontrado em {SOURCE_BASE_DIR}. "
                                "Verifique o caminho ou o padrão de diretórios (YYYY_MM_DD).")
                return


            all_archivelog_files = []
            for source_dir in source_dirs:
                files = get_archivelog_files(source_dir)
                if files:
                    all_archivelog_files.extend(files)


            # Ordenar todos os arquivos pelo sequence number, independente do diretório
            all_archivelog_files.sort(key=lambda x: x[1])


            for source_file_path, sequence_number in all_archivelog_files:
                      filename = os.path.basename(source_file_path)
                if sequence_number > highest_applied_seq: # Apenas se o archivelog ainda não foi aplicado no DR
                    if is_file_stable(source_file_path) and not file_exists_on_dr(filename): # E se o arquivo estiver estáveland not file_exists_on_dr(filename)  and not file_exists_on_dr(filename):
                      
                        future = executor.submit(execute_rsync, source_file_path, dest_dir_remote, sequence_number)
                        futures.append(future)
                    else:
                        logging.info(f"Archivelog {os.path.basename(source_file_path)} (seq {sequence_number}) ainda está sendo gravado ou não está estável. Pulando por agora.")
                else:
                    logging.debug(f"Archivelog {os.path.basename(source_file_path)} (seq {sequence_number}) já foi aplicado no DR. Pulando.")


        for future in futures:
            try:
                success, message = future.result()
                if not success:
                    logging.error(f"Falha na transferência de archivelog: {message}")
            except Exception as e:
                logging.error(f"Erro ao obter resultado de uma tarefa de transferência: {e}")


    logging.info("=====================================================")
    logging.info("🎉 Processo de replicação de archivelogs concluído.")
    logging.info("=====================================================")


# --- 🏁 Ponto de Entrada Principal ---
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
        logging.critical(f"Erro FATAL no script principal de replicação de archivelogs: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if lock_fd_held:
            release_lock(lock_fd_held)
