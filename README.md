# Oracle Archivelog Replicator

Python script para replicação automatizada de archivelogs Oracle de um servidor de produção para um servidor DR (Disaster Recovery) utilizando rsync com capacidade de retomada automática em caso de timeout.

## Features

- **Transferência com Retomada Automática**: Usa `rsync` com `--partial`, `--inplace`, `--append-verify` e `--timeout` para permitir que transferências incompletas sejam retomadas no próximo ciclo sem recomeçar do zero.
- **Filtro por Faixa de Sequence**: Parâmetros `SEQ_FILTER_MIN` e `SEQ_FILTER_MAX` para limitar transferências a um intervalo específico de archivelogs.
- **Verificação Dual-Path no DR**: Verifica se o arquivo já existe em `/u12` OU `/u15` no servidor de destino, evitando duplicatas e transferências desnecessárias.
- **Paralelismo Controlado**: Usa `ThreadPoolExecutor` com 5 workers simultâneos para otimizar a largura de banda.
- **Lock com Timeout**: Mecanismo de lock robusto que evita múltiplas instâncias paralelas e limpa locks antigos após 30 minutos.
- **Transferência de Arquivo Específico**: Suporte a parâmetro de linha de comando para enviar um arquivo específico.
- **Logging Completo**: Registra todas as operações em arquivo de log e console.

## Pré-requisitos

- Python 3.6+
- SSH configurado para acesso sem senha (ssh-key) entre `oracle` em ambos os servidores
- `rsync` instalado em ambos os servidores
- SQLPlus disponível no servidor DR para consultar sequence number
- Usuário `alkdba` existente no servidor de origem

## Configuração

Edite as variáveis de configuração no início do script:

```python
SOURCEBASEDIR = "/u02/flash_recovery_area/DBPROD/archivelog"
DESTBASEDIR = "/u12/flash_recovery_area/DBPROD/archivelog"
DESTBASEDIR_U15 = "/u15/flash_recovery_area/DBPROD/archivelog"
DRSERVER = "PAMVS0003L"
SSHUSER = "oracle"
MAXWORKERS = 5

# Filtro opcional
SEQ_FILTER_MIN = None  # Ex.: 198000
SEQ_FILTER_MAX = None  # Ex.: 198099
```

## Uso

### Replicação padrão (todos os logs não aplicados)
```bash
python3 envia_archivelogs.py
```

### Transferência de arquivo específico
```bash
python3 envia_archivelogs.py o1_mf_1_197943_nnv4fjcl_.arc
```

## Comportamento em Caso de Timeout

1. Quando o rsync atinge o `--timeout=300` (5 minutos) por falta de progresso, a transferência é abortada.
2. O arquivo parcial permanece no destino com o nome final (ex: `arquivo.arc`).
3. No próximo ciclo de execução, o rsync detecta o arquivo parcial e continua a transferência do ponto em que parou.
4. Sem `--inplace`, o rsync criaria um arquivo temporário `.arquivo.arc.randomico` e descartaria ao timeout; com `--inplace`, o arquivo final cresce incrementalmente.

## Logs

Os logs são salvos em `~alkdba/log/archivelog_replication.log` e também exibidos no console.

## Integração com NetBackup e RMAN

O script respeita o histórico de aplicação de archivelogs consultando o banco de dados DR via SSH/SQLPlus. Apenas logs que ainda não foram aplicados (sequence > MAX(SEQUENCE#) no DR) são considerados para transferência.

## Licença

MIT
