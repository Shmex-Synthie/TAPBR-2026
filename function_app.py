import logging
import os
import json
from datetime import datetime, timezone, date
from decimal import Decimal
from uuid import UUID

from azure.storage.blob import BlobServiceClient

import azure.functions as func
import pyodbc

app = func.FunctionApp()


def conectar_sql_destino():
    host = os.getenv("DEST_SQL_HOST")
    database = os.getenv("DEST_SQL_DATABASE")
    user = os.getenv("DEST_SQL_USER")
    password = os.getenv("DEST_SQL_PASSWORD")


    if not all([host, database, user, password]):
        raise ValueError("Variáveis de conexão com o SQL de destino incompletas.")


    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER=tcp:{host},1433;"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    connection = pyodbc.connect(connection_string)

    return connection


def ler_json_bronze(entidade):

    storage = BlobServiceClient.from_connection_string(
        os.environ["BRONZE_CONNECTION_STRING"]
    )

    container = storage.get_container_client("bronze")

    
    arquivos = [arquivo for arquivo in container.list_blobs(name_starts_with=f"itsm/{entidade}/") 
    if arquivo.name.lower().endswith(".json") and arquivo.size > 0]


    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum JSON encontrado para {entidade}"
        )

    arquivo_recente = max(
        arquivos,
        key=lambda arquivo: arquivo.last_modified
    )

    blob = container.get_blob_client(
        arquivo_recente.name
    )

    conteudo = blob.download_blob().readall()


    logging.info(f"Arquivo selecionado: {arquivo_recente.name}")

    logging.info(f"Tamanho do arquivo: {len(conteudo)} bytes")

    logging.info(f"Início do conteúdo: {repr(conteudo[:100])}")

    registros = json.loads(
        conteudo.decode("utf-8")
    )

    return registros



def importar_tabelas():

    tabelas = {
        "fila": "id_fila",
        "cliente_organizacao": "id_cliente_organizacao",
        "categoria": "id_categoria",
        "sla": "id_sla",
        "analista": "id_analista",
        "solicitante": "id_solicitante",
        "chamados": "id_chamado",
        "chamado_sla": "id_chamado_sla",
        "chamado_status_historico": "id_chamado_status_historico",
        "csat_avaliacao": "id_csat_avaliacao"
    }

    resumo = {}

    for tabela, chave_primaria in tabelas.items():

        logging.info(f"Iniciando importacao: {tabela}")

        registros = ler_json_bronze(tabela)

        if not isinstance(registros, list) or not registros:
            raise ValueError(
                f"JSON vazio ou invalido para {tabela}"
            )

        connection = conectar_sql_destino()

        try:
            cursor = connection.cursor()

            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'dbo'
                  AND TABLE_NAME = ?
                """,
                tabela
            )

            colunas_sql = {
                linha[0] for linha in cursor.fetchall()
            }

            colunas = list(registros[0].keys())

            if set(colunas) != colunas_sql:
                raise ValueError(
                    f"Colunas incompatíveis na tabela {tabela}"
                )

            if chave_primaria not in colunas:
                raise ValueError(
                    f"Chave primaria ausente em {tabela}"
                )

            for registro in registros:
                if not isinstance(registro, dict):
                    raise ValueError(
                        f"Registro invalido em {tabela}"
                    )

                if set(registro.keys()) != set(colunas):
                    raise ValueError(
                        f"Registro com colunas diferentes em {tabela}"
                    )

            cursor.execute(
                f"SELECT [{chave_primaria}] FROM dbo.[{tabela}]"
            )

            ids_existentes = {
                linha[0] for linha in cursor.fetchall()
            }

            nomes_colunas = ", ".join(
                f"[{coluna}]" for coluna in colunas
            )

            parametros = ", ".join(
                "?" for _ in colunas
            )

            sql = (
                f"INSERT INTO dbo.[{tabela}] "
                f"({nomes_colunas}) "
                f"VALUES ({parametros})"
            )

            novos_registros = []

            for registro in registros:

                identificador = registro[chave_primaria]

                if identificador in ids_existentes:
                    continue

                valores = tuple(
                    registro[coluna] for coluna in colunas
                )

                novos_registros.append(valores)
                ids_existentes.add(identificador)

            if novos_registros:
                cursor.executemany(sql, novos_registros)

            connection.commit()

            inseridos = len(novos_registros)

            resumo[tabela] = {
                "lidos": len(registros),
                "inseridos": inseridos,
                "ignorados": len(registros) - inseridos
            }

            logging.info(
                f"{tabela}: {inseridos} registros inseridos."
            )

        except Exception:
            connection.rollback()
            logging.exception(
                f"Erro na importacao de {tabela}"
            )
            raise

        finally:
            connection.close()

    return resumo


def salvar_bronze(cursor, entidade):

    colunas = [coluna[0] for coluna in cursor.description]
    registros = [dict(zip(colunas, linha)) for linha in cursor.fetchall()]

    agora = datetime.now(timezone.utc)
    data = agora.strftime("%Y%m%dT%H%M%SZ")
    caminho = f"itsm/{entidade}/ano={agora:%Y}/mes={agora:%m}/dia={agora:%d}/{entidade}_{data}.json"
    conteudo = json.dumps(registros, ensure_ascii=False, default=converter_json, indent=2)
    storage = BlobServiceClient.from_connection_string(os.environ["BRONZE_CONNECTION_STRING"])
    blob = storage.get_blob_client(container="bronze", blob=caminho)

    blob.upload_blob(conteudo.encode("utf-8"), overwrite=True)
    logging.info("Arquivo salvo: bronze/%s (%s registros)", caminho, len(registros))

def converter_json(valor):
    if isinstance(valor, (datetime, date)):
        return valor.isoformat()
    if isinstance(valor, (Decimal, UUID)):
        return str(valor)
    if isinstance(valor, bytes):
        return valor.hex()
    raise TypeError(f"Tipo não suportado: {type(valor).__name__}")


@app.function_name(name="TimerAnalista")
@app.timer_trigger(schedule="0 0 23 * * *", arg_name="myTimer", run_on_startup=False,
            use_monitor=False) 

def extract_analista(myTimer: func.TimerRequest) -> None:

    host = os.getenv("host")
    database = os.getenv("database")
    user = os.getenv("user")
    password = os.getenv("password")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    connection = pyodbc.connect(conn_str)
    logging.info("Conectado!")
    cursor = connection.cursor()
    
    cursor.execute("SELECT * FROM itsm.analista")
    salvar_bronze(cursor, "analista")


@app.function_name(name="TimerChamados")
@app.timer_trigger(schedule="0 0 23 * * *", arg_name="myTimer", run_on_startup=False,
            use_monitor=False) 

def extract_chamados(myTimer: func.TimerRequest) -> None:

    host = os.getenv("host")
    database = os.getenv("database")
    user = os.getenv("user")
    password = os.getenv("password")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    connection = pyodbc.connect(conn_str)
    logging.info("Conectado!")
    cursor = connection.cursor()
    
    cursor.execute("SELECT * FROM itsm.chamado")
    salvar_bronze(cursor, "chamados")


@app.function_name(name="TimerCategoria")
@app.timer_trigger(schedule="0 0 23 * * *", arg_name="myTimer", run_on_startup=False,
            use_monitor=False) 

def extract_categorias(myTimer: func.TimerRequest) -> None:

    host = os.getenv("host")
    database = os.getenv("database")
    user = os.getenv("user")
    password = os.getenv("password")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    connection = pyodbc.connect(conn_str)
    logging.info("Conectado!")
    cursor = connection.cursor()
    
    cursor.execute("SELECT * FROM itsm.categoria")
    salvar_bronze(cursor, "categoria")


@app.function_name(name="TimerChamadoSLA")
@app.timer_trigger(schedule="0 0 23 * * *", arg_name="myTimer", run_on_startup=False,
            use_monitor=False) 

def extract_chamados_sla(myTimer: func.TimerRequest) -> None:

    host = os.getenv("host")
    database = os.getenv("database")
    user = os.getenv("user")
    password = os.getenv("password")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    connection = pyodbc.connect(conn_str)
    logging.info("Conectado!")
    cursor = connection.cursor()
    
    cursor.execute("SELECT * FROM itsm.chamado_sla")
    salvar_bronze(cursor, "chamado_sla")



@app.function_name(name="TimerChamadoStatusHist")
@app.timer_trigger(schedule="0 0 23 * * *", arg_name="myTimer", run_on_startup=False,
            use_monitor=False) 

def extract_chamados_status_hist(myTimer: func.TimerRequest) -> None:

    host = os.getenv("host")
    database = os.getenv("database")
    user = os.getenv("user")
    password = os.getenv("password")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    connection = pyodbc.connect(conn_str)
    logging.info("Conectado!")
    cursor = connection.cursor()
    
    cursor.execute("SELECT * FROM itsm.chamado_status_historico")
    salvar_bronze(cursor, "chamado_status_historico")


@app.function_name(name="TimerClienteOrganizacao")
@app.timer_trigger(schedule="0 0 23 * * *", arg_name="myTimer", run_on_startup=False,
            use_monitor=False) 

def extract_clientes_organizacao(myTimer: func.TimerRequest) -> None:

    host = os.getenv("host")
    database = os.getenv("database")
    user = os.getenv("user")
    password = os.getenv("password")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    connection = pyodbc.connect(conn_str)
    logging.info("Conectado!")
    cursor = connection.cursor()
    
    cursor.execute("SELECT * FROM itsm.cliente_organizacao")
    salvar_bronze(cursor, "cliente_organizacao")


@app.function_name(name="Timercsat")
@app.timer_trigger(schedule="0 0 23 * * *", arg_name="myTimer", run_on_startup=False,
            use_monitor=False) 

def extract_csat(myTimer: func.TimerRequest) -> None:

    host = os.getenv("host")
    database = os.getenv("database")
    user = os.getenv("user")
    password = os.getenv("password")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    connection = pyodbc.connect(conn_str)
    logging.info("Conectado!")
    cursor = connection.cursor()
    
    cursor.execute("SELECT * FROM itsm.csat_avaliacao")
    salvar_bronze(cursor, "csat_avaliacao")

@app.function_name(name="TimerFila")
@app.timer_trigger(schedule="0 0 23 * * *", arg_name="myTimer", run_on_startup=False,
            use_monitor=False) 

def extract_fila(myTimer: func.TimerRequest) -> None:

    host = os.getenv("host")
    database = os.getenv("database")
    user = os.getenv("user")
    password = os.getenv("password")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    connection = pyodbc.connect(conn_str)
    logging.info("Conectado!")
    cursor = connection.cursor()
    
    cursor.execute("SELECT * FROM itsm.fila")
    salvar_bronze(cursor, "fila")


@app.function_name(name="TimeSLA")
@app.timer_trigger(schedule="0 0 23 * * *", arg_name="myTimer", run_on_startup=False,
            use_monitor=False) 

def extract_sla(myTimer: func.TimerRequest) -> None:

    host = os.getenv("host")
    database = os.getenv("database")
    user = os.getenv("user")
    password = os.getenv("password")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    connection = pyodbc.connect(conn_str)
    logging.info("Conectado!")
    cursor = connection.cursor()
    
    cursor.execute("SELECT * FROM itsm.sla")
    salvar_bronze(cursor, "sla")




@app.function_name(name="TimerSolicitante")
@app.timer_trigger(schedule="0 0 23 * * *", arg_name="myTimer", run_on_startup=False,
            use_monitor=False) 

def extract_solicitante(myTimer: func.TimerRequest) -> None:

    host = os.getenv("host")
    database = os.getenv("database")
    user = os.getenv("user")
    password = os.getenv("password")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    connection = pyodbc.connect(conn_str)
    logging.info("Conectado!")
    cursor = connection.cursor()
    
    cursor.execute("SELECT * FROM itsm.solicitante")
    salvar_bronze(cursor, "solicitante")



@app.route(route="teste-destino", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)

def teste_destino(req: func.HttpRequest) -> func.HttpResponse:

    connection = None

    try:
        connection = conectar_sql_destino()
        cursor = connection.cursor()

        cursor.execute("SELECT DB_NAME()")
        resultado = cursor.fetchone()

        nome_banco = resultado[0]

        logging.info("Conexão com SQL de destino realizada.")

        return func.HttpResponse(
            f"Conectado ao banco: {nome_banco}",
            status_code=200
        )

    except Exception:
        logging.exception("Erro ao conectar ao SQL de destino.")

        return func.HttpResponse(
            "Erro ao conectar ao banco. Consulte os logs.",
            status_code=500
        )

    finally:
        if connection is not None:
            connection.close()

@app.route(
    route="teste-json",
    methods=["GET"],
    auth_level=func.AuthLevel.FUNCTION
)
def teste_json(req: func.HttpRequest) -> func.HttpResponse:

    try:
        registros = ler_json_bronze("fila")

        quantidade = len(registros)

        colunas = (
            list(registros[0].keys())
            if registros else []
        )

        return func.HttpResponse(
            f"Registros encontrados: {quantidade}\n"
            f"Colunas: {', '.join(colunas)}",
            status_code=200,
            mimetype="text/plain"
        )

    except Exception:
        logging.exception("Erro ao ler JSON do Storage")

        return func.HttpResponse(
            "Erro ao ler o JSON. Consulte os logs.",
            status_code=500
        )


@app.route(route="importar-tudo", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def executar_importacao(req: func.HttpRequest) -> func.HttpResponse:

    try:
        resumo = importar_tabelas()

        return func.HttpResponse(
            json.dumps(resumo, ensure_ascii=False, indent=2),
            status_code=200,
            mimetype="application/json"
        )

    except Exception:
        logging.exception(
            "Erro durante a importacao."
        )

        return func.HttpResponse(
            "Erro na importacao. Consulte os logs.",
            status_code=500
        )


@app.timer_trigger(schedule="0 30 23 * * *", arg_name="timer", run_on_startup=False, use_monitor=True)
def importacao_diaria(timer: func.TimerRequest):

    logging.info("Iniciando importacao diaria.")

    try:
        resumo = importar_tabelas()

        logging.info(
            "Importacao diaria concluida: %s",
            json.dumps(resumo, ensure_ascii=False)
        )

    except Exception:
        logging.exception(
            "Erro durante a importacao diaria."
        )
        raise
