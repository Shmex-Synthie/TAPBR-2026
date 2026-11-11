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

    arquivos = list(
        container.list_blobs(
            name_starts_with=f"itsm/{entidade}/"
        )
    )

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

    registros = json.loads(
        conteudo.decode("utf-8")
    )

    return registros


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
