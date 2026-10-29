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