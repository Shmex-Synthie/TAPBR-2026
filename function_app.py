import logging
import os

import azure.functions as func
import pyodbc

app = func.FunctionApp()

@app.function_name(name="TimerAnalista")
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
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
    for i in cursor.fetchall():
        logging.info(i)


@app.function_name(name="TimerChamados")
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
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
    for i in cursor.fetchall():
        logging.info(i)


@app.function_name(name="TimerCategoria")
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
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
    for i in cursor.fetchall():
        logging.info(i)


@app.function_name(name="TimerChamadoSLA")
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
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
    for i in cursor.fetchall():
        logging.info(i)



@app.function_name(name="TimerChamadoStatusHist")
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
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
    for i in cursor.fetchall():
        logging.info(i)


@app.function_name(name="TimerClienteOrganizacao")
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
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
    for i in cursor.fetchall():
        logging.info(i)


@app.function_name(name="Timercsat")
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
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
    for i in cursor.fetchall():
        logging.info(i)

@app.function_name(name="TimerFila")
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
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
    for i in cursor.fetchall():
        logging.info(i)


@app.function_name(name="TimeSLA")
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
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
    for i in cursor.fetchall():
        logging.info(i)




@app.function_name(name="TimerSolicitante")
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
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
    for i in cursor.fetchall():
        logging.info(i)