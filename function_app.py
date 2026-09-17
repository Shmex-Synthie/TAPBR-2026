import azure.functions as func
import logging
import requests

app = func.FunctionApp()

@app.function_name(name="TimerLogger")
@app.timer_trigger(schedule="0 */1 * * * *", arg_name="mytimer", run_on_startup=False)
def timer_logger(mytimer: func.TimerRequest) -> None:
    logging.info("TimerLogger executado: este e o log da funcao de timer simples.")


@app.function_name(name="HttpEcho")
@app.route(route="httpecho", methods=["GET"])
def http_echo(req: func.HttpRequest) -> func.HttpResponse:
    nome = req.params.get("nome")

    if not nome:
        return func.HttpResponse(
            "Passe um parametro 'nome' na URL. Exemplo: ?nome=Fulano",
            status_code=400
        )

    logging.info(f"HttpEcho recebeu o parametro: {nome}")

    return func.HttpResponse(
        f"Parametro recebido: {nome}",
        status_code=200
    )

@app.function_name(name="TimerCaller")
@app.timer_trigger(schedule="0 */2 * * * *", arg_name="mytimer", run_on_startup=False)
def timer_caller(mytimer: func.TimerRequest) -> None:
    url = "https://taprb2026-app-crceahanbsfwb0cj.eastus-01.azurewebsites.net/api/httpecho?code=LTR3n2SKoeMBijyjgJ3YS68l_kW2KO4_YM7qGeAcyEATAzFu18T-dA%3D%3D&nome=enéas"

    try:
        resposta = requests.get(url, timeout=10)
        logging.info(
            f"TimerCaller chamou a HttpEcho. "
            f"Resposta recebida: '{resposta.text}' - [chamada feita pelo TimerCaller]"
        )
    except Exception as e:
        logging.error(f"TimerCaller falhou ao chamar a HttpEcho: {e}")