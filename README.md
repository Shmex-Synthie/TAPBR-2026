# TAPRB-2026

Projeto de Azure Functions - Timer Trigger e HTTP Trigger.

## Integrantes

- Gabriel Wulff Mesadri
- Gustavo Frazzon 
- Gustavo Henrique Schumacher

## Funções

- **TimerLogger**: timer trigger que imprime um log a cada execução.
- **HttpEcho**: HTTP trigger (GET) que recebe um parametro `nome` via query string e o exibe.
- **TimerCaller**: timer trigger que chama a função `HttpEcho` via HTTP e imprime a resposta com um texto adicional de identificação.
