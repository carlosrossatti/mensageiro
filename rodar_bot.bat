@echo off
REM === ir para a pasta do projeto ===
cd C:\Users\carlos.rossatti_meut\monitoramento_esteiras

REM === ativar o ambiente virtual ===
call venv\Scripts\activate

REM === rodar o bot ===
python bot_monitoramento.py

REM === manter janela aberta em caso de erro ===
pause
