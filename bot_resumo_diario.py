import os
import time
import socket
import pandas as pd
import requests
import schedule
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from zoneinfo import ZoneInfo  # Python 3.9+

# === Fuso horário oficial do bot ===
TZ = ZoneInfo("America/Fortaleza")

# === Carrega variáveis do .env ===
load_dotenv()

SLACK_TOKEN = os.getenv("SLACK_TOKEN")
SLACK_CHANNEL = "#geral-ops-privado"

DB_CFG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# === Query Resumo Diário Privado ===
SQL_RESUMO = """
WITH base AS (
    SELECT 
        OD.*,
        cc.contractid,
        ee.agreementid,

        -- Flag contrato pago 
        CASE 
            WHEN OD.info_etapa IN ('5. Pago','6. Pago','7. Pago') THEN 1 
            ELSE 0 
        END AS flag_paid,

        -- Data de pagamento
        CASE 
            WHEN OD.loanstatus = 'PAID' AND OD.prod = 'PORTABILITY' THEN OD.paid_date
            WHEN OD.loanstatus = 'PAID' AND OD.paymentdate IS NULL THEN OD.paid_date
            ELSE OD.paymentdate - INTERVAL 3 HOUR
        END AS data_pagamento,

        -- Produto normalizado
        CASE 
            WHEN OD.prod IN ('PIX_CONSIGNED','FUTUREMARGIN','NEW_PRIVATE_CONSIGNMENT') THEN 'NEW'
            WHEN OD.prod IN ('REFIN_PRIVATE_CONSIGNMENT') THEN 'REFIN'
            WHEN OD.prod IN ('PORTABILITY','PORTABILITY_PRIVATE_CONSIGNMENT') THEN 'PORTABILITY'
            ELSE OD.prod
        END AS produto

    FROM gold.cor_contrato_operacoes_movimento OD
    JOIN gold.tudoprod_contract cc 
      ON cc.contractid = OD.contractid
    JOIN gold.tudoprod_enrollment ee 
      ON ee.id = cc.enrollmentid
    WHERE ee.agreementid = '10'          -- consignado privado
),

-- Apenas produtos de interesse
filtro AS (
    SELECT *
    FROM base
    WHERE produto IN ('NEW','REFIN','PORTABILITY')
),

-- Agregação por produto
agg AS (
    SELECT
        produto,

        -- Contratos pagos HOJE
        SUM(
            CASE 
                WHEN DATE(data_pagamento) = CURRENT_DATE 
                     AND flag_paid = 1 
                THEN 1 ELSE 0 
            END
        ) AS qtd_dia,

        SUM(
            CASE 
                WHEN DATE(data_pagamento) = CURRENT_DATE 
                     AND flag_paid = 1 
                THEN OD.grossvalue ELSE 0 
            END
        ) AS grossvalue_dia,

        -- REFIN: valor de depósito
        SUM(
            CASE 
                WHEN produto = 'REFIN'
                     AND DATE(data_pagamento) = CURRENT_DATE 
                     AND flag_paid = 1
                THEN OD.valuefordeposit ELSE 0 
            END
        ) AS valor_deposito_dia,

        -- PORTABILITY: saldos pagos
        SUM(
            CASE 
                WHEN produto = 'PORTABILITY'
                     AND DATE(data_pagamento) = CURRENT_DATE 
                     AND flag_paid = 1
                THEN OD.outstandingbalance ELSE 0 
            END
        ) AS saldos_pagos_dia,

        -- Base p/ % aproveitamento (dia)
        SUM(
            CASE 
                WHEN DATE(OD.completeddate) = CURRENT_DATE 
                THEN 1 ELSE 0 
            END
        ) AS base_dia,

        SUM(
            CASE 
                WHEN DATE(OD.completeddate) = CURRENT_DATE 
                THEN flag_paid ELSE 0 
            END
        ) AS pagos_dia,

        -- Base p/ % aproveitamento (mês)
        SUM(
            CASE 
                WHEN DATE_TRUNC('month', OD.completeddate) = DATE_TRUNC('month', CURRENT_DATE)
                THEN 1 ELSE 0 
            END
        ) AS base_mes,

        SUM(
            CASE 
                WHEN DATE_TRUNC('month', OD.completeddate) = DATE_TRUNC('month', CURRENT_DATE)
                THEN flag_paid ELSE 0 
            END
        ) AS pagos_mes
    FROM filtro OD
    GROUP BY produto
)

SELECT
    produto,
    qtd_dia            AS quantidade,
    grossvalue_dia     AS grossvalue,

    CASE WHEN produto = 'REFIN'       THEN valor_deposito_dia END AS valor_de_deposito,
    CASE WHEN produto = 'PORTABILITY' THEN saldos_pagos_dia   END AS saldos_pagos,

    -- % aproveitamento dia
    CASE 
        WHEN base_dia > 0 
        THEN ROUND(pagos_dia * 100.0 / base_dia, 2)
        ELSE NULL 
    END AS perc_aproveitamento_dia,

    -- % aproveitamento mês
    CASE 
        WHEN base_mes > 0 
        THEN ROUND(pagos_mes * 100.0 / base_mes, 2)
        ELSE NULL 
    END AS perc_aproveitamento_mes

FROM agg
ORDER BY produto;
"""

# ========== Infra de conexão (igual seus outros bots) ==========

def wait_for_vpn_and_db(host: str, port: int = 5432, interval: int = 900):
    """
    interval = 900s = 15 minutos
    """
    print(f"[BOOT RESUMO] Verificando acesso ao DB ({host}:{port})...")

    while True:
        try:
            with socket.create_connection((host, port), timeout=5):
                print("[BOOT RESUMO] Banco/VPN acessível. Seguindo execução.")
                return
        except OSError:
            agora = datetime.now(TZ).strftime("%d/%m/%Y %H:%M:%S")
            print(f"[{agora}] Banco ainda inacessível (VPN desligada?). Nova tentativa em 15 minutos...")
            time.sleep(interval)

def run_query(sql: str) -> pd.DataFrame:
    wait_for_vpn_and_db(DB_CFG["host"], DB_CFG["port"])

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=DB_CFG["user"],
        password=DB_CFG["password"],
        host=DB_CFG["host"],
        port=DB_CFG["port"],
        database=DB_CFG["dbname"],
    )
    engine = create_engine(url, pool_pre_ping=True)

    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn)

    engine.dispose()
    return df

def format_brl(value) -> str:
    if value is None:
        return "-"
    try:
        v = float(value)
    except Exception:
        return "-"
    return ("R$ " + f"{v:,.2f}").replace(",", "X").replace(".", ",").replace("X", ".")

def format_pct(value) -> str:
    if value is None:
        return "-"
    try:
        v = float(value)
    except Exception:
        return "-"
    return f"{v:.2f}%"

# ========== Formatação e envio pro Slack ==========

def send_resumo_to_slack(df: pd.DataFrame):
    agora = datetime.now(TZ).strftime("%d/%m/%Y %H:%M")
    header = (
        "*Resumo Diário Privado — Consignado Privado*\n"
        f"📅 {agora} (America/Fortaleza)\n\n"
    )

    if df.empty:
        text = header + "_Sem registros para o dia atual._"
    else:
        lines = []
        for _, r in df.iterrows():
            produto = str(r.get("produto", "")).upper()
            qtd = int(r.get("quantidade") or 0)
            gross = format_brl(r.get("grossvalue"))
            valor_dep = format_brl(r.get("valor_de_deposito"))
            saldos_pagos = format_brl(r.get("saldos_pagos"))
            pct_dia = format_pct(r.get("perc_aproveitamento_dia"))
            pct_mes = format_pct(r.get("perc_aproveitamento_mes"))

            bloco = [f"*{produto}*"]
            bloco.append(f"  • Quantidade: *{qtd}*")
            bloco.append(f"  • Grossvalue: {gross}")

            if produto == "REFIN":
                bloco.append(f"  • Valor Depósito: {valor_dep}")
            if produto == "PORTABILITY":
                bloco.append(f"  • Saldos Pagos: {saldos_pagos}")

            bloco.append(f"  • Aproveitamento (dia): {pct_dia}")
            bloco.append(f"  • Aproveitamento (mês): {pct_mes}")

            lines.append("\n".join(bloco))

        text = header + "\n\n".join(lines)

    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_TOKEN}"},
        json={"channel": SLACK_CHANNEL, "text": text, "mrkdwn": True},
        timeout=20,
    )

    if not r.ok or not r.json().get("ok"):
        print("[RESUMO] Erro ao enviar mensagem para o Slack:", r.text)

# ========== Job e agendamento ==========

def job_resumo():
    agora = datetime.now(TZ)
    print(f"[{agora}] [RESUMO] Iniciando resumo diário...")
    try:
        df = run_query(SQL_RESUMO)
        send_resumo_to_slack(df)
        print(f"[{agora}] [RESUMO] Mensagem enviada com sucesso!")
    except Exception as e:
        print(f"[{agora}] [RESUMO] ❌ Erro: {e}")

# Execução 2x por dia — segunda a sábado — 11:30 e 17:30
schedule.every().monday.at("11:30").do(job_resumo)
schedule.every().monday.at("17:30").do(job_resumo)

schedule.every().tuesday.at("11:30").do(job_resumo)
schedule.every().tuesday.at("17:30").do(job_resumo)

schedule.every().wednesday.at("11:30").do(job_resumo)
schedule.every().wednesday.at("17:30").do(job_resumo)

schedule.every().thursday.at("11:30").do(job_resumo)
schedule.every().thursday.at("17:30").do(job_resumo)

schedule.every().friday.at("11:30").do(job_resumo)
schedule.every().friday.at("17:30").do(job_resumo)

schedule.every().saturday.at("11:30").do(job_resumo)
schedule.every().saturday.at("17:30").do(job_resumo)

print("⏰ Bot 'Resumo Diário Privado' iniciado.")
print("   Executará 2x por dia (segunda a sábado): 11:30 e 17:30 (America/Fortaleza).")

# opcional: dispara uma vez ao iniciar para teste
# comente esta linha se não quiser enviar na hora que subir o bot
# job_resumo()

while True:
    schedule.run_pending()
    time.sleep(30)
