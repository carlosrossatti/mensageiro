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

# Canal das esteiras
SLACK_CHANNEL_ESTEIRAS = "#monitoramento-privado"

DB_CFG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# === Query Produto NOVO (esteiras) ===
SQL_NOVO = """
select
  oo.operationsteptype as last_steptype,
  count(*) as qtd,
  sum(cl.grossvalue) as sum_gross
from tudoprod.contract cc
join tudoprod.enrollment ee
  on cc.enrollmentid = ee.id
 and ee.agreementid = '10'
join tudoprod.contractloan cl
  on cl.contractid = cc.id
 and cl.loantype = 'NEW'
left join lateral (
  select *
  from tudoprod.operationsteplog oo
  where oo.contractid = cc.id
  order by oo.logdate desc
  limit 1
) oo on true
where (oo.logdate at time zone 'America/Fortaleza')::date =
      (now() at time zone 'America/Fortaleza')::date
group by oo.operationsteptype
order by count(*) desc;
"""

# === Query Produto REFIN (esteiras) ===
SQL_REFIN = """
select
  oo.operationsteptype as last_steptype,
  count(*) as qtd,
  sum(cl.grossvalue) as sum_gross
from tudoprod.contract cc
join tudoprod.enrollment ee
  on cc.enrollmentid = ee.id
 and ee.agreementid = '10'
join tudoprod.contractloan cl
  on cl.contractid = cc.id
 and cl.loantype = 'REFIN'
left join lateral (
  select *
  from tudoprod.operationsteplog oo
  where oo.contractid = cc.id
  order by oo.logdate desc
  limit 1
) oo on true
where (oo.logdate at time zone 'America/Fortaleza')::date =
      (now() at time zone 'America/Fortaleza')::date
group by oo.operationsteptype
order by count(*) desc;
"""

# === Query Produto PORTABILITY (esteiras) ===
SQL_PORTABILITY = """
select
  oo.operationsteptype as last_steptype,
  count(*) as qtd,
  sum(cl.grossvalue) as sum_gross
from tudoprod.contract cc
join tudoprod.enrollment ee
  on cc.enrollmentid = ee.id
 and ee.agreementid = '10'
join tudoprod.contractloan cl
  on cl.contractid = cc.id
 and cl.loantype = 'PORTABILITY'
left join lateral (
  select *
  from tudoprod.operationsteplog oo
  where oo.contractid = cc.id
  order by oo.logdate desc
  limit 1
) oo on true
where (oo.logdate at time zone 'America/Fortaleza')::date =
      (now() at time zone 'America/Fortaleza')::date
group by oo.operationsteptype
order by count(*) desc;
"""

# ============================================
# Função para garantir que a VPN/DB esteja acessível
# ============================================
def wait_for_vpn_and_db(host: str, port: int = 5432, interval: int = 900):
    """
    interval = 900s = 15 minutos
    """
    print(f"[BOOT] Verificando acesso ao DB ({host}:{port})...")

    while True:
        try:
            with socket.create_connection((host, port), timeout=5):
                print("[BOOT] Banco/VPN acessível. Seguindo execução.")
                return
        except OSError:
            agora = datetime.now(TZ).strftime("%d/%m/%Y %H:%M:%S")
            print(f"[{agora}] Banco ainda inacessível (VPN desligada?). Nova tentativa em 15 minutos...")
            time.sleep(interval)

# === Execução SQL (Postgres / tudoprod) ===
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

# === Envio ao Slack — esteiras (por produto) ===
def post_to_slack(df: pd.DataFrame, produto_label: str):
    agora = datetime.now(TZ).strftime("%d/%m/%Y %H:%M")
    header = (
        f"*Monitoramento de Esteiras — {produto_label}*\n"
        f"📅 {agora} (America/Fortaleza)\n\n"
    )

    if df.empty:
        text = header + "_Sem registros para o dia atual._"
    else:
        lines = [
            f"• `{r['last_steptype']}` — {int(r['qtd'])} contratos — R$ {float(r['sum_gross']):,.2f}"
            .replace(",", "X").replace(".", ",").replace("X", ".")
            for _, r in df.iterrows()
        ]
        text = header + "\n".join(lines)

    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_TOKEN}"},
        json={"channel": SLACK_CHANNEL_ESTEIRAS, "text": text, "mrkdwn": True},
        timeout=20,
    )
    if not r.ok or not r.json().get("ok"):
        print("Erro ao enviar mensagem para o Slack (esteiras):", r.text)

# === Janela de execução esteiras: 06:00–20:00, exceto domingo ===
def dentro_da_janela_execucao(agora: datetime) -> bool:
    if agora.weekday() == 6:  # domingo
        return False
    return 6 <= agora.hour < 20

# === Jobs Esteiras ===
def job_novo():
    agora = datetime.now(TZ)
    if not dentro_da_janela_execucao(agora):
        print(f"[{agora}] [NOVO] Fora da janela. Aguardando próxima execução.")
        return
    try:
        print(f"[{agora}] [NOVO] Iniciando...")
        df = run_query(SQL_NOVO)
        post_to_slack(df, "Produto NOVO (Consignado Privado)")
        print(f"[{agora}] [NOVO] Enviado com sucesso!")
    except Exception as e:
        print(f"[{agora}] [NOVO] ❌ Erro: {e}")

def job_refin():
    agora = datetime.now(TZ)
    if not dentro_da_janela_execucao(agora):
        print(f"[{agora}] [REFIN] Fora da janela. Aguardando próxima execução.")
        return
    try:
        print(f"[{agora}] [REFIN] Iniciando...")
        df = run_query(SQL_REFIN)
        post_to_slack(df, "Produto REFIN (Consignado Privado)")
        print(f"[{agora}] [REFIN] Enviado com sucesso!")
    except Exception as e:
        print(f"[{agora}] [REFIN] ❌ Erro: {e}")

def job_portability():
    agora = datetime.now(TZ)
    if not dentro_da_janela_execucao(agora):
        print(f"[{agora}] [PORTABILITY] Fora da janela. Aguardando próxima execução.")
        return
    try:
        print(f"[{agora}] [PORTABILITY] Iniciando...")
        df = run_query(SQL_PORTABILITY)
        post_to_slack(df, "Produto PORTABILITY (Consignado Privado)")
        print(f"[{agora}] [PORTABILITY] Enviado com sucesso!")
    except Exception as e:
        print(f"[{agora}] [PORTABILITY] ❌ Erro: {e}")

# === AGENDAMENTOS ESTEIRAS ===

schedule.every(30).minutes.do(job_novo)          # NOVO: 30 min
schedule.every(40).minutes.do(job_refin)         # REFIN: 40 min
schedule.every(50).minutes.do(job_portability)   # PORTABILITY: 50 min

print("⏰ Bot Monitoramento Esteiras Privado iniciado.")
print("   Esteiras: NOVO(30min) | REFIN(40min) | PORT(50min) — 06:00–20:00, exceto domingos.")

# Rodar uma vez ao iniciar (opcional)
job_novo()
job_refin()
job_portability()

while True:
    schedule.run_pending()
    time.sleep(30)
