import os
import requests
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


# ----- Config via env (GitHub Secrets) -----
SUPERSET_URL = os.environ["SUPERSET_URL"]
SUPERSET_USERNAME = os.environ["SUPERSET_USER"]
SUPERSET_PASSWORD = os.environ["SUPERSET_PASSWORD"]
CHART_ID = int(os.environ["SUPERSET_CHART_ID"])

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL_ID = os.environ["SLACK_CHANNEL_ID"]


# ----- Superset helpers -----
def get_superset_token() -> str:
    """Autentica no Superset e retorna o access_token (JWT)."""
    resp = requests.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={
            "provider": "db",
            "username": SUPERSET_USERNAME,
            "password": SUPERSET_PASSWORD,
            "refresh": True,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["access_token"]


def get_chart_data(token: str, chart_id: int) -> dict:
    """Busca os dados de um chart específico (id=chart_id)."""
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(
        f"{SUPERSET_URL}/api/v1/chart/{chart_id}/data",
        headers=headers,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


# ----- Regras de negócio / mensagem -----
def processar(chart_data: dict) -> str:
    """
    Recebe o JSON do Superset e monta o texto pro Slack.
    Depois a gente adapta com sua lógica real (SLA, esteiras, etc.).
    """
    results = chart_data.get("result", [])
    if not results:
        return "⚠ Nenhum dado retornado pelo chart 5840."

    data = results[0].get("data", [])

    total_registros = len(data)

    mensagem = (
        f"*Monitoramento automático via Superset*\n"
        f"- Chart ID: `5840`\n"
        f"- Total de registros retornados: *{total_registros}*\n\n"
        f"(Depois ajustamos essa mensagem com as métricas certas.)"
    )

    return mensagem


# ----- Slack bot -----
def enviar_slack(texto: str):
    client = WebClient(token=SLACK_BOT_TOKEN)
    try:
        client.chat_postMessage(
            channel=SLACK_CHANNEL_ID,
            text=texto,
        )
    except SlackApiError as e:
        print(f"Erro ao enviar mensagem pro Slack: {e.response['error']}")
        raise


# ----- Main -----
def main():
    print("Autenticando no Superset...")
    token = get_superset_token()

    print("Buscando dados do chart...")
    chart_data = get_chart_data(token, CHART_ID)

    print("Processando resultados...")
    mensagem = processar(chart_data)

    print("Enviando mensagem pro Slack...")
    enviar_slack(mensagem)

    print("Concluído.")


if __name__ == "__main__":
    main()
