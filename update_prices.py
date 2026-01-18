import requests
import logging
from datetime import datetime, date, timedelta
from dateutil import parser
import os
from dotenv import load_dotenv
import holidays

load_dotenv() # Carrega variáveis de ambiente do arquivo .env

# CONFIGURAÇÕES ----------------------
NOTION_TOKEN = os.getenv('NOTION_TOKEN')
VI_ASSETS_DATABASE_ID = os.getenv('VI_ASSETS_DATABASE_ID')
FI_ASSETS_DATABASE_ID = os.getenv('FI_ASSETS_DATABASE_ID')
TWELVE_DATA_API_KEY = os.getenv('TWELVE_DATA_API_KEY')
YAHOO_FINANCE_API_KEY = os.getenv('YAHOO_FINANCE_API_KEY')
BRAPI_TOKEN = os.getenv('BRAPI_TOKEN')
EOD_HISTORICAL_DATA_API_TOKEN = os.getenv('EOD_HISTORICAL_DATA_API_TOKEN')
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')
# -------------------------------------

# Propriedades dos ativos de renda variável
VI_TICKER = 'Ticker'
VI_TYPE = 'Type'
VI_UNIT_PRICE = 'Unit Price'
VI_UPDATE_DATE = 'Last Update'

# Propriedades dos ativos de renda fixa
FI_TYPE = "Type" # Tipo de investimento (CDB, LCI, Tesouro Direto, etc)
FI_INDEXER = "Indexer" # Indexador (Selic, IPCA, CDI)
FI_INDEXER_PCT = "Indexer %" # Percentual do indexador (ex: 100% do CDI)
FI_ADDITIONAL_FIXED_RATE = "Additional Fixed Rate" # Taxa fixa adicional (para IPCA+ ou pré-fixado)
FI_BALANCE = "Balance" # Saldo atual com juros compostos
FI_TOTAL_INVESTED = "Total Invested" # Total investido
FI_TOTAL_WITHDRAWN = "Total Withdrawn" # Total sacado
FI_LAST_UPDATE = "Last Update" # Data da última atualização
FI_INVESTMENT_DATE = "Investment Date" # Data da compra
FI_DUE_DATE = "Due Date" # Data de vencimento
FI_INFLATION = "Inflation" # Inflação (IPCA)

br_holidays = holidays.country_holidays('BR')

notion_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

# CONFIGURAÇÃO DO LOG ----------------
logging.basicConfig(
    filename='update_prices.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
# -------------------------------------

# Valida se as variáveis de ambiente foram carregadas
if not all([NOTION_TOKEN, TWELVE_DATA_API_KEY, YAHOO_FINANCE_API_KEY, BRAPI_TOKEN, VI_ASSETS_DATABASE_ID, FI_ASSETS_DATABASE_ID, EOD_HISTORICAL_DATA_API_TOKEN, ALPHA_VANTAGE_API_KEY, FINNHUB_API_KEY]):
    message = "Erro: Uma ou mais variáveis de ambiente não foram definidas. Verifique seu arquivo .env."
    print(message)
    logging.critical(message)
    exit(1)

def get_usd_brl_rate():
    log_and_print("Buscando cotação USD/BRL...")
    """Busca cotação USD/BRL usando cascata de APIs"""
    # Tentativa 1 - Twelve Data
    try:
        url = f"https://api.twelvedata.com/price?symbol=USD/BRL&apikey={TWELVE_DATA_API_KEY}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if "price" in data:
            return float(data["price"])
    except Exception as e:
        print(f"Erro Twelve Data USD/BRL: {e}")
        
    # Tentativa 2 - Yahoo Finance
    try:
        url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/market/v2/get-quotes"
        querystring = {"symbols": "USDBRL=X", "region": "BR"}
        headers = {
            "X-RapidAPI-Key": YAHOO_FINANCE_API_KEY,
            "X-RapidAPI-Host": "apidojo-yahoo-finance-v1.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers, params=querystring, timeout=10)
        response.raise_for_status()
        data = response.json()
        price = data["quoteResponse"]["result"][0]["regularMarketPrice"]
        if price:
            return float(price)
    except Exception as e:
        print(f"Erro Yahoo Finance USD/BRL: {e}")

    print("Não foi possível obter a cotação USD/BRL.")
    return None

def log_and_print(message, level='info'):
    print(message)
    if level == 'info':
        logging.info(message)
    elif level == 'error':
        logging.error(message)
    elif level == 'warning':
        logging.warning(message)
        
# ------------------ FUNÇÕES GERAIS -------------------------

def get_net_workdays(start_date, end_date):
    """Conta dias úteis entre duas datas excluindo feriados e fins de semana"""
    # Se usar numpy: return np.busday_count(start_date, end_date)
    # Implementação simples com loop (para volumes pequenos de dados é ok):
    days = 0
    current = start_date
    while current < end_date:
        current += timedelta(days=1)
        if current.weekday() < 5 and current not in br_holidays:
            days += 1
    return days

def get_assets_from_notion(DATABASE_ID):
    try:
        url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
        response = requests.post(url, headers=notion_headers, timeout=20)
        response.raise_for_status()
        data = response.json()
        pages = data['results']
        return pages
    except Exception as e:
        log_and_print(f"Erro ao buscar dados do Notion: {e}", level='error')
        return []

def extract_asset_name_from_title(page):
    try:
        properties = page['properties'] # Pega as propriedades da página
        title_field = properties[VI_TICKER] # Pega o campo do título
        title_content = title_field.get("title", []) # O title_content é um array de objetos com o texto do título

        if title_content:
            return title_content[0].get("plain_text", "").strip() # Pega o texto do título
        else:
            return None
    except Exception as e:
        log_and_print(f"Erro ao extrair título da página: {e}", level='error')
        return None
# ---------------- FUNÇÕES RENDA VARIÁVEL -------------------

def get_price_from_apis(ticker):
    """Lógica de cascata priorizando APIs com maior cobertura de ativos e número de requisições gratuitas"""

    # 1) EOD Historical Data (forte global + BR)
    log_and_print("Buscando preço no EOD Historical Data...")
    price = get_from_eod(ticker)
    if price:
        return price

    # 2) BRAPI (forte para Brasil)
    log_and_print(f"EOD Historical Data não encontrou {ticker}, tentando BRAPI...")
    price = get_from_brapi(ticker)
    if price:
        return price

    # 3) Twelve Data (global)
    log_and_print(f"BRAPI não encontrou {ticker}, tentando Twelve Data...")
    price = get_from_twelve_data(ticker)
    if price:
        return price

    # 4) Alpha Vantage (fallback global)
    log_and_print(f"Twelve Data não encontrou {ticker}, tentando Alpha Vantage...")
    price = get_from_alpha_vantage(ticker)
    if price:
        return price

    # 5) Finnhub (fallback global)
    log_and_print(f"Alpha Vantage não encontrou {ticker}, tentando Finnhub...")
    price = get_from_finnhub(ticker)
    if price:
        return price

    # 6) Yahoo Finance (fallback final) - tenta múltiplas variações de ticker
    log_and_print(f"Finnhub não encontrou {ticker}, tentando Yahoo Finance...")
    yahoo_variations = [
        (ticker, "US"),  # Tenta primeiro com o ticker original (US)
    ]
    
    # Se parecer um ticker brasileiro (últimos 2 chars são letras e não tem ponto), adiciona .SA
    if len(ticker) >= 2 and ticker[-2:].isalpha() and "." not in ticker:
        yahoo_variations.append((f"{ticker}.SA", "BR"))
    
    # Adiciona variação com .US
    yahoo_variations.append((f"{ticker}.US", "US"))
    
    for yahoo_ticker, region in yahoo_variations:
        price = get_from_yahoo_finance(yahoo_ticker, region=region)
        if price:
            return price

    log_and_print(f"Não foi possível encontrar preço para {ticker} em nenhuma API.", level='warning')
    return None

# Twelve Data API para buscar o preço dos ativos dos EUA
# https://twelvedata.com/docs/api/price
def get_from_twelve_data(ticker):
    try:
        url = f"https://api.twelvedata.com/price?symbol={ticker}&apikey={TWELVE_DATA_API_KEY}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        price_info = response.json()
        if "price" in price_info:
            return float(price_info["price"])
        else:
            return None
    except Exception as e:
        log_and_print(f"Erro ao buscar preço de {ticker} na Twelve Data: {e}", level='error')
        return None

def get_from_yahoo_finance(ticker, region):
    try:
        # URL da API do Yahoo Finance
        #url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-timeseries"
        url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-summary"

        querystring = {"symbol": ticker, "region": region}
        
        headers = {
            "X-RapidAPI-Key": YAHOO_FINANCE_API_KEY,
            "X-RapidAPI-Host": "apidojo-yahoo-finance-v1.p.rapidapi.com"
        }
        
        response = requests.get(url, headers=headers, params=querystring, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        market_price = data.get("price", {}).get("regularMarketPrice", {}).get("raw", None)
        
        if market_price:
            return float(market_price)        
        else:
            
            log_and_print(f"Preço não encontrado para {ticker} no Yahoo Finance: {data}", level='warning')
            return None
    except Exception as e:
        log_and_print(f"Erro ao buscar preço de {ticker} no Yahoo Finance: {e}", level='error')
        return None

def get_from_brapi(ticker):
    try:
        url = f"https://brapi.dev/api/quote/{ticker.upper()}?token={BRAPI_TOKEN}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])
        if results and "regularMarketPrice" in results[0]:
            return float(results[0]["regularMarketPrice"])
        else:
            log_and_print(f"Preço não encontrado para {ticker} na Brapi: {data}", level='warning')
            return None
    except Exception as e:
        log_and_print(f"Erro ao buscar {ticker} na Brapi: {e}", level='error')
        return None

def get_from_eod(ticker):
    try:
        # tenta primeiro Brasil (.SA) se parecer B3
        query_ticker = ticker
        if ticker[-2:].isalpha() and not "." in ticker:
            query_ticker = f"{ticker}.SA"
        url = f"https://eodhistoricaldata.com/api/real-time/{query_ticker}?api_token={EOD_HISTORICAL_DATA_API_TOKEN}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        # depende do formato JSON, mas normalmente:
        if "close" in data and data["close"]:
            return float(data["close"])
        # fallback para price
        if "price" in data and data["price"]:
            return float(data["price"])
    except Exception as e:
        log_and_print(f"Erro EOD Historical para {ticker}: {e}", level='error')
    return None

def get_from_alpha_vantage(ticker):
    try:
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={ALPHA_VANTAGE_API_KEY}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        price_str = data.get("Global Quote", {}).get("05. price")
        if price_str:
            return float(price_str)
    except Exception as e:
        log_and_print(f"Erro AlphaVantage {ticker}: {e}", level='error')
    return None

def get_from_finnhub(ticker):
    try:
        # ajusta ticker para US ou BR
        query_ticker = ticker
        if ticker[-2:].isalpha() and not "." in ticker:
            # B3 FIIs/ações
            query_ticker = f"{ticker}.SA"
        url = f"https://finnhub.io/api/v1/quote?symbol={query_ticker}&token={FINNHUB_API_KEY}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        price = data.get("c")
        if price:
            return float(price)
    except Exception as e:
        log_and_print(f"Erro Finnhub {ticker}: {e}", level='error')
    return None


def update_variable_income_asset_price_in_notion(page_id, price):
    try:
        url = f"https://api.notion.com/v1/pages/{page_id}"
        data = {
            "properties": {
                VI_UNIT_PRICE: {"number": float(price)},
                VI_UPDATE_DATE: {
                    "date": {
                        "start": datetime.now().isoformat()
                    }
                }
            }
        }
        response = requests.patch(url, headers=notion_headers, json=data)
        
        if response.status_code == 200:
            log_and_print(f"Preço atualizado com sucesso no Notion para {page_id}.")
        else:
            log_and_print(f"Erro ao atualizar o preço no Notion: {response.status_code} - {response.text}", level='error')
        
    except Exception as e:
        log_and_print(f"Erro ao atualizar preço no Notion para {page_id}: {e}", level='error')

def update_variable_income_assets():
    log_and_print("Atualizando valores dos ativos de renda variável...")
    # Atualiza valor dos ativos de renda variável
    pages = get_assets_from_notion(VI_ASSETS_DATABASE_ID)

    if not pages:
        log_and_print("Nenhum ativo encontrado ou erro na consulta!", level='warning')
        return

    for page in pages:
        page_id = page['id'] # Pega o ID da página

        # Agora pegamos o ticker direto do título
        ticker = extract_asset_name_from_title(page)

        if not ticker:
            log_and_print(f"Página {page_id} sem título (Ticker), pulando.", level='warning')
            continue
        
        print(f"Encontrado ticker: {ticker}")

        log_and_print(f"Atualizando {ticker}...")
        price = get_price_from_apis(ticker)

        if price:
            update_variable_income_asset_price_in_notion(page_id, price)
            log_and_print(f"Preço atualizado: {ticker} -> {price}")
        else:
            log_and_print(f"Não foi possível atualizar {ticker}.", level='warning')

# ------------------ API Banco Central ------------------

def get_selic_over():
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados/ultimos/1?formato=json"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        valor = float(data[0]["valor"].replace(",", "."))
        return valor / 100  # taxa em decimal
    except Exception as e:
        print(f"Erro ao buscar Selic Over: {e}")
        return None

def get_ipca_accumulated(purchase_date: date, end_date: date) -> float:
    """Calcula o IPCA acumulado (composto) entre purchase_date e end_date"""
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?"
        f"dataInicial={purchase_date.strftime('%d/%m/%Y')}&"
        f"dataFinal={end_date.strftime('%d/%m/%Y')}&formato=json"
    )
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 404:
            print(f"Nenhum dado IPCA disponível entre {purchase_date} e {end_date}. Retornando 0.")
            return 0.0
        response.raise_for_status()
        data = response.json()
        
        if not data:
            print(f"Sem dados IPCA entre {purchase_date} e {end_date}. Retornando 0.")
            return 0.0

        acumulado = 1.0
        for d in data:
            valor = float(d["valor"].replace(",", "."))
            acumulado *= (1 + valor / 100)

        return acumulado - 1  # retorna em decimal
    except Exception as e:
        print(f"Erro ao buscar IPCA acumulado ({purchase_date} → {end_date}): {e}")
        return 0.0

# ---------------- FUNÇÕES RENDA FIXA -------------------

def calculate_fixed_income(investment, selic, today: date):
    amount = investment["Amount"]
    purchase_date = investment["Purchase Date"]
    due_date = investment["Due Date"]
    days_elapsed = investment["Days Elapsed"]
    indexer = investment["Indexer"]
    indexer_pct = investment["Indexer %"] / 100  # Converte para decimal
    fixed_rate = investment["Additional Fixed Rate"] / 100  # Converte para decimal
    investment_type = investment["Type"]
    
    if not amount or not purchase_date:
        return None, None

    # Se já venceu, não calcular
    if due_date and today > due_date:
        print(f"Investimento vencido ({due_date}), pulando cálculo.")
        return None, None

    end_date = min(today, due_date) if due_date else today
    #days_elapsed = (end_date - purchase_date).days
    if days_elapsed < 0:
        return None, None

    current_amount = amount
    inflation_accum = None

    # CDI ≈ Selic - 0.1% a.a
    cdi = selic - 0.001
    
    if indexer == "Selic" or investment_type == "Tesouro Selic":
        taxa_diaria = selic / 252
        taxa_final = taxa_diaria * indexer_pct
        current_amount = amount * ((1 + taxa_final) ** days_elapsed)
    
    elif indexer == "IPCA" or investment_type in ["Tesouro IPCA+"]:
        ipca = get_ipca_accumulated(purchase_date, today)
        inflation_accum = ipca
        taxa_fixa_diaria = (1 + fixed_rate) ** (1 / 252) - 1
        current_amount = amount * ((1 + taxa_fixa_diaria) ** days_elapsed)
        if ipca is not None:
            current_amount *= (1 + ipca)
    
    return current_amount, inflation_accum


def update_fixed_income_assets():
    log_and_print("Atualizando ativos de renda fixa...")

    today = date.today()
    selic = get_selic_over()
    
    if selic is None:
        log_and_print(f"Não foi possível obter Selic.", level="error")

    pages = get_assets_from_notion(FI_ASSETS_DATABASE_ID)
    if not pages:
        log_and_print("Nenhum ativo de renda fixa encontrado.", level="warning")
        return

    for page in pages:
        props = page["properties"]
        page_id = page["id"]

        try:
            # Datas
            if not props[FI_INVESTMENT_DATE]["rollup"]["date"]:
                log_and_print(f"Ativo {page_id} sem data de investimento. Pulando.")
                continue
            investment_date_str = props[FI_INVESTMENT_DATE]["rollup"]["date"]["start"]
            due_date_str = props[FI_DUE_DATE]["date"]["start"]
            last_update_str = props[FI_LAST_UPDATE]["date"]["start"] if props["Last Update"]["date"] else None

            investment_date = parser.parse(investment_date_str).date()
            due_date = parser.parse(due_date_str).date()
            last_update = parser.parse(last_update_str).date() if last_update_str else investment_date

            end_date = min(today, due_date)
            if last_update >= end_date:
                log_and_print(f"Ativo {page_id} já atualizado. Pulando.")
                continue

            days = (end_date - last_update).days
            if days <= 0:
                continue
            
            # Valores Base
            total_invested = props[FI_TOTAL_INVESTED]["rollup"]["number"] or 0
            total_withdrawn = props[FI_TOTAL_WITHDRAWN]["rollup"]["number"] or 0
            balance = props[FI_BALANCE]["number"] or 0
            
            principal = balance if balance > 0 else (total_invested - total_withdrawn)
            if principal <= 0:
                log_and_print(f"Ativo {page_id} sem saldo. Pulando.")
                continue
    
            # Indexadores
            indexer = props[FI_INDEXER]["select"]["name"]
            indexer_pct = props[FI_INDEXER_PCT]["number"] or 1.0
            fixed_rate = props[FI_ADDITIONAL_FIXED_RATE]["number"] or 0.0

            # Cálculo
            new_balance = principal
            
            if indexer in ("SELIC", "CDI") and selic is not None:
                base_rate = selic if indexer == "SELIC" else max(selic - 0.001, 0)
                annual_rate = base_rate * indexer_pct + fixed_rate
                daily_rate = (1 + annual_rate) ** (1 / 252) - 1
                new_balance *= ((1 + daily_rate) ** days)

            elif indexer == "IPCA":
                ipca_acc = get_ipca_accumulated(last_update, end_date) or 0
                if ipca_acc:
                    new_balance *= (1 + ipca_acc)

                real_growth = (1 + fixed_rate) ** (days / 365)
                new_balance *= real_growth

            else:
                log_and_print(f"Indexador desconhecido: {indexer}", level="warning")
                continue

            # Atualiza Notion
            update_url = f"https://api.notion.com/v1/pages/{page_id}"
            payload = {
                "properties": {
                    FI_BALANCE: {"number": round(new_balance, 2)},
                    FI_LAST_UPDATE: {"date": {"start": datetime.now().isoformat()}}
                }
            }

            resp = requests.patch(update_url, headers=notion_headers, json=payload, timeout=20)
            resp.raise_for_status()

            log_and_print(f"Renda fixa atualizada: {round(new_balance, 2)}")

        except Exception as e:
            log_and_print(f"Erro ao atualizar renda fixa {page_id}: {e}", level="error")


def main():
    log_and_print("Iniciando atualização de investimentos...")
    
    #update_variable_income_assets()

    update_fixed_income_assets()
    
    log_and_print("Atualização concluída.")

if __name__ == "__main__":
    main()