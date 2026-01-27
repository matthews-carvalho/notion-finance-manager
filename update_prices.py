import requests
import logging
from datetime import datetime, date, timedelta
from dateutil import parser
import os
from dotenv import load_dotenv
import holidays
from typing import Optional, Tuple
import re

load_dotenv() # Carrega variáveis de ambiente do arquivo .env

# CONFIGURAÇÕES ----------------------
NOTION_TOKEN = os.getenv('NOTION_TOKEN')
VI_ASSETS_DATABASE_ID = os.getenv('VI_ASSETS_DATABASE_ID')
VI_FOREIGN_ASSETS_DATABASE_ID = os.getenv('VI_FOREIGN_ASSETS_DATABASE_ID')
FI_CONTRACTS_DATABASE_ID = os.getenv('FI_CONTRACTS_DATABASE_ID')
FI_CONTRIBUTIONS_DATABASE_ID = os.getenv('FI_CONTRIBUTIONS_DATABASE_ID')
FI_ASSETS_DATABASE_ID = os.getenv('FI_ASSETS_DATABASE_ID')
FI_WITHDRAWALS_DATABASE_ID = os.getenv('FI_WITHDRAWALS_DATABASE_ID')
FI_ALLOCATIONS_DATABASE_ID = os.getenv('FI_ALLOCATIONS_DATABASE_ID')
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

# Propriedades dos contratos de renda fixa
FI_TYPE = "Type" # Tipo de investimento (Tesouro Direto, Renda Fixa, etc)
FI_INDEXER = "Indexer" # Indexador (SELIC, IPCA, CDI)
FI_INDEXER_PCT = "Indexer %" # Percentual do indexador (ex: 100% do CDI)
FI_ADDITIONAL_FIXED_RATE = "Additional Fixed Rate" # Taxa fixa adicional (para IPCA+ ou pré-fixado)
FI_BALANCE = "Balance" # Saldo atual com juros compostos
FI_VARIATION = "Variation" # Percentual de variação do saldo
FI_LAST_UPDATE = "Last Update" # Data da última atualização
FI_CONTRIBUTION_DATE = "Contribution Date" # Data da compra
FI_DUE_DATE = "Due Date" # Data de vencimento
FI_INFLATION = "Inflation" # Inflação (IPCA)
FI_CLOSED = "Closed" # Fechado
FI_CONTRACT_UNIQUE_ID = "ID" # Propriedade unique id on contracts (tie-breaker)

# Propriedades dos aportes de renda fixa
FIC_ASSET = "Asset"                     # Relation → Fixed Income Assets
FIC_CONTRACT = "Contract"               # Relation → Fixed Income Contracts
FIC_AMOUNT = "Amount"                   # Number
FIC_DATE = "Date"                       # Date
FIC_ADDITIONAL_FIXED_RATE = "Additional Fixed Rate"  # Number
FIC_CONTRIBUTION_REL = "Contribution"  # Relation

# Propriedades da tabela de saques de renda fixa
FIW_ASSET = "Asset"
FIW_AMOUNT = "Amount"
FIW_PROCESSED = "Processed"
FIW_PROCESSING_DATE = "Processing Date"
FIW_ALLOCATIONS_REL = "Allocations"
FIW_PROCESSED_AMOUNT = "Processed Amount"

# Propriedades de Allocation table
FIA_WITHDRAWAL_REL = "Withdrawal"
FIA_CONTRACT_REL = "Contract"
FIA_AMOUNT = "Amount"
FIA_OPERATION_DATE = "Date"

br_holidays = holidays.country_holidays('BR')
BUSY_DAYS_IN_YEAR = 252

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
if not all([NOTION_TOKEN, TWELVE_DATA_API_KEY, YAHOO_FINANCE_API_KEY, BRAPI_TOKEN, VI_ASSETS_DATABASE_ID, VI_FOREIGN_ASSETS_DATABASE_ID, FI_CONTRACTS_DATABASE_ID, EOD_HISTORICAL_DATA_API_TOKEN, ALPHA_VANTAGE_API_KEY, FINNHUB_API_KEY]):
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

def get_net_workdays(start_date, end_date) -> int:
    """Conta dias úteis entre duas datas excluindo feriados e fins de semana"""
    # Se usar numpy: return np.busday_count(start_date, end_date)
    # Implementação simples com loop (para volumes pequenos de dados é ok):
    days = 0
    current_date = start_date
    while current_date < end_date:
        current_date += timedelta(days=1)
        if current_date.weekday() < 5 and current_date not in br_holidays:
            days += 1
    return days

def get_pages_from_notion(DATABASE_ID) -> Optional[list]:
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

def extract_asset_name_from_title(page) -> Optional[str]:
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
    
def is_brazilian_ticker(ticker: str) -> bool:
    if not ticker:
        return False

    ticker = ticker.upper().strip()

    pattern = r"^[A-Z0-9]{4}\d{1,2}$"
    return bool(re.match(pattern, ticker))    

# ---------------- FUNÇÕES RENDA VARIÁVEL -------------------

def get_price_from_apis(ticker) -> Optional[float]:
    """Lógica de cascata priorizando APIs com maior cobertura de ativos e número de requisições gratuitas"""

    # 1) EOD Historical Data (forte global + BR)
    log_and_print("Buscando preço no EOD Historical Data...")
    eod_variations = []
    
    # Se parecer um ticker brasileiro, adiciona .SA
    if is_brazilian_ticker(ticker):
        eod_variations.append(f"{ticker}.SA")
    
    # Adiciona variação com .US
    eod_variations.append(f"{ticker}.US")
    
    # Adiciona variação sem sufixo
    eod_variations.append(ticker)
    
    for eod_ticker in eod_variations:
        price = get_from_eod(eod_ticker)
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
    
    # Se parecer um ticker brasileiro, adiciona .SA
    if is_brazilian_ticker(ticker):
        yahoo_variations.append((f"{ticker}.SA", "BR"))
    
    # Adiciona variação com .US
    yahoo_variations.append((f"{ticker}.US", "US"))
    
    for eod_ticker, region in yahoo_variations:
        price = get_from_yahoo_finance(eod_ticker, region=region)
        if price:
            return price

    log_and_print(f"Não foi possível encontrar preço para {ticker} em nenhuma API.", level='warning')
    return None

# Twelve Data API para buscar o preço dos ativos dos EUA
# https://twelvedata.com/docs/api/price
def get_from_twelve_data(ticker) -> Optional[float]:
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

def get_from_yahoo_finance(ticker, region) -> Optional[float]:
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

def get_from_brapi(ticker) -> Optional[float]:
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

def get_from_eod(ticker) -> Optional[float]:
    try:
        url = f"https://eodhistoricaldata.com/api/eod/{ticker}?api_token={EOD_HISTORICAL_DATA_API_TOKEN}&fmt=json"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            log_and_print("EOD retornou lista vazia")
            return None
        last = data[-1]
        price = last.get("close")
        if price:
            return float(price)
    except Exception as e:
        log_and_print(f"Erro EOD Historical para {ticker}: {e}", level='error')
    return None

def get_from_alpha_vantage(ticker) -> Optional[float]:
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

def get_from_finnhub(ticker) -> Optional[float]:
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

def update_variable_income_assets(database_id):
    log_and_print("Atualizando valores dos ativos de renda variável...")
    # Atualiza valor dos ativos de renda variável
    pages = get_pages_from_notion(database_id)

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
            log_and_print(f"Preço atualizado: {ticker} -> R${price}")
        else:
            log_and_print(f"Não foi possível atualizar {ticker}.", level='warning')

# ------------------ API Banco Central ------------------

def get_selic_over() -> Optional[float]:
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

def get_cdi_rate() -> Optional[float]:
    """
    Retorna a última taxa CDI (DI Over anualizada) disponível.
    Busca uma janela de dias para garantir retorno mesmo em feriados ou antes da divulgação.
    
    Fonte: Banco Central do Brasil - SGS série 4389 (Taxa DI anualizada base 252)
    Retorno:
        (data_da_taxa, taxa_anual_decimal)
    """
    try:
        today = date.today()
        # Voltamos 10 dias para garantir que pegaremos o último dia útil 
        # mesmo se houver feriados prolongados (ex: Carnaval)
        start_date = today - timedelta(days=10)
        
        url = (
            "https://api.bcb.gov.br/dados/serie/bcdata.sgs.4389/dados"
            f"?formato=json&dataInicial={start_date.strftime("%d/%m/%Y")}"
            f"&dataFinal={today.strftime("%d/%m/%Y")}"
        )

        response = requests.get(url, timeout=15)
        response.raise_for_status()

        data = response.json()

        if not data:
            print("CDI: Nenhum dado retornado pelo BCB na janela solicitada.")
            return None

        # Pega o ÚLTIMO elemento da lista, que é a data mais recente disponível
        last = data[-1]

        rate_value = float(last["valor"]) / 100  # Ex: 11.15 viram 0.1115

        return rate_value

    except Exception as e:
        print(f"Erro ao buscar CDI: {e}")
        return None

def get_accumulated_ipca(purchase_date: date, end_date: date) -> float:
    """Calcula o IPCA acumulado (composto) entre purchase_date e end_date"""
    # Força o dia 1 para garantir que a API retorne o índice do mês da compra
    # O BCB registra o IPCA sempre no dia 01/MM/AAAA
    start_query = purchase_date.replace(day=1)
    
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?"
        f"dataInicial={start_query.strftime('%d/%m/%Y')}&"
        f"dataFinal={end_date.strftime('%d/%m/%Y')}&formato=json"
    )
    try:
        response = requests.get(url, timeout=10)      
        # Tratamento para quando não há dados (ex: data muito recente onde o IPCA ainda não saiu)
        if response.status_code == 404 or not response.json():
            # Se for data recente (ex: mês atual), é normal não ter IPCA ainda.
            print(f"Nenhum dado IPCA disponível entre {start_query} e {end_date}. Retornando 0.")
            return 0.0
        response.raise_for_status()
        data = response.json()
        
        if not data:
            print(f"Sem dados IPCA entre {start_query} e {end_date}. Retornando 0.")
            return 0.0

        accumulated = 1.0
        for d in data:
            value = float(d["valor"].replace(",", "."))
            accumulated *= (1 + value / 100)

        return accumulated - 1  # retorna em decimal
    except Exception as e:
        print(f"Erro ao buscar IPCA acumulado ({start_query} → {end_date}): {e}")
        return 0.0

# ---------------- FUNÇÕES RENDA FIXA -------------------

def update_fixed_income_contracts():
    log_and_print("Atualizando ativos de renda fixa...")

    today = date.today()
    selic = get_selic_over()
    cdi = get_cdi_rate()

    pages = get_pages_from_notion(FI_CONTRACTS_DATABASE_ID)
    if not pages:
        log_and_print("Nenhum ativo de renda fixa encontrado.", level="warning")
        return

    for page in pages:
        props = page["properties"]
        page_id = page["id"]

        try:
            # Indexadores
            indexer_rollup = props[FI_INDEXER]["rollup"]["array"]
            if not indexer_rollup:
                log_and_print(f"Ativo {page_id} sem indexador. Pulando.")
                continue  
            else:
                indexer = indexer_rollup[0]["select"]["name"].upper()
          
            indexer_pct = props[FI_INDEXER_PCT]["rollup"]["number"] or 1.0
            fixed_rate = props[FI_ADDITIONAL_FIXED_RATE]["number"] or 0.0
            
            # Datas
            if not props[FI_CONTRIBUTION_DATE]["date"]:
                log_and_print(f"Ativo {page_id} sem data de aporte. Pulando.")
                continue
            last_update_str = props[FI_LAST_UPDATE]["date"]["start"] if props[FI_LAST_UPDATE]["date"] else None

            contribution_date = parser.parse(props[FI_CONTRIBUTION_DATE]["date"]["start"]).date()
            due_date = None

            rollup = props[FI_DUE_DATE]["rollup"]
            items = rollup["array"]

            if items:
                date_obj = items[0]["date"]
                if date_obj and date_obj["start"]:
                    due_date = parser.parse(date_obj["start"]).date()

            start_date = parser.parse(last_update_str).date() if last_update_str else contribution_date

            if start_date >= today:
                log_and_print(f"Ativo {page_id} já atualizado. Pulando.")
                continue
            
            end_date = min(today, due_date) if due_date else today
            
            balance = props[FI_BALANCE]["number"] or 0
            
            if start_date >= end_date:
                log_and_print(f"Ativo {page_id} vencido ou sem período para calcular.")
                new_balance = balance
            else:
                factor = 1.0               
                
                if indexer in ("SELIC", "CDI"):
                    interval_workdays = get_net_workdays(start_date, end_date)
                    annual_rate = selic if indexer == "SELIC" else cdi
                    if annual_rate is None:
                        log_and_print(f"Taxa anual do indexador '{indexer}' não encontrada. Pulando.", level="warning")
                        continue
                    
                    # Calcula taxa efetiva anual: (indexer * percentual) + spread fixo
                    # Ex: CDI 100% + 5% = (CDI * 1.0) + 0.05
                    effective_annual_rate = (annual_rate * indexer_pct) + fixed_rate
                    
                    # Converte taxa anual para fator diário e compõe pelos dias úteis
                    daily_factor = (1 + effective_annual_rate) ** (1 / BUSY_DAYS_IN_YEAR)
                    factor = daily_factor ** interval_workdays
                    
                elif indexer == "IPCA":
                    interval_workdays = get_net_workdays(start_date, end_date)
                    acc_ipca = get_accumulated_ipca(start_date, end_date)
                    real_factor = (1 + fixed_rate) ** (interval_workdays / BUSY_DAYS_IN_YEAR)                   
                    factor = (1 + acc_ipca) * real_factor

                else:
                    log_and_print(f"Indexador desconhecido: {indexer}", level="warning")
                    continue
                
                new_balance = balance * factor
                    
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

            log_and_print(f"Renda fixa atualizada: R${round(balance, 2)} -> R${round(new_balance, 2)}")

        except Exception as e:
            log_and_print(f"Erro ao atualizar renda fixa {page_id}: {e}", level="error")

def get_unlinked_fixed_income_contributions():
    """
    Retorna aportes de renda fixa que ainda não possuem contrato vinculado
    """
    url = f"https://api.notion.com/v1/databases/{FI_CONTRIBUTIONS_DATABASE_ID}/query"

    payload = {
        "filter": {
            "property": FIC_CONTRACT,
            "relation": {
                "is_empty": True
            }
        }
    }

    response = requests.post(url, headers=notion_headers, json=payload, timeout=20)
    response.raise_for_status()
    data = response.json()
    pages = data["results"]
    return pages

def create_contract_from_contribution(contribution_page):
    props = contribution_page["properties"]
    contribution_id = contribution_page["id"]

    # --- Validações básicas ---
    if not props[FIC_ASSET]["relation"]:
        raise ValueError("Aporte sem ativo vinculado")

    asset_id = props[FIC_ASSET]["relation"][0]["id"]
    amount = props[FIC_AMOUNT]["number"]
    contribution_date = parser.parse(
        props[FIC_DATE]["date"]["start"]
    ).date()

    additional_fixed_rate = props[FIC_ADDITIONAL_FIXED_RATE]["number"] or 0.0

    # --- Criação do contrato ---
    payload = {
        "parent": {
            "database_id": FI_CONTRACTS_DATABASE_ID
        },
        "properties": {
            "Name": {
                "title": [
                    {
                        "text": {
                            "content": f"Contract {contribution_date.strftime('%Y-%m-%d')}"
                        }
                    }
                ]
            },
            "Asset": {
                "relation": [{"id": asset_id}]
            },
            FIC_CONTRIBUTION_REL: {
                "relation": [{"id": contribution_id}]
            },
            FI_CONTRIBUTION_DATE: {
                "date": {"start": contribution_date.isoformat()}
            },
            FI_ADDITIONAL_FIXED_RATE: {
                "number": additional_fixed_rate
            },
            FI_BALANCE: {
                "number": amount
            }
        }
    }

    response = requests.post(
        "https://api.notion.com/v1/pages",
        headers=notion_headers,
        json=payload,
        timeout=20
    )
    response.raise_for_status()

def process_fixed_income_contributions():
    log_and_print("Processando aportes de renda fixa...")

    contributions = get_unlinked_fixed_income_contributions()

    if not contributions:
        log_and_print("Nenhum aporte novo para processar.")
        return

    for contribution in contributions:
        try:
            create_contract_from_contribution(contribution)

            log_and_print(
                f"Contrato criado e vinculado com sucesso para aporte {contribution['id']}"
            )

        except Exception as e:
            log_and_print(
                f"Erro ao processar aporte {contribution['id']}: {e}",
                level="error"
            )

def get_unprocessed_withdrawals() -> list:
    """
    Retorna saques não processados (Processed checkbox == False)
    """
    url = f"https://api.notion.com/v1/databases/{FI_WITHDRAWALS_DATABASE_ID}/query"
    payload = {
        "filter": {
            "property": FIW_PROCESSED,
            "checkbox": {"equals": False}
        }
    }
    response = requests.post(url, headers=notion_headers, json=payload, timeout=20)
    response.raise_for_status()
    data = response.json()
    pages = data["results"]
    return pages

def get_contracts_lifo_for_asset(asset_id: str) -> list:
    """
    Busca contratos do asset ordenados por Contribution Date desc e ID desc (LIFO).
    """
    url = f"https://api.notion.com/v1/databases/{FI_CONTRACTS_DATABASE_ID}/query"

    payload = {
        "filter": {
            "property": "Asset",
            "relation": {"contains": asset_id}
        },
        "sorts": [
            {"property": FI_CONTRIBUTION_DATE, "direction": "descending"},
            {"property": FI_CONTRACT_UNIQUE_ID, "direction": "descending"}
        ],
        "page_size": 100
    }

    response = requests.post(url, headers=notion_headers, json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    pages = data["results"]
    return pages


def compute_withdrawal_allocations_for_asset(asset_id: str, amount: float) -> list:
    """
    Calcula (em memória) a lista de alocações (contract_id, deduction)
    e verifica se há saldo suficiente. Não grava nada.
    Retorna lista de dicts: [{"contract_id": id, "deduction": X}, ...]
    """
    contracts = get_contracts_lifo_for_asset(asset_id)
    remaining = float(amount)
    allocations = []

    for contract in contracts:
        if remaining <= 0:
            break
        balance = contract["properties"].get(FI_BALANCE, {}).get("number") or 0.0
        if balance <= 0:
            continue
        deduct = min(balance, remaining)
        allocations.append({"contract_id": contract["id"], "deduction": round(deduct, 2)})
        remaining -= deduct

    return allocations


def create_allocation_record(withdrawal_id: str, contract_id: str, amount: float):
    """
    Cria um registro na tabela Withdrawal Allocations ligado ao saque e contrato.
    Retorna allocation_id.
    """
    payload = {
        "parent": {"database_id": FI_ALLOCATIONS_DATABASE_ID},
        "properties": {
            "Name": {
                "title": [{"text": {"content": f"Allocation {withdrawal_id} -> {contract_id}"}}]
            },
            FIA_WITHDRAWAL_REL: {"relation": [{"id": withdrawal_id}]},
            FIA_CONTRACT_REL: {"relation": [{"id": contract_id}]},
            FIA_AMOUNT: {"number": round(amount, 2)},
            FIA_OPERATION_DATE: {"date": {"start": datetime.now().isoformat()}}
        }
    }

    response = requests.post("https://api.notion.com/v1/pages", headers=notion_headers, json=payload, timeout=20)
    response.raise_for_status()
    data = response.json()
    allocation_id = data["id"]
    return allocation_id

def update_contract_balance_and_withdrawn(contract_id: str, deduction: float):
    """
    Subtrai deduction do Balance na página do contrato.
    """
    # Buscar a página atual para ler as propriedades
    url_get = f"https://api.notion.com/v1/pages/{contract_id}"
    resp_get = requests.get(url_get, headers=notion_headers, timeout=20)
    resp_get.raise_for_status()
    contract = resp_get.json()
    props = contract["properties"]

    current_balance = props.get(FI_BALANCE, {}).get("number") or 0.0

    new_balance = round(max(0.0, current_balance - deduction), 2)

    payload = {
        "properties": {
            FI_BALANCE: {"number": new_balance}
        }
    }
    
    # Apenas define FI_CLOSED como True quanto balance chegar a 0
    if new_balance == 0:
        payload["properties"][FI_CLOSED] = {"checkbox": True}

    resp = requests.patch(f"https://api.notion.com/v1/pages/{contract_id}", headers=notion_headers, json=payload, timeout=20)
    resp.raise_for_status()

def link_withdrawal_to_allocations(withdrawal_id: str, allocation_ids: list, processed_amount: float):
    """
    Atualiza a página do saque para relacionar as alocações (campo Allocations), salvar data, valor processado e marca como processado.
    """
    url = f"https://api.notion.com/v1/pages/{withdrawal_id}"
    payload = {
        "properties": {
            FIW_ALLOCATIONS_REL: {"relation": [{"id": aid} for aid in allocation_ids]},
            FIW_PROCESSED: {"checkbox": True},
            FIW_PROCESSED_AMOUNT: {"number": round(processed_amount, 2)},
            FIW_PROCESSING_DATE: {"date": {"start": datetime.now().isoformat()}}
        }
    }
    response = requests.patch(url, headers=notion_headers, json=payload, timeout=20)
    response.raise_for_status()


def process_withdrawals_lifo():
    log_and_print("Processando saques (LIFO)...")
    withdrawals = get_unprocessed_withdrawals()
    if not withdrawals:
        log_and_print("Nenhum saque não-processado.")
        return

    for wd in withdrawals:
        try:
            props = wd["properties"]
            withdrawal_id = wd["id"]

            if not props.get(FIW_ASSET, {}).get("relation"):
                log_and_print(f"Saque {withdrawal_id} sem asset definido. Pulando.", level="warning")
                continue

            asset_id = props[FIW_ASSET]["relation"][0]["id"]
            amount = props.get(FIW_AMOUNT, {}).get("number") or 0.0

            # calcula alocações em memória e valida saldo
            allocations = compute_withdrawal_allocations_for_asset(asset_id, amount)
            
            # calcula o valor processado total (antes do loop para garantir que está sempre definido)
            processed_amount = round(sum(allocation["deduction"] for allocation in allocations), 2)

            # Persiste: cria alocações e atualiza contratos
            allocation_ids = []
            for alloc in allocations:
                contract_id = alloc["contract_id"]
                deduct = alloc["deduction"]

                # cria allocation record
                alloc_id = create_allocation_record(withdrawal_id, contract_id, deduct)
                allocation_ids.append(alloc_id)
                
                # atualiza contrato (balance e total withdrawn)
                update_contract_balance_and_withdrawn(contract_id, deduct)
            
            # linka o saque às alocações e marca processed
            link_withdrawal_to_allocations(withdrawal_id, allocation_ids, processed_amount)

            log_and_print(f"Saque {withdrawal_id} processado com sucesso. Alocações: {allocation_ids}")

        except Exception as e:
            log_and_print(f"Erro ao processar saque {wd['id']}: {e}", level="error")

def main():
    log_and_print("Iniciando atualização de investimentos...")
    
    update_variable_income_assets(VI_ASSETS_DATABASE_ID)
    update_variable_income_assets(VI_FOREIGN_ASSETS_DATABASE_ID)

    process_fixed_income_contributions()
    process_withdrawals_lifo()
    update_fixed_income_contracts()
    
    log_and_print("Atualização concluída.")

if __name__ == "__main__":
    main()