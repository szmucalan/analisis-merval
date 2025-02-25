import yfinance as yf
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import os
import json
import pytz

# Configuración de Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = os.getenv('GOOGLE_CREDENTIALS')
creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
sheet = client.open_by_key('1VxrU9jQnBoShNWY1zfbMSFCWc-tISgolOGm-zUbY_4Q')
data_sheet = sheet.sheet1

# Zona horaria de Buenos Aires
buenos_aires_tz = pytz.timezone('America/Argentina/Buenos_Aires')
now = datetime.now(buenos_aires_tz)

# Verificar horario de trading (Lunes a Viernes, 11:00-18:00 ART)
weekday = now.weekday()
hour = now.hour
if weekday >= 5 or hour < 11 or hour >= 18:
    print(f"No se actualiza: Fuera del horario de trading (Lun-Vie 11:00-18:00 ART). Día: {weekday}, Hora: {hour}")
    exit()

# Verificación de última actualización
try:
    last_update_str = data_sheet.acell('A1').value
    if last_update_str.startswith('*Última actualización*: '):
        last_update_str = last_update_str.replace('*Última actualización*: ', '')
    last_update = datetime.strptime(last_update_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=buenos_aires_tz) if last_update_str else None
except Exception as e:
    print(f"Error al leer A1: {e}")
    last_update = None

if last_update:
    time_diff = now - last_update
    print(f"Diferencia de tiempo: {time_diff}")

# Lista de tickers y categorías
tickers_dict = {
    'YPF': 'Acciones Líderes', 'TGS': 'Acciones Líderes', 'PAM': 'Acciones Líderes',
    'VIST': 'Acciones Líderes', 'GGAL': 'Acciones Líderes', 'CEPU': 'Acciones Líderes',
    'LOMA': 'Acciones Líderes', 'EDN': 'Acciones Líderes', 'BBAR': 'Acciones Líderes',
    'SUPV': 'Acciones Líderes', 'CRESY': 'Acciones Líderes', 'YPFD.BA': 'Acciones Líderes',
    'GLOB': 'CEDEARs', 'BMA': 'Acciones Líderes', 'NU': 'CEDEARs', 'TSLA': 'CEDEARs',
    'GPRK': 'Acciones Líderes', 'MELI': 'CEDEARs', 'AMD': 'CEDEARs', 'BABA': 'CEDEARs',
    'PYPL': 'CEDEARs', 'PAGS': 'CEDEARs', 'SID': 'CEDEARs', 'AVGO': 'CEDEARs',
    'MORI.BA': 'Acciones del Panel General', 'META': 'CEDEARs', 'GOOG': 'CEDEARs',
    'QQQ': 'CEDEARs', 'AMZN': 'CEDEARs', 'AAPL': 'CEDEARs', 'NVDA': 'CEDEARs',
    'NFLX': 'CEDEARs', 'UBER': 'CEDEARs', 'MERV': 'Índice'
}
tickers = list(tickers_dict.keys())

# Descargar datos
print("Descargando datos de Yahoo Finance...")
data = yf.download(tickers, period="6mo", group_by="ticker", threads=True)
print("Datos descargados.")

def get_currency(ticker):
    return "ARS" if ticker.endswith('.BA') else "USD"

def calculate_indicators(ticker_data, ticker):
    if ticker_data['Close'].isna().all():
        return None
    
    delta = ticker_data['Close'].diff()
    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    
    ema6 = ticker_data['Close'].ewm(span=6, adjust=False).mean()
    ema13 = ticker_data['Close'].ewm(span=13, adjust=False).mean()
    macd = ema6 - ema13
    signal = macd.ewm(span=5, adjust=False).mean()
    macd_value = macd - signal
    
    ema50 = ticker_data['Close'].ewm(span=50, adjust=False).mean()
    ema100 = ticker_data['Close'].ewm(span=100, adjust=False).mean()
    
    atr = pd.concat([ticker_data['High'] - ticker_data['Low'], 
                     abs(ticker_data['High'] - ticker_data['Close'].shift()), 
                     abs(ticker_data['Low'] - ticker_data['Close'].shift())], axis=1).max(axis=1).rolling(window=14).mean()
    
    stoch_k = 100 * (ticker_data['Close'] - ticker_data['Low'].rolling(window=14).min()) / (ticker_data['High'].rolling(window=14).max() - ticker_data['Low'].rolling(window=14).min())
    stoch_d = stoch_k.rolling(window=3).mean()
    
    price = ticker_data['Close'].iloc[-1]
    change_1d = ((price - ticker_data['Close'].iloc[-2]) / ticker_data['Close'].iloc[-2]) * 100 if len(ticker_data) >= 2 else 0
    vol_avg = ticker_data['Volume'].tail(5).mean() if len(ticker_data) >= 5 else ticker_data['Volume'].iloc[-1]
    vol_increase = ticker_data['Volume'].iloc[-1] > vol_avg * 2.0
    vol_relative = ticker_data['Volume'].iloc[-1] / vol_avg if vol_avg > 0 else 1.0
    
    if ticker != 'MERV' and not data['MERV']['Close'].isna().all():
        ticker_returns = (ticker_data['Close'].iloc[-1] / ticker_data['Close'].iloc[-20] - 1) if len(ticker_data) >= 20 else 0
        merv_returns = (data['MERV']['Close'].iloc[-1] / data['MERV']['Close'].iloc[-20] - 1) if len(data['MERV']) >= 20 else 0
        rs = ticker_returns / merv_returns if merv_returns != 0 else 1.0
    else:
        rs = 1.0
    
    return {
        'rsi': rsi.iloc[-1],
        'volume': ticker_data['Volume'].iloc[-1],
        'macd': macd_value.iloc[-1],
        'macd_last': macd.iloc[-1],
        'macd_prev': macd.iloc[-2] if len(macd) >= 2 else 0,
        'signal_last': signal.iloc[-1],
        'signal_prev': signal.iloc[-2] if len(signal) >= 2 else 0,
        'change_1d': change_1d,
        'vol_increase': vol_increase,
        'vol_relative': vol_relative,
        'ema50': ema50.iloc[-1],
        'ema100': ema100.iloc[-1],
        'atr': atr.iloc[-1],
        'stoch_k': stoch_k.iloc[-1],
        'stoch_d': stoch_d.iloc[-1] if len(stoch_d) >= 3 else stoch_k.iloc[-1],
        'price': price,
        'rs': rs
    }

def suggest_action(indicators, currency, market_trend):
    rsi = indicators['rsi']
    macd_last = indicators['macd_last']
    macd_prev = indicators['macd_prev']
    signal_last = indicators['signal_last']
    signal_prev = indicators['signal_prev']
    change_1d = indicators['change_1d']
    vol_increase = indicators['vol_increase']
    price = indicators['price']
    ema50 = indicators['ema50']
    ema100 = indicators['ema100']
    atr = indicators['atr']
    stoch_k = indicators['stoch_k']
    stoch_d = indicators['stoch_d']
    rs = indicators['rs']
    
    macd_cross_up = macd_last > signal_last and macd_prev <= signal_prev
    macd_cross_down = macd_last < signal_last and macd_prev >= signal_prev
    stoch_oversold = stoch_k < 20 and stoch_d < 20
    stoch_overbought = stoch_k > 80 and stoch_d > 80
    
    if rsi < 30 and macd_cross_up and vol_increase and stoch_oversold and rs > 1.0 and market_trend != "Bajista":
        target_price = price * (1 + atr / price * 2)
        stop_loss = price * (1 - atr / price)
        rr_ratio = (target_price - price) / (price - stop_loss)
        if rr_ratio >= 2:
            return "Comprar", f"Comprar a {currency} {price:.2f}, TP: {target_price:.2f}, SL: {stop_loss:.2f}, RR: {rr_ratio:.1f}"
    
    if rsi > 70 and macd_cross_down and vol_increase and stoch_overbought and rs < 1.0 and market_trend != "Alcista":
        target_price = price * (1 - atr / price * 2)
        stop_loss = price * (1 + atr / price)
        rr_ratio = (price - target_price) / (stop_loss - price)
        if rr_ratio >= 2:
            return "Vender", f"Vender a {currency} {price:.2f}, TP: {target_price:.2f}, SL: {stop_loss:.2f}, RR: {rr_ratio:.1f}"
    
    return "Mantener", f"Mantener en {currency} {price:.2f}"

def get_trend(price, ema50, ema100):
    if price > ema50 > ema100:
        return "Alcista"
    elif price < ema50 < ema100:
        return "Bajista"
    else:
        return "Neutral"

# Tendencia del mercado (MERVAL)
market_data = data['MERV'].dropna(how='all')
market_indicators = calculate_indicators(market_data, 'MERV') if not market_data.empty else {'ema50': 0, 'ema100': 0, 'price': 0}
market_trend = get_trend(market_indicators['price'], market_indicators['ema50'], market_indicators['ema100'])

# Preparar datos para actualización masiva
data_rows = []
for ticker in tickers:
    if ticker == 'MERV':
        continue
    try:
        ticker_data = data[ticker].dropna(how='all')
        if ticker_data.empty:
            print(f"No hay datos para {ticker}")
            continue
        
        indicators = calculate_indicators(ticker_data, ticker)
        if indicators is None:
            print(f"No hay datos válidos para {ticker}")
            continue
        
        currency = get_currency(ticker)
        action, detail = suggest_action(indicators, currency, market_trend)
        trend = get_trend(indicators['price'], indicators['ema50'], indicators['ema100'])
        
        rsi_status = "Sobrecompra" if indicators['rsi'] > 70 else "Sobreventa" if indicators['rsi'] < 30 else ""
        rsi_str = f"[{rsi_status}] {indicators['rsi']:.2f}" if rsi_status else f"{indicators['rsi']:.2f}"
        
        data_rows.append([
            ticker, tickers_dict[ticker], currency, indicators['price'], rsi_str, 
            int(indicators['volume']), f"{indicators['macd']:.2f}", f"{indicators['change_1d']:.2f}%", 
            f"{indicators['vol_relative']:.1f}x", trend, f"{indicators['atr']:.2f}", 
            f"{indicators['rs']:.2f}", action, detail
        ])
    except Exception as e:
        print(f"Error con {ticker}: {e}")

# Interpretaciones de columnas
interpretations = [
    "Nombre del activo (buscalo en tu plataforma)",
    "Tipo de activo (considerá riesgo según categoría)",
    "Moneda del precio (ajustá según ARS/USD)",
    "Precio actual (compará con TP/SL)",
    "Momentum: <30 compra, >70 venta si se confirma",
    "Actividad diaria (alto volumen confirma señales)",
    "Tendencia: + y subiendo = compra, - y bajando = venta",
    "% cambio diario (>0 suba, <0 baja)",
    "Volumen vs. promedio (>2x confirma señal)",
    "Dirección general (Alcista: compra, Bajista: venta)",
    "Volatilidad (alto = más riesgo/movimiento)",
    "Fuerza vs. MERVAL (>1 compra, <1 venta)",
    "Qué hacer (ejecutá si coincide con Detalle)",
    "Detalles: precio, TP, SL, RR (usa para órdenes)"
]

# Preparar datos para actualización masiva
update_data = [
    [f"*Última actualización*: {datetime.now(buenos_aires_tz).strftime('%Y-%m-%d %H:%M:%S')}"],
    interpretations
] + [
    ["Ticker", "Categoría", "Moneda", "Precio", "RSI", "Volumen", "MACD", 
     "Cambio 1D", "Volumen Relativo", "Tendencia", "ATR", "Fuerza Relativa", 
     "Acción Sugerida", "Detalle Acción"]
] + data_rows

# Actualizar Google Sheets
print("Actualizando Google Sheet...")
data_sheet.update('A1:N' + str(len(update_data)), update_data)
print("Sheet actualizado.")

# Formatear columnas como moneda
currency_cols = ['D']  # Solo Precio
for col in currency_cols:
    data_sheet.format(f"{col}4:{col}{len(data_rows)+3}", {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}})

# Formato condicional
data_sheet.format("M4:M" + str(len(data_rows)+3), {"backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}})
data_sheet.format("M4:M" + str(len(data_rows)+3), {"textFormat": {"bold": True}, "backgroundColor": {"red": 0, "green": 1, "blue": 0}}, condition_type="TEXT_CONTAINS", condition_values=["Comprar"])
data_sheet.format("M4:M" + str(len(data_rows)+3), {"textFormat": {"bold": True}, "backgroundColor": {"red": 1, "green": 0, "blue": 0}}, condition_type="TEXT_CONTAINS", condition_values=["Vender"])

print("Google Sheet actualizado con éxito el", datetime.now(buenos_aires_tz).strftime("%Y-%m-%d %H:%M:%S"))