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
    'NFLX': 'CEDEARs', 'UBER': 'CEDEARs'
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
    
    ema12 = ticker_data['Close'].ewm(span=12, adjust=False).mean()
    ema26 = ticker_data['Close'].ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    macd_value = macd - signal
    
    ema50 = ticker_data['Close'].ewm(span=50, adjust=False).mean()
    ema100 = ticker_data['Close'].ewm(span=100, adjust=False).mean()
    
    price = ticker_data['Close'].iloc[-1]
    change_1d = ((price - ticker_data['Close'].iloc[-2]) / ticker_data['Close'].iloc[-2]) * 100 if len(ticker_data) >= 2 else 0
    change_5d = ((price - ticker_data['Close'].iloc[-6]) / ticker_data['Close'].iloc[-6]) * 100 if len(ticker_data) >= 6 else 0
    
    vol_avg = ticker_data['Volume'].tail(5).mean() if len(ticker_data) >= 5 else ticker_data['Volume'].iloc[-1]
    vol_increase = ticker_data['Volume'].iloc[-1] > vol_avg * 1.5
    vol_relative = ticker_data['Volume'].iloc[-1] / vol_avg if vol_avg > 0 else 1.0
    
    return {
        'rsi': rsi.iloc[-1],
        'volume': ticker_data['Volume'].iloc[-1],
        'macd': macd_value.iloc[-1],
        'macd_last': macd.iloc[-1],
        'macd_prev': macd.iloc[-2] if len(macd) >= 2 else 0,
        'signal_last': signal.iloc[-1],
        'signal_prev': signal.iloc[-2] if len(signal) >= 2 else 0,
        'change_1d': change_1d,
        'change_5d': change_5d,
        'vol_increase': vol_increase,
        'vol_relative': vol_relative,
        'ema50': ema50.iloc[-1],
        'ema100': ema100.iloc[-1],
        'price': price
    }

def suggest_action(indicators, currency):
    rsi = indicators['rsi']
    macd_last = indicators['macd_last']
    macd_prev = indicators['macd_prev']
    signal_last = indicators['signal_last']
    signal_prev = indicators['signal_prev']
    change_1d = indicators['change_1d']
    change_5d = indicators['change_5d']
    vol_increase = indicators['vol_increase']
    price = indicators['price']
    ema50 = indicators['ema50']
    ema100 = indicators['ema100']
    
    macd_cross_up = macd_last > signal_last and macd_prev <= signal_prev
    macd_cross_down = macd_last < signal_last and macd_prev >= signal_prev
    ema_cross_up = price > ema50 and price > ema100
    
    if (rsi < 35) or macd_cross_up or (change_1d < -5 and vol_increase) or (change_5d < -10 and vol_increase):
        target_price = price * 1.05
        if rsi < 35:
            reason = "RSI en sobreventa"
        elif macd_cross_up:
            reason = "Cruce alcista del MACD"
        elif ema_cross_up:
            reason = "Cruce alcista con EMAs"
        else:
            reason = f"Caída reciente ({change_1d:.1f}% 1d, {change_5d:.1f}% 5d) con volumen"
        return "Comprar", f"Comprar a {currency} {target_price:.2f}, debido a {reason}"
    
    if (rsi > 65) or macd_cross_down or (change_1d > 5 and vol_increase) or (change_5d > 10 and vol_increase):
        target_price = price * 0.95
        if rsi > 65:
            reason = "RSI en sobrecompra"
        elif macd_cross_down:
            reason = "Cruce bajista del MACD"
        else:
            reason = f"Subida reciente ({change_1d:.1f}% 1d, {change_5d:.1f}% 5d) con volumen"
        return "Vender", f"Vender a {currency} {target_price:.2f}, debido a {reason}"
    
    return "Mantener", f"Mantener en {currency} {price:.2f}, sin señal clara}"

def get_trend(price, ema50, ema100):
    if price > ema50 > ema100:
        return "Alcista"
    elif price < ema50 < ema100:
        return "Bajista"
    else:
        return "Neutral"

# Preparar datos para actualización masiva
data_rows = []
for ticker in tickers:
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
        action, detail = suggest_action(indicators, currency)
        trend = get_trend(indicators['price'], indicators['ema50'], indicators['ema100'])
        
        rsi_status = "Sobrecompra" if indicators['rsi'] > 65 else "Sobreventa" if indicators['rsi'] < 35 else ""
        rsi_str = f"[{rsi_status}] {indicators['rsi']:.2f}" if rsi_status else f"{indicators['rsi']:.2f}"
        
        data_rows.append([
            ticker, tickers_dict[ticker], currency, indicators['price'], rsi_str, 
            int(indicators['volume']), f"{indicators['macd']:.2f}", 
            f"{indicators['change_1d']:.2f}%", f"{indicators['vol_relative']:.1f}x", trend,
            action, detail
        ])
    except Exception as e:
        print(f"Error con {ticker}: {e}")

# Preparar datos para actualización masiva
update_data = [
    [f"*Última actualización*: {datetime.now(buenos_aires_tz).strftime('%Y-%m-%d %H:%M:%S')}"]
] + [
    ["Ticker", "Categoría", "Moneda", "Precio", "RSI", "Volumen", "MACD", 
     "Cambio 1D", "Volumen Relativo", "Tendencia", "Acción Sugerida", "Detalle Acción"]
] + data_rows

# Actualizar Google Sheets
print("Actualizando Google Sheet...")
data_sheet.update('A1:L' + str(len(update_data)), update_data)
print("Sheet actualizado.")

# Formatear columnas como moneda
currency_cols = ['D']  # Solo Precio
for col in currency_cols:
    data_sheet.format(f"{col}3:{col}{len(data_rows)+2}", {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}})

print("Google Sheet actualizado con éxito el", datetime.now(buenos_aires_tz).strftime("%Y-%m-%d %H:%M:%S"))