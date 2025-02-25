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
print("Credenciales obtenidas del entorno:", creds_json[:50] if creds_json else "No se encontraron credenciales")
creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
sheet = client.open_by_key('1VxrU9jQnBoShNWY1zfbMSFCWc-tISgolOGm-zUbY_4Q')
data_sheet = sheet.sheet1

# Zona horaria de Buenos Aires
buenos_aires_tz = pytz.timezone('America/Argentina/Buenos_Aires')
now = datetime.now(buenos_aires_tz)
print(f"Hora actual en ART: {now.strftime('%Y-%m-%d %H:%M:%S')}")

# Verificación de tiempo desde la última actualización (sin restricción)
try:
    last_update_str = data_sheet.acell('A1').value
    print(f"Última actualización encontrada en A1: {last_update_str}")
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

# Descargar datos de todos los tickers en una sola llamada
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
    
    sma20 = ticker_data['Close'].rolling(window=20).mean()
    std20 = ticker_data['Close'].rolling(window=20).std()
    bollinger_upper = sma20 + 2 * std20
    bollinger_lower = sma20 - 2 * std20
    bollinger_width = (bollinger_upper - bollinger_lower) / sma20
    
    tr = pd.concat([ticker_data['High'] - ticker_data['Low'], 
                    abs(ticker_data['High'] - ticker_data['Close'].shift()), 
                    abs(ticker_data['Low'] - ticker_data['Close'].shift())], axis=1).max(axis=1)
    atr = tr.rolling(window=14).mean()
    plus_dm = (ticker_data['High'] - ticker_data['High'].shift()).where(lambda x: x > 0, 0)
    minus_dm = (ticker_data['Low'].shift() - ticker_data['Low']).where(lambda x: x > 0, 0)
    plus_di = 100 * (plus_dm.rolling(window=14).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(window=14).mean() / atr)
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    adx = dx.rolling(window=14).mean()
    
    soporte_diario = ticker_data['Low'].iloc[-2] if len(ticker_data) >= 2 else None
    resistencia_diario = ticker_data['High'].iloc[-2] if len(ticker_data) >= 2 else None
    soporte_semanal = ticker_data['Low'].tail(5).min() if len(ticker_data) >= 5 else None
    resistencia_semanal = ticker_data['High'].tail(5).max() if len(ticker_data) >= 5 else None
    
    change_1d = ((ticker_data['Close'].iloc[-1] - ticker_data['Close'].iloc[-2]) / ticker_data['Close'].iloc[-2]) * 100 if len(ticker_data) >= 2 else 0
    change_5d = ((ticker_data['Close'].iloc[-1] - ticker_data['Close'].iloc[-6]) / ticker_data['Close'].iloc[-6]) * 100 if len(ticker_data) >= 6 else 0
    
    vol_avg = ticker_data['Volume'].tail(5).mean() if len(ticker_data) >= 5 else ticker_data['Volume'].iloc[-1]
    vol_increase = ticker_data['Volume'].iloc[-1] > vol_avg * 1.5
    
    return {
        'rsi': rsi.iloc[-1],
        'volume': ticker_data['Volume'].iloc[-1],
        'macd': macd_value.iloc[-1],
        'macd_last': macd.iloc[-1],
        'macd_prev': macd.iloc[-2] if len(macd) >= 2 else 0,
        'signal_last': signal.iloc[-1],
        'signal_prev': signal.iloc[-2] if len(signal) >= 2 else 0,
        'soporte_diario': soporte_diario,
        'resistencia_diario': resistencia_diario,
        'soporte_semanal': soporte_semanal,
        'resistencia_semanal': resistencia_semanal,
        'change_1d': change_1d,
        'change_5d': change_5d,
        'vol_increase': vol_increase,
        'ema50': ema50.iloc[-1],
        'ema100': ema100.iloc[-1],
        'bollinger_width': bollinger_width.iloc[-1] if not pd.isna(bollinger_width.iloc[-1]) else 0,
        'adx': adx.iloc[-1] if not pd.isna(adx.iloc[-1]) else 0,
        'price': ticker_data['Close'].iloc[-1]
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
    
    return "Mantener", f"Mantener en {currency} {price:.2f}, sin señal clara"

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
        
        rsi_status = "Sobrecompra" if indicators['rsi'] > 65 else "Sobreventa" if indicators['rsi'] < 35 else ""
        rsi_str = f"[{rsi_status}] {indicators['rsi']:.2f}" if rsi_status else f"{indicators['rsi']:.2f}"
        
        data_rows.append([
            ticker, tickers_dict[ticker], currency, indicators['price'], rsi_str, int(indicators['volume']), 
            f"{indicators['macd']:.2f}", f"{indicators['ema50']:.2f}", f"{indicators['ema100']:.2f}", 
            f"{indicators['bollinger_width']:.2f}", f"{indicators['adx']:.2f}",
            f"{indicators['soporte_diario']:.2f}" if indicators['soporte_diario'] else "N/A", 
            f"{indicators['resistencia_diario']:.2f}" if indicators['resistencia_diario'] else "N/A", 
            f"{indicators['soporte_semanal']:.2f}" if indicators['soporte_semanal'] else "N/A", 
            f"{indicators['resistencia_semanal']:.2f}" if indicators['resistencia_semanal'] else "N/A", 
            action, detail
        ])
    except Exception as e:
        print(f"Error con {ticker}: {e}")

# Preparar datos para actualización masiva
update_data = [
    [f"*Última actualización*: {datetime.now(buenos_aires_tz).strftime('%Y-%m-%d %H:%M:%S')}"]
] + [
    ["Ticker", "Categoría", "Moneda", "Precio", "RSI", "Volumen", "MACD", 
     "EMA 50", "EMA 100", "Bollinger Width", "ADX",
     "Soporte Diario", "Resistencia Diaria", "Soporte Semanal", "Resistencia Semanal", 
     "Acción Sugerida", "Detalle Acción"]
] + data_rows

# Actualizar Google Sheets en una sola llamada
print("Actualizando Google Sheet...")
data_sheet.update('A1:Q' + str(len(update_data)), update_data)
print("Sheet actualizado.")

# Formatear columnas como moneda
currency_cols = ['D', 'H', 'I', 'L', 'M', 'N', 'O']  # Precio, EMA 50, EMA 100, Soporte/Resistencia
for col in currency_cols:
    data_sheet.format(f"{col}3:{col}{len(data_rows)+2}", {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}})

print("Google Sheet actualizado con éxito el", datetime.now(buenos_aires_tz).strftime("%Y-%m-%d %H:%M:%S"))