import yfinance as yf
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# Configuración de Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
client = gspread.authorize(creds)
sheet = client.open_by_key('1VxrU9jQnBoShNWY1zfbMSFCWc-tISgolOGm-zUbY_4Q')
data_sheet = sheet.sheet1
meta_sheet = sheet.get_worksheet(1) if len(sheet.worksheets()) > 1 else sheet.add_worksheet(title="Meta", rows=10, cols=10)

# Verificar tiempo desde la última actualización
try:
    last_update_str = meta_sheet.acell('A1').value
    last_update = datetime.strptime(last_update_str, "%Y-%m-%d %H:%M:%S") if last_update_str else None
except:
    last_update = None

if last_update:
    time_diff = datetime.now() - last_update
    if time_diff < timedelta(minutes=15):
        minutes_left = 15 - time_diff.total_seconds() // 60
        print(f"Error: No pasaron los 15 minutos. Restan {int(minutes_left)} minutos para la próxima actualización.")
        exit()

# Lista de tickers y categorías
tickers = {
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

def get_currency(ticker):
    return "ARS" if ticker.endswith('.BA') else "USD"

def calculate_indicators(data):
    delta = data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    
    ema12 = data['Close'].ewm(span=12, adjust=False).mean()
    ema26 = data['Close'].ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    macd_value = macd - signal
    
    ema50 = data['Close'].ewm(span=50, adjust=False).mean()
    ema100 = data['Close'].ewm(span=100, adjust=False).mean()
    
    sma20 = data['Close'].rolling(window=20).mean()
    std20 = data['Close'].rolling(window=20).std()
    bollinger_upper = sma20 + 2 * std20
    bollinger_lower = sma20 - 2 * std20
    bollinger_width = (bollinger_upper - bollinger_lower) / sma20
    
    tr = pd.concat([data['High'] - data['Low'], 
                    abs(data['High'] - data['Close'].shift()), 
                    abs(data['Low'] - data['Close'].shift())], axis=1).max(axis=1)
    atr = tr.rolling(window=14).mean()
    plus_dm = (data['High'] - data['High'].shift()).where(lambda x: x > 0, 0)
    minus_dm = (data['Low'].shift() - data['Low']).where(lambda x: x > 0, 0)
    plus_di = 100 * (plus_dm.rolling(window=14).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(window=14).mean() / atr)
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    adx = dx.rolling(window=14).mean()
    
    soporte_diario = data['Low'].iloc[-2] if len(data) >= 2 else None
    resistencia_diario = data['High'].iloc[-2] if len(data) >= 2 else None
    soporte_semanal = data['Low'].tail(5).min() if len(data) >= 5 else None
    resistencia_semanal = data['High'].tail(5).max() if len(data) >= 5 else None
    
    change_1d = ((data['Close'].iloc[-1] - data['Close'].iloc[-2]) / data['Close'].iloc[-2]) * 100 if len(data) >= 2 else 0
    change_5d = ((data['Close'].iloc[-1] - data['Close'].iloc[-6]) / data['Close'].iloc[-6]) * 100 if len(data) >= 6 else 0
    
    vol_avg = data['Volume'].tail(5).mean() if len(data) >= 5 else data['Volume'].iloc[-1]
    vol_increase = data['Volume'].iloc[-1] > vol_avg * 1.5
    
    macd_last = macd.iloc[-1] if len(macd) >= 1 else 0
    macd_prev = macd.iloc[-2] if len(macd) >= 2 else 0
    signal_last = signal.iloc[-1] if len(signal) >= 1 else 0
    signal_prev = signal.iloc[-2] if len(signal) >= 2 else 0
    
    return (rsi.iloc[-1], data['Volume'].iloc[-1], macd_value.iloc[-1], 
            macd_last, macd_prev, signal_last, signal_prev, 
            soporte_diario, resistencia_diario, soporte_semanal, resistencia_semanal, 
            change_1d, change_5d, vol_increase, 
            ema50.iloc[-1], ema100.iloc[-1], 
            bollinger_width.iloc[-1] if not pd.isna(bollinger_width.iloc[-1]) else 0, 
            adx.iloc[-1] if not pd.isna(adx.iloc[-1]) else 0)

def suggest_action(rsi, macd_last, macd_prev, signal_last, signal_prev, change_1d, change_5d, vol_increase, price, ema50, ema100, currency):
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

# Obtener datos y actualizar Google Sheets
data_rows = []
for ticker, category in tickers.items():
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="6mo")
        if hist.empty:
            print(f"No hay datos para {ticker}")
            continue
        (rsi, volume, macd, macd_last, macd_prev, signal_last, signal_prev, 
         soporte_d, resistencia_d, soporte_s, resistencia_s, change_1d, change_5d, vol_increase, 
         ema50, ema100, bollinger_width, adx) = calculate_indicators(hist)
        price = hist['Close'].iloc[-1]
        currency = get_currency(ticker)
        action, detail = suggest_action(rsi, macd_last, macd_prev, signal_last, signal_prev, 
                                        change_1d, change_5d, vol_increase, price, ema50, ema100, currency)
        
        rsi_status = "Sobrecompra" if rsi > 65 else "Sobreventa" if rsi < 35 else ""
        rsi_str = f"[{rsi_status}] {rsi:.2f}" if rsi_status else f"{rsi:.2f}"
        data_rows.append([
            ticker, category, currency, price, rsi_str, int(volume), f"{macd:.2f}", 
            f"{ema50:.2f}", f"{ema100:.2f}", f"{bollinger_width:.2f}", f"{adx:.2f}",
            f"{soporte_d:.2f}" if soporte_d else "N/A", f"{resistencia_d:.2f}" if resistencia_d else "N/A", 
            f"{soporte_s:.2f}" if soporte_s else "N/A", f"{resistencia_s:.2f}" if resistencia_s else "N/A", 
            action, detail
        ])
    except Exception as e:
        print(f"Error con {ticker}: {e}")

# Actualizar Google Sheets
data_sheet.clear()
data_sheet.append_row(["Ticker", "Categoría", "Moneda", "Precio", "RSI", "Volumen", "MACD", 
                       "EMA 50", "EMA 100", "Bollinger Width", "ADX",
                       "Soporte Diario", "Resistencia Diaria", "Soporte Semanal", "Resistencia Semanal", 
                       "Acción Sugerida", "Detalle Acción"])
for row in data_rows:
    data_sheet.append_row(row)

# Formatear columnas como moneda
currency_cols = ['D', 'H', 'I', 'L', 'M', 'N', 'O']  # Precio, EMA 50, EMA 100, Soporte/Resistencia
for col in currency_cols:
    data_sheet.format(f"{col}2:{col}{len(data_rows)+1}", {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}})

meta_sheet.update_cell(1, 1, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("Google Sheet actualizado con éxito el", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
