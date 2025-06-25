import yfinance as yf
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

def get_m7_stock_df():
    # -----------------------
    # 1. Ticker settings
    # -----------------------
    tickers = {
        'NASDAQ': '^IXIC',
        'S&P500': '^GSPC',
        'Apple': 'AAPL',
        'Microsoft': 'MSFT',
        'Google': 'GOOGL',
        'Amazon': 'AMZN',
        'Nvidia': 'NVDA',
        'Meta': 'META',
        'Tesla': 'TSLA'
    }

    # -----------------------
    # 2. Period settings    
    # -----------------------
    period = "5d"  # 5 days (or '1mo' also possible)
    interval = "1d"

    # -----------------------
    # 3. Calculate return and current price
    # -----------------------
    results = []


    for name, ticker in tickers.items():
        data = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=True)
        start_date = data.index[0].strftime('%Y-%m-%d')
        end_date = data.index[-1].strftime('%Y-%m-%d')
        
        if not data.empty:
            start_price = float(data["Close"].iloc[0])
            end_price = float(data["Close"].iloc[-1])
            low_price = float(data["Low"].min())
            high_price = float(data["High"].max())
            return_pct = ((end_price - start_price) / start_price) * 100
            
            results.append({
                "Name": name,
                "Ticker": ticker,
                "Start Date": start_date,
                "Start Price": round(start_price, 2),
                "Today": end_date,
                "Current Price": round(end_price, 2),
                "The Lowest Price this week": round(low_price, 2),
                "The Highest Price this week": round(high_price, 2),
                "Return (%)": round(return_pct, 2),
            })

    # -----------------------
    # 4. Print results
    # -----------------------
    m7_stock_df = pd.DataFrame(results)
    return m7_stock_df