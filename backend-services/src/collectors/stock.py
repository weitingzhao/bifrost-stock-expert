"""
股票数据采集占位。实际可接入：
- akshare: 免费，安装 pip install akshare
- tushare: 需 token，安装 pip install tushare
"""
# 示例：使用 akshare 获取日线（取消注释并安装 akshare 后使用）
# import akshare as ak
# def fetch_daily(code: str, start: str, end: str):
#     df = ak.stock_zh_a_hist(symbol=code, start_date=start, end_date=end, adjust="qfq")
#     return df

def fetch_daily(code: str, start: str, end: str):
    """占位：返回空列表，接入数据源后返回 DataFrame 或 list of dict"""
    return []

def fetch_minute(code: str, period: str = "15"):
    """占位：分钟线"""
    return []
