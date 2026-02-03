from fastapi import FastAPI, Query, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import time

# Configuration
EXPECTED_API_KEY = os.getenv("FUND_API_KEY", "")

# Initialize FastAPI app
app = FastAPI(title="Fund Data API")

# Rate limiter setup
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fund-api")


def get_prev_trading_date():
    """获取最近一个交易日（排除周末）"""
    today = datetime.now()
    for i in range(1, 10):
        d = today - timedelta(days=i)
        if d.weekday() < 5:  # Monday=0, Sunday=6
            return d.strftime("%Y-%m-%d")
    return (today - timedelta(days=1)).strftime("%Y-%m-%d")


def safe_akshare_call(func, max_retries=2):
    """
    安全调用 AKShare 接口，支持重试机制。
    防止因网络波动或反爬导致的临时失败。
    """
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            logger.warning(f"AKShare attempt {attempt+1} failed: {str(e)}")
            if attempt == max_retries:
                raise HTTPException(status_code=503, detail="数据源暂时不可用")
            time.sleep(1)


def verify_api_key(request: Request):
    """验证 API Key（支持 header 或 query 参数）"""
    key = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if not EXPECTED_API_KEY or key == EXPECTED_API_KEY:
        return True
    raise HTTPException(status_code=403, detail="Invalid API Key")


@app.get("/health")
async def health_check():
    """健康检查接口"""
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.get("/fund/single")
@limiter.limit("20/minute")
async def get_single_fund(
    request: Request,
    fund_code: str = Query(..., regex=r"^\d{6}$"),
    _: bool = Depends(verify_api_key)
):
    """获取单只基金的最新净值和收益率"""
    try:
        target_date = get_prev_trading_date()
        
        def fetch_data():
            # 获取单位净值走势
            nav_df = ak.fund_em_open_fund_info(fund=fund_code, indicator="单位净值走势")
            if nav_df.empty:
                raise ValueError("No NAV data found")
            
            # 转换日期并筛选目标日期
            nav_df['净值日期'] = pd.to_datetime(nav_df['净值日期'])
            target_row = nav_df[nav_df['净值日期'].dt.strftime('%Y-%m-%d') == target_date]
            if target_row.empty:
                target_row = nav_df.iloc[-1:]  # 降级到最新一条
            
            # 获取基金名称
            name_df = ak.fund_name_em()
            fund_name = "Unknown"
            if not name_df.empty and fund_code in name_df['基金代码'].values:
                fund_name = name_df[name_df['基金代码'] == fund_code]['基金简称'].iloc[0]
            
            return {
                "fund_code": fund_code,
                "fund_name": fund_name,
                "query_date": target_date,
                "unit_nav": float(target_row['单位净值'].iloc[0]),
                "daily_return_pct": float(target_row['日增长率'].iloc[0]) if not pd.isna(target_row['日增长率'].iloc[0]) else None,
                "accumulative_nav": float(target_row['累计净值'].iloc[0])
            }
        
        data = safe_akshare_call(fetch_data)
        return JSONResponse(content={"code": 200, "data": data})
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fund {fund_code} error: {str(e)}")
        return JSONResponse(status_code=500, content={"code": 500, "error": "Data processing error"})


@app.get("/fund/market-flow")
@limiter.limit("10/minute")
async def get_market_flow(
    request: Request, 
    _: bool = Depends(verify_api_key)):
    """获取市场资金流（行业 & 概念板块）"""
    try:
        def fetch_flow():
            sector = ak.stock_sector_fund_flow_rank(indicator="今日")
            concept = ak.stock_fund_flow_concept(symbol="即时")
            return {
                "date": datetime.now().strftime("%Y-%m-%d"),
                "top_industries": sector.nlargest(5, '净流入')[['行业', '净流入']].to_dict('records'),
                "top_concepts": concept.nlargest(5, '净流入')[['概念名称', '净流入']].to_dict('records')
            }
        
        data = safe_akshare_call(fetch_flow)
        return JSONResponse(content={"code": 200, "data": data})
    
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"code": 500, "error": "Market flow fetch failed"})

