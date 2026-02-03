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
from functools import wraps

# ================== 配置 ==================
# 从环境变量读取 API Key（部署时设置）
EXPECTED_API_KEY = os.getenv(FUND_API_KEY, your-secret-key-here)

# 初始化
app = FastAPI(title=基金数据 API, version=2.0)
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS（允许 DifyCoze 调用）
app.add_middleware(
    CORSMiddleware,
    allow_origins=[],
    allow_methods=[],
    allow_headers=[],
)

# 日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(fund-api)

# ================== 工具函数 ==================
def get_prev_trading_date()
    today = datetime.now()
    for i in range(1, 10)
        d = today - timedelta(days=i)
        if d.weekday()  5
            return d.strftime(%Y-%m-%d)
    return (today - timedelta(days=1)).strftime(%Y-%m-%d)

def safe_akshare_call(func, max_retries=2, timeout=10)
    安全调用 AKShare，带重试和超时
    for attempt in range(max_retries + 1)
        try
            start = time.time()
            result = func()
            duration = time.time() - start
            logger.info(fAKShare call success in {duration.2f}s)
            return result
        except Exception as e
            logger.warning(fAKShare attempt {attempt+1} failed {str(e)})
            if attempt == max_retries
                raise HTTPException(status_code=503, detail=数据源暂时不可用，请稍后重试)
            time.sleep(1)  # 等待后重试

# ================== 认证依赖 ==================
def verify_api_key(request Request)
    key = request.headers.get(X-API-Key) or request.query_params.get(api_key)
    if not EXPECTED_API_KEY or key == EXPECTED_API_KEY
        return True
    raise HTTPException(status_code=403, detail=无效的 API Key)

# ================== API 路由 ==================

@app.get(health)
async def health_check()
    return {status ok, timestamp datetime.now().isoformat()}

@app.get(fundsingle)
@limiter.limit(20minute)  # 每分钟最多20次
async def get_single_fund(
    fund_code str = Query(..., regex=r^d{6}$, description=6位基金代码),
    _ bool = Depends(verify_api_key)
)
    获取单一基金前一交易日数据（带认证+限流）
    try
        target_date = get_prev_trading_date()
        
        def fetch_data()
            nav_df = ak.fund_em_open_fund_info(fund=fund_code, indicator=单位净值走势)
            if nav_df.empty
                raise ValueError(无净值数据)
            
            nav_df['净值日期'] = pd.to_datetime(nav_df['净值日期'])
            target_row = nav_df[nav_df['净值日期'].dt.strftime('%Y-%m-%d') == target_date]
            
            if target_row.empty
                target_row = nav_df.iloc[-1]  # 降级到最新
            
            name_df = ak.fund_name_em()
            fund_name = name_df[name_df['基金代码'] == fund_code]['基金简称'].iloc[0] if not name_df.empty else 未知
            
            return {
                fund_code fund_code,
                fund_name fund_name,
                query_date target_date,
                unit_nav float(target_row['单位净值'].iloc[0]),
                daily_return_pct float(target_row['日增长率'].iloc[0]) if not pd.isna(target_row['日增长率'].iloc[0]) else None,
                accumulative_nav float(target_row['累计净值'].iloc[0])
            }
        
        data = safe_akshare_call(fetch_data)
        return JSONResponse(content={code 200, data data})
    
    except HTTPException
        raise
    except Exception as e
        logger.error(fFund {fund_code} error {str(e)})
        return JSONResponse(
            status_code=500,
            content={code 500, error 数据处理异常，请检查基金代码}
        )

@app.get(fundmarket-flow)
@limiter.limit(10minute)
async def get_market_flow(_ bool = Depends(verify_api_key))
    市场资金流向（行业+概念）
    try
        def fetch_flow()
            sector = ak.stock_sector_fund_flow_rank(indicator=今日)
            concept = ak.stock_fund_flow_concept(symbol=即时)
            
            return {
                date datetime.now().strftime(%Y-%m-%d),
                top_industries sector.nlargest(5, '净流入')[['行业', '净流入']].to_dict('records'),
                top_concepts concept.nlargest(5, '净流入')[['概念名称', '净流入']].to_dict('records')
            }
        
        data = safe_akshare_call(fetch_flow)
        return JSONResponse(content={code 200, data data})
    
    except HTTPException
        raise
    except Exception as e
        return JSONResponse(
            status_code=500,
            content={code 500, error 市场资金流获取失败}
        )