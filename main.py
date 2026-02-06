import os
import time
import pandas as pd
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import akshare as ak

app = FastAPI(
    title="金融数据服务",
    description="基金净值 + 市场资金流向（Railway 免费版优化）",
    version="6.0"
)

# ======================
# 辅助函数
# ======================
def safe_float(val, default=None):
    if val is None or val in ["-", "", "None"] or pd.isna(val):
        return default
    try:
        s = str(val).strip()
        if s.endswith('%'):
            return float(s.rstrip('%'))
        if '亿' in s:
            return float(s.replace('亿', ''))
        return float(s)
    except (ValueError, TypeError):
        return default

def safe_str(val, default=""):
    if val is None or val in ["-", "", "None"] or pd.isna(val):
        return default
    return str(val).strip()

# ======================
# 健康检查（用于验证服务是否启动）
# ======================
@app.get("/health")
async def health_check():
    return {"status": "ok", "timestamp": int(time.time())}

# ======================
# 基金详情接口
# ======================
@app.get("/fund/single")
async def get_fund_info(
    fund_code: str = Query(..., regex=r"^\d{6}$", description="6位基金代码"),
    api_key: str = Query(..., description="API密钥")
):
    expected_key = os.getenv("FUND_API_KEY", "test")
    if api_key != expected_key:
        raise HTTPException(status_code=403, detail="Invalid API Key")

    try:
        # 仅请求单位净值走势（最小数据量，避免内存溢出）
        df = ak.fund_em_open_fund_info(fund=fund_code, indicator="单位净值走势")
        
        if df.empty or len(df) == 0:
            raise HTTPException(status_code=404, detail="基金无数据")

        # 最新净值通常在最后一行
        latest = df.iloc[-1]
        unit_nav = safe_str(latest.get("单位净值"))
        if not unit_nav or unit_nav == "-":
            raise HTTPException(status_code=404, detail="净值暂不可用")

        return JSONResponse({
            "code": 200,
            "data": {
                "fund_code": fund_code,
                "unit_nav": safe_float(unit_nav),
                "date": safe_str(latest.get("净值日期")),
                "daily_return_pct": safe_float(latest.get("日增长率"))
            }
        })

    except HTTPException:
        raise
    except Exception as e:
        print(f"[FUND_ERROR] {str(e)[:200]}")
        raise HTTPException(status_code=500, detail="基金查询失败，请稍后再试")

# ======================
# 市场资金流向接口
# ======================
@app.get("/market/flow")
async def get_market_flow(
    market: str = Query("all", enum=["sh", "sz", "all"], description="市场类型"),
    api_key: str = Query(..., description="API密钥")
):
    expected_key = os.getenv("FUND_API_KEY", "test")
    if api_key != expected_key:
        raise HTTPException(status_code=403, detail="Invalid API Key")

    try:
        df = ak.stock_market_fund_flow()
        if df.empty:
            raise HTTPException(status_code=500, detail="资金流数据源返回空")

        # 映射市场名称
        name_map = {"sh": "沪市", "sz": "深市", "all": "沪深两市"}
        target_name = name_map[market]

        # 精确匹配
        row = df[df['板块'] == target_name]
        if row.empty:
            # 尝试模糊匹配（兼容不同 AKShare 版本）
            mask = df['板块'].astype(str).str.contains(
                "沪" if market == "sh" else "深" if market == "sz" else "两|沪深",
                na=False
            )
            candidates = df[mask]
            if not candidates.empty:
                row = candidates.head(1)
            else:
                raise HTTPException(status_code=404, detail="未找到对应市场数据")

        d = row.iloc[0]
        return JSONResponse({
            "code": 200,
            "data": {
                "market": safe_str(d.get("板块", target_name)),
                "main_net_inflow": safe_float(d.get("主力净流入-净额")),      # 亿元
                "retail_net_inflow": safe_float(d.get("散户净流入-净额")),     # 亿元
                "main_net_ratio": safe_float(d.get("主力净流入-净占比")),     # %
                "update_time": safe_str(d.get("日期")) or safe_str(d.get("更新时间"))
            }
        })

    except HTTPException:
        raise
    except Exception as e:
        err_msg = str(e)[:200]
        print(f"[MARKET_ERROR] {err_msg}")
        if 'df' in locals():
            print(f"[DEBUG_COLUMNS] {list(df.columns)}")
        raise HTTPException(status_code=500, detail="市场资金查询失败，请稍后再试")
