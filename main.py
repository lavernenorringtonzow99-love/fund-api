import os
import time
import pandas as pd
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import akshare as ak
from functools import lru_cache

app = FastAPI(
    title="金融数据 API",
    description="基金净值 + 市场资金流向",
    version="3.1"
)

# ======================
# 初始化优化（关键！）
# ======================
@lru_cache(maxsize=1)
def _init_akshare():
    """预加载 AKShare，避免请求时卡住"""
    try:
        # 触发基金模块初始化
        ak.fund_em_open_fund_info(fund="005827", indicator="单位净值走势")
        # 触发资金流模块初始化
        ak.stock_market_fund_flow()
        print("[INIT] AKShare modules loaded")
    except Exception as e:
        print(f"[INIT] Warning: {e}")

# 应用启动时预加载
_init_akshare()

# ======================
# 工具函数
# ======================
def safe_get_value(series, key, default=None):
    """安全获取 pandas Series 中的值"""
    try:
        val = series.get(key, default)
        if pd.isna(val) or val in ["-", "", "None", None]:
            return default
        return str(val).strip()
    except:
        return default

def safe_float(value, default=None):
    """安全转换为浮点数"""
    if value is None:
        return default
    try:
        if isinstance(value, str) and value.endswith('%'):
            return float(value.rstrip('%'))
        if isinstance(value, str) and '亿' in value:
            return float(value.replace('亿', ''))
        return float(value)
    except (ValueError, TypeError):
        return default

# ======================
# 路由：基金详情
# ======================
@app.get("/fund/single")
async def get_single_fund(
    fund_code: str = Query(..., regex=r"^\d{6}$", description="6位基金代码"),
    api_key: str = Query(..., description="API 密钥")
):
    # 验证 API Key
    expected_key = os.getenv("FUND_API_KEY", "test")
    if api_key != expected_reserved_key:
        raise HTTPException(status_code=403, detail="Invalid API Key")

    try:
        # 仅请求必要数据（减少内存）
        df = ak.fund_em_open_fund_info(fund=fund_code, indicator="单位净值走势")
        
        if df.empty:
            raise HTTPException(status_code=404, detail="基金代码不存在或暂无数据")

        latest = df.iloc[0]
        unit_nav = safe_get_value(latest, "单位净值")
        if not unit_nav or unit_nav == "-":
            raise HTTPException(status_code=404, detail="暂无净值数据")

        # 获取完整信息（第二次请求，但已缓存）
        full_df = ak.fund_em_open_fund_info(fund=fund_code)
        full_info = full_df.iloc[0] if not full_df.empty else {}

        return JSONResponse({
            "code": 200,
            "data": {
                "fund_code": fund_code,
                "fund_name": safe_get_value(full_info, "基金简称", f"未知基金({fund_code})"),
                "unit_nav": safe_float(unit_nav),
                "accum_nav": safe_float(safe_get_value(full_info, "累计净值")),
                "daily_return_pct": safe_float(safe_get_value(full_info, "日增长率")),
                "query_date": safe_get_value(latest, "净值日期"),
                "company": safe_get_value(full_info, "基金管理人"),
                "manager": safe_get_value(full_info, "基金经理"),
                "fund_type": safe_get_value(full_info, "基金类型"),
                "establishment_date": safe_get_value(full_info, "成立日期")
            }
        })

    except HTTPException:
        raise
    except Exception as e:
        print(f"[FUND_ERROR] Code={fund_code}, Error={str(e)}")
        raise HTTPException(status_code=500, detail="基金数据查询失败")

# ======================
# 路由：市场资金流向
# ======================
@app.get("/market/flow")
async def get_market_fund_flow(
    market: str = Query("all", enum=["sh", "sz", "all"], description="市场: sh=沪市, sz=深市, all=沪深两市"),
    api_key: str = Query(..., description="API 密钥")
):
    expected_key = os.getenv("FUND_API_KEY", "test")
    if api_key != expected_key:
        raise HTTPException(status_code=403, detail="Invalid API Key")

    try:
        # 获取市场资金流
        df = ak.stock_market_fund_flow()
        
        # 映射市场名称
        market_map = {"sh": "沪市", "sz": "深市", "all": "沪深两市"}
        target_name = market_map[market]
        
        # 查找对应行
        target_row = df[df['板块'] == target_name]
        if target_row.empty:
            raise HTTPException(status_code=404, detail=f"{target_name}数据暂不可用")
            
        data = target_row.iloc[0]

        return JSONResponse({
            "code": 200,
            "data": {
                "market": target_name,
                "main_net_inflow": safe_float(safe_get_value(data, "主力净流入-净额")),      # 亿元
                "retail_net_inflow": safe_float(safe_get_value(data, "散户净流入-净额")),     # 亿元
                "big_order_net_inflow": safe_float(safe_get_value(data, "大单净流入-净额")),  # 亿元
                "small_order_net_inflow": safe_float(safe_get_value(data, "小单净流入-净额")),# 亿元
                "main_net_ratio": safe_float(safe_get_value(data, "主力净流入-净占比")),     # %
                "update_time": safe_get_value(data, "日期") or safe_get_value(data, "更新时间")
            }
        })

    except HTTPException:
        raise
    except Exception as e:
        print(f"[MARKET_ERROR] Market={market}, Error={str(e)}")
        if 'df' in locals():
            print(f"[DEBUG] Columns: {list(df.columns)}")
        raise HTTPException(status_code=500, detail="市场资金数据获取失败")

# ======================
# 启动配置（适配 Railway）
# ======================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
