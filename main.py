import os
import pandas as pd
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import akshare as ak

app = FastAPI(
    title="金融数据 API",
    description="基金净值 + 市场资金流向",
    version="3.0"
)

EXPECTED_API_KEY = os.getenv("FUND_API_KEY", "your-default-key-here")

def safe_get_value(series, key, default=None):
    try:
        val = series.get(key, default)
        if pd.isna(val) or val in ["-", "", "None", None]:
            return default
        return str(val).strip()
    except:
        return default

def safe_float(value, default=None):
    if value is None:
        return default
    try:
        if isinstance(value, str) and value.endswith('%'):
            return float(value.rstrip('%'))
        return float(value)
    except (ValueError, TypeError):
        return default

# ========== 原有基金接口 ==========
@app.get("/fund/single")
async def get_single_fund(
    fund_code: str = Query(..., regex=r"^\d{6}$"),
    api_key: str = Query(...)
):
    if api_key != EXPECTED_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")

    try:
        # 获取基金详细信息（AKShare 完整数据）
        df = ak.fund_em_open_fund_info(fund=fund_code)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="基金代码不存在或暂无数据")

        latest = df.iloc[0]
        
        # === 提取核心净值数据 ===
        unit_nav = safe_float(safe_get_value(latest, "单位净值"))
        accum_nav = safe_float(safe_get_value(latest, "累计净值"))
        daily_return = safe_float(safe_get_value(latest, "日增长率"))
        
        if unit_nav is None:
            raise HTTPException(status_code=404, detail="暂无净值数据")

        # === 提取基金基本信息 ===
        fund_name = safe_get_value(latest, "基金简称", f"未知基金({fund_code})")
        establishment_date = safe_get_value(latest, "成立日期")
        fund_type = safe_get_value(latest, "基金类型")
        company = safe_get_value(latest, "基金管理人")
        manager = safe_get_value(latest, "基金经理")
        tracking_index = safe_get_value(latest, "跟踪标的")  # 指数基金专用
        
        # === 提取费率信息 ===
        subscription_fee = safe_get_value(latest, "申购费率")
        redemption_fee = safe_get_value(latest, "赎回费率")
        
        # === 提取风险指标 ===
        risk_level = safe_get_value(latest, "风险等级")
        min_purchase = safe_float(safe_get_value(latest, "最低申购金额(元)"), 0.0)
        
        # === 构造完整响应 ===
        return JSONResponse({
            "code": 200,
            "data": {
                # 基础信息
                "fund_code": fund_code,
                "fund_name": fund_name,
                "fund_type": fund_type,
                "establishment_date": establishment_date,
                "company": company,
                "manager": manager,
                "tracking_index": tracking_index,
                
                # 净值数据
                "unit_nav": unit_nav,
                "accum_nav": accum_nav,
                "daily_return_pct": daily_return,
                "query_date": safe_get_value(latest, "净值日期"),
                
                # 费率与规则
                "subscription_fee": subscription_fee,
                "redemption_fee": redemption_fee,
                "risk_level": risk_level,
                "min_purchase_amount": min_purchase
            }
        })

    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] Fund {fund_code} query failed: {str(e)}")
        if 'df' in locals():
            print(f"[DEBUG] Available columns: {list(df.columns)}")
            print(f"[DEBUG] First row: {latest.to_dict()}")
        raise HTTPException(status_code=500, detail="数据查询失败，请稍后再试")

# ========== 新增：市场资金流向接口 ==========
@app.get("/market/flow")
async def get_market_fund_flow(
    market: str = Query("sh", enum=["sh", "sz", "all"], description="市场: sh=沪市, sz=深市, all=沪深两市"),
    api_key: str = Query(...)
):
    """
    获取市场级别资金流向（主力/散户净流入）
    """
    if api_key != EXPECTED_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")

    try:
        # 调用 AKShare 获取市场资金流
        if market == "sh":
            df = ak.stock_market_fund_flow()
            # 筛选沪市数据（AKShare 返回多行，取第一行作为最新）
            sh_data = df[df['板块'] == '沪市'].iloc[0] if not df[df['板块'] == '沪市'].empty else None
            if sh_data is None:
                raise HTTPException(status_code=404, detail="沪市数据暂不可用")
            data = sh_data
            market_name = "沪市"
        elif market == "sz":
            df = ak.stock_market_fund_flow()
            sz_data = df[df['板块'] == '深市'].iloc[0] if not df[df['板块'] == '深市'].empty else None
            if sz_data is None:
                raise HTTPException(status_code=404, detail="深市数据暂不可用")
            data = sz_data
            market_name = "深市"
        else:  # all
            df = ak.stock_market_fund_flow()
            all_data = df[df['板块'] == '沪深两市'].iloc[0] if not df[df['板块'] == '沪深两市'].empty else None
            if all_data is None:
                raise HTTPException(status_code=404, detail="沪深两市数据暂不可用")
            data = all_data
            market_name = "沪深两市"

        # 提取关键字段（AKShare 字段名可能变化，请根据实际调整）
        def extract_field(name):
            val = safe_get_value(data, name)
            if val and '亿' in val:
                # 转换 "123.45亿" → 123.45
                try:
                    return float(val.replace('亿', ''))
                except:
                    return val
            return val

        return JSONResponse({
            "code": 200,
            "data": {
                "market": market_name,
                "main_net_inflow": extract_field("主力净流入-净额"),      # 主力净流入（亿元）
                "retail_net_inflow": extract_field("散户净流入-净额"),     # 散户净流入（亿元）
                "big_order_net_inflow": extract_field("大单净流入-净额"),  # 大单净流入（亿元）
                "small_order_net_inflow": extract_field("小单净流入-净额"),# 小单净流入（亿元）
                "main_net_ratio": safe_float(safe_get_value(data, "主力净流入-净占比")),   # 主力净占比（%）
                "update_time": safe_get_value(data, "日期") or safe_get_value(data, "更新时间")
            }
        })

    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] Market flow query failed: {str(e)}")
        if 'df' in locals():
            print(f"[DEBUG] Columns: {list(df.columns)}")
            print(f"[DEBUG] Data: {df.head().to_dict()}")
        raise HTTPException(status_code=500, detail="市场资金数据获取失败")

# ===== 新增：环境变量调试接口 =====
@app.get("/debug/env")
async def debug_environment():
    """
    返回关键环境变量的值（仅用于调试）
    """
    return {
        "FUND_API_KEY": os.getenv("FUND_API_KEY", "NOT_SET"),
        "PORT": os.getenv("PORT", "NOT_SET"),
        "status": "Environment variables loaded successfully"
    }
    
# 启动配置
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

