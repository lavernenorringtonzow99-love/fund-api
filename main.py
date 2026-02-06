import os
import json
import time
import httpx
import pandas as pd
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import akshare as ak

app = FastAPI(title="金融数据服务", version="7.0")

def safe_float(val, default=None):
    if val is None or val in ["-", "", "None"]:
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
# 健康检查
# ======================
@app.get("/health")
async def health():
    return {"status": "ok", "time": int(time.time())}

# ======================
# 基金详情（使用天天基金官方 API）
# ======================
@app.get("/fund/single")
async def get_fund(
    fund_code: str = Query(..., regex=r"^\d{6}$"),
    api_key: str = Query(...)
):
    expected_key = os.getenv("FUND_API_KEY", "test")
    if api_key != expected_key:
        raise HTTPException(status_code=403, detail="Invalid API Key")

    try:
        url = f"http://fundgz.1234567.com.cn/js/{fund_code}.js"
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url)
        
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail="基金接口无响应")
            
        text = response.text
        if not text.startswith('jsonpgz') or '(' not in text:
            raise HTTPException(status_code=500, detail="基金数据格式异常")
            
        start = text.find('(') + 1
        end = text.rfind(')')
        data = json.loads(text[start:end])
        
        if data.get("code") != fund_code:
            raise HTTPException(status_code=404, detail="基金代码无效")
            
        return JSONResponse({
            "code": 200,
            "data": {
                "fund_code": data["code"],
                "fund_name": data["name"],
                "unit_nav": safe_float(data.get("dwjz")),      # 单位净值（T-1）
                "estimate_nav": safe_float(data.get("gsz")),   # 估算净值（实时）
                "estimate_growth": safe_float(data.get("gszzl")),  # 估算涨幅%
                "date": data.get("gztime", "")[:10]
            }
        })
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"[FUND_ERROR] {str(e)} | Code: {fund_code}")
        raise HTTPException(status_code=500, detail="基金查询失败")

# ======================
# 市场资金流向（AKShare + 容错）
# ======================
@app.get("/market/flow")
async def get_market_flow(
    market: str = Query("all", enum=["sh", "sz", "all"]),
    api_key: str = Query(...)
):
    expected_key = os.getenv("FUND_API_KEY", "test")
    if api_key != expected_key:
        raise HTTPException(status_code=403, detail="Invalid API Key")

    try:
        # 尝试获取资金流数据
        df = ak.stock_market_fund_flow()
        if df.empty:
            raise HTTPException(status_code=500, detail="资金流数据为空")

        name_map = {"sh": "沪市", "sz": "深市", "all": "沪深两市"}
        target = name_map[market]

        # 先精确匹配
        row = df[df['板块'] == target]
        if row.empty:
            # 再模糊匹配（兼容不同版本字段）
            mask = df['板块'].astype(str).str.contains(
                "沪" if market == "sh" else "深" if market == "sz" else "两|沪深",
                na=False
            )
            candidates = df[mask]
            if candidates.empty:
                raise HTTPException(status_code=404, detail="未找到市场数据")
            row = candidates.head(1)

        d = row.iloc[0]
        return JSONResponse({
            "code": 200,
            "data": {
                "market": safe_str(d.get("板块", target)),
                "main_net_inflow": safe_float(d.get("主力净流入-净额")),      # 亿元
                "retail_net_inflow": safe_float(d.get("散户净流入-净额")),     # 亿元
                "main_net_ratio": safe_float(d.get("主力净流入-净占比")),     # %
                "update_time": safe_str(d.get("日期")) or safe_str(d.get("更新时间"))
            }
        })

    except HTTPException:
        raise
    except Exception as e:
        err = str(e)[:200]
        print(f"[MARKET_ERROR] {err}")
        if 'df' in locals():
            print(f"[COLUMNS] {list(df.columns)}")
        raise HTTPException(status_code=500, detail="资金流查询失败")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, workers=1)
