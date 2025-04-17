import requests
import pandas as pd
import time

# PancakeSwap V2 Subgraph端点
SUBGRAPH_URL = "https://gateway.thegraph.com/api/subgraphs/id/Aj9TDh9SPcn7cz4DXW26ga22VnBzHhPVuKGmE4YBzDFj"


# 查询代币对和事件
def fetch_pancakeswap_data():
    # 查询代币对（pairs）
    pairs_query = """
    {
      pairs(first: 1000, orderBy: createdAtTimestamp, orderDirection: asc, where: { createdAtTimestamp_gte: 1704067200, createdAtTimestamp_lte: 1735689599 }) {
        id
        token0 { id symbol }
        token1 { id symbol }
        reserve0
        reserve1
        createdAtTimestamp
        txCount
      }
    }
    """
    # 查询事件（mints, burns, swaps）
    events_query_template = """
    {
      mints(first: 1000, skip: {skip}, orderBy: timestamp, orderDirection: asc, where: { timestamp_gte: 1704067200, timestamp_lte: 1735689599 }) {
        pair { id }
        sender
        amount0
        amount1
        timestamp
      }
      burns(first: 1000, skip: {skip}, orderBy: timestamp, orderDirection: asc, where: { timestamp_gte: 1704067200, timestamp_lte: 1735689599 }) {
        pair { id }
        sender
        amount0
        amount1
        timestamp
      }
      swaps(first: 1000, skip: {skip}, orderBy: timestamp, orderDirection: asc, where: { timestamp_gte: 1704067200, timestamp_lte: 1735689599 }) {
        pair { id }
        sender
        amount0In
        amount0Out
        amount1In
        amount1Out
        timestamp
      }
    }
    """

    # 获取代币对
    pairs_response = requests.post(SUBGRAPH_URL, json={"query": pairs_query})
    pairs_data = pairs_response.json()["data"]["pairs"]

    # 获取所有事件（通过分页）
    all_mints, all_burns, all_swaps = [], [], []
    skip = 0
    while True:
        events_query = events_query_template.format(skip=skip)
        response = requests.post(SUBGRAPH_URL, json={"query": events_query})
        data = response.json()["data"]

        mints = data["mints"]
        burns = data["burns"]
        swaps = data["swaps"]

        all_mints.extend(mints)
        all_burns.extend(burns)
        all_swaps.extend(swaps)

        if len(mints) < 1000 and len(burns) < 1000 and len(swaps) < 1000:
            break

        skip += 1000
        time.sleep(1)  # 避免触发速率限制

    return pairs_data, all_mints, all_burns, all_swaps


# 保存数据
def save_data():
    pairs_data, all_mints, all_burns, all_swaps = fetch_pancakeswap_data()

    pd.DataFrame(pairs_data).to_csv("pancakeswap_pairs.csv", index=False)
    pd.DataFrame(all_mints).to_csv("pancakeswap_mints.csv", index=False)
    pd.DataFrame(all_burns).to_csv("pancakeswap_burns.csv", index=False)
    pd.DataFrame(all_swaps).to_csv("pancakeswap_swaps.csv", index=False)
    print("All data saved.")


if __name__ == "__main__":
    save_data()