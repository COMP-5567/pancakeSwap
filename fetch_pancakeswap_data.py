# import requests
# import pandas as pd
# from datetime import datetime

# def fetch_pancakeswap_data():
#     # Subgraph URL for PancakeSwap V2
#     subgraph_url = "https://gateway.thegraph.com/api/subgraphs/id/Aj9TDh9SPcn7cz4DXW26ga22VnBzHhPVuKGmE4YBzDFj"
    
#     # 这里添加你的API key
#     API_KEY = "8a4fd40de81aac7587e04b393647c02e"  # 请替换为你的实际API key
    
#     # 添加带API key的请求头
#     headers = {
#         "Authorization": f"Bearer {API_KEY}"
#     }
    
#     print("Starting data fetch...")
    
#     # 测试查询，确保API正常工作
#     test_query = """
#     {
#       pairs(first: 1) {
#         id
#       }
#     }
#     """
    
#     print("Fetching test query...")
#     response = requests.post(subgraph_url, json={'query': test_query}, headers=headers)
#     print(f"Test Response Status Code: {response.status_code}")
#     print(f"Test Response JSON: {response.json()}")
    
#     # 获取前30条交易对数据，不限时间范围
#     print("Fetching pairs data (limited to 30 records)...")
#     pairs_query = """
#     {
#       pairs(first: 30, orderBy: volumeUSD, orderDirection: desc) {
#         id
#         token0 {
#           id
#           symbol
#         }
#         token1 {
#           id
#           symbol
#         }
#         volumeUSD
#         txCount
#       }
#     }
#     """
    
#     response = requests.post(subgraph_url, json={'query': pairs_query}, headers=headers)
#     print(f"Pairs Response Status Code: {response.status_code}")
#     result = response.json()
    
#     # 检查是否有数据
#     if 'data' not in result or 'pairs' not in result['data'] or not result['data']['pairs']:
#         print("警告：未找到任何交易对数据。API响应:", result)
#         return [], [], [], []
    
#     pairs_data = result['data']['pairs']
#     print(f"找到 {len(pairs_data)} 个交易对")
    
#     # 收集所有交易对ID
#     pair_ids = [pair['id'] for pair in pairs_data]
    
#     # 获取铸币(Mint)事件
#     all_mints = []
#     for i, pair_id in enumerate(pair_ids):
#         print(f"获取交易对 {i+1}/{len(pair_ids)} 的铸币数据")
#         mints_query = f"""
#         {{
#           mints(first: 10, where: {{ pair: "{pair_id}" }}, orderBy: timestamp, orderDirection: desc) {{
#             id
#             timestamp
#             pair {{
#               id
#               token0 {{
#                 symbol
#               }}
#               token1 {{
#                 symbol
#               }}
#             }}
#             amount0
#             amount1
#             amountUSD
#           }}
#         }}
#         """
        
#         response = requests.post(subgraph_url, json={'query': mints_query}, headers=headers)
#         if response.status_code == 200:
#             result = response.json()
#             if 'data' in result and 'mints' in result['data']:
#                 all_mints.extend(result['data']['mints'])
#         else:
#             print(f"获取铸币数据失败: {response.status_code}, {response.text}")
    
#     # 获取销毁(Burn)事件
#     all_burns = []
#     for i, pair_id in enumerate(pair_ids):
#         print(f"获取交易对 {i+1}/{len(pair_ids)} 的销毁数据")
#         burns_query = f"""
#         {{
#           burns(first: 10, where: {{ pair: "{pair_id}" }}, orderBy: timestamp, orderDirection: desc) {{
#             id
#             timestamp
#             pair {{
#               id
#               token0 {{
#                 symbol
#               }}
#               token1 {{
#                 symbol
#               }}
#             }}
#             amount0
#             amount1
#             amountUSD
#           }}
#         }}
#         """
        
#         response = requests.post(subgraph_url, json={'query': burns_query}, headers=headers)
#         if response.status_code == 200:
#             result = response.json()
#             if 'data' in result and 'burns' in result['data']:
#                 all_burns.extend(result['data']['burns'])
#         else:
#             print(f"获取销毁数据失败: {response.status_code}, {response.text}")
    
#     # 获取交换(Swap)事件
#     all_swaps = []
#     for i, pair_id in enumerate(pair_ids):
#         print(f"获取交易对 {i+1}/{len(pair_ids)} 的交换数据")
#         swaps_query = f"""
#         {{
#           swaps(first: 10, where: {{ pair: "{pair_id}" }}, orderBy: timestamp, orderDirection: desc) {{
#             id
#             timestamp
#             pair {{
#               id
#               token0 {{
#                 symbol
#               }}
#               token1 {{
#                 symbol
#               }}
#             }}
#             amount0In
#             amount1In
#             amount0Out
#             amount1Out
#             amountUSD
#           }}
#         }}
#         """
        
#         response = requests.post(subgraph_url, json={'query': swaps_query}, headers=headers)
#         if response.status_code == 200:
#             result = response.json()
#             if 'data' in result and 'swaps' in result['data']:
#                 all_swaps.extend(result['data']['swaps'])
#         else:
#             print(f"获取交换数据失败: {response.status_code}, {response.text}")
    
#     print(f"获取了 {len(all_mints)} 条铸币数据")
#     print(f"获取了 {len(all_burns)} 条销毁数据")
#     print(f"获取了 {len(all_swaps)} 条交换数据")
    
#     return pairs_data, all_mints, all_burns, all_swaps

# def save_data():
#     try:
#         pairs_data, all_mints, all_burns, all_swaps = fetch_pancakeswap_data()
        
#         # 如果所有数据都为空，提示用户
#         if not pairs_data and not all_mints and not all_burns and not all_swaps:
#             print("警告：未获取到任何数据。")
#             return
        
#         # 保存数据到CSV文件
#         pd.DataFrame(pairs_data).to_csv("pancakeswap_v2_pairs.csv", index=False)
#         print(f"已保存 {len(pairs_data)} 条交易对数据到 pancakeswap_v2_pairs.csv")
        
#         if all_mints:
#             pd.DataFrame(all_mints).to_csv("pancakeswap_v2_mints.csv", index=False)
#             print(f"已保存 {len(all_mints)} 条铸币数据到 pancakeswap_v2_mints.csv")
            
#         if all_burns:
#             pd.DataFrame(all_burns).to_csv("pancakeswap_v2_burns.csv", index=False)
#             print(f"已保存 {len(all_burns)} 条销毁数据到 pancakeswap_v2_burns.csv")
            
#         if all_swaps:
#             pd.DataFrame(all_swaps).to_csv("pancakeswap_v2_swaps.csv", index=False)
#             print(f"已保存 {len(all_swaps)} 条交换数据到 pancakeswap_v2_swaps.csv")
        
#         print("所有数据已成功保存。")
#     except Exception as e:
#         print(f"保存数据时发生错误: {e}")

# if __name__ == "__main__":
#     save_data()


import requests
import pandas as pd
import time
from datetime import datetime
import json

def fetch_pancakeswap_data():
    # Subgraph URL for PancakeSwap V2
    subgraph_url = "https://gateway.thegraph.com/api/subgraphs/id/A1fvJWQLBeUAggX2WQTMm3FKjXTekNXo77ZySun4YN2m"
    
    # 这里添加你的API key
    API_KEY = "8a4fd40de81aac7587e04b393647c02e"  # 请替换为你的实际API key
    
    # 添加带API key的请求头
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    
    print("Starting data fetch...")
    
    # 首先，使用内省查询来获取schema信息
    introspection_query = """
    {
      __schema {
        queryType {
          name
          fields {
            name
          }
        }
      }
    }
    """
    
    print("Fetching schema information...")
    response = requests.post(subgraph_url, json={'query': introspection_query}, headers=headers)
    print(f"Schema Response Status Code: {response.status_code}")
    
    available_queries = []
    if response.status_code == 200:
        result = response.json()
        if 'data' in result and '__schema' in result['data']:
            available_queries = [field['name'] for field in result['data']['__schema']['queryType']['fields']]
            print(f"可用的查询: {available_queries}")
    else:
        print(f"获取Schema失败: {response.status_code}, {response.text}")
        # 如果内省查询失败，我们尝试一些通用查询
        available_queries = ['swaps', 'bundles', 'tokens', 'pools', 'factories']
    
    # 定义数据容器
    factory_data = []
    tokens_data = []
    pairs_data = []
    swaps_data = []
    
    # 尝试获取工厂数据 (factories 或 pancakeFactories)
    if 'factories' in available_queries:
        print("Fetching factory data...")
        factory_query = """
        {
          factories(first: 5) {
            id
            poolCount
            txCount
            totalVolumeUSD
            totalFeesUSD
          }
        }
        """
        
        response = requests.post(subgraph_url, json={'query': factory_query}, headers=headers)
        if response.status_code == 200:
            result = response.json()
            if 'data' in result and 'factories' in result['data']:
                factory_data = result['data']['factories']
                print(f"找到 {len(factory_data)} 个工厂")
    elif 'pancakeFactories' in available_queries:
        print("Fetching pancakeFactory data...")
        factory_query = """
        {
          pancakeFactories(first: 5) {
            id
            pairCount
            totalVolumeUSD
            totalVolumeBNB
          }
        }
        """
        
        response = requests.post(subgraph_url, json={'query': factory_query}, headers=headers)
        if response.status_code == 200:
            result = response.json()
            if 'data' in result and 'pancakeFactories' in result['data']:
                factory_data = result['data']['pancakeFactories']
                print(f"找到 {len(factory_data)} 个工厂")
    
    time.sleep(0.5)  # 避免请求过快
    
    # 尝试获取代币数据
    if 'tokens' in available_queries:
        print("Fetching token data...")
        token_query = """
        {
          tokens(first: 1000,skip:10000) {
            id
            symbol
            name
            decimals
          }
        }
        """
        
        response = requests.post(subgraph_url, json={'query': token_query}, headers=headers)
        if response.status_code == 200:
            result = response.json()
            if 'data' in result and 'tokens' in result['data']:
                tokens_data = result['data']['tokens']
                print(f"找到 {len(tokens_data)} 个代币")
    
    time.sleep(0.5)  # 避免请求过快
    
    # 尝试获取交易对数据 (pairs 或 pools)
    if 'pairs' in available_queries:
        print("Fetching pairs data...")
        pairs_query = """
        {
          pairs(first: 50) {
            id
            token0 {
              id
              symbol
              name
            }
            token1 {
              id
              symbol
              name
            }
            volumeUSD
            txCount
          }
        }
        """
        
        response = requests.post(subgraph_url, json={'query': pairs_query}, headers=headers)
        if response.status_code == 200:
            result = response.json()
            if 'data' in result and 'pairs' in result['data']:
                pairs_data = result['data']['pairs']
                print(f"找到 {len(pairs_data)} 个交易对")
    elif 'pools' in available_queries:
        print("Fetching pools data...")
        pairs_query = """
        {
          pools(first: 1000, skip:9000) {
            id
            token0 {
              id
              symbol
              name
            }
            token1 {
              id
              symbol
              name
            }
            volumeUSD
            txCount
          }
        }
        """
        
        response = requests.post(subgraph_url, json={'query': pairs_query}, headers=headers)
        if response.status_code == 200:
            result = response.json()
            if 'data' in result and 'pools' in result['data']:
                pairs_data = result['data']['pools']
                print(f"找到 {len(pairs_data)} 个流动池")
    
    time.sleep(0.5)  # 避免请求过快
    
    # 尝试获取交换(Swap)事件
    if 'swaps' in available_queries:
        print("Fetching swap data...")
        swaps_query = """
        {
          swaps(first: 1000, skip:10000) {
            id
            timestamp
            pool {
              id
              token0 {
                symbol
                name
              }
              token1 {
                symbol
                name
              }
            }
            amount0
            amount1
            amountUSD
          }
        }
        """
        
        response = requests.post(subgraph_url, json={'query': swaps_query}, headers=headers)
        if response.status_code == 200:
            result = response.json()
            if 'data' in result and 'swaps' in result['data']:
                swaps_data = result['data']['swaps']
                print(f"找到 {len(swaps_data)} 条交换数据")
    
    # 打印获取的数据汇总
    print("\n数据获取汇总:")
    print(f"工厂数据: {len(factory_data)} 条")
    print(f"代币数据: {len(tokens_data)} 条")
    print(f"交易对/流动池数据: {len(pairs_data)} 条")
    print(f"交换数据: {len(swaps_data)} 条")
    
    return factory_data, tokens_data, pairs_data, swaps_data

def save_data():
    try:
        factory_data, tokens_data, pairs_data, swaps_data = fetch_pancakeswap_data()
        
        # 检查是否有任何数据可保存
        has_data = any([len(factory_data) > 0, len(tokens_data) > 0, len(pairs_data) > 0, len(swaps_data) > 0])
        if not has_data:
            print("警告：未获取到任何数据。")
            return
        
        # 保存工厂数据
        if factory_data:
            pd.DataFrame(factory_data).to_csv("pancakeswap_factories.csv", index=False)
            print(f"已保存 {len(factory_data)} 条工厂数据到 pancakeswap_factories.csv")
        
        # 保存代币数据
        if tokens_data:
            pd.DataFrame(tokens_data).to_csv("pancakeswap_tokens10.csv", index=False)
            print(f"已保存 {len(tokens_data)} 条代币数据到 pancakeswap_tokens.csv")
        
        # 保存交易对数据
        if pairs_data:
            # 将嵌套的token0和token1对象展平
            flat_pairs = []
            for pair in pairs_data:
                flat_pair = {'id': pair['id']}
                
                # 处理token0数据
                if 'token0' in pair:
                    for key in pair['token0']:
                        flat_pair[f'token0_{key}'] = pair['token0'][key]
                
                # 处理token1数据
                if 'token1' in pair:
                    for key in pair['token1']:
                        flat_pair[f'token1_{key}'] = pair['token1'][key]
                
                # 添加其他非嵌套字段
                for key in pair:
                    if key not in ['token0', 'token1']:
                        flat_pair[key] = pair[key]
                
                flat_pairs.append(flat_pair)
            
            pd.DataFrame(flat_pairs).to_csv("pancakeswap_pairs9.csv", index=False)
            print(f"已保存 {len(pairs_data)} 条交易对数据到 pancakeswap_pairs.csv")
        
        # 保存交换数据
        if swaps_data:
            # 展平嵌套数据
            flat_swaps = []
            for swap in swaps_data:
                flat_swap = {'id': swap['id']}
                
                # 处理pool/pair数据
                pool_key = 'pool' if 'pool' in swap else 'pair'
                if pool_key in swap:
                    flat_swap[f'{pool_key}_id'] = swap[pool_key]['id']
                    
                    # 处理token0数据
                    if 'token0' in swap[pool_key]:
                        for key in swap[pool_key]['token0']:
                            flat_swap[f'token0_{key}'] = swap[pool_key]['token0'][key]
                    
                    # 处理token1数据
                    if 'token1' in swap[pool_key]:
                        for key in swap[pool_key]['token1']:
                            flat_swap[f'token1_{key}'] = swap[pool_key]['token1'][key]
                
                # 添加其他非嵌套字段
                for key in swap:
                    if key not in [pool_key]:
                        flat_swap[key] = swap[key]
                
                flat_swaps.append(flat_swap)
            
            pd.DataFrame(flat_swaps).to_csv("pancakeswap_swaps10.csv", index=False)
            print(f"已保存 {len(swaps_data)} 条交换数据到 pancakeswap_swaps.csv")
        
        print("所有数据已成功保存。")
    except Exception as e:
        print(f"保存数据时发生错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    save_data()