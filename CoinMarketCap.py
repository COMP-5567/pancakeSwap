import requests
import pandas as pd
import time
import os
import json

def fetch_cmc_map():
    """从CoinMarketCap API获取代币映射数据"""
    
    print("正在从CoinMarketCap获取代币映射数据...")
    
    # CoinMarketCap API URL for cryptocurrency map
    CMC_MAP_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
    
    # 使用API密钥
    api_key = "7040f455-80a8-469f-82be-ade4e3e4feb8"  # 你的API密钥
    
    headers = {
        'X-CMC_PRO_API_KEY': api_key,
        'Accept': 'application/json'
    }
    
    # 设置查询参数
    params = {
        "limit": 5000,  # 获取最多3000个代币
        "listing_status": "active",  # 只获取活跃的代币
        "sort": "cmc_rank"  # 按CoinMarketCap排名排序
    }
    
    try:
        print("发送请求到CoinMarketCap Map API...")
        response = requests.get(CMC_MAP_URL, headers=headers, params=params)
        
        # 检查响应状态码
        if response.status_code != 200:
            print(f"错误：API请求失败，状态码: {response.status_code}")
            print(f"响应内容: {response.text}")
            return None
        
        print("成功接收到Map API响应！")
        
        # 解析JSON响应
        response_json = response.json()
        
        # 检查响应中是否含有数据字段
        if "data" not in response_json:
            print(f"错误：API响应中缺少'data'字段")
            print(f"响应内容: {json.dumps(response_json, indent=2)}")
            return None
        
        data = response_json["data"]
        print(f"成功获取到 {len(data)} 个代币的映射数据")
        return data
    
    except Exception as e:
        print(f"获取映射数据时发生错误: {e}")
        return None

def fetch_cmc_listings():
    """从CoinMarketCap API获取代币列表数据"""
    
    print("正在从CoinMarketCap获取代币列表数据...")
    
    # CoinMarketCap API URL for listings
    CMC_LISTINGS_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
    
    # 使用API密钥
    api_key = "7040f455-80a8-469f-82be-ade4e3e4feb8"  # 你的API密钥
    
    headers = {
        'X-CMC_PRO_API_KEY': api_key,
        'Accept': 'application/json'
    }
    
    # 设置查询参数
    params = {
        "start": 1,
        "limit": 5000,  # 获取最多3000个代币
        "convert": "USD"  # 转换为美元
    }
    
    try:
        print("发送请求到CoinMarketCap Listings API...")
        response = requests.get(CMC_LISTINGS_URL, headers=headers, params=params)
        
        # 检查响应状态码
        if response.status_code != 200:
            print(f"错误：API请求失败，状态码: {response.status_code}")
            print(f"响应内容: {response.text}")
            return None
        
        print("成功接收到Listings API响应！")
        
        # 解析JSON响应
        response_json = response.json()
        
        # 检查响应中是否含有数据字段
        if "data" not in response_json:
            print(f"错误：API响应中缺少'data'字段")
            print(f"响应内容: {json.dumps(response_json, indent=2)}")
            return None
        
        data = response_json["data"]
        print(f"成功获取到 {len(data)} 个代币的列表数据")
        return data
    
    except Exception as e:
        print(f"获取列表数据时发生错误: {e}")
        return None

def save_map_data_to_csv():
    """将映射数据保存到CSV文件"""
    map_data = fetch_cmc_map()
    
    if not map_data:
        print("没有获取到映射数据，无法保存CSV文件。")
        return False
    
    try:
        # 将API响应直接转换为DataFrame
        df = pd.DataFrame(map_data)
        
        # 保存到CSV文件
        df.to_csv("cmc_map_data.csv", index=False)
        print(f"已保存 {len(df)} 条映射数据到 cmc_map_data.csv")
        
        # 显示数据概览
        print("\nMap数据概览:")
        print(df.head())
        print(f"\n列信息: {df.columns.tolist()}")
        
        return True
    
    except Exception as e:
        print(f"保存映射数据时发生错误: {e}")
        return False

def save_listings_data_to_csv():
    """将列表数据保存到CSV文件"""
    listings_data = fetch_cmc_listings()
    
    if not listings_data:
        print("没有获取到列表数据，无法保存CSV文件。")
        return False
    
    try:
        # 展平嵌套的JSON数据
        processed_data = []
        for item in listings_data:
            flat_item = {
                'id': item.get('id'),
                'name': item.get('name'),
                'symbol': item.get('symbol'),
                'slug': item.get('slug'),
                'cmc_rank': item.get('cmc_rank'),
                'num_market_pairs': item.get('num_market_pairs'),
                'date_added': item.get('date_added'),
                'tags': ', '.join(item.get('tags', [])),
                'max_supply': item.get('max_supply'),
                'circulating_supply': item.get('circulating_supply'),
                'total_supply': item.get('total_supply'),
                'last_updated': item.get('last_updated')
            }
            
            # 处理USD价格信息
            if 'quote' in item and 'USD' in item['quote']:
                for key, value in item['quote']['USD'].items():
                    flat_item[f'USD_{key}'] = value
            
            processed_data.append(flat_item)
        
        # 转换为DataFrame并保存
        df = pd.DataFrame(processed_data)
        df.to_csv("cmc_listings_data.csv", index=False)
        print(f"已保存 {len(df)} 条列表数据到 cmc_listings_data.csv")
        
        # 显示数据概览
        print("\nListings数据概览:")
        print(df.head())
        print(f"\n列信息: {df.columns.tolist()}")
        
        return True
    
    except Exception as e:
        print(f"保存列表数据时发生错误: {e}")
        return False

if __name__ == "__main__":
    print("开始CoinMarketCap数据获取程序...")
    
    print("\n1. 获取并保存映射数据 (Map API)")
    map_success = save_map_data_to_csv()
    
    print("\n2. 获取并保存列表数据 (Listings API)")
    listings_success = save_listings_data_to_csv()
    
    if map_success and listings_success:
        print("\n所有数据获取和保存成功！")
    elif map_success:
        print("\n只有映射数据获取成功。")
    elif listings_success:
        print("\n只有列表数据获取成功。")
    else:
        print("\n所有数据获取失败。")