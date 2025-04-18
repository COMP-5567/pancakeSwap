import pandas as pd
import json
import os

# 相对路径（相对于脚本所在目录）
input_file = os.path.join("data", "processed data", "cmc_address_data.csv")
output_file = os.path.join("data", "processed data", "cmc_address_data_modified.csv")

# 确保输出目录存在
output_dir = os.path.dirname(output_file)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# 自动检测CSV分隔符
def detect_delimiter(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        first_line = f.readline()
        for delim in [',', ';', '\t']:
            if delim in first_line:
                return delim
    return ','  # 默认逗号

# 读取CSV文件
try:
    delimiter = detect_delimiter(input_file)
    encodings = ['utf-8', 'gbk', 'latin1']
    df = None
    for encoding in encodings:
        try:
            df = pd.read_csv(input_file, sep=delimiter, encoding=encoding)
            print(f"Successfully loaded {input_file} with encoding {encoding} and delimiter '{delimiter}'")
            break
        except UnicodeDecodeError:
            continue
    if df is None:
        raise ValueError("Failed to read CSV file with supported encodings.")
except Exception as e:
    print(f"Error reading input file: {e}")
    exit(1)

# 提取token_address
def extract_token_address(platform_str):
    if pd.isna(platform_str) or not isinstance(platform_str, str):
        return None
    try:
        # 解析platform列的JSON字符串
        platform_dict = json.loads(platform_str.replace("'", "\""))  # 替换单引号为双引号
        token_address = platform_dict.get("token_address", None)
        return token_address.lower() if token_address else None  # 转换为小写
    except (json.JSONDecodeError, AttributeError) as e:
        print(f"Error parsing platform: {platform_str}, Error: {e}")
        return None

# 添加token_address列
df["token_address"] = df["platform"].apply(extract_token_address)

# 保存为新的CSV文件（保留platform列）
try:
    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Modified CSV saved to {output_file}")
except Exception as e:
    print(f"Error saving CSV file: {e}")

# 打印前几行以验证结果
print("\nFirst few rows of modified data:")
print(df.head())