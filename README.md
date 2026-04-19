# Polymarket BTC 15 分钟工具集

这个仓库用于从 Dome 拉取 BTC 15 分钟 `Up/Down` Polymarket 市场的订单簿快照，对齐 Chainlink `btc/usd` 价格，转换成 CSV，并生成用于快速核验的图表。

## 仓库功能

- 解析形如 `btc-updown-15m-{slot_start_utc_timestamp}` 的市场名称
- 只拉取市场真实活跃交易的 15 分钟窗口
- 保存原始订单簿历史，或转换后的扁平化 CSV 快照
- 为每个 15 分钟市场回填 Chainlink BTC 价格文件
- 将 BTC 价格列合并回订单簿 CSV
- 生成市场概览图，便于快速核验数据

## 环境准备

建议使用 Python 3.10 及以上版本。

先安装依赖：

```powershell
pip install dome-api-sdk pandas matplotlib seaborn
```

如果你使用 Conda，也可以先创建虚拟环境再安装：

```powershell
conda create -n polymarket python=3.11 -y
conda activate polymarket
pip install dome-api-sdk pandas matplotlib seaborn
```

## 配置 API Key

项目默认从根目录的 `key.txt` 读取 Dome API Key。

你需要在仓库根目录新建一个 `key.txt` 文件，二选一即可：

```text
your_api_key_here
```

或者：

```text
key = your_api_key_here
```

注意：

- `key.txt` 已经写入 `.gitignore`，不会被 git 上传
- 不要把真实密钥写进 Python 脚本或 README
- 如果你想用别的文件名，可以在命令里加 `--key-file <路径>`

## 主要脚本

- `fetch_btc_15m_orderbooks.py`：拉取原始订单簿历史并保存为 `jsonl.gz`
- `convert_orderbooks_to_csv.py`：将原始 `jsonl.gz` 转成扁平 CSV，并保留前 N 档
- `fetch_btc_15m_orderbook_csvs.py`：直接导出订单簿 CSV
- `backfill_btc_price_files.py`：为每个市场写出 BTC 价格 JSON/CSV
- `merge_btc_prices_into_csvs.py`：把 BTC 价格列合并回订单簿 CSV
- `fetch_chainlink_prices.py`：单独拉取 Chainlink 价格
- `plot_market_overview.py`：为单个增强版市场绘制概览图
- `plot_up_down_btc_dual_axis.py`：绘制简单的双 Y 轴对比图
- `plot_up_down_combined_svg.py`：输出 SVG 版的 Up/Down 对比图

## 推荐使用流程

推荐按下面 3 步顺序执行：

1. 先拉订单簿 CSV
2. 再拉 BTC 价格文件
3. 最后把 BTC 价格合并回订单簿 CSV

### 第一步：拉取订单簿 CSV

拉取最近 30 天的 BTC 15 分钟市场订单簿：

```powershell
python fetch_btc_15m_orderbook_csvs.py --days 30 --output-dir data/btc_15m_orderbook_csv_month_seq
```

如果只想先测试最近几个市场：

```powershell
python fetch_btc_15m_orderbook_csvs.py --days 30 --max-slots 4 --output-dir data/btc_15m_orderbook_csv_probe
```

### 第二步：回填每个市场对应的 BTC 价格文件

```powershell
python backfill_btc_price_files.py --input-dir data/btc_15m_orderbook_csv_month_seq
```

这一步会在输入目录下生成：

```text
data/btc_15m_orderbook_csv_month_seq/prices/YYYY-MM-DD/{market_slug}__btc_price.csv
data/btc_15m_orderbook_csv_month_seq/prices/YYYY-MM-DD/{market_slug}__btc_price.json
```

### 第三步：把 BTC 价格合并回订单簿 CSV

```powershell
python merge_btc_prices_into_csvs.py `
  --input-dir data/btc_15m_orderbook_csv_month_seq `
  --output-dir data/btc_15m_orderbook_csv_month_enriched
```

合并后，每条订单簿快照会多出这些字段：

- `btc_price`
- `btc_price_timestamp`
- `btc_price_timestamp_utc`
- `btc_price_match`

## 单独功能示例

### 拉取某个时间段的 Chainlink BTC 价格

```powershell
python fetch_chainlink_prices.py `
  --currency btc/usd `
  --start 2026-04-15T14:15:00+00:00 `
  --end 2026-04-15T14:30:00+00:00 `
  --output-dir data/chainlink_prices_example
```

### 拉取原始订单簿历史并保存为 jsonl.gz

```powershell
python fetch_btc_15m_orderbooks.py --days 7 --output-dir data/btc_15m_orderbooks_raw
```

### 把原始 jsonl.gz 转成 CSV

```powershell
python convert_orderbooks_to_csv.py `
  --input data/btc_15m_orderbooks_raw `
  --output-dir data/btc_15m_orderbooks_raw_csv `
  --depth 5 `
  --overwrite
```

## 画图

为单个增强版市场生成概览图：

```powershell
python plot_market_overview.py `
  --up data/btc_15m_orderbook_csv_month_enriched/2026-03-19/btc-updown-15m-1773893700__up.csv `
  --down data/btc_15m_orderbook_csv_month_enriched/2026-03-19/btc-updown-15m-1773893700__down.csv `
  --output docs/examples/market_overview_1773893700.png
```

这张概览图是单面板，包含：

- `Up` 和 `Down` 的最优档中间价
- 右侧 Y 轴上的 BTC/USD 价格
- Up/Down 两边各自的 bid/ask 价格带

## 输出目录说明

常见目录结构如下：

```text
data/
  btc_15m_orderbook_csv_month_seq/
    2026-03-19/
      btc-updown-15m-...__up.csv
      btc-updown-15m-...__down.csv
    prices/
      2026-03-19/
        btc-updown-15m-...__btc_price.csv
        btc-updown-15m-...__btc_price.json
  btc_15m_orderbook_csv_month_enriched/
    2026-03-19/
      btc-updown-15m-...__up.csv
      btc-updown-15m-...__down.csv
```

## 示例图

### 示例 1

`btc-updown-15m-1773893700`

![Market overview 1773893700](docs/examples/market_overview_1773893700.png)

原始数据：

- [btc-updown-15m-1773893700__up.csv](docs/example_csv/btc-updown-15m-1773893700__up.csv)
- [btc-updown-15m-1773893700__down.csv](docs/example_csv/btc-updown-15m-1773893700__down.csv)

### 示例 2

`btc-updown-15m-1773947700`

![Market overview 1773947700](docs/examples/market_overview_1773947700.png)

原始数据：

- [btc-updown-15m-1773947700__up.csv](docs/example_csv/btc-updown-15m-1773947700__up.csv)
- [btc-updown-15m-1773947700__down.csv](docs/example_csv/btc-updown-15m-1773947700__down.csv)

### 示例 3

`btc-updown-15m-1774040400`

![Market overview 1774040400](docs/examples/market_overview_1774040400.png)

原始数据：

- [btc-updown-15m-1774040400__up.csv](docs/example_csv/btc-updown-15m-1774040400__up.csv)
- [btc-updown-15m-1774040400__down.csv](docs/example_csv/btc-updown-15m-1774040400__down.csv)

## 常见问题

### 1. 为什么会出现几十秒没有订单簿快照？

Dome 的订单簿历史是事件驱动的，不是固定频率采样，所以长时间空档并不一定表示脚本出错。

### 2. 为什么 `Up` 和 `Down` 看起来像互补？

这是预测市场的正常表现。很多时候 `Up mid + Down mid` 会非常接近 `1`。

### 3. 如果接口返回 502 怎么办？

脚本里已经带了重试逻辑。再次运行时也可以基于已有文件继续，不需要每次从头开始。

## 说明

- `key.txt` 和 `data/` 已被 `.gitignore` 忽略，不会上传到仓库
- 示例图使用的是仓库里的样例 CSV，不依赖你的本地密钥
- CSV 转换逻辑会先将 bids 按价格降序排序、asks 按价格升序排序，再截取最优档位
