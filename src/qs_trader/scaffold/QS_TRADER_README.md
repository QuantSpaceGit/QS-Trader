# QS-Trader Project

> Congratulations! If you are here it means you have already installed QS-Trader as a package in your environment and have run the `qs-trader init-project <PATH>` command to scaffold a new backtesting project.

Welcome to your QS-Trader backtesting environment! This project was initialized with `qs-trader init-project` and includes everything you need to start running backtests.

## 📁 Project Structure

```text
|-- QS_TRADER_README.md              # Scaffold-specific README for this project
|-- config                         # Global system & data-source configuration
|   |-- data_sources.yaml          # Defines available datasets/adapters
|   `-- qs_trader.yaml               # Engine/system settings (execution, portfolio, paths)
|-- data                           # Local market data cache
|   |-- sample-csv                 # Tiny bundled sample dataset
|   |   |-- AAPL.csv               # Example OHLCV for AAPL
|   |   `-- README.md              # Notes about the sample data
|   `-- us-equity-yahoo-csv        # Yahoo Finance daily OHLCV store
|       |-- AAPL.csv               # Cached CSV for AAPL
|       `-- universe.json          # Symbol universe used by yahoo-update CLI
|-- experiments                    # Experiment definitions (what to backtest)
|   |-- buy_hold
|   |   |-- README.md              # Notes/documentation for this experiment
|   |   `-- buy_hold.yaml          # Canonical buy & hold experiment config
|   |-- sma_crossover
|   |   |-- README.md
|   |   `-- sma_crossover.yaml     # SMA crossover experiment config
|   |-- template
|   |   |-- README.md
|   |   `-- template.yaml          # Full configuration template to copy from
|   `-- weekly_monday_friday
|       |-- README.md
|       `-- weekly_monday_friday.yaml # Weekly entry/exit example experiment
`-- library                        # Your custom code extensions
  |-- __init__.py
  |-- adapters                     # Custom data adapters
  |   |-- README.md
  |   |-- __init__.py
  |   |-- models
  |   |   |-- __init__.py
  |   |   `-- ohlcv_csv.py         # Pydantic model for OHLCV CSV rows
  |   `-- ohlcv_csv.py             # Built-in CSV adapter implementation
  |-- indicators                   # Custom technical indicators
  |   |-- README.md
  |   `-- template.py              # Indicator template to copy
  |-- risk_policies                # Position sizing / risk rules
  |   |-- README.md
  |   `-- template.yaml            # Risk policy config template
  `-- strategies                   # Custom trading strategies
    |-- README.md
    |-- __init__.py
    |-- buy_and_hold.py            # Example buy & hold strategy
    |-- sma_crossover.py           # Example SMA crossover strategy
    `-- weekly_monday_friday.py    # Example weekday-based strategy

```

## 🚀 Quick Start

### 1. Get Data

**Option A: Use Sample Data (Limited)**

The project includes a small sample dataset in `data/sample-csv/AAPL.csv`. This is suitable for testing but has limited history. Note: AAPL is just one example ticker - you'll need to add more tickers to the universe in your experiment configuration if you want to backtest multiple symbols.

**Option B: Download Full Data with CLI**

Use QS-Trader's built-in data downloader to fetch historical data from Yahoo Finance:

```bash
# Preferred: Update all symbols in universe.json (incremental)
qs-trader data yahoo-update

# Download data for specific tickers
qs-trader data yahoo-update AAPL MSFT GOOGL

# Download for a date range
qs-trader data yahoo-update --start 2020-01-01 --end 2024-12-31

# Force full refresh (re-download everything)
qs-trader data yahoo-update --full-refresh
```

**How it works:**

- Without symbols: Reads from `data/us-equity-yahoo-csv/universe.json`
- With symbols: Updates only those tickers
- Incremental by default (only downloads missing dates)
- Data is automatically saved to `data/us-equity-yahoo-csv/` in the correct format

**Setting up your universe:**

Create `data/us-equity-yahoo-csv/universe.json`:

```json
["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]
```

Then run `qs-trader data yahoo-update` to download all tickers.

**Option C: Manual Download**

Download historical data from your preferred source and place CSV files in `data/us-equity-yahoo-csv/`.

Expected format:

```csv
Date,Open,High,Low,Close,Adj Close,Volume
2020-01-02,74.06,75.15,73.80,75.09,74.35,135480400
```

### 2. Review Experiment Configuration

Before running backtests, review the experiment configuration files to understand what will be tested:

```bash
# View buy and hold experiment config
cat experiments/buy_hold/buy_hold.yaml

# View SMA crossover experiment config
cat experiments/sma_crossover/sma_crossover.yaml
```

Key configuration sections to review:

- **backtest_id**: Unique identifier for the experiment
- **start_date / end_date**: Date range for the backtest
- **initial_equity**: Starting capital
- **data.sources**: Which data sources and symbols to use
- **strategies**: Which strategies to run and their parameters
- **reporting**: Performance metrics and output options

Make sure the symbols in your experiment configuration match the data you downloaded in step 2.

### 3. Run Example Experiments

```bash
# Run buy and hold experiment
qs-trader backtest experiments/buy_hold

# Run SMA crossover experiment
qs-trader backtest experiments/sma_crossover
```

### 4. View Results

Each experiment run creates an isolated directory with full provenance:

```
experiments/{experiment_id}/runs/{timestamp}/
├── manifest.json             # Run metadata (git info, environment, status)
├── config_snapshot.yaml      # Config used for this run
├── events.parquet            # Complete event history
├── performance.json          # Summary metrics (Sharpe, returns, drawdown)
├── timeseries/
│   ├── equity_curve.json     # Time series of portfolio value
│   ├── returns.json          # Period and cumulative returns
│   ├── trades.json           # Complete trade history
│   └── drawdowns.json        # Drawdown periods
├── trades.parquet            # All executed trades
├── returns.parquet           # Daily returns
└── drawdowns.parquet         # Drawdown analysis
```

The `latest` symlink always points to the most recent run.

## 📝 Creating Your Own Strategies

### Using Templates

Generate new strategy templates:

```bash
qs-trader init-library ./library --type strategy
```

### Registering Custom Components

Edit `config/system.yaml` and point to your custom library:

```yaml
custom_libraries:
  strategies: "./library/strategies"  # Or path to your custom library
  risk_policies: "./library/risk_policies"
  adapters: "./library/adapters"
  indicators: "./library/indicators"
```

### Creating a New Experiment

**Step 1:** Create a new experiment directory:

```bash
mkdir experiments/my_strategy
```

**Step 2:** Create configuration file (must match directory name):

```bash
cp experiments/template/template.yaml experiments/my_strategy/my_strategy.yaml
```

**Step 3:** Edit `experiments/my_strategy/my_strategy.yaml`:

- Set `backtest_id: "my_strategy"`
- Configure dates, symbols, strategies

**Step 4:** Run your experiment:

```bash
qs-trader backtest experiments/my_strategy
```

Each run creates a timestamped directory with complete provenance tracking.

## 🔧 Configuration

### System Configuration (`config/system.yaml`)

Controls HOW the system operates:

- Execution settings (slippage, commission)
- Portfolio accounting
- Data sources location
- Custom library paths
- Output directory structure
- Metadata capture (git, environment)

### Experiment Configuration (`experiments/{name}/{name}.yaml`)

Controls WHAT to backtest:

- Experiment identification
- Date range
- Universe (symbols)
- Initial capital
- Strategy selection
- Risk policy
- Reporting options

**Key Principle:** Each experiment has its own directory with canonical `{name}.yaml` file.

## 💻 CLI Command Reference

### Backtest Commands

**Run a backtest:**

```bash
qs-trader backtest <CONFIG_PATH>

# Examples:
qs-trader backtest experiments/buy_hold                    # Directory-based
qs-trader backtest experiments/buy_hold/buy_hold.yaml      # Direct file path
qs-trader backtest experiments/sma_crossover --silent      # Silent mode
qs-trader backtest experiments/buy_hold --start-date 2020-01-01 --end-date 2020-12-31
```

**Options:**

- `--silent, -s`: Silent mode (no event display, fastest execution)
- `--replay-speed, -r`: Override replay speed (-1=silent, 0=instant, >0=delay in seconds)
- `--start-date`: Override start date (YYYY-MM-DD)
- `--end-date`: Override end date (YYYY-MM-DD)
- `--log-level, -l`: Set logging level (DEBUG, INFO, WARNING, ERROR)

### Data Commands

**Update Yahoo Finance data:**

```bash
qs-trader data yahoo-update [SYMBOLS...]

# Examples:
qs-trader data yahoo-update                           # Update all symbols in universe.json
qs-trader data yahoo-update AAPL MSFT GOOGL          # Update specific symbols
qs-trader data yahoo-update --start 2020-01-01       # With date range
qs-trader data yahoo-update --full-refresh           # Force re-download all data
qs-trader data yahoo-update --data-source my-source  # Use different data source
```

**Options:**

- `--start`: Start date (YYYY-MM-DD)
- `--end`: End date (YYYY-MM-DD)
- `--full-refresh`: Re-download all data (not just incremental)
- `--data-source`: Data source name from data_sources.yaml (default: yahoo-us-equity-1d-csv)
- `--data-dir`: Override data directory path

**List available datasets:**

```bash
qs-trader data list           # Show all configured datasets
qs-trader data list --verbose # Show detailed information
```

**Browse raw data:**

```bash
qs-trader data raw --symbol AAPL --start-date 2020-01-01 --end-date 2020-12-31 --dataset yahoo-us-equity-1d-csv
```

**Cache management:**

```bash
qs-trader data cache-info --dataset yahoo-us-equity-1d-csv    # Show cache statistics
qs-trader data cache-clear --dataset yahoo-us-equity-1d-csv   # Clear cache for dataset
```

### Project Initialization Commands

**Initialize a new QS-Trader project:**

```bash
qs-trader init-project <PATH>

# Examples:
qs-trader init-project my-trading-system    # Create new project
qs-trader init-project .                    # Initialize in current directory
qs-trader init-project my-system --force    # Overwrite existing
```

**Initialize custom library components:**

```bash
qs-trader init-library <PATH> [OPTIONS]

# Examples:
qs-trader init-library ./library                    # Initialize complete library
qs-trader init-library ./library --type strategy    # Only strategy template
qs-trader init-library ./library --type indicator   # Only indicator template
qs-trader init-library ./library --type adapter     # Only adapter template
```

**Options:**

- `--type`: Component type (strategy, indicator, adapter, risk_policy)
- `--force, -f`: Overwrite existing files

### Getting Help

```bash
qs-trader --help                    # Show all commands
qs-trader backtest --help          # Show backtest command help
qs-trader data --help              # Show data commands help
qs-trader data yahoo-update --help # Show specific command help
```

## 📚 Documentation

- [QS-Trader Documentation](https://github.com/QuantSpaceGit/QS-Trader)
- [Indicators Reference](https://github.com/QuantSpaceGit/QS-Trader/tree/master/docs/packages/indicators)
- [Strategy Development Guide](https://github.com/QuantSpaceGit/QS-Trader/tree/master/docs/packages/strategy.md)

## 🤝 Need Help?

- Check the example experiments in `experiments/buy_hold/` and `experiments/sma_crossover/`
- Review example strategies in `library/strategies/`
- See `experiments/template/template.yaml` for all configuration options
- Use `qs-trader init-library .` to scaffold custom components

## 🎯 Next Steps

1. Review example experiments in `experiments/`
1. Run an example: `qs-trader backtest experiments/buy_hold`
1. Explore example strategies in `library/strategies/`
1. Customize `config/system.yaml` for your needs
1. Create your own experiment directory with canonical `{name}.yaml` file
1. Run your experiment: `qs-trader backtest experiments/my_strategy`

## 📊 Experiment Management

**Key Concepts:**

- **Experiments** = Logical groupings of related runs (e.g., "momentum_strategy")
- **Runs** = Individual executions with full provenance tracking
- **Provenance** = Git commit, environment, timestamps, config snapshot
- **Isolation** = Each run in its own timestamped directory

**Directory Naming Convention:**

```
experiments/{experiment_id}/{experiment_id}.yaml  ✅ Correct
experiments/my_strategy/my_strategy.yaml          ✅ Matches

experiments/my_strategy/config.yaml               ❌ Wrong
experiments/my_strategy/backtest.yaml             ❌ Wrong
```

**Running Experiments:**

```bash
# Preferred: Directory-based
qs-trader backtest experiments/my_strategy

# Also works: Direct file path
qs-trader backtest experiments/my_strategy/my_strategy.yaml

# Override options
qs-trader backtest experiments/my_strategy --silent
qs-trader backtest experiments/my_strategy --start-date 2020-01-01
```

Happy backtesting! 📈
