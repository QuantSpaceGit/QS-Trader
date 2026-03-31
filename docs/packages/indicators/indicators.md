# Technical Indicators

QS-Trader provides 22 technical indicators across 5 categories. All indicators follow a consistent API with support for both stateful (streaming) and stateless (batch) computation modes.

## Table of Contents

- [Quick Start](#quick-start)
- [Usage Patterns](#usage-patterns)
- [Moving Averages](#moving-averages)
- [Momentum Indicators](#momentum-indicators)
- [Volatility Indicators](#volatility-indicators)
- [Volume Indicators](#volume-indicators)
- [Trend Indicators](#trend-indicators)

## Quick Start

```python
from qs_trader.libraries.indicators import RSI, MACD, ATR

# Create indicator
rsi = RSI(period=14)

# Stateful mode - process bars sequentially
for bar in bars:
    value = rsi.update(bar)
    if rsi.is_ready:
        print(f"RSI: {value:.2f}")

# Stateless mode - batch calculation
values = rsi.calculate(bars)
```

## Usage Patterns

### Stateful Mode (Streaming)

Best for real-time or sequential data processing:

```python
indicator = RSI(period=14)

for bar in bars:
    value = indicator.update(bar)

    # Check if indicator has enough data
    if indicator.is_ready:
        print(f"Value: {value}")
```

### Stateless Mode (Batch)

Best for historical analysis or vectorized operations:

```python
indicator = RSI(period=14)
values = indicator.calculate(bars)  # Returns list

# Process results
for i, value in enumerate(values):
    if value is not None:
        print(f"Bar {i}: RSI = {value:.2f}")
```

### Common Properties

All indicators provide:

```python
indicator.value       # Current value (None if not ready)
indicator.is_ready    # True if enough data processed
indicator.reset()     # Clear state for reuse
```

______________________________________________________________________

## Moving Averages

### SMA (Simple Moving Average)

Arithmetic mean of prices over a period.

**Parameters:**

- `period` (int, default=20): Number of bars to average

**Returns:** `float | None`

**Formula:**

```
SMA = (Sum of closes over period) / period
```

**Example:**

```python
from qs_trader.libraries.indicators import SMA

sma = SMA(period=20)
for bar in bars:
    value = sma.update(bar)
    if value is not None:
        print(f"20-period SMA: {value:.2f}")
```

**Use Cases:**

- Trend identification (price above/below MA)
- Support/resistance levels
- Moving average crossovers

______________________________________________________________________

### EMA (Exponential Moving Average)

Weighted average that gives more importance to recent prices.

**Parameters:**

- `period` (int, default=12): Number of bars for smoothing

**Returns:** `float | None`

**Formula:**

```
Multiplier = 2 / (period + 1)
EMA = (Close - Previous EMA) × Multiplier + Previous EMA
```

**Example:**

```python
from qs_trader.libraries.indicators import EMA

# Common EMA periods
ema_fast = EMA(period=12)
ema_slow = EMA(period=26)

for bar in bars:
    fast = ema_fast.update(bar)
    slow = ema_slow.update(bar)

    if fast and slow and fast > slow:
        print("Bullish crossover")
```

**Use Cases:**

- Faster response to price changes than SMA
- Trend following
- EMA crossover strategies

______________________________________________________________________

### WMA (Weighted Moving Average)

Linear weighted average, recent prices weighted more heavily.

**Parameters:**

- `period` (int, default=20): Number of bars

**Returns:** `float | None`

**Formula:**

```
WMA = Σ(Price × Weight) / Σ(Weight)
Where Weight = period, period-1, ..., 2, 1
```

**Example:**

```python
from qs_trader.libraries.indicators import WMA

wma = WMA(period=20)
value = wma.update(bar)
```

**Use Cases:**

- Reduced lag compared to SMA
- Smoother than EMA with less whipsaw

______________________________________________________________________

### DEMA (Double Exponential Moving Average)

Reduces lag of traditional EMA by applying EMA twice.

**Parameters:**

- `period` (int, default=20): Number of bars

**Returns:** `float | None`

**Formula:**

```
DEMA = 2 × EMA(period) - EMA(EMA(period))
```

**Example:**

```python
from qs_trader.libraries.indicators import DEMA

dema = DEMA(period=20)
value = dema.update(bar)
```

______________________________________________________________________

### TEMA (Triple Exponential Moving Average)

Further lag reduction using triple EMA calculation.

**Parameters:**

- `period` (int, default=20): Number of bars

**Returns:** `float | None`

**Formula:**

```
TEMA = 3 × EMA - 3 × EMA(EMA) + EMA(EMA(EMA))
```

**Example:**

```python
from qs_trader.libraries.indicators import TEMA

tema = TEMA(period=20)
value = tema.update(bar)
```

______________________________________________________________________

### HMA (Hull Moving Average)

Combines WMA for smoothness with reduced lag.

**Parameters:**

- `period` (int, default=20): Number of bars

**Returns:** `float | None`

**Formula:**

```
HMA = WMA(2 × WMA(period/2) - WMA(period), sqrt(period))
```

**Example:**

```python
from qs_trader.libraries.indicators import HMA

hma = HMA(period=20)
value = hma.update(bar)
```

______________________________________________________________________

### SMMA (Smoothed Moving Average)

Similar to EMA with different smoothing factor.

**Parameters:**

- `period` (int, default=20): Number of bars

**Returns:** `float | None`

**Formula:**

```
SMMA = (Previous SMMA × (period - 1) + Current Price) / period
```

**Example:**

```python
from qs_trader.libraries.indicators import SMMA

smma = SMMA(period=20)
value = smma.update(bar)
```

______________________________________________________________________

## Momentum Indicators

### RSI (Relative Strength Index)

Measures momentum on a scale of 0-100, identifying overbought/oversold conditions.

**Parameters:**

- `period` (int, default=14): Number of bars for calculation

**Returns:** `float | None` (0-100)

**Formula:**

```
RS = Average Gain / Average Loss
RSI = 100 - (100 / (1 + RS))
```

**Interpretation:**

- RSI > 70: Overbought (potential reversal down)
- RSI < 30: Oversold (potential reversal up)
- RSI = 50: Neutral momentum

**Example:**

```python
from qs_trader.libraries.indicators import RSI

rsi = RSI(period=14)
for bar in bars:
    value = rsi.update(bar)
    if value is not None:
        if value > 70:
            print(f"Overbought: RSI = {value:.2f}")
        elif value < 30:
            print(f"Oversold: RSI = {value:.2f}")
```

**Use Cases:**

- Identify overbought/oversold conditions
- Divergence analysis (price vs RSI)
- Trend strength confirmation

______________________________________________________________________

### MACD (Moving Average Convergence Divergence)

Trend-following momentum indicator showing relationship between two EMAs.

**Parameters:**

- `fast_period` (int, default=12): Fast EMA period
- `slow_period` (int, default=26): Slow EMA period
- `signal_period` (int, default=9): Signal line EMA period

**Returns:** `dict[str, float] | None`

- `macd`: MACD line value
- `signal`: Signal line value
- `histogram`: MACD - Signal

**Formula:**

```
MACD Line = EMA(fast) - EMA(slow)
Signal Line = EMA(MACD Line, signal_period)
Histogram = MACD Line - Signal Line
```

**Interpretation:**

- MACD crosses above Signal: Bullish signal
- MACD crosses below Signal: Bearish signal
- Histogram expanding: Trend strengthening
- Histogram contracting: Trend weakening

**Example:**

```python
from qs_trader.libraries.indicators import MACD

macd = MACD(fast_period=12, slow_period=26, signal_period=9)
for bar in bars:
    result = macd.update(bar)
    if result:
        macd_line = result['macd']
        signal_line = result['signal']
        histogram = result['histogram']

        if macd_line > signal_line:
            print(f"Bullish: MACD ({macd_line:.2f}) > Signal ({signal_line:.2f})")
```

**Use Cases:**

- Trend identification and reversals
- Crossover trading signals
- Divergence analysis

______________________________________________________________________

### Stochastic Oscillator

Compares closing price to high-low range over period (0-100).

**Parameters:**

- `k_period` (int, default=14): %K period (fast line)
- `d_period` (int, default=3): %D period (slow line, SMA of %K)

**Returns:** `dict[str, float] | None`

- `k`: Fast stochastic line
- `d`: Slow stochastic line

**Formula:**

```
%K = 100 × (Close - Lowest Low) / (Highest High - Lowest Low)
%D = SMA(%K, d_period)
```

**Interpretation:**

- %K > 80: Overbought
- %K < 20: Oversold
- %K crosses above %D: Bullish signal
- %K crosses below %D: Bearish signal

**Example:**

```python
from qs_trader.libraries.indicators import Stochastic

stoch = Stochastic(k_period=14, d_period=3)
for bar in bars:
    result = stoch.update(bar)
    if result:
        k = result['k']
        d = result['d']

        if k > 80:
            print(f"Overbought: %K = {k:.2f}")
        elif k < 20:
            print(f"Oversold: %K = {k:.2f}")
```

______________________________________________________________________

### CCI (Commodity Channel Index)

Measures deviation of price from its statistical mean.

**Parameters:**

- `period` (int, default=20): Number of bars
- `constant` (float, default=0.015): Scaling constant

**Returns:** `float | None` (unbounded, typically -100 to +100)

**Formula:**

```
Typical Price = (High + Low + Close) / 3
CCI = (Typical Price - SMA(Typical Price)) / (constant × Mean Deviation)
```

**Interpretation:**

- CCI > +100: Strong uptrend, overbought
- CCI < -100: Strong downtrend, oversold
- CCI between -100 and +100: No clear trend

**Example:**

```python
from qs_trader.libraries.indicators import CCI

cci = CCI(period=20)
for bar in bars:
    value = cci.update(bar)
    if value is not None:
        if value > 100:
            print(f"Overbought: CCI = {value:.2f}")
        elif value < -100:
            print(f"Oversold: CCI = {value:.2f}")
```

______________________________________________________________________

### ROC (Rate of Change)

Measures percentage change in price over a period.

**Parameters:**

- `period` (int, default=12): Number of bars to look back

**Returns:** `float | None` (percentage)

**Formula:**

```
ROC = ((Current Close - Close N periods ago) / Close N periods ago) × 100
```

**Interpretation:**

- ROC > 0: Upward momentum
- ROC < 0: Downward momentum
- ROC magnitude: Strength of momentum

**Example:**

```python
from qs_trader.libraries.indicators import ROC

roc = ROC(period=12)
for bar in bars:
    value = roc.update(bar)
    if value is not None:
        print(f"12-period ROC: {value:.2f}%")
```

______________________________________________________________________

### Williams %R

Momentum oscillator similar to Stochastic but inverted (-100 to 0).

**Parameters:**

- `period` (int, default=14): Number of bars

**Returns:** `float | None` (-100 to 0)

**Formula:**

```
%R = -100 × (Highest High - Close) / (Highest High - Lowest Low)
```

**Interpretation:**

- %R > -20: Overbought
- %R < -80: Oversold
- Crosses above -80: Potential buy signal
- Crosses below -20: Potential sell signal

**Example:**

```python
from qs_trader.libraries.indicators import WilliamsR

williams = WilliamsR(period=14)
for bar in bars:
    value = williams.update(bar)
    if value is not None:
        if value > -20:
            print(f"Overbought: %R = {value:.2f}")
        elif value < -80:
            print(f"Oversold: %R = {value:.2f}")
```

______________________________________________________________________

## Volatility Indicators

### ATR (Average True Range)

Measures market volatility using true range.

**Parameters:**

- `period` (int, default=14): Number of bars for smoothing

**Returns:** `float | None`

**Formula:**

```
True Range = max(High - Low, |High - Prev Close|, |Low - Prev Close|)
ATR = Smoothed average of True Range (Wilder's smoothing)
```

**Interpretation:**

- High ATR: High volatility, wider stops
- Low ATR: Low volatility, tighter stops
- Rising ATR: Increasing volatility
- Falling ATR: Decreasing volatility

**Example:**

```python
from qs_trader.libraries.indicators import ATR

atr = ATR(period=14)
for bar in bars:
    value = atr.update(bar)
    if value is not None:
        # Use ATR for stop-loss placement
        stop_distance = 2 * value  # 2x ATR
        print(f"ATR: {value:.2f}, Suggested stop: {stop_distance:.2f}")
```

**Use Cases:**

- Position sizing based on volatility
- Stop-loss placement
- Breakout confirmation
- Market regime identification

______________________________________________________________________

### Bollinger Bands

Volatility bands around a moving average.

**Parameters:**

- `period` (int, default=20): Moving average period
- `std_dev` (float, default=2.0): Standard deviations for bands

**Returns:** `dict[str, float] | None`

- `upper`: Upper band
- `middle`: Middle band (SMA)
- `lower`: Lower band

**Formula:**

```
Middle Band = SMA(period)
Upper Band = Middle + (std_dev × Standard Deviation)
Lower Band = Middle - (std_dev × Standard Deviation)
```

**Interpretation:**

- Price near upper band: Overbought
- Price near lower band: Oversold
- Bands widening: Increasing volatility
- Bands contracting: Decreasing volatility ("Squeeze")

**Example:**

```python
from qs_trader.libraries.indicators import BollingerBands

bb = BollingerBands(period=20, std_dev=2.0)
for bar in bars:
    result = bb.update(bar)
    if result:
        upper = result['upper']
        middle = result['middle']
        lower = result['lower']

        if bar.close > upper:
            print(f"Price above upper band: potential reversal")
        elif bar.close < lower:
            print(f"Price below lower band: potential reversal")

        # Measure squeeze
        bandwidth = (upper - lower) / middle * 100
        print(f"Bandwidth: {bandwidth:.2f}%")
```

**Use Cases:**

- Overbought/oversold conditions
- Volatility analysis
- Mean reversion strategies
- Breakout detection

______________________________________________________________________

### StdDev (Standard Deviation)

Measures price dispersion around the mean.

**Parameters:**

- `period` (int, default=20): Number of bars

**Returns:** `float | None`

**Formula:**

```
StdDev = sqrt(Σ(Close - Mean)² / period)
```

**Example:**

```python
from qs_trader.libraries.indicators import StdDev

stddev = StdDev(period=20)
for bar in bars:
    value = stddev.update(bar)
    if value is not None:
        print(f"Volatility (StdDev): {value:.2f}")
```

**Use Cases:**

- Volatility measurement
- Risk assessment
- Component of Bollinger Bands

______________________________________________________________________

## Volume Indicators

### VWAP (Volume Weighted Average Price)

Average price weighted by volume, typically reset daily.

**Parameters:**

- None (cumulative from start)

**Returns:** `float | None`

**Formula:**

```
Typical Price = (High + Low + Close) / 3
VWAP = Σ(Typical Price × Volume) / Σ(Volume)
```

**Interpretation:**

- Price above VWAP: Bullish (buyers in control)
- Price below VWAP: Bearish (sellers in control)
- VWAP as support/resistance level

**Example:**

```python
from qs_trader.libraries.indicators import VWAP

vwap = VWAP()
for bar in bars:
    value = vwap.update(bar)
    if value is not None:
        if bar.close > value:
            print(f"Price above VWAP ({value:.2f}) - bullish")
        else:
            print(f"Price below VWAP ({value:.2f}) - bearish")
```

**Use Cases:**

- Institutional trading benchmark
- Intraday support/resistance
- Trade execution quality assessment
- Trend direction confirmation

______________________________________________________________________

### OBV (On-Balance Volume)

Cumulative volume indicator based on price direction.

**Parameters:**

- None (cumulative from start)

**Returns:** `float | None`

**Formula:**

```
If Close > Previous Close: OBV = Previous OBV + Volume
If Close < Previous Close: OBV = Previous OBV - Volume
If Close = Previous Close: OBV = Previous OBV
```

**Interpretation:**

- Rising OBV: Accumulation (buying pressure)
- Falling OBV: Distribution (selling pressure)
- OBV divergence from price: Potential reversal

**Example:**

```python
from qs_trader.libraries.indicators import OBV

obv = OBV()
prev_obv = None

for bar in bars:
    value = obv.update(bar)
    if value is not None and prev_obv is not None:
        if value > prev_obv:
            print(f"Accumulation: OBV rising to {value:,.0f}")
        elif value < prev_obv:
            print(f"Distribution: OBV falling to {value:,.0f}")
        prev_obv = value
```

**Use Cases:**

- Volume trend confirmation
- Divergence analysis
- Accumulation/distribution detection

______________________________________________________________________

### A/D (Accumulation/Distribution Line)

Money flow indicator using price position within range.

**Parameters:**

- None (cumulative from start)

**Returns:** `float | None`

**Formula:**

```
Money Flow Multiplier = ((Close - Low) - (High - Close)) / (High - Low)
Money Flow Volume = Money Flow Multiplier × Volume
A/D = Previous A/D + Money Flow Volume
```

**Interpretation:**

- Rising A/D: Accumulation (buying pressure)
- Falling A/D: Distribution (selling pressure)
- A/D divergence from price: Potential reversal
- Close near high → Multiplier near +1 (buying)
- Close near low → Multiplier near -1 (selling)

**Example:**

```python
from qs_trader.libraries.indicators import AD

ad = AD()
prev_ad = None

for bar in bars:
    value = ad.update(bar)
    if value is not None and prev_ad is not None:
        if value > prev_ad and bar.close < prev_close:
            print("Bullish divergence: A/D rising, price falling")
        prev_ad = value
```

______________________________________________________________________

### CMF (Chaikin Money Flow)

Period-based version of A/D, normalized between -1 and +1.

**Parameters:**

- `period` (int, default=20): Number of bars

**Returns:** `float | None` (-1 to +1)

**Formula:**

```
CMF = Σ(Money Flow Volume over period) / Σ(Volume over period)
```

**Interpretation:**

- CMF > 0.25: Strong buying pressure
- CMF > 0: Accumulation
- CMF < 0: Distribution
- CMF < -0.25: Strong selling pressure

**Example:**

```python
from qs_trader.libraries.indicators import CMF

cmf = CMF(period=20)
for bar in bars:
    value = cmf.update(bar)
    if value is not None:
        if value > 0.25:
            print(f"Strong buying pressure: CMF = {value:.2f}")
        elif value < -0.25:
            print(f"Strong selling pressure: CMF = {value:.2f}")
```

**Use Cases:**

- Buying/selling pressure measurement
- Trend confirmation
- Divergence analysis

______________________________________________________________________

## Trend Indicators

### ADX (Average Directional Index)

Measures trend strength (0-100), regardless of direction.

**Parameters:**

- `period` (int, default=14): Number of bars

**Returns:** `dict[str, float] | None`

- `adx`: ADX value (trend strength)
- `plus_di`: +DI (positive directional indicator)
- `minus_di`: -DI (negative directional indicator)

**Formula:**

```
True Range = max(High - Low, |High - Prev Close|, |Low - Prev Close|)
+DM = High - Prev High (if positive and > -DM, else 0)
-DM = Prev Low - Low (if positive and > +DM, else 0)
+DI = 100 × Smoothed(+DM) / Smoothed(TR)
-DI = 100 × Smoothed(-DM) / Smoothed(TR)
DX = 100 × |+DI - -DI| / (+DI + -DI)
ADX = Smoothed(DX)
```

**Interpretation:**

- **ADX Values:**
  - < 20: Weak or absent trend
  - 20-25: Developing trend
  - 25-50: Strong trend
  - > 50: Very strong trend
- **Directional Indicators:**
  - +DI > -DI: Uptrend
  - -DI > +DI: Downtrend
- **ADX Movement:**
  - Rising: Trend strengthening
  - Falling: Trend weakening

**Example:**

```python
from qs_trader.libraries.indicators import ADX

adx = ADX(period=14)
for bar in bars:
    result = adx.update(bar)
    if result:
        adx_value = result['adx']
        plus_di = result['plus_di']
        minus_di = result['minus_di']

        # Check trend strength
        if adx_value > 25:
            trend = "up" if plus_di > minus_di else "down"
            print(f"Strong {trend}trend: ADX = {adx_value:.2f}")
            print(f"+DI = {plus_di:.2f}, -DI = {minus_di:.2f}")
        elif adx_value < 20:
            print(f"Weak trend: ADX = {adx_value:.2f}")
```

**Use Cases:**

- Determine if market is trending or ranging
- Filter trades (avoid trading in weak trends)
- Trend strength confirmation
- Combine with directional indicators for trade direction

**Strategy Ideas:**

```python
# Example: Only trade when trend is strong
if result['adx'] > 25:
    if result['plus_di'] > result['minus_di']:
        # Strong uptrend - look for long entries
        pass
    else:
        # Strong downtrend - look for short entries
        pass
```

______________________________________________________________________

### Aroon

Identifies trend changes and measures trend strength.

**Parameters:**

- `period` (int, default=25): Number of bars to look back

**Returns:** `dict[str, float] | None`

- `aroon_up`: Aroon Up value (0-100)
- `aroon_down`: Aroon Down value (0-100)
- `oscillator`: Aroon Up - Aroon Down

**Formula:**

```
Aroon Up = ((period - periods since highest high) / period) × 100
Aroon Down = ((period - periods since lowest low) / period) × 100
Aroon Oscillator = Aroon Up - Aroon Down
```

**Interpretation:**

- **Aroon Up:**
  - > 70: Strong uptrend
  - = 100: New high just made
  - < 30: No upward momentum
- **Aroon Down:**
  - > 70: Strong downtrend
  - = 100: New low just made
  - < 30: No downward momentum
- **Both < 50:** Consolidation period
- **Oscillator > 0:** Uptrend
- **Oscillator < 0:** Downtrend

**Crossover Signals:**

- Aroon Up crosses above Aroon Down: Bullish signal
- Aroon Down crosses above Aroon Up: Bearish signal

**Example:**

```python
from qs_trader.libraries.indicators import Aroon

aroon = Aroon(period=25)
for bar in bars:
    result = aroon.update(bar)
    if result:
        aroon_up = result['aroon_up']
        aroon_down = result['aroon_down']
        oscillator = result['oscillator']

        # Identify strong trends
        if aroon_up > 70 and aroon_down < 30:
            print(f"Strong uptrend: Up={aroon_up:.0f}, Down={aroon_down:.0f}")
        elif aroon_down > 70 and aroon_up < 30:
            print(f"Strong downtrend: Up={aroon_up:.0f}, Down={aroon_down:.0f}")
        elif aroon_up < 50 and aroon_down < 50:
            print(f"Consolidation: Up={aroon_up:.0f}, Down={aroon_down:.0f}")

        # Oscillator signal
        if oscillator > 50:
            print("Strong bullish momentum")
        elif oscillator < -50:
            print("Strong bearish momentum")
```

**Use Cases:**

- Identify new trends early
- Detect consolidation periods
- Trend strength measurement
- Complement to ADX (Aroon shows direction and recency)

**Strategy Ideas:**

```python
# Example: Trend change detection
prev_result = None
for bar in bars:
    result = aroon.update(bar)
    if result and prev_result:
        # Bullish crossover
        if (result['aroon_up'] > result['aroon_down'] and
            prev_result['aroon_up'] <= prev_result['aroon_down']):
            print("Bullish crossover - potential buy signal")

        # Bearish crossover
        if (result['aroon_down'] > result['aroon_up'] and
            prev_result['aroon_down'] <= prev_result['aroon_up']):
            print("Bearish crossover - potential sell signal")

        prev_result = result
```

______________________________________________________________________

## Best Practices

### Indicator Combination

Combine indicators from different categories for better signals:

```python
# Trend + Momentum confirmation
adx = ADX(period=14)
rsi = RSI(period=14)

for bar in bars:
    adx_result = adx.update(bar)
    rsi_value = rsi.update(bar)

    if adx_result and rsi_value:
        # Strong uptrend + not overbought
        if (adx_result['adx'] > 25 and
            adx_result['plus_di'] > adx_result['minus_di'] and
            rsi_value < 70):
            print("Strong confirmed uptrend - good long opportunity")
```

### Performance Considerations

```python
# For large datasets, use batch mode
rsi = RSI(period=14)
macd = MACD()

# More efficient than update() in loop
rsi_values = rsi.calculate(bars)
macd_values = macd.calculate(bars)

# Process results
for i, (rsi_val, macd_val) in enumerate(zip(rsi_values, macd_values)):
    if rsi_val is not None and macd_val is not None:
        # Analyze signals
        pass
```

### Error Handling

```python
from qs_trader.libraries.indicators import RSI

# Invalid parameters raise ValueError
try:
    rsi = RSI(period=0)  # Will raise ValueError
except ValueError as e:
    print(f"Invalid parameter: {e}")

# Check if indicator is ready before using value
rsi = RSI(period=14)
for bar in bars:
    value = rsi.update(bar)

    if not rsi.is_ready:
        print("Warming up...")
        continue

    # Safe to use value
    if value > 70:
        print("Overbought")
```

### State Management

```python
# Reset indicator to process new data
rsi = RSI(period=14)

# Process first dataset
for bar in first_dataset:
    rsi.update(bar)

# Reset for second dataset
rsi.reset()

# Process second dataset
for bar in second_dataset:
    rsi.update(bar)
```

______________________________________________________________________

## Additional Resources

- [QS-Trader Main Documentation](../../README.md)
- [Strategy Development Guide](../strategies/README.md)
- [Backtesting Guide](../cli/backtest.md)

______________________________________________________________________

## Contributing

To add new indicators:

1. Inherit from `BaseIndicator`
1. Implement required methods: `calculate()`, `update()`, `reset()`, `value`, `is_ready`
1. Add comprehensive tests
1. Update this documentation

See existing indicator implementations in `src/qs_trader/libraries/indicators/buildin/` for examples.
