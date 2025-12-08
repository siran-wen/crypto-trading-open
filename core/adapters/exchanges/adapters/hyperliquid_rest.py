"""
Hyperliquid REST API模块

基于ccxt实现的REST API功能，包含市场数据、账户管理、交易操作等
"""

import asyncio
import ccxt
from datetime import datetime
from typing import Dict, List, Optional, Any
from decimal import Decimal
import ssl
import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

from .hyperliquid_base import HyperliquidBase
from ..models import (
    TickerData, OrderBookData, TradeData, BalanceData, PositionData,
    OrderData, OHLCVData, ExchangeInfo, OrderBookLevel,
    OrderSide, OrderType, OrderStatus, PositionSide, MarginMode, ExchangeType
)


class SSLAdapter(HTTPAdapter):
    """自定义 SSL 适配器 - 禁用 SSL 验证以兼容 Python 3.13"""
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        kwargs['ssl_context'] = context
        return super().init_poolmanager(*args, **kwargs)


class HyperliquidRest(HyperliquidBase):
    """Hyperliquid REST API类"""

    def __init__(self, config=None, logger=None):
        super().__init__(config)
        self.logger = logger
        self.exchange: Optional[ccxt.hyperliquid] = None
        self.max_retries = 3
        self.retry_delay = 1.0

    async def connect(self) -> bool:
        """建立连接"""
        try:
            # 创建ccxt交易所实例
            exchange_config = {
                'enableRateLimit': True,
                'options': {
                    'defaultType': 'spot',  # 使用现货类型访问永续合约
                },
            }

            # 🔥 修复：Hyperliquid使用privateKey和walletAddress认证
            if self.config and self.config.api_key:
                exchange_config['privateKey'] = self.config.api_key

                # 如果有钱包地址，添加到配置
                if self.config.wallet_address:
                    exchange_config['walletAddress'] = self.config.wallet_address

            self.exchange = ccxt.hyperliquid(exchange_config)
            
            # 🔥 修复：Python 3.13 SSL 兼容性 - 使用自定义 SSL 适配器
            if hasattr(self.exchange, 'session'):
                # 创建新的 session 并应用自定义 SSL 适配器
                session = requests.Session()
                session.mount('https://', SSLAdapter())
                session.verify = False
                self.exchange.session = session

            # 加载市场信息
            await asyncio.get_event_loop().run_in_executor(
                None, self.exchange.load_markets
            )

            if self.logger:
                auth_mode = "认证模式" if (
                    self.config and self.config.api_key) else "公共访问模式"
                self.logger.info(
                    f"Hyperliquid REST连接成功 ({auth_mode})，加载 {len(self.exchange.markets)} 个市场")

                # 🔍 调试：打印一些实际的符号格式以了解正确格式
                if self.exchange.markets:
                    sample_symbols = list(self.exchange.markets.keys())[:10]
                    self.logger.info(
                        f"🔍 Hyperliquid实际符号格式示例: {sample_symbols}")

                    # 特别检查SOL相关的符号
                    sol_symbols = [
                        s for s in self.exchange.markets.keys() if 'SOL' in s.upper()][:5]
                    if sol_symbols:
                        self.logger.info(f"🔍 SOL相关符号: {sol_symbols}")
                    else:
                        self.logger.warning("⚠️  未找到SOL相关符号")

            return True

        except Exception as e:
            if self.logger:
                self.logger.error(f"Hyperliquid REST连接失败: {str(e)}")
            return False

    async def disconnect(self) -> None:
        """断开连接"""
        if self.exchange:
            # ccxt没有显式的close方法，只需清理引用
            self.exchange = None
            if self.logger:
                self.logger.info("Hyperliquid REST连接已断开")

    async def _execute_with_retry(self, func, *args, operation_name=None, **kwargs):
        """带重试的API调用 - 支持 async 和同步函数"""
        last_error = None

        for attempt in range(self.max_retries):
            try:
                # 🔥 检查是否是协程函数
                if asyncio.iscoroutinefunction(func):
                    # 如果是 async 函数，直接await
                    result = await func(*args, **kwargs)
                else:
                    # 如果是同步函数，在 executor 中运行
                    result = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: func(*args, **kwargs)
                    )
                return result
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    operation = operation_name or func.__name__
                    if self.logger:
                        self.logger.warning(
                            f"{operation} API调用失败 (尝试 {attempt + 1}/{self.max_retries}): {str(e)}")
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    operation = operation_name or func.__name__
                    if self.logger:
                        self.logger.error(f"{operation} API调用最终失败: {str(e)}")

        raise last_error

    # === 市场数据API ===

    async def get_exchange_info(self) -> ExchangeInfo:
        """获取交易所信息"""
        return ExchangeInfo(
            name="Hyperliquid",
            id="hyperliquid",
            type=ExchangeType.PERPETUAL,
            supported_features=[
                "spot_trading", "perpetual_trading", "websocket",
                "orderbook", "ticker", "ohlcv", "user_stream"
            ],
            rate_limits=self.config.rate_limits if self.config else {},
            precision=self.config.precision if self.config else {},
            fees={},  # TODO: 获取实际费率
            markets=self.exchange.markets if self.exchange else {},
            status="operational",
            timestamp=datetime.now()
        )

    async def get_ticker(self, symbol: str) -> TickerData:
        """获取单个交易对行情"""
        mapped_symbol = self.map_symbol(symbol)

        ticker_data = await self._execute_with_retry(
            self.exchange.fetch_ticker,
            mapped_symbol,
            operation_name="get_ticker"
        )

        return self._parse_ticker(ticker_data, symbol)

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """获取多个交易对行情"""
        if symbols:
            # 获取指定交易对行情
            tasks = [self.get_ticker(symbol) for symbol in symbols]
            return await asyncio.gather(*tasks)
        else:
            # 获取所有交易对行情
            tickers_data = await self._execute_with_retry(
                self.exchange.fetch_tickers,
                operation_name="get_tickers"
            )

            return [
                self._parse_ticker(
                    ticker_data, self.reverse_map_symbol(market_symbol))
                for market_symbol, ticker_data in tickers_data.items()
            ]

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """获取订单簿"""
        mapped_symbol = self.map_symbol(symbol)

        orderbook_data = await self._execute_with_retry(
            self._fetch_orderbook,
            mapped_symbol,
            limit,
            operation_name="get_orderbook"
        )

        return self._parse_orderbook(orderbook_data, symbol)

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OHLCVData]:
        """获取K线数据"""
        mapped_symbol = self.map_symbol(symbol)
        since_timestamp = int(since.timestamp() * 1000) if since else None

        ohlcv_data = await self._execute_with_retry(
            self._fetch_ohlcv,
            mapped_symbol,
            timeframe,
            since_timestamp,
            limit,
            operation_name="get_ohlcv"
        )

        return [
            self._parse_ohlcv(candle, symbol, timeframe)
            for candle in ohlcv_data
        ]

    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[TradeData]:
        """获取最近成交记录"""
        mapped_symbol = self.map_symbol(symbol)
        since_timestamp = int(since.timestamp() * 1000) if since else None

        trades_data = await self._execute_with_retry(
            self._fetch_trades,
            mapped_symbol,
            since_timestamp,
            limit,
            operation_name="get_trades"
        )

        return [
            self._parse_trade(trade, symbol)
            for trade in trades_data
        ]

    # === 账户API ===

    async def get_balances(self) -> List[BalanceData]:
        """获取现货账户余额"""
        balance_data = await self._execute_with_retry(
            self._fetch_account_balance,
            operation_name="get_balances"
        )

        return [
            self._parse_balance(currency, balance_info)
            for currency, balance_info in balance_data.items()
            if balance_info.get('total', 0) > 0
        ]

    async def get_swap_balances(self) -> List[BalanceData]:
        """获取合约账户余额"""
        balance_data = await self._execute_with_retry(
            self._fetch_swap_account_balance,
            operation_name="get_swap_balances"
        )

        # 处理不同的数据格式，过滤掉非余额项
        result = []
        # 过滤掉系统字段
        excluded_keys = {'info', 'timestamp',
                         'datetime', 'free', 'used', 'total'}

        for currency, balance_info in balance_data.items():
            # 跳过系统字段
            if currency in excluded_keys:
                continue

            # 根据实际数据格式处理
            if isinstance(balance_info, dict):
                # 字典格式，检查total
                if balance_info.get('total', 0) > 0:
                    result.append(self._parse_balance(currency, balance_info))
            elif isinstance(balance_info, (int, float)):
                # 数值格式，直接检查值
                if balance_info > 0:
                    # 构建字典格式
                    balance_dict = {
                        'free': balance_info,
                        'used': 0.0,
                        'total': balance_info
                    }
                    result.append(self._parse_balance(currency, balance_dict))

        return result

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """获取持仓信息"""
        positions_data = await self._execute_with_retry(
            self._fetch_positions,
            operation_name="get_positions"
        )

        positions = []
        for position_info in positions_data:
            position = self._parse_position(position_info)

            # 过滤指定符号
            if symbols is None or position.symbol in symbols:
                positions.append(position)

        return positions

    # === 交易API ===

    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> OrderData:
        """创建订单"""
        mapped_symbol = self.map_symbol(symbol)
        
        # 🔥 调试日志：记录订单参数（INFO级别以便查看）
        if self.logger:
            self.logger.info(
                f"📝 创建订单参数: symbol={mapped_symbol}, "
                f"order_type={order_type.value}, side={side.value}, "
                f"amount={float(amount)}, price={float(price) if price else None}, "
                f"params={params or {}}"
            )

        order_data = await self._execute_with_retry(
            self._place_order,
            mapped_symbol,
            order_type.value,
            side.value,
            float(amount),
            float(price) if price else None,
            params or {},
            operation_name="create_order"
        )

        return self._parse_order(order_data, symbol)

    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        """取消订单"""
        mapped_symbol = self.map_symbol(symbol)

        order_data = await self._execute_with_retry(
            self._cancel_single_order,
            order_id,
            mapped_symbol,
            operation_name="cancel_order"
        )

        return self._parse_order(order_data, symbol)

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """取消所有订单"""
        if symbol:
            mapped_symbol = self.map_symbol(symbol)
            orders_data = await self._execute_with_retry(
                self._cancel_orders_by_symbol,
                mapped_symbol,
                operation_name="cancel_all_orders"
            )
        else:
            orders_data = await self._execute_with_retry(
                self._cancel_all_open_orders,
                operation_name="cancel_all_orders"
            )

        orders = []
        for order_data in orders_data:
            order = self._parse_order(
                order_data, symbol or order_data.get('symbol', ''))
            orders.append(order)

        return orders

    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """获取订单信息"""
        mapped_symbol = self.map_symbol(symbol)

        order_data = await self._execute_with_retry(
            self._fetch_order_info,
            order_id,
            mapped_symbol,
            operation_name="get_order"
        )

        return self._parse_order(order_data, symbol)

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """获取开放订单"""
        if symbol:
            mapped_symbol = self.map_symbol(symbol)
            orders_data = await self._execute_with_retry(
                self._fetch_open_orders_by_symbol,
                mapped_symbol,
                operation_name="get_open_orders"
            )
        else:
            orders_data = await self._execute_with_retry(
                self._fetch_all_open_orders,
                operation_name="get_open_orders"
            )

        return [
            self._parse_order(order_data, symbol or self.reverse_map_symbol(
                order_data.get('symbol', '')))
            for order_data in orders_data
        ]

    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OrderData]:
        """获取历史订单"""
        if symbol:
            mapped_symbol = self.map_symbol(symbol)
        else:
            mapped_symbol = None

        since_timestamp = int(since.timestamp() * 1000) if since else None

        orders_data = await self._execute_with_retry(
            self._fetch_order_history,
            mapped_symbol,
            since_timestamp,
            limit,
            operation_name="get_order_history"
        )

        return [
            self._parse_order(order_data, symbol or self.reverse_map_symbol(
                order_data.get('symbol', '')))
            for order_data in orders_data
        ]

    # === 交易设置API ===

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆倍数"""
        mapped_symbol = self.map_symbol(symbol)

        result = await self._execute_with_retry(
            self._set_position_leverage,
            mapped_symbol,
            leverage,
            operation_name="set_leverage"
        )

        return result

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置保证金模式"""
        mapped_symbol = self.map_symbol(symbol)

        result = await self._execute_with_retry(
            self._set_position_margin_mode,
            mapped_symbol,
            margin_mode,
            operation_name="set_margin_mode"
        )

        return result

    # === CCXT API调用方法 ===

    async def _fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """获取行情数据"""
        # 确保连接已建立
        if not self.exchange:
            await self.connect()

        if not self.exchange:
            raise Exception("无法建立Hyperliquid连接")

        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_ticker, symbol
        )

    async def _fetch_all_tickers(self) -> Dict[str, Any]:
        """获取所有行情数据"""
        # 确保连接已建立
        if not self.exchange:
            await self.connect()

        if not self.exchange:
            raise Exception("无法建立Hyperliquid连接")

        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_tickers
        )

    async def _fetch_orderbook(self, symbol: str, limit: Optional[int]) -> Dict[str, Any]:
        """获取订单簿"""
        # 确保连接已建立
        if not self.exchange:
            await self.connect()

        if not self.exchange:
            raise Exception("无法建立Hyperliquid连接")

        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_order_book, symbol, limit
        )

    async def _fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[int],
        limit: Optional[int]
    ) -> List[List[float]]:
        """获取K线数据"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_ohlcv, symbol, timeframe, since, limit
        )

    async def _fetch_trades(
        self,
        symbol: str,
        since: Optional[int],
        limit: Optional[int]
    ) -> List[Dict[str, Any]]:
        """获取成交数据"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_trades, symbol, since, limit
        )

    async def _fetch_account_balance(self) -> Dict[str, Any]:
        """获取现货账户余额"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_balance
        )

    async def _fetch_swap_account_balance(self) -> Dict[str, Any]:
        """获取合约账户余额"""
        # 临时切换到swap类型
        original_type = self.exchange.options.get('defaultType', 'spot')
        self.exchange.options['defaultType'] = 'swap'

        try:
            balance = await asyncio.get_event_loop().run_in_executor(
                None, self.exchange.fetch_balance
            )
            return balance
        finally:
            # 恢复原来的类型
            self.exchange.options['defaultType'] = original_type

    async def _fetch_positions(self) -> List[Dict[str, Any]]:
        """获取持仓信息"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_positions
        )

    async def _place_order(
        self,
        symbol: str,
        order_type: str,
        side: str,
        amount: float,
        price: Optional[float],
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """下单"""
        # 🔥 调试日志：记录ccxt调用参数（INFO级别以便查看）
        if self.logger:
            self.logger.info(
                f"🔍 ccxt.create_order调用: symbol={symbol}, "
                f"type={order_type}, side={side}, amount={amount}, "
                f"price={price}, params={params}, defaultType={self.exchange.options.get('defaultType', 'unknown')}"
            )
        
        # 🔥 确保params不为None（ccxt要求dict）
        if params is None:
            params = {}
        
        # 🔥 临时切换到swap类型（永续合约需要）
        original_type = self.exchange.options.get('defaultType', 'spot')
        self.exchange.options['defaultType'] = 'swap'
        
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, self.exchange.create_order, symbol, order_type, side, amount, price, params
            )
            return result
        finally:
            # 恢复原来的类型
            self.exchange.options['defaultType'] = original_type

    async def _cancel_single_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """取消单个订单"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.cancel_order, order_id, symbol
        )

    async def _cancel_orders_by_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """取消指定交易对的所有订单"""
        orders = await self._fetch_open_orders_by_symbol(symbol)
        results = []
        for order in orders:
            try:
                result = await self._cancel_single_order(order['id'], symbol)
                results.append(result)
            except Exception as e:
                if self.logger:
                    self.logger.error(f"取消订单失败 {order['id']}: {str(e)}")
        return results

    async def _cancel_all_open_orders(self) -> List[Dict[str, Any]]:
        """取消所有开放订单"""
        orders = await self._fetch_all_open_orders()
        results = []
        for order in orders:
            try:
                result = await self._cancel_single_order(order['id'], order['symbol'])
                results.append(result)
            except Exception as e:
                if self.logger:
                    self.logger.error(f"取消订单失败 {order['id']}: {str(e)}")
        return results

    async def _fetch_order_info(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """获取订单信息"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_order, order_id, symbol
        )

    async def _fetch_open_orders_by_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """获取指定交易对的开放订单"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_open_orders, symbol
        )

    async def _fetch_all_open_orders(self) -> List[Dict[str, Any]]:
        """获取所有开放订单"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_open_orders
        )

    async def _fetch_order_history(
        self,
        symbol: Optional[str],
        since: Optional[int],
        limit: Optional[int]
    ) -> List[Dict[str, Any]]:
        """获取历史订单"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.fetch_orders, symbol, since, limit
        )

    async def _set_position_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置持仓杠杆"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.set_leverage, leverage, symbol
        )

    async def _set_position_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置持仓保证金模式"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.exchange.set_margin_mode, margin_mode, symbol
        )

    # === 数据解析方法 ===

    def _parse_ticker(self, ticker_data: Dict[str, Any], symbol: str) -> TickerData:
        """解析行情数据"""
        from datetime import datetime

        return TickerData(
            symbol=symbol,
            # === 基础价格信息 ===
            bid=self._safe_decimal(ticker_data.get('bid')),
            ask=self._safe_decimal(ticker_data.get('ask')),
            bid_size=self._safe_decimal(ticker_data.get('bidVolume')),
            ask_size=self._safe_decimal(ticker_data.get('askVolume')),
            last=self._safe_decimal(ticker_data.get('last')),
            open=self._safe_decimal(ticker_data.get('open')),
            high=self._safe_decimal(ticker_data.get('high')),
            low=self._safe_decimal(ticker_data.get('low')),
            close=self._safe_decimal(ticker_data.get('close')),

            # === 成交量信息 ===
            volume=self._safe_decimal(ticker_data.get('baseVolume')),
            quote_volume=self._safe_decimal(ticker_data.get('quoteVolume')),
            trades_count=ticker_data.get('count'),

            # === 价格变化信息 ===
            change=self._safe_decimal(ticker_data.get('change')),
            percentage=self._safe_decimal(ticker_data.get('percentage')),

            # === 合约特有信息（期货/永续合约） ===
            funding_rate=None,  # Hyperliquid需要单独获取
            predicted_funding_rate=None,
            funding_time=None,
            next_funding_time=None,
            funding_interval=None,

            # === 价格参考信息 ===
            index_price=None,   # Hyperliquid需要单独获取
            mark_price=None,
            oracle_price=None,

            # === 持仓和合约信息 ===
            open_interest=None,  # Hyperliquid需要单独获取
            open_interest_value=None,
            delivery_date=None,

            # === 时间相关信息 ===
            high_time=None,
            low_time=None,
            start_time=None,
            end_time=None,

            # === 合约标识信息 ===
            contract_id=None,
            contract_name=symbol,
            base_currency=symbol.split('/')[0] if '/' in symbol else None,
            quote_currency=symbol.split(
                '/')[1].split(':')[0] if '/' in symbol and ':' in symbol else None,
            contract_size=None,
            tick_size=None,
            lot_size=None,

            # === 时间戳链条 ===
            timestamp=self._safe_parse_timestamp(
                ticker_data.get('timestamp')) or datetime.now(),
            exchange_timestamp=self._safe_parse_timestamp(
                ticker_data.get('timestamp')),
            received_timestamp=datetime.now(),
            processed_timestamp=None,
            sent_timestamp=None,

            # === 原始数据保留 ===
            raw_data=ticker_data
        )

    def _safe_parse_timestamp(self, timestamp_value: Any) -> datetime:
        """安全解析时间戳"""
        try:
            if timestamp_value is None:
                return datetime.now()

            if isinstance(timestamp_value, (int, float)):
                # 如果是毫秒时间戳，转换为秒
                if timestamp_value > 1e10:  # 毫秒级时间戳
                    return datetime.fromtimestamp(timestamp_value / 1000)
                else:  # 秒级时间戳
                    return datetime.fromtimestamp(timestamp_value)

            return datetime.now()

        except (ValueError, TypeError, OverflowError):
            return datetime.now()

    def _parse_orderbook(self, orderbook_data: Dict[str, Any], symbol: str) -> OrderBookData:
        """解析订单簿数据"""
        bids = [
            OrderBookLevel(
                price=self._safe_decimal(bid[0]),
                size=self._safe_decimal(bid[1])
            )
            for bid in orderbook_data.get('bids', [])
        ]

        asks = [
            OrderBookLevel(
                price=self._safe_decimal(ask[0]),
                size=self._safe_decimal(ask[1])
            )
            for ask in orderbook_data.get('asks', [])
        ]

        return OrderBookData(
            symbol=symbol,
            bids=bids,
            asks=asks,
            timestamp=self._safe_parse_timestamp(
                orderbook_data.get('timestamp')),
            nonce=orderbook_data.get('nonce'),
            raw_data=orderbook_data
        )

    def _parse_ohlcv(self, candle: List[float], symbol: str, timeframe: str) -> OHLCVData:
        """解析K线数据"""
        return OHLCVData(
            symbol=symbol,
            timeframe=timeframe,
            timestamp=self._safe_parse_timestamp(
                candle[0]) if candle else datetime.now(),
            open=self._safe_decimal(candle[1]) if len(candle) > 1 else None,
            high=self._safe_decimal(candle[2]) if len(candle) > 2 else None,
            low=self._safe_decimal(candle[3]) if len(candle) > 3 else None,
            close=self._safe_decimal(candle[4]) if len(candle) > 4 else None,
            volume=self._safe_decimal(candle[5]) if len(candle) > 5 else None,
            quote_volume=None,
            trades_count=None,
            raw_data={'candle': candle}
        )

    def _parse_trade(self, trade_data: Dict[str, Any], symbol: str) -> TradeData:
        """解析成交数据"""
        return TradeData(
            id=str(trade_data.get('id', '')),
            symbol=symbol,
            side=OrderSide.BUY if trade_data.get(
                'side') == 'buy' else OrderSide.SELL,
            amount=self._safe_decimal(trade_data.get('amount')),
            price=self._safe_decimal(trade_data.get('price')),
            cost=self._safe_decimal(trade_data.get('cost')),
            fee=trade_data.get('fee'),
            timestamp=self._safe_parse_timestamp(trade_data.get('timestamp')),
            order_id=trade_data.get('order'),
            raw_data=trade_data
        )

    def _parse_balance(self, currency: str, balance_info: Dict[str, Any]) -> BalanceData:
        """解析余额数据"""
        return BalanceData(
            currency=currency,
            free=self._safe_decimal(balance_info.get('free')),
            used=self._safe_decimal(balance_info.get('used')),
            total=self._safe_decimal(balance_info.get('total')),
            usd_value=None,
            timestamp=datetime.now(),
            raw_data=balance_info
        )

    def _parse_position(self, position_info: Dict[str, Any]) -> PositionData:
        """解析持仓数据"""
        symbol = self.reverse_map_symbol(position_info.get('symbol', ''))
        side = PositionSide.LONG if position_info.get(
            'side') == 'long' else PositionSide.SHORT

        return PositionData(
            symbol=symbol,
            side=side,
            size=self._safe_decimal(position_info.get('contracts', 0)),
            entry_price=self._safe_decimal(position_info.get('entryPrice')),
            mark_price=self._safe_decimal(position_info.get('markPrice')),
            current_price=self._safe_decimal(position_info.get('markPrice')),
            unrealized_pnl=self._safe_decimal(
                position_info.get('unrealizedPnl')),
            realized_pnl=self._safe_decimal(position_info.get('realizedPnl')),
            percentage=self._safe_decimal(position_info.get('percentage')),
            leverage=self._safe_int(position_info.get('leverage', 1)),
            margin_mode=MarginMode.CROSS if position_info.get(
                'marginType') == 'cross' else MarginMode.ISOLATED,
            margin=self._safe_decimal(position_info.get('initialMargin')),
            liquidation_price=self._safe_decimal(
                position_info.get('liquidationPrice')),
            timestamp=datetime.now(),
            raw_data=position_info
        )

    def _parse_order(self, order_data: Dict[str, Any], symbol: str) -> OrderData:
        """解析订单数据"""
        # 映射订单状态
        status_mapping = {
            'open': OrderStatus.OPEN,
            'closed': OrderStatus.FILLED,
            'canceled': OrderStatus.CANCELED,
            'cancelled': OrderStatus.CANCELED,
            'rejected': OrderStatus.REJECTED,
            'expired': OrderStatus.EXPIRED
        }

        status = status_mapping.get(
            order_data.get('status'), OrderStatus.UNKNOWN)

        # 映射订单类型
        type_mapping = {
            'market': OrderType.MARKET,
            'limit': OrderType.LIMIT,
            'stop': OrderType.STOP,
            'stop_limit': OrderType.STOP_LIMIT,
            'take_profit': OrderType.TAKE_PROFIT,
            'take_profit_limit': OrderType.TAKE_PROFIT_LIMIT
        }

        order_type = type_mapping.get(order_data.get('type'), OrderType.LIMIT)

        return OrderData(
            id=str(order_data.get('id', '')),
            client_id=order_data.get('clientOrderId'),
            symbol=symbol,
            side=OrderSide.BUY if order_data.get(
                'side') == 'buy' else OrderSide.SELL,
            type=order_type,
            amount=self._safe_decimal(order_data.get('amount')),
            price=self._safe_decimal(order_data.get('price')),
            filled=self._safe_decimal(order_data.get('filled')),
            remaining=self._safe_decimal(order_data.get('remaining')),
            cost=self._safe_decimal(order_data.get('cost')),
            average=self._safe_decimal(order_data.get('average')),
            status=status,
            timestamp=self._safe_parse_timestamp(order_data.get('timestamp')),
            updated=self._safe_parse_timestamp(order_data.get(
                'lastTradeTimestamp')) if order_data.get('lastTradeTimestamp') else None,
            fee=order_data.get('fee'),
            trades=order_data.get('trades', []),
            params={},
            raw_data=order_data
        )
