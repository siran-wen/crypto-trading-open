"""
Lighter交易所适配器 - REST API模块

封装Lighter SDK的REST API功能，提供市场数据、账户信息和交易功能

⚠️  Lighter交易所特殊说明：

1. Market ID从0开始（不是1）：
   - market_id=0: ETH
   - market_id=1: BTC
   - market_id=2: SOL

2. 动态价格精度（关键特性）：
   - 不同交易对使用不同的价格精度
   - 价格乘数公式: price_int = price_usd × (10 ** price_decimals)
   - 数量使用1e5: base_amount = quantity × 100000 (实际测试确认)

   示例：
   - ETH (2位小数): $4127.39 × 100 = 412739
   - BTC (1位小数): $114357.8 × 10 = 1143578
   - SOL (3位小数): $199.058 × 1000 = 199058
   - DOGE (6位小数): $0.202095 × 1000000 = 202095

3. 必须使用order_books() API：
   - 获取完整市场列表必须用order_books()
   - order_book_details(market_id) 只返回活跃市场
   - 不能通过循环遍历market_id来发现市场

这些设计是Lighter作为Layer 2 DEX的优化选择，与传统CEX不同！

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔥 重要修复说明（2025-11-03）：价格精度问题（双重保险）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

【修复目标】
确保 _query_order_index() 能够正确找到刚下的订单，即使传入的价格精度不正确。

【修复方案】
在 _query_order_index() 开头，对传入的 price 和 amount 做 quantize 处理：
- 获取市场信息，得到 price_decimals
- 对 price 做 quantize（与下单时相同的精度）
- 对 amount 做 quantize（避免浮点误差）

【为什么需要双重保险】
虽然 grid_config.py 已经修复了价格范围的精度问题，但这里的修复可以：
1. 防止其他地方传入错误精度的价格
2. 确保查询逻辑的健壮性
3. 作为最后一道防线，确保查询一定能成功

【影响范围】
所有交易所均受益，特别是价格移动网格模式。
"""

from typing import Dict, Any, Optional, List
from decimal import Decimal
from datetime import datetime
import asyncio
import logging

try:
    import lighter
    from lighter import Configuration, ApiClient, SignerClient
    from lighter.api import AccountApi, OrderApi, TransactionApi, CandlestickApi, FundingApi
    LIGHTER_AVAILABLE = True
except ImportError:
    LIGHTER_AVAILABLE = False
    logging.warning(
        "lighter SDK未安装。请执行: pip install git+https://github.com/elliottech/lighter-python.git")

from .lighter_base import LighterBase
from ..models import (
    TickerData, OrderBookData, TradeData, BalanceData,
    OrderData, PositionData, ExchangeInfo, OrderBookLevel, OrderSide, OrderType, OrderStatus
)

# 配置 logger 输出到文件
logger = logging.getLogger(__name__)
if not logger.handlers:
    import os
    from logging.handlers import RotatingFileHandler
    from pathlib import Path

    # 确保日志目录存在
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    # 添加文件处理器
    log_file = log_dir / "ExchangeAdapter.log"
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5*1024*1024,  # 5MB
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)  # 🔥 修改为 INFO，确保关键日志能输出
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)  # 🔥 修改为 INFO


class LighterRest(LighterBase):
    """Lighter REST API封装类"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化Lighter REST客户端

        Args:
            config: 配置字典
        """
        if not LIGHTER_AVAILABLE:
            raise ImportError("lighter SDK未安装，无法使用Lighter适配器")

        super().__init__(config)

        # 初始化客户端
        self.api_client: Optional[ApiClient] = None
        self.signer_client: Optional[SignerClient] = None

        # API实例
        self.account_api: Optional[AccountApi] = None
        self.order_api: Optional[OrderApi] = None
        self.transaction_api: Optional[TransactionApi] = None
        self.candlestick_api: Optional[CandlestickApi] = None
        self.funding_api: Optional[FundingApi] = None

        # 连接状态
        self._connected = False

        # 🔥 初始化markets字典（用于WebSocket共享）
        self.markets = {}

        # 🔥 滑点配置（默认万分之2 = 0.02%）
        self.base_slippage = Decimal(str(config.get('slippage', '0.0002')))

        # 🔥 市场信息缓存（避免频繁调用API触发429限流）
        # 这是关键修复！没有这个缓存会导致批量下单时触发429
        self._market_info_cache = {}  # {symbol: {info, timestamp}}

        # 🔥 初始化WebSocket模块（用于订单成交监控）
        try:
            from .lighter_websocket import LighterWebSocket
            self._websocket = LighterWebSocket(config)
            logger.info("✅ Lighter WebSocket模块已初始化")
        except Exception as e:
            logger.warning(f"⚠️ Lighter WebSocket初始化失败: {e}")
            self._websocket = None

        logger.info("Lighter REST客户端初始化完成")

    def is_connected(self) -> bool:
        """检查是否已连接"""
        return self._connected

    async def connect(self):
        """连接（调用initialize）"""
        await self.initialize()

    async def initialize(self):
        """初始化API客户端"""
        try:
            # 创建API客户端
            configuration = Configuration(host=self.base_url)
            self.api_client = ApiClient(configuration=configuration)

            # 创建各种API实例
            self.account_api = AccountApi(self.api_client)
            self.order_api = OrderApi(self.api_client)
            self.transaction_api = TransactionApi(self.api_client)
            self.candlestick_api = CandlestickApi(self.api_client)
            self.funding_api = FundingApi(self.api_client)

            # 如果配置了私钥，创建签名客户端
            if self.api_key_private_key:
                # 🔥 调试：打印私钥信息（不打印完整私钥，只打印长度和前几个字符）
                private_key_len = len(self.api_key_private_key)
                private_key_preview = self.api_key_private_key[:20] + "..." if private_key_len > 20 else self.api_key_private_key
                logger.info(f"🔍 调试：私钥长度={private_key_len}, 预览={private_key_preview}, account_index={self.account_index}, api_key_index={self.api_key_index}")
                
                self.signer_client = SignerClient(
                    url=self.base_url,
                    private_key=self.api_key_private_key,
                    account_index=self.account_index,
                    api_key_index=self.api_key_index,
                )

                # 检查客户端
                err = self.signer_client.check_client()
                if err is not None:
                    error_msg = self.parse_error(err)
                    logger.error(f"SignerClient检查失败: {error_msg}")
                    raise Exception(f"SignerClient初始化失败: {error_msg}")

            self._connected = True
            logger.info("Lighter REST客户端连接成功")

            # 加载市场信息
            await self._load_markets()

            # 🔥 连接WebSocket（如果已初始化）
            if self._websocket:
                try:
                    logger.info(
                        f"🔗 准备连接WebSocket - markets数量: {len(self.markets)}, _markets_cache数量: {len(self._markets_cache)}")

                    # 共享markets信息给WebSocket
                    self._websocket.markets = self.markets
                    self._websocket._markets_cache = getattr(
                        self, '_markets_cache', {})

                    await self._websocket.connect()
                    logger.info("✅ Lighter WebSocket已连接")
                except Exception as ws_err:
                    logger.warning(
                        f"⚠️ Lighter WebSocket连接失败: {ws_err}，将使用fallback方案")
                    import traceback
                    logger.debug(f"WebSocket错误详情:\n{traceback.format_exc()}")
            else:
                logger.warning("⚠️ WebSocket模块未初始化")

        except Exception as e:
            logger.error(f"Lighter REST客户端初始化失败: {e}")
            raise

    async def _find_and_close_sessions(self, obj, visited=None):
        """
        递归查找并关闭所有 aiohttp ClientSession 对象（异步版本）
        
        Args:
            obj: 要搜索的对象
            visited: 已访问的对象集合（防止循环引用）
        """
        if visited is None:
            visited = set()
        
        # 防止循环引用
        obj_id = id(obj)
        if obj_id in visited:
            return
        visited.add(obj_id)
        
        try:
            import aiohttp
            
            # 如果对象本身就是 ClientSession，关闭它
            if isinstance(obj, aiohttp.ClientSession):
                if not obj.closed:
                    try:
                        await obj.close()
                        logger.debug("✅ 已关闭一个 ClientSession")
                    except Exception as e:
                        logger.debug(f"关闭 ClientSession 时出错: {e}")
                return
            
            # 递归搜索对象的属性
            if hasattr(obj, '__dict__'):
                for attr_name, attr_value in obj.__dict__.items():
                    try:
                        await self._find_and_close_sessions(attr_value, visited)
                    except Exception:
                        pass  # 忽略无法访问的属性
            
            # 如果是列表或元组，递归搜索每个元素
            if isinstance(obj, (list, tuple)):
                for item in obj:
                    await self._find_and_close_sessions(item, visited)
            
            # 如果是字典，递归搜索每个值
            if isinstance(obj, dict):
                for value in obj.values():
                    await self._find_and_close_sessions(value, visited)
                    
        except Exception as e:
            logger.debug(f"搜索 ClientSession 时出错: {e}")

    async def close(self):
        """关闭连接"""
        try:
            # 🔥 断开WebSocket
            if self._websocket:
                try:
                    await self._websocket.disconnect()
                except Exception as ws_err:
                    logger.warning(f"断开WebSocket时出错: {ws_err}")

            # 🔥 关闭 SignerClient 和 ApiClient
            if self.signer_client:
                try:
                    await self.signer_client.close()
                    # 递归查找并关闭 SignerClient 内部的所有 ClientSession
                    await self._find_and_close_sessions(self.signer_client)
                except Exception as e:
                    logger.debug(f"关闭 SignerClient 时出错: {e}")
            
            if self.api_client:
                try:
                    await self.api_client.close()
                    # 递归查找并关闭 ApiClient 内部的所有 ClientSession
                    await self._find_and_close_sessions(self.api_client)
                except Exception as e:
                    logger.debug(f"关闭 ApiClient 时出错: {e}")
            
            # 🔥 额外等待，确保所有异步操作完成
            await asyncio.sleep(0.5)
            
            self._connected = False
            logger.info("Lighter REST客户端已关闭")
        except Exception as e:
            logger.error(f"关闭Lighter REST客户端时出错: {e}")

    async def disconnect(self):
        """断开连接（调用close）"""
        await self.close()

    # ============= 市场数据 =============

    async def _load_markets(self):
        """
        加载市场信息

        ⚠️  重要说明：
        - Lighter的market_id从0开始，不是从1开始
        - ETH的market_id是0（这是最重要的市场）
        - 必须使用order_books() API获取完整列表，不能用循环遍历market_id
        - order_book_details(market_id) 只返回有交易的市场，可能漏掉不活跃的市场

        市场ID示例：
        - market_id=0: ETH (价格精度: 2位小数, 乘数: 100)
        - market_id=1: BTC (价格精度: 1位小数, 乘数: 10)
        - market_id=2: SOL (价格精度: 3位小数, 乘数: 1000)
        """
        try:
            # 获取订单簿列表（包含市场信息）
            # ⚠️ 必须使用此API，它会返回所有市场包括market_id=0的ETH
            response = await self.order_api.order_books()

            if hasattr(response, 'order_books'):
                markets = []
                for order_book_info in response.order_books:
                    if hasattr(order_book_info, 'symbol') and hasattr(order_book_info, 'market_id'):
                        market_info = {
                            "market_id": order_book_info.market_id,  # 使用API返回的真实 market_id
                            "symbol": order_book_info.symbol,
                        }
                        markets.append(market_info)

                        # 🔥 同时填充 self.markets 字典（用于WebSocket）
                        self.markets[order_book_info.symbol] = market_info

                self.update_markets_cache(markets)
                logger.info(f"加载了 {len(markets)} 个市场")

        except Exception as e:
            logger.error(f"加载市场信息失败: {e}")

    async def get_exchange_info(self) -> ExchangeInfo:
        """
        获取交易所信息

        Returns:
            ExchangeInfo对象
        """
        try:
            # 获取订单簿信息
            response = await self.order_api.order_books()

            symbols = []
            if hasattr(response, 'order_books'):
                for ob in response.order_books:
                    if hasattr(ob, 'symbol') and hasattr(ob, 'market_id'):
                        # 🔥 关键修复：提取price_decimals，确保WebSocket解析时能正确获取
                        # 从order_book对象中提取price_decimals（如果有的话）
                        price_decimals = self._extract_price_decimals(ob)

                        # 保持原始符号格式，在监控层统一处理
                        symbols.append({
                            "symbol": ob.symbol,
                            "market_id": ob.market_id,  # 使用 market_id 而不是 index
                            "market_index": ob.market_id,  # 🔥 添加market_index别名，与缓存键一致
                            "base_asset": ob.symbol.split('-')[0] if '-' in ob.symbol else "",
                            "quote_asset": ob.symbol.split('-')[1] if '-' in ob.symbol else "USD",
                            "status": getattr(ob, 'status', 'trading'),
                            "price_decimals": price_decimals,  # 🔥 添加价格精度
                            # 🔥 添加价格乘数
                            "price_multiplier": Decimal(10 ** price_decimals),
                        })

            # 创建 ExchangeInfo 对象
            info = ExchangeInfo(
                name="Lighter",
                id="lighter",
                type=None,
                supported_features=[],
                rate_limits={},
                precision={},
                fees={},
                markets={s['symbol']: s for s in symbols},
                status="online",
                timestamp=datetime.now()
            )
            info.symbols = symbols  # 添加 symbols 属性以保持兼容性
            return info

        except Exception as e:
            logger.error(f"获取交易所信息失败: {e}")
            raise

    async def get_ticker(self, symbol: str) -> Optional[TickerData]:
        """
        获取ticker数据

        Args:
            symbol: 交易对符号

        Returns:
            TickerData对象
        """
        try:
            market_id = self.get_market_index(symbol)
            if market_id is None:
                logger.warning(f"未找到交易对 {symbol} 的市场ID")
                return None

            # 获取市场统计信息（包含价格信息）
            response = await self.order_api.order_book_details(market_id=market_id)

            if not response or not hasattr(response, 'order_book_details') or not response.order_book_details:
                return None

            detail = response.order_book_details[0]

            # 解析ticker数据（基于实际API返回字段）
            # 🔥 修复：不使用0作为默认值，避免返回无效价格（多交易所兼容性）
            last_price = self._safe_decimal(
                getattr(detail, 'last_trade_price', None))
            daily_high = self._safe_decimal(
                getattr(detail, 'daily_price_high', None))
            daily_low = self._safe_decimal(
                getattr(detail, 'daily_price_low', None))
            daily_volume = self._safe_decimal(
                getattr(detail, 'daily_base_token_volume', None))

            # 尝试获取最佳买卖价（从订单簿）
            bid_price = last_price
            ask_price = last_price
            try:
                orderbook_response = await self.order_api.order_book_orders(
                    market_id=market_id, limit=1)
                if orderbook_response.bids:
                    bid_price = self._safe_decimal(
                        orderbook_response.bids[0].price)
                if orderbook_response.asks:
                    ask_price = self._safe_decimal(
                        orderbook_response.asks[0].price)
            except Exception as e:
                logger.debug(f"无法获取订单簿最佳价格: {e}")

            return TickerData(
                symbol=symbol,
                last=last_price,
                bid=bid_price,
                ask=ask_price,
                volume=daily_volume,
                high=daily_high,
                low=daily_low,
                timestamp=datetime.now()
            )

        except Exception as e:
            logger.error(f"获取ticker失败 {symbol}: {e}")
            return None

    async def get_orderbook(self, symbol: str, limit: int = 20) -> Optional[OrderBookData]:
        """
        获取订单簿

        Args:
            symbol: 交易对符号
            limit: 深度限制

        Returns:
            OrderBookData对象
        """
        try:
            market_id = self.get_market_index(symbol)
            if market_id is None:
                logger.warning(f"未找到交易对 {symbol} 的市场ID")
                return None

            # 使用 order_book_orders 获取订单簿深度
            response = await self.order_api.order_book_orders(market_id=market_id, limit=limit)

            if not response:
                return None

            # 解析买单和卖单
            # Lighter返回完整订单对象：{price, remaining_base_amount, order_id, ...}
            bids = []
            asks = []

            if hasattr(response, 'bids') and response.bids:
                for bid in response.bids[:limit]:
                    # 提取 price 和 remaining_base_amount
                    price = self._safe_decimal(getattr(bid, 'price', 0))
                    quantity = self._safe_decimal(
                        getattr(bid, 'remaining_base_amount', 0))

                    if price > 0 and quantity > 0:
                        bids.append(OrderBookLevel(
                            price=price,
                            size=quantity
                        ))

            if hasattr(response, 'asks') and response.asks:
                for ask in response.asks[:limit]:
                    # 提取 price 和 remaining_base_amount
                    price = self._safe_decimal(getattr(ask, 'price', 0))
                    quantity = self._safe_decimal(
                        getattr(ask, 'remaining_base_amount', 0))

                    if price > 0 and quantity > 0:
                        asks.append(OrderBookLevel(
                            price=price,
                            size=quantity
                        ))

            return OrderBookData(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=datetime.now(),
                nonce=None
            )

        except Exception as e:
            logger.error(f"获取订单簿失败 {symbol}: {e}")
            return None

    async def get_recent_trades(self, symbol: str, limit: int = 100) -> List[TradeData]:
        """
        获取最近成交

        Args:
            symbol: 交易对符号
            limit: 数量限制

        Returns:
            TradeData列表
        """
        try:
            market_id = self.get_market_index(symbol)
            if market_id is None:
                logger.warning(f"未找到交易对 {symbol} 的市场ID")
                return []

            # 获取最近成交
            response = await self.order_api.recent_trades(market_id=market_id, limit=limit)

            trades = []
            if hasattr(response, 'trades') and response.trades:
                for trade in response.trades:
                    price = self._safe_decimal(trade.price) if hasattr(
                        trade, 'price') else Decimal("0")
                    amount = self._safe_decimal(trade.size) if hasattr(
                        trade, 'size') else Decimal("0")
                    cost = price * amount

                    # 解析成交方向
                    is_ask = getattr(trade, 'is_ask', False)
                    side = OrderSide.SELL if is_ask else OrderSide.BUY

                    trades.append(TradeData(
                        id=str(getattr(trade, 'trade_id', '')),
                        symbol=symbol,
                        side=side,
                        amount=amount,
                        price=price,
                        cost=cost,
                        fee=None,
                        timestamp=self._parse_timestamp(
                            getattr(trade, 'timestamp', None)) or datetime.now(),
                        order_id=str(getattr(trade, 'order_id', '')),
                        raw_data={'trade': trade}
                    ))

            return trades

        except Exception as e:
            logger.error(f"获取最近成交失败 {symbol}: {e}")
            return []

    # ============= 账户信息 =============

    async def get_account_balance(self) -> List[BalanceData]:
        """
        获取账户余额

        Returns:
            BalanceData列表
        """
        if not self.signer_client:
            logger.error("未配置SignerClient，无法获取账户信息")
            return []

        try:
            # 获取账户信息
            response = await self.account_api.account(by="index", value=str(self.account_index))

            balances = []

            # 解析 DetailedAccounts 结构
            if hasattr(response, 'accounts') and response.accounts:
                # 获取第一个账户（通常就是查询的账户）
                account = response.accounts[0]

                # 获取可用余额和抵押品（USDC余额）
                available_balance = self._safe_decimal(
                    getattr(account, 'available_balance', 0))
                collateral = self._safe_decimal(
                    getattr(account, 'collateral', 0))

                # 计算锁定余额（抵押品 - 可用余额）
                locked = max(collateral - available_balance, Decimal("0"))

                # USDC 余额（Lighter是合约交易所，只有USDC保证金）
                if collateral > 0:
                    from datetime import datetime
                    balances.append(BalanceData(
                        currency="USDC",
                        free=available_balance,
                        used=locked,
                        total=collateral,
                        usd_value=collateral,
                        timestamp=datetime.now(),
                        raw_data={'account': account}
                    ))

                # 注意：Lighter是合约交易所，持仓不是余额
                # 持仓应该通过 get_positions() 方法查询

            return balances

        except Exception as e:
            logger.error(f"获取账户余额失败: {e}")
            import traceback
            traceback.print_exc()
            return []

    def _is_token_expired_error(self, error: Exception) -> bool:
        """
        检测是否是 token 过期错误
        
        Args:
            error: 异常对象
            
        Returns:
            是否是 token 过期错误
        """
        # 获取异常的字符串表示
        error_str = str(error).lower()
        error_repr = repr(error).lower()
        
        # 检测多种 token 过期错误格式
        token_expired_indicators = [
            "expired token",
            "invalid auth: expired token",
            "401",
            "unauthorized",
            "20013",  # Lighter 特定的错误代码
            "code:20013",  # JSON 格式的错误代码
            '"code":20013',  # JSON 格式（带引号）
        ]
        
        # 检查异常字符串
        for indicator in token_expired_indicators:
            if indicator in error_str or indicator in error_repr:
                logger.debug(f"🔍 检测到 token 过期指示符: {indicator}")
                return True
        
        # 检查异常对象的属性（Lighter SDK 的 ApiException 可能有 status、body 等属性）
        if hasattr(error, 'status'):
            status = getattr(error, 'status', None)
            if status == 401:
                logger.debug(f"🔍 检测到 HTTP 401 状态码")
                return True
        
        if hasattr(error, 'body'):
            body = str(getattr(error, 'body', ''))
            if any(indicator in body.lower() for indicator in token_expired_indicators):
                logger.debug(f"🔍 检测到 token 过期错误在 body 中: {body[:100]}")
                return True
        
        if hasattr(error, 'reason'):
            reason = str(getattr(error, 'reason', '')).lower()
            if "unauthorized" in reason:
                logger.debug(f"🔍 检测到 Unauthorized reason")
                return True
        
        # 检查是否有 api_err 属性
        if hasattr(error, 'api_err'):
            api_err_str = str(getattr(error, 'api_err', '')).lower()
            if any(indicator in api_err_str for indicator in token_expired_indicators):
                logger.debug(f"🔍 检测到 token 过期错误在 api_err 中")
                return True
        
        # 🔥 最后检查：如果异常字符串包含 "401" 和 "expired" 或 "unauthorized"
        if "401" in error_str and ("expired" in error_str or "unauthorized" in error_str):
            logger.debug(f"🔍 检测到 401 + expired/unauthorized 组合")
            return True
        
        return False

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """
        获取活跃订单（支持 token 过期自动重试）

        Args:
            symbol: 交易对符号（可选，为None时获取所有）

        Returns:
            OrderData列表
        """
        if not self.signer_client:
            logger.error("未配置SignerClient，无法获取订单信息")
            return []

        max_retries = 3  # 最多重试3次（初始尝试 + 2次重试）
        
        for attempt in range(max_retries):
            try:
                # 🔥 在重试前检查 SignerClient 状态
                if attempt > 0:
                    import time
                    logger.info(f"🔄 重试前检查 SignerClient 状态 (尝试 {attempt + 1}/{max_retries})...")
                    err = self.signer_client.check_client()
                    if err is not None:
                        error_msg = self.parse_error(err)
                        logger.warning(f"⚠️ SignerClient 状态检查失败: {error_msg}，尝试重新初始化...")
                        # 尝试重新创建 SignerClient
                        try:
                            import lighter
                            self.signer_client = SignerClient(
                                url=self.base_url,
                                private_key=self.api_key_private_key,
                                account_index=self.account_index,
                                api_key_index=self.api_key_index,
                            )
                            err = self.signer_client.check_client()
                            if err is not None:
                                logger.error(f"❌ 重新初始化 SignerClient 失败: {self.parse_error(err)}")
                                return []
                            logger.info(f"✅ SignerClient 重新初始化成功")
                        except Exception as init_err:
                            logger.error(f"❌ 重新初始化 SignerClient 异常: {init_err}")
                            return []
                    else:
                        logger.info(f"✅ SignerClient 状态检查通过")
                
                # 🔥 修复：Lighter 需要使用专门的订单查询 API，而不是 account API
                # 生成认证令牌（使用SDK推荐的10分钟过期时间）
                import lighter
                import time
                token_start_time = time.time()
                logger.info(f"🔑 正在生成认证令牌 (尝试 {attempt + 1}/{max_retries})...")
                auth_token, err = self.signer_client.create_auth_token_with_expiry(
                    lighter.SignerClient.DEFAULT_10_MIN_AUTH_EXPIRY
                )
                token_gen_time = time.time() - token_start_time
                if err:
                    logger.error(f"❌ 生成认证令牌失败: {err} (耗时 {token_gen_time:.3f}秒)")
                    # 如果是 token 生成失败，也尝试重试
                    if attempt < max_retries - 1:
                        logger.warning(f"⚠️ Token 生成失败，等待后重试...")
                        await asyncio.sleep(1.0)  # 等待更长时间
                        continue
                    return []
                # 🔥 诊断：验证token是否由SDK正确生成
                if not auth_token:
                    logger.error(f"❌ Token生成失败：auth_token为空或None")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(1.0)
                        continue
                    return []
                
                # 🔥 诊断：记录token的基本信息（不记录完整token，只记录前20个字符用于验证）
                token_preview = auth_token[:20] + "..." if len(auth_token) > 20 else auth_token
                logger.info(f"✅ 认证令牌生成成功 (耗时 {token_gen_time:.3f}秒, token长度={len(auth_token)}, 预览={token_preview})")
                
                # 🔥 诊断：检查系统时间同步问题
                import datetime
                system_time = datetime.datetime.now()
                logger.debug(f"🔍 系统时间: {system_time.isoformat()}")

                # 获取 market_id
                market_id = None
                if symbol:
                    market_id = self.get_market_index(symbol)
                    if market_id is None:
                        logger.warning(f"未找到交易对 {symbol} 的市场索引")
                        return []

                # 使用 account_active_orders API（SDK 方法是异步的，直接 await）
                api_start_time = time.time()
                time_since_token_gen = api_start_time - token_start_time
                logger.info(f"📡 调用 API (token生成后 {time_since_token_gen:.3f}秒)...")
                
                # 🔥 诊断：如果时间差过大，记录警告
                if time_since_token_gen > 1.0:
                    logger.warning(f"⚠️ Token生成到API调用的时间差较大: {time_since_token_gen:.3f}秒，可能影响token有效性")
                
                # 🔥 诊断：验证token是否正确传递给API
                if not auth_token:
                    logger.error(f"❌ Token为空，无法传递给API")
                    return []
                
                logger.debug(f"🔍 准备调用API: account_index={self.account_index}, market_id={market_id if market_id is not None else 255}, token长度={len(auth_token)}")
                response = await self.order_api.account_active_orders(
                    account_index=self.account_index,
                    market_id=market_id if market_id is not None else 255,  # 255 = 所有市场
                    auth=auth_token  # 🔥 Token由Lighter SDK的SignerClient生成，直接传递给SDK的OrderApi
                )
                api_time = time.time() - api_start_time
                logger.info(f"✅ API 调用成功 (耗时 {api_time:.3f}秒)")

                orders = []

                # 🔥 account_active_orders 返回 orders 列表，不是 accounts
                if hasattr(response, 'orders') and response.orders:
                    logger.info(f"🔍 REST API返回 {len(response.orders)} 个活跃订单")

                    for order_info in response.orders:
                        order_symbol = self._get_symbol_from_market_index(
                            getattr(order_info, 'market_index', None))

                        # 如果指定了symbol，过滤
                        if symbol and order_symbol != symbol:
                            continue

                        orders.append(self._parse_order(order_info, order_symbol))
                else:
                    logger.info(f"✅ REST API确认无活跃订单")

                return orders

            except Exception as e:
                # 🔥 检测 token 过期错误并自动重试
                is_token_expired = self._is_token_expired_error(e)
                
                if is_token_expired:
                    logger.warning(f"⚠️ 检测到 token 过期错误 (尝试 {attempt + 1}/{max_retries})")
                    logger.info(f"错误详情: {type(e).__name__}: {e}")
                    
                    # 🔥 详细的错误诊断信息
                    logger.info("=" * 80)
                    logger.info("🔍 详细错误诊断信息")
                    logger.info("=" * 80)
                    
                    # 1. HTTP响应信息
                    if hasattr(e, 'status'):
                        logger.info(f"📡 HTTP 状态码: {e.status}")
                    if hasattr(e, 'reason'):
                        logger.info(f"📡 HTTP 原因: {e.reason}")
                    if hasattr(e, 'body'):
                        logger.info(f"📡 HTTP 响应体: {e.body}")
                    
                    # 2. Token信息
                    logger.info(f"🔑 Token信息:")
                    logger.info(f"   - Token长度: {len(auth_token) if auth_token else 0}")
                    logger.info(f"   - Token预览: {auth_token[:30] + '...' if auth_token and len(auth_token) > 30 else auth_token}")
                    logger.info(f"   - Token生成耗时: {token_gen_time:.3f}秒")
                    logger.info(f"   - Token生成到API调用时间差: {time_since_token_gen:.3f}秒")
                    
                    # 3. SignerClient信息
                    logger.info(f"🔧 SignerClient信息:")
                    logger.info(f"   - Account Index: {self.account_index}")
                    logger.info(f"   - API Key Index: {self.api_key_index}")
                    logger.info(f"   - Base URL: {self.base_url}")
                    if self.signer_client:
                        try:
                            err_check = self.signer_client.check_client()
                            logger.info(f"   - SignerClient状态: {'正常' if err_check is None else f'异常: {err_check}'}")
                        except Exception as check_err:
                            logger.info(f"   - SignerClient状态检查失败: {check_err}")
                    
                    # 4. SDK版本信息
                    try:
                        import lighter
                        sdk_version = getattr(lighter, '__version__', '未知')
                        logger.info(f"📦 SDK版本信息:")
                        logger.info(f"   - lighter版本: {sdk_version}")
                        # 检查是否有DEFAULT_10_MIN_AUTH_EXPIRY常量
                        if hasattr(lighter.SignerClient, 'DEFAULT_10_MIN_AUTH_EXPIRY'):
                            expiry = lighter.SignerClient.DEFAULT_10_MIN_AUTH_EXPIRY
                            if expiry == -1:
                                logger.info(f"   - 默认Token过期时间: {expiry} (使用SDK默认值，通常为10分钟)")
                            else:
                                logger.info(f"   - 默认Token过期时间: {expiry}秒 ({expiry/60:.1f}分钟)")
                        else:
                            logger.warning(f"   - DEFAULT_10_MIN_AUTH_EXPIRY 常量不存在，SDK版本可能过旧")
                    except Exception as sdk_err:
                        logger.warning(f"   - 无法获取SDK版本: {sdk_err}")
                    
                    # 5. 系统时间信息
                    import datetime
                    system_time = datetime.datetime.now()
                    utc_time = datetime.datetime.utcnow()
                    logger.info(f"⏰ 系统时间信息:")
                    logger.info(f"   - 本地时间: {system_time.isoformat()}")
                    logger.info(f"   - UTC时间: {utc_time.isoformat()}")
                    logger.info(f"   - 时间差: {(system_time - utc_time).total_seconds() / 3600:.1f}小时")
                    
                    # 从HTTP响应头获取服务器时间
                    if hasattr(e, 'headers') and e.headers:
                        server_date = e.headers.get('Date', '')
                        if server_date:
                            logger.info(f"   - 服务器时间 (HTTP Date头): {server_date}")
                            # 尝试解析服务器时间并计算时间差
                            try:
                                from email.utils import parsedate_to_datetime
                                server_time = parsedate_to_datetime(server_date)
                                time_diff = (utc_time - server_time.replace(tzinfo=None)).total_seconds()
                                logger.info(f"   - 与服务器时间差: {time_diff:.1f}秒 ({abs(time_diff):.1f}秒{'快' if time_diff > 0 else '慢'})")
                                if abs(time_diff) > 60:
                                    logger.warning(f"   ⚠️ 时间差超过60秒，可能导致token验证失败！")
                            except Exception as parse_err:
                                logger.debug(f"   无法解析服务器时间: {parse_err}")
                    
                    # 6. API调用参数
                    logger.info(f"📋 API调用参数:")
                    logger.info(f"   - account_index: {self.account_index}")
                    logger.info(f"   - market_id: {market_id if market_id is not None else 255}")
                    logger.info(f"   - auth参数: 已传递 (长度={len(auth_token) if auth_token else 0})")
                    
                    logger.info("=" * 80)
                    
                    if attempt < max_retries - 1:
                        logger.info(f"🔄 正在重新生成 token 并重试 (等待 {1.0 * (attempt + 1)} 秒)...")
                        await asyncio.sleep(1.0 * (attempt + 1))  # 递增等待时间：1秒、2秒
                        continue
                    else:
                        logger.error(f"❌ Token 过期，已重试 {max_retries} 次仍失败")
                        # 🔥 打印完整的错误堆栈和详细信息
                        import traceback
                        logger.error(f"最终错误详情:\n{traceback.format_exc()}")
                        
                        # 🔥 提供详细的解决方案建议
                        logger.error("=" * 80)
                        logger.error("💡 可能的解决方案:")
                        logger.error("=" * 80)
                        logger.error("1. 检查API Key权限:")
                        logger.error("   - 登录 https://app.lighter.xyz")
                        logger.error("   - 检查API Key是否有查询订单的权限")
                        logger.error("   - 确认API Key未过期或被禁用")
                        logger.error("")
                        logger.error("2. 检查系统时间同步:")
                        logger.error("   - 确保系统时间与服务器时间同步")
                        logger.error("   - Windows: 运行 'w32tm /resync' 同步时间")
                        logger.error("")
                        logger.error("3. 检查SignerClient配置:")
                        logger.error(f"   - account_index: {self.account_index}")
                        logger.error(f"   - api_key_index: {self.api_key_index}")
                        logger.error("   - 确认这些值与Lighter前端显示的配置一致")
                        logger.error("")
                        logger.error("4. 检查SDK版本:")
                        logger.error("   - 当前版本可能不兼容，尝试更新SDK:")
                        logger.error("   - pip install --upgrade git+https://github.com/elliottech/lighter-python.git")
                        logger.error("")
                        logger.error("5. 使用WebSocket替代REST API:")
                        logger.error("   - WebSocket可能不受此问题影响")
                        logger.error("   - 检查WebSocket连接是否正常工作")
                        logger.error("=" * 80)
                        return []
                else:
                    # 非 token 过期错误，直接返回
                    logger.error(f"获取活跃订单失败: {type(e).__name__}: {e}")
                    if attempt == max_retries - 1:
                        import traceback
                        logger.debug(f"错误详情:\n{traceback.format_exc()}")
                    return []

    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """
        获取单个订单信息（网格系统关键方法）

        Args:
            order_id: 订单ID
            symbol: 交易对符号

        Returns:
            OrderData对象

        Raises:
            Exception: 如果订单不存在或查询失败
        """
        if not self.signer_client:
            logger.error("未配置SignerClient，无法获取订单信息")
            raise Exception("未配置SignerClient")

        try:
            # 获取所有活跃订单
            open_orders = await self.get_open_orders(symbol)

            # 在活跃订单中查找
            for order in open_orders:
                if order.id == order_id:
                    logger.debug(f"找到订单: {order_id}, 状态={order.status.value}")
                    return order

            # 如果在活跃订单中没找到，尝试从历史订单查找
            # 注意：Lighter可能没有直接的单订单查询API
            logger.warning(f"订单 {order_id} 不在活跃订单列表中，可能已成交或取消")

            # 返回一个占位符OrderData，表示订单可能已完成
            # 网格系统会根据订单不在open_orders中判断其已成交
            return OrderData(
                id=order_id,
                client_id=None,
                symbol=symbol,
                side=OrderSide.BUY,  # 占位符
                type=OrderType.LIMIT,  # 占位符
                amount=Decimal("0"),
                price=None,
                filled=Decimal("0"),
                remaining=Decimal("0"),
                cost=Decimal("0"),
                average=None,
                status=OrderStatus.FILLED,  # 假设已成交
                timestamp=datetime.now(),
                updated=None,
                fee=None,
                trades=[],
                params={},
                raw_data={}
            )

        except Exception as e:
            logger.error(f"获取订单 {order_id} 失败: {e}")
            raise

    async def get_order_history(self, symbol: Optional[str] = None, limit: int = 100) -> List[OrderData]:
        """
        获取历史订单（已完成/取消）

        Args:
            symbol: 交易对符号（可选）
            limit: 返回数量限制

        Returns:
            OrderData列表
        """
        if not self.signer_client:
            logger.error("未配置SignerClient，无法获取订单历史")
            return []

        try:
            # 生成认证令牌
            import lighter
            auth_token, err = self.signer_client.create_auth_token_with_expiry(
                lighter.SignerClient.DEFAULT_10_MIN_AUTH_EXPIRY
            )
            if err:
                logger.error(f"生成认证令牌失败: {err}")
                return []

            # 获取市场ID（如果指定了symbol）
            market_id = None
            if symbol:
                market_id = self.get_market_index(symbol)

            # 获取历史订单
            response = await self.order_api.account_inactive_orders(
                account_index=self.account_index,
                limit=limit,
                auth=auth_token,
                market_id=market_id if market_id is not None else 255  # 255表示所有市场
            )

            orders = []
            if hasattr(response, 'orders') and response.orders:
                for order_info in response.orders:
                    order_symbol = self._get_symbol_from_market_index(
                        getattr(order_info, 'market_index', None))

                    orders.append(self._parse_order(order_info, order_symbol))

            return orders

        except Exception as e:
            logger.error(f"获取历史订单失败: {e}")
            import traceback
            traceback.print_exc()
            return []

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """
        获取持仓信息

        Args:
            symbols: 交易对符号列表（Lighter会忽略，返回所有持仓）

        Returns:
            PositionData列表
        """
        if not self.signer_client:
            logger.error("未配置SignerClient，无法获取持仓信息")
            return []

        # 🔥 429错误重试机制：最多重试2次，使用指数退避
        max_retries = 2
        retry_delays = [2, 5]  # 第1次等2秒，第2次等5秒

        for attempt in range(max_retries + 1):
            try:
                # 获取账户信息（包含持仓）
                response = await self.account_api.account(by="index", value=str(self.account_index))
                # 成功获取，跳出重试循环
                break
            except Exception as e:
                error_str = str(e)
                # 检测429错误
                if "429" in error_str or "Too Many Requests" in error_str:
                    if attempt < max_retries:
                        wait_time = retry_delays[attempt]
                        logger.warning(
                            f"⚠️ 获取持仓遇到限流(429)，{wait_time}秒后重试 ({attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"❌ 获取持仓失败: 达到最大重试次数，限流持续")
                        return []
                else:
                    # 非429错误，直接抛出
                    raise

        # 继续原有的持仓解析逻辑
        positions = []
        # 解析 DetailedAccounts 结构
        if hasattr(response, 'accounts') and response.accounts:
            account = response.accounts[0]

            if hasattr(account, 'positions') and account.positions:

                for idx, position_info in enumerate(account.positions):
                    symbol = position_info.symbol if hasattr(
                        position_info, 'symbol') else ""

                    # 获取持仓数据
                    position_raw = position_info.position if hasattr(
                        position_info, 'position') else 0

                    position_size = self._safe_decimal(position_raw) if hasattr(
                        position_info, 'position') else Decimal("0")

                    if position_size == 0:
                        continue

                    from datetime import datetime
                    from ..models import PositionSide, MarginMode

                    # 🔥 Lighter持仓方向定义（与传统CEX一致）
                    # 正数 = 多头 (LONG) | 负数 = 空头 (SHORT)
                    # ✅ 测试验证：BUY订单成交后，position返回正数，表示做多
                    position_side = PositionSide.LONG if position_size > 0 else PositionSide.SHORT

                    positions.append(PositionData(
                        symbol=symbol,
                        side=position_side,
                        size=abs(position_size),
                        entry_price=self._safe_decimal(
                            getattr(position_info, 'avg_entry_price', 0)),
                        mark_price=None,  # Lighter不提供标记价格
                        current_price=None,  # 需要单独查询
                        unrealized_pnl=self._safe_decimal(
                            getattr(position_info, 'unrealized_pnl', 0)),
                        realized_pnl=self._safe_decimal(
                            getattr(position_info, 'realized_pnl', 0)),
                        percentage=None,  # 可以计算
                        leverage=int(
                            getattr(position_info, 'leverage', 1)),
                        margin_mode=MarginMode.CROSS if getattr(
                            position_info, 'margin_mode', 0) == 0 else MarginMode.ISOLATED,
                        margin=self._safe_decimal(
                            getattr(position_info, 'allocated_margin', 0)),
                        liquidation_price=self._safe_decimal(getattr(position_info, 'liquidation_price', 0)) if getattr(
                            position_info, 'liquidation_price', '0') != '0' else None,
                        timestamp=datetime.now(),
                        raw_data={'position_info': position_info}
                    ))

        # 🔥 如果指定了symbols，只返回匹配的持仓
        if symbols:
            positions = [p for p in positions if p.symbol in symbols]

        return positions

    # ============= 交易功能 =============

    def _validate_order_preconditions(self) -> bool:
        """验证下单前置条件"""
        if not self.signer_client:
            logger.error("❌ 未配置SignerClient，无法下单")
            logger.error(
                "   请检查lighter_config.yaml中的api_key_private_key和account_index")
            return False
        return True

    def _extract_price_decimals(self, market_details) -> int:
        """从市场详情中提取价格精度"""
        if hasattr(market_details, 'order_book_details') and market_details.order_book_details:
            return market_details.order_book_details[0].price_decimals
        return 1  # 默认值

    async def _get_market_info(self, symbol: str) -> Optional[Dict]:
        """
        获取市场信息（索引、价格精度、乘数）

        🔥 使用缓存机制避免频繁API调用触发429限流
        这是关键修复！批量下单时必须使用缓存

        Returns:
            包含 market_index, price_decimals, price_multiplier 的字典，或 None
        """
        import time

        # 🔥 检查缓存（5分钟有效期）
        # 注意：缓存的是市场的静态配置（市场索引、价格精度），不是动态数据
        # 这些配置基本不会变化，所以可以缓存较长时间
        if symbol in self._market_info_cache:
            cache_entry = self._market_info_cache[symbol]
            cache_age = time.time() - cache_entry['timestamp']
            if cache_age < 300:  # 缓存5分钟内有效（300秒）
                logger.debug(f"✅ 使用缓存的市场信息: {symbol} (缓存年龄: {cache_age:.1f}秒)")
                return cache_entry['info']

        try:
            market_index = self.get_market_index(symbol)
            logger.debug(f"✅ 获取market_index: {market_index}")

            if market_index is None:
                logger.error(f"❌ 未找到交易对 {symbol} 的市场索引")
                logger.error(
                    f"   可用市场: {list(self.markets.keys()) if self.markets else '未加载'}")
                return None

            # 获取市场详情，动态获取价格精度
            logger.debug(f"🔍 获取市场详情: market_id={market_index}")
            market_details = await self.order_api.order_book_details(market_id=market_index)
            price_decimals = self._extract_price_decimals(market_details)

            logger.debug(f"✅ 获取价格精度成功: price_decimals={price_decimals}")

            price_multiplier = Decimal(10 ** price_decimals)
            logger.debug(
                f"{symbol} 价格精度: {price_decimals}位小数, 乘数: {price_multiplier}")

            market_info = {
                'market_index': market_index,
                'price_decimals': price_decimals,
                'price_multiplier': price_multiplier
            }

            # 🔥 缓存市场信息（关键！）
            self._market_info_cache[symbol] = {
                'info': market_info,
                'timestamp': time.time()
            }
            logger.debug(f"💾 已缓存市场信息: {symbol}")

            return market_info

        except Exception as e:
            logger.error(f"获取市场信息失败: {e}")
            return None

    async def _calculate_slippage_protection_price(
        self,
        symbol: str,
        side: str,
        provided_price: Optional[Decimal] = None,
        slippage_multiplier: Decimal = Decimal("1.0")
    ) -> Optional[Decimal]:
        """
        计算市价单的滑点保护价格（支持动态滑点倍数）

        Args:
            symbol: 交易对符号
            side: 订单方向
            provided_price: 用户提供的价格（如果有）
            slippage_multiplier: 滑点倍数（1.0=0.01%, 10.0=0.1%, 100.0=1%）

        Returns:
            滑点保护价格，或 None
        """
        if provided_price:
            return provided_price

        try:
            orderbook = await self.get_orderbook(symbol)
            if not orderbook or not orderbook.bids or not orderbook.asks:
                logger.error(f"无法获取{symbol}的订单簿，市价单需要价格")
                return None

            # 🔥 使用配置的基础滑点（默认0.02% = 万分之2）
            base_slippage = self.base_slippage * slippage_multiplier

            is_sell = (side.lower() == "sell")
            if is_sell:
                # 卖单：使用买1价格并减少滑点
                base_price = orderbook.bids[0].price
                protection_price = base_price * (Decimal("1") - base_slippage)
            else:
                # 买单：使用卖1价格并增加滑点
                base_price = orderbook.asks[0].price
                protection_price = base_price * (Decimal("1") + base_slippage)

            slippage_percent = base_slippage * 100
            logger.info(
                f"💰 市价单滑点: {slippage_percent:.3f}% (倍数: {slippage_multiplier}x, "
                f"基准: {base_price}, 保护价: {protection_price})")
            return protection_price

        except Exception as e:
            logger.error(f"计算滑点保护价格失败: {e}")
            return None

    def _convert_market_order_params(
        self,
        market_info: Dict,
        quantity: Decimal,
        avg_execution_price: Decimal,
        side: str,
        **kwargs
    ) -> Dict:
        """转换市价单参数为Lighter格式"""
        # 🔥 先对价格应用精度规则（与限价单保持一致）
        price_decimals = market_info['price_decimals']
        if price_decimals == 0:
            quantize_precision = Decimal("1")
        else:
            quantize_precision = Decimal(10) ** (-price_decimals)

        avg_execution_price_rounded = avg_execution_price.quantize(
            quantize_precision)

        # 🔥🔥🔥 关键发现：数量乘数因市场而异！
        # 规律：quantity_multiplier = 10^(6 - price_decimals)
        # price_decimals=1 (BTC): 10^5 = 100,000
        # price_decimals=2 (ETH): 10^4 = 10,000
        # price_decimals=3: 10^3 = 1,000
        # price_decimals=4 (0G): 10^2 = 100
        quantity_multiplier = Decimal(10 ** (6 - price_decimals))

        base_amount_int = int(quantity * quantity_multiplier)
        avg_price_int = int(avg_execution_price_rounded *
                            market_info['price_multiplier'])
        is_ask = (side.lower() == "sell")

        logger.debug(f"  symbol参数中的market_index={market_info['market_index']}")
        logger.debug(
            f"  价格精度: {market_info['price_decimals']}位小数, 乘数: {market_info['price_multiplier']}")
        logger.debug(
            f"  avg_execution_price={avg_execution_price} -> 四舍五入后={avg_execution_price_rounded}, avg_price_int={avg_price_int}")
        logger.debug(
            f"  reduce_only={kwargs.get('reduce_only', False)}")

        # 🔥 处理 client_order_id（支持字符串和整数，向后兼容）
        client_order_id = kwargs.get("client_order_id")
        if client_order_id is not None:
            # 如果是字符串，转换为整数
            if isinstance(client_order_id, str):
                try:
                    client_order_id = int(client_order_id)
                except ValueError:
                    logger.warning(
                        f"⚠️ client_order_id 不是有效整数: {client_order_id}，使用自动生成")
                    client_order_id = int(
                        asyncio.get_event_loop().time() * 1000)
        else:
            # 如果未提供，生成默认值
            client_order_id = int(asyncio.get_event_loop().time() * 1000)

        # 🔥 注意：Lighter SDK 的 create_market_order 不接受 margin_mode 参数
        # SDK 签名: create_market_order(market_index, client_order_index, base_amount,
        #                               avg_execution_price, is_ask, reduce_only)

        return {
            'market_index': market_info['market_index'],
            'client_order_index': client_order_id,
            'base_amount': base_amount_int,
            'avg_execution_price': avg_price_int,
            'is_ask': is_ask,
            'reduce_only': kwargs.get("reduce_only", False)
        }

    def _convert_limit_order_params(
        self,
        market_info: Dict,
        quantity: Decimal,
        price: Decimal,
        side: str,
        **kwargs
    ) -> Dict:
        """转换限价单参数为Lighter格式"""
        import lighter

        # 🔥 根据price_decimals动态调整价格精度（直接使用quantize避免浮点误差）
        # 例如：price_decimals=1 -> quantize(Decimal("0.1"))
        #      price_decimals=2 -> quantize(Decimal("0.01"))
        price_decimals = market_info['price_decimals']
        if price_decimals == 0:
            quantize_precision = Decimal("1")
        else:
            quantize_precision = Decimal(10) ** (-price_decimals)

        price_rounded = price.quantize(quantize_precision)

        # 🔥🔥🔥 关键发现：数量乘数因市场而异！
        # 规律：quantity_multiplier = 10^(6 - price_decimals)
        # price_decimals=1 (BTC): 10^5 = 100,000
        # price_decimals=2 (ETH): 10^4 = 10,000
        # price_decimals=3: 10^3 = 1,000
        # price_decimals=4 (0G): 10^2 = 100
        quantity_multiplier = Decimal(10 ** (6 - price_decimals))

        base_amount_int = int(quantity * quantity_multiplier)
        price_int = int(price_rounded * market_info['price_multiplier'])
        is_ask = (side.lower() == "sell")

        # 🔍 简化日志：只在 DEBUG 级别输出详细参数
        logger.debug(f"Lighter限价单参数: market_id={market_info['market_index']}, "
                     f"price={price_rounded}, quantity={quantity}, "
                     f"base_amount={base_amount_int}, is_ask={is_ask}")

        time_in_force = kwargs.get("time_in_force", "GTT")
        tif_map = {
            "IOC": lighter.SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
            "GTT": lighter.SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            "POST_ONLY": lighter.SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY,
        }

        # 🔥 注意：Lighter SDK 的 create_order 不接受 margin_mode 参数
        # margin_mode 需要在账户级别设置，不是在订单级别
        # SDK 签名: create_order(market_index, client_order_index, base_amount, price,
        #                        is_ask, order_type, time_in_force, reduce_only, trigger_price)

        return {
            'market_index': market_info['market_index'],
            'client_order_index': kwargs.get("client_order_id",
                                             int(asyncio.get_event_loop().time() * 1000)),
            'base_amount': base_amount_int,
            'price': price_int,
            'is_ask': is_ask,
            'order_type': lighter.SignerClient.ORDER_TYPE_LIMIT,
            'time_in_force': tif_map.get(time_in_force,
                                         lighter.SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME),
            'reduce_only': kwargs.get("reduce_only", False),
            'trigger_price': 0
        }

    async def _execute_market_order(
        self,
        symbol: str,
        side: str,
        quantity: Decimal,
        provided_price: Optional[Decimal],
        market_info: Dict,
        **kwargs
    ) -> Optional[OrderData]:
        """执行市价单"""
        # 🔥 生成唯一的 client_order_id（确保整个流程使用同一个值）
        if "client_order_id" not in kwargs:
            kwargs["client_order_id"] = int(
                asyncio.get_event_loop().time() * 1000)

        # 🔥 获取滑点倍数（支持动态调整）
        slippage_multiplier = kwargs.pop("slippage_multiplier", Decimal("1.0"))

        # 计算滑点保护价格
        avg_execution_price = await self._calculate_slippage_protection_price(
            symbol, side, provided_price, slippage_multiplier
        )
        if not avg_execution_price:
            return None

        # 转换参数
        params = self._convert_market_order_params(
            market_info, quantity, avg_execution_price, side, **kwargs
        )

        # 执行下单
        try:
            tx, tx_hash, err = await self.signer_client.create_market_order(**params)

            # 处理结果
            return await self._handle_order_result(
                tx, tx_hash, err, symbol, side, "market",
                quantity, avg_execution_price, **kwargs
            )
        except Exception as e:
            logger.error(f"执行市价单失败: {e}")
            return None

    async def _execute_limit_order(
        self,
        symbol: str,
        side: str,
        quantity: Decimal,
        price: Optional[Decimal],
        market_info: Dict,
        **kwargs
    ) -> Optional[OrderData]:
        """执行限价单"""
        if not price:
            logger.error("限价单必须指定价格")
            return None

        # 🔥 生成唯一的 client_order_id（确保整个流程使用同一个值）
        if "client_order_id" not in kwargs:
            kwargs["client_order_id"] = int(
                asyncio.get_event_loop().time() * 1000)

        # 转换参数
        params = self._convert_limit_order_params(
            market_info, quantity, price, side, **kwargs
        )

        # 执行下单
        try:
            import lighter
            tx, tx_hash, err = await self.signer_client.create_order(**params)

            # 🔥 处理结果（使用调整后的价格，与_convert_limit_order_params保持一致）
            price_decimals = market_info['price_decimals']
            if price_decimals == 0:
                quantize_precision = Decimal("1")
            else:
                quantize_precision = Decimal(10) ** (-price_decimals)

            price_rounded = price.quantize(quantize_precision)

            return await self._handle_order_result(
                tx, tx_hash, err, symbol, side, "limit",
                quantity, price_rounded, **kwargs
            )
        except Exception as e:
            logger.error(f"执行限价单失败: {e}")
            return None

    async def _handle_order_result(
        self,
        tx,
        tx_hash,
        err,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal,
        price: Decimal,
        **kwargs
    ) -> Optional[OrderData]:
        """
        处理下单结果

        ⚠️ 重要：Lighter下单API返回的是transaction hash，不是order_id
        真正的order_id需要从WebSocket推送或REST查询中获取
        """
        logger.debug(
            f"📋 _handle_order_result: side={side}, price={price}, qty={quantity}"
        )

        # 检查错误
        if err:
            error_msg = self.parse_error(err) if err else "未知错误"
            logger.error(f"❌ Lighter下单失败: {error_msg}")
            logger.error(f"   订单类型: {order_type}, 方向: {side}, 数量: {quantity}")
            if order_type == "market":
                logger.error(f"   市价单保护价格: {price}")
            else:
                logger.error(f"   限价单价格: {price}")

            # 🔥 特殊处理：invalid nonce 错误
            # 这通常发生在批量取消订单失败后，nonce 状态被破坏
            if "invalid nonce" in str(error_msg).lower() or "21104" in str(error_msg):
                logger.warning("⚠️ 检测到 invalid nonce 错误")
                logger.warning("💡 原因：可能是批量取消订单失败后，nonce 状态不同步")
                logger.warning("💡 建议：等待 1-2 秒后重试，或检查是否有批量取消操作失败")
                logger.warning("💡 临时解决方案：系统会自动重试，但如果持续失败，可能需要重新初始化连接")

            return None

        # 检查返回值
        if not tx and not tx_hash:
            logger.error(f"❌ Lighter下单失败: tx和tx_hash都为空（无错误信息）")
            logger.error(f"   这可能是钱包未授权或gas不足")
            return None

        # 🔥 提取transaction hash（这不是order_id！）
        tx_hash_str = str(tx_hash.tx_hash) if tx_hash and hasattr(
            tx_hash, 'tx_hash') else str(tx_hash)
        logger.debug(f"🌐 [REST] 下单成功: tx_hash={tx_hash_str}")

        # 🔥 Lighter特殊处理：REST API无法立即查询到新下的订单
        # 原因：
        # 1. Lighter是Layer 2，订单需要时间上链
        # 2. account_api.account()不返回订单列表（只返回账户信息）
        # 3. order_api.account_active_orders()需要复杂的认证token
        #
        # 解决方案：
        # 1. 返回带tx_hash的临时OrderData
        # 2. 依赖WebSocket推送真正的order_id和状态
        # 3. 网格系统通过WebSocket回调更新订单信息
        logger.debug(
            f"🌐 [REST] 等待WebSocket推送order_id (tx_hash: {tx_hash_str})")

        # 🔥 新逻辑：不再查询 order_index
        # - 直接使用 client_order_id 作为临时ID
        # - WebSocket 推送会包含真正的 order_index
        # - 反手单完全依赖 WebSocket 推送触发，不需要本地记录 order_index
        from datetime import datetime

        client_order_id_str = str(kwargs.get("client_order_id", int(
            asyncio.get_event_loop().time() * 1000)))
        order_id = client_order_id_str
        logger.debug(
            f"🌐 [REST] 使用临时 client_order_id={order_id}"
        )

        return OrderData(
            # 🔥 id 和 client_id 使用相同的值（order_index 或 client_order_id）
            id=order_id,
            client_id=order_id,  # 统一标识，消除双键问题
            symbol=symbol,
            side=OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL,
            type=OrderType.MARKET if order_type == "market" else OrderType.LIMIT,
            amount=quantity,
            price=price,
            filled=Decimal("0"),
            remaining=quantity,
            cost=Decimal("0"),
            average=None,
            status=OrderStatus.PENDING,
            timestamp=datetime.now(),
            updated=None,
            fee=None,
            trades=[],
            params=kwargs,
            raw_data={'tx': tx, 'tx_hash': tx_hash, 'tx_hash_str': tx_hash_str}
        )

    # 🔥 已删除 _query_order_index 方法
    # 新逻辑：完全依赖 WebSocket 推送获取 order_index，不再主动查询

    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        **kwargs
    ) -> Optional[OrderData]:
        """
        下单（主流程编排）

        Args:
            symbol: 交易对符号
            side: 订单方向 ("buy" 或 "sell")
            order_type: 订单类型 ("limit" 或 "market")
            quantity: 数量
            price: 价格（限价单必需）
            **kwargs: 其他参数

        Returns:
            OrderData对象

        ⚠️  Lighter价格精度说明：
        - Lighter使用动态价格乘数，不同交易对精度不同
        - 价格乘数公式: price_int = price_usd × (10 ** price_decimals)
        - 数量始终使用1e6: base_amount = quantity × 1000000

        示例：
        - ETH (price_decimals=2): $4127.39 × 100 = 412739
        - BTC (price_decimals=1): $114357.8 × 10 = 1143578
        - SOL (price_decimals=3): $199.058 × 1000 = 199058
        - DOGE (price_decimals=6): $0.202095 × 1000000 = 202095

        注意：这与大多数交易所不同！
        - 大多数CEX使用固定的1e8或1e6
        - Lighter根据价格大小动态选择精度，以优化Layer 2性能
        """
        # 🔥 nonce冲突已通过grid_engine_impl.py中的串行下单解决
        # 串行下单确保了nonce自然递增，无需在此添加延迟

        logger.debug(
            f"📝 开始下单: symbol={symbol}, side={side}, type={order_type}, qty={quantity}")

        try:
            # 1. 验证前置条件
            if not self._validate_order_preconditions():
                return None

            # 2. 获取市场信息（索引、价格精度、乘数）
            market_info = await self._get_market_info(symbol)
            if not market_info:
                return None

            # 3. 根据订单类型执行下单
            if order_type.lower() == "market":
                return await self._execute_market_order(
                    symbol, side, quantity, price, market_info, **kwargs
                )
            else:
                return await self._execute_limit_order(
                    symbol, side, quantity, price, market_info, **kwargs
                )

        except Exception as e:
            logger.error(f"下单失败 {symbol}: {e}")
            return None

    async def set_margin_mode(self, symbol: str, margin_mode: str, leverage: int = 1) -> bool:
        """
        设置交易对的保证金模式

        Args:
            symbol: 交易对符号
            margin_mode: 保证金模式 ("isolated"=逐仓, "cross"=全仓)
            leverage: 杠杆倍数（默认1倍）

        Returns:
            是否设置成功
        """
        if not self.signer_client:
            logger.error("未配置SignerClient，无法设置保证金模式")
            return False

        try:
            # 获取市场索引
            market_info = await self._get_market_info(symbol)
            if not market_info:
                logger.error(f"未找到市场信息: {symbol}")
                return False

            market_index = market_info['market_index']

            # 转换保证金模式
            import lighter
            if margin_mode.lower() == "isolated":
                margin_mode_value = lighter.SignerClient.ISOLATED_MARGIN_MODE  # 1
            else:
                margin_mode_value = lighter.SignerClient.CROSS_MARGIN_MODE  # 0

            logger.info(f"🔧 设置保证金模式: {symbol} (market_index={market_index}), "
                        f"模式={margin_mode}({margin_mode_value}), 杠杆={leverage}x")

            # 调用 SDK 设置保证金模式和杠杆
            tx, tx_hash, err = await self.signer_client.update_leverage(
                market_index=market_index,
                margin_mode=margin_mode_value,
                leverage=leverage
            )

            if err:
                error_msg = self.parse_error(err)
                logger.error(f"❌ 设置保证金模式失败: {error_msg}")
                return False

            logger.info(
                f"✅ 保证金模式设置成功: {symbol}, {margin_mode}模式, {leverage}x杠杆, tx_hash={tx_hash}")
            return True

        except Exception as e:
            logger.error(f"设置保证金模式异常: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """
        取消订单

        Args:
            symbol: 交易对符号
            order_id: 订单ID

        Returns:
            是否成功
        """
        if not self.signer_client:
            logger.error("未配置SignerClient，无法取消订单")
            return False

        try:
            market_index = self.get_market_index(symbol)
            if market_index is None:
                logger.error(f"未找到交易对 {symbol} 的市场索引")
                return False

            # 🔥 检查order_id是否为tx_hash（128字符十六进制）
            if len(order_id) > 20:  # tx_hash通常是128字符，order_index是整数
                logger.warning(
                    f"⚠️ 订单ID似乎是tx_hash（长度{len(order_id)}），无法直接取消。"
                    f"需要等待WebSocket更新为真实的order_index"
                )
                # 尝试从挂单列表中查找真实的order_index
                try:
                    orders = await self.get_open_orders(symbol)
                    for order in orders:
                        # 通过client_order_id或其他方式匹配
                        # 暂时返回False，等待WebSocket更新
                        pass
                except Exception as e:
                    logger.error(f"查询挂单失败: {e}")
                return False

            # 取消订单
            tx, tx_hash, err = await self.signer_client.cancel_order(
                market_index=market_index,
                order_index=int(order_id),
            )

            if err:
                logger.error(f"取消订单失败: {self.parse_error(err)}")
                return False

            return True

        except Exception as e:
            logger.error(f"取消订单失败 {symbol}/{order_id}: {e}")
            return False

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """
        批量取消所有订单（Lighter专用批量取消API）

        🔥 使用Lighter的批量取消功能（TX_TYPE_CANCEL_ALL_ORDERS）
        比逐个取消更快速、更可靠

        注意：Lighter的批量取消API会取消所有交易对的所有订单
        如果指定了symbol，批量取消后需要验证该交易对的订单是否已取消

        Args:
            symbol: 交易对符号（可选，批量取消会取消所有交易对的订单）

        Returns:
            被取消的订单列表（取消前的订单列表）
        """
        if not self.signer_client:
            logger.error("未配置SignerClient，无法批量取消订单")
            return []

        try:
            import lighter
            import time

            # 🔥 批量取消前，先获取订单列表（用于返回）
            orders_before_cancel = await self.get_open_orders(symbol)
            if not orders_before_cancel:
                logger.info("没有待取消的订单")
                return []

            logger.info(f"📋 准备批量取消 {len(orders_before_cancel)} 个订单...")

            # 🔥 使用批量取消API（IMMEDIATE表示立即取消所有订单）
            # 注意：Lighter的批量取消会取消所有交易对的所有订单
            # 🔥 修复：直接传递 time=0，避免先失败再重试导致事件循环关闭和nonce状态破坏
            try:
                tx, tx_hash, err = await self.signer_client.cancel_all_orders(
                    time_in_force=lighter.SignerClient.CANCEL_ALL_TIF_IMMEDIATE,
                    time=0,  # 🔥 直接传递time参数，避免TypeError
                )
            except RuntimeError as e:
                # 事件循环关闭错误：检查是否是事件循环问题
                if "Event loop is closed" in str(e):
                    logger.error("⚠️ 事件循环已关闭，无法执行批量取消")
                    logger.warning("💡 建议：跳过批量取消，使用逐个取消或重新初始化连接")
                    # 不降级为逐个取消，因为事件循环已关闭，所有异步操作都会失败
                    return orders_before_cancel
                raise
            except TypeError as e:
                # 如果参数签名仍然不匹配，记录详细错误
                logger.error(f"⚠️ cancel_all_orders参数错误: {e}")
                logger.debug("尝试不传递time参数...")
                try:
                    tx, tx_hash, err = await self.signer_client.cancel_all_orders(
                        time_in_force=lighter.SignerClient.CANCEL_ALL_TIF_IMMEDIATE,
                    )
                except Exception as e2:
                    logger.error(f"⚠️ 不传递time参数也失败: {e2}")
                    raise

            if err:
                logger.error(f"批量取消订单失败: {self.parse_error(err)}")
                # 降级：尝试逐个取消
                logger.info("降级为逐个取消模式...")
                return await self._cancel_all_orders_fallback(symbol)

            logger.info(f"✅ 批量取消订单成功，交易哈希: {tx_hash}")

            # 等待交易所处理（链上确认需要时间）
            await asyncio.sleep(2)

            # 验证订单是否真的被取消
            remaining_orders = await self.get_open_orders(symbol)
            if remaining_orders:
                logger.warning(
                    f"⚠️ 批量取消后仍有 {len(remaining_orders)} 个订单未取消，"
                    f"可能是链上确认延迟，等待中..."
                )
                await asyncio.sleep(3)
                remaining_orders = await self.get_open_orders(symbol)

                if remaining_orders:
                    logger.warning(
                        f"⚠️ 仍有 {len(remaining_orders)} 个订单未取消，"
                        f"可能需要手动处理或使用逐个取消"
                    )

            # 返回取消前的订单列表
            return orders_before_cancel

        except Exception as e:
            logger.error(f"批量取消订单失败: {e}")
            import traceback
            logger.error(traceback.format_exc())

            # 🔥 如果事件循环关闭或网络错误，可能破坏了 nonce 状态
            # 重置 signer_client 的连接状态（如果需要）
            if "Event loop is closed" in str(e) or "RuntimeError" in str(type(e).__name__):
                logger.warning("⚠️ 检测到事件循环问题，可能影响后续下单的 nonce 状态")
                logger.warning("💡 建议：等待一段时间后再重试，或重新初始化连接")

            # 降级：尝试逐个取消（但如果事件循环已关闭，这个也会失败）
            logger.info("降级为逐个取消模式...")
            try:
                return await self._cancel_all_orders_fallback(symbol)
            except RuntimeError as e2:
                if "Event loop is closed" in str(e2):
                    logger.error("⚠️ 事件循环已关闭，无法执行逐个取消")
                    return orders_before_cancel
                raise

    async def _cancel_all_orders_fallback(self, symbol: Optional[str] = None) -> List[OrderData]:
        """
        降级方案：逐个取消订单（当批量取消失败时使用）

        Args:
            symbol: 交易对符号

        Returns:
            被取消的订单列表
        """
        try:
            orders = await self.get_open_orders(symbol)
            cancelled_orders = []

            for order in orders:
                try:
                    # 🔥 修复：OrderData 使用 id 属性，不是 order_id
                    order_id = order.id
                    cancelled = await self.cancel_order(order.symbol, order_id)
                    if cancelled:
                        cancelled_orders.append(order)
                except Exception as e:
                    # 🔥 修复：使用 order.id 而不是 order.order_id
                    logger.error(f"取消订单 {order.id} 失败: {e}")

            return cancelled_orders

        except Exception as e:
            logger.error(f"逐个取消订单失败: {e}")
            return []

    async def place_market_order(
            self,
            symbol: str,
            side: OrderSide,
            quantity: Decimal,
            reduce_only: bool = False,
            skip_order_index_query: bool = False,
            client_order_id: Optional[str] = None,
            **extra_kwargs) -> Optional[OrderData]:
        """
        下市价单（便捷方法）

        Args:
            symbol: 交易对符号
            side: 订单方向
            quantity: 数量
            reduce_only: 只减仓模式（平仓专用，不会开新仓或加仓）
            skip_order_index_query: 跳过 order_index 查询（Volume Maker 使用）
            client_order_id: 客户端订单ID（可选，用于订单追踪）
            **extra_kwargs: 额外参数（如 slippage_multiplier）

        Returns:
            订单数据 或 None
        """
        logger.debug(
            f"🚀 place_market_order被调用: symbol={symbol}, side={side}, qty={quantity}, "
            f"reduce_only={reduce_only}, client_id={client_order_id or 'auto'}")

        # 转换OrderSide枚举为字符串
        side_str = "buy" if side == OrderSide.BUY else "sell"

        logger.debug(f"   转换side: {side} → {side_str}")

        # 🔥 构建kwargs（向后兼容：client_order_id 为可选参数）
        kwargs = {
            "reduce_only": reduce_only,
            "skip_order_index_query": skip_order_index_query
        }

        # 仅在提供 client_order_id 时传递（不影响网格程序）
        if client_order_id is not None:
            kwargs["client_order_id"] = client_order_id

        # 🔥 合并额外参数（如 slippage_multiplier）
        kwargs.update(extra_kwargs)

        return await self.place_order(
            symbol=symbol,
            side=side_str,  # 🔥 修复：传递字符串而不是枚举
            order_type="market",  # 🔥 修复：传递字符串
            quantity=quantity,
            **kwargs
        )

    # ============= 辅助方法 =============

    def _get_symbol_from_market_index(self, market_index: int) -> str:
        """从市场索引获取符号"""
        market_info = self._markets_cache.get(market_index)
        if market_info:
            return market_info.get("symbol", "")
        return ""

    def _parse_order(self, order_info: Any, symbol: str) -> OrderData:
        """
        解析订单信息

        根据Lighter API文档，Order对象包含:
        - order_index: INTEGER (真正的订单ID)
        - order_id: STRING (order_index的字符串形式)
        - client_order_index: INTEGER
        - client_order_id: STRING
        """
        from datetime import datetime

        # 🔥 获取真正的订单ID (优先使用order_index，然后是order_id)
        order_index = getattr(order_info, 'order_index', None)
        order_id_str = getattr(order_info, 'order_id', None)

        # 如果有order_index，使用它；否则使用order_id
        final_order_id = order_id_str if order_id_str else (
            str(order_index) if order_index is not None else '')

        logger.debug(
            f"解析订单: order_index={order_index}, order_id={order_id_str}, final_id={final_order_id}")

        # 解析数量信息
        initial_amount = self._safe_decimal(
            getattr(order_info, 'initial_base_amount', 0))
        filled_amount = self._safe_decimal(
            getattr(order_info, 'filled_base_amount', 0))
        remaining_amount = self._safe_decimal(
            getattr(order_info, 'remaining_base_amount', 0))

        # 解析价格
        price = self._safe_decimal(getattr(order_info, 'price', 0))
        filled_quote = self._safe_decimal(
            getattr(order_info, 'filled_quote_amount', 0))

        # 计算平均价格
        average_price = filled_quote / filled_amount if filled_amount > 0 else None

        # 🔥 获取 client_order_index (注意是 index 不是 id)
        client_order_index = getattr(order_info, 'client_order_index', None)
        client_id_str = str(
            client_order_index) if client_order_index is not None else ''

        return OrderData(
            # ✅ 使用真正的order_id（order_index的字符串形式）
            id=final_order_id,
            # 🔥 关键修复：使用 client_order_index 而不是 client_order_id
            client_id=client_id_str,
            symbol=symbol,
            side=self._parse_order_side(getattr(order_info, 'is_ask', False)),
            type=self._parse_order_type(getattr(order_info, 'type', 'limit')),
            amount=initial_amount,
            price=price if price > 0 else None,
            filled=filled_amount,
            remaining=remaining_amount,
            cost=filled_quote,
            average=average_price,
            status=self._parse_order_status(
                getattr(order_info, 'status', 'unknown')),
            timestamp=self._parse_timestamp(
                getattr(order_info, 'timestamp', None)) or datetime.now(),
            updated=None,
            fee=None,
            trades=[],
            params={},
            raw_data={'order_info': order_info}
        )

    def _parse_order_side(self, is_ask: bool) -> OrderSide:
        """解析订单方向"""
        return OrderSide.SELL if is_ask else OrderSide.BUY

    def _parse_order_type(self, order_type_str: str) -> OrderType:
        """解析订单类型"""
        type_mapping = {
            'market': OrderType.MARKET,
            'limit': OrderType.LIMIT,
            'stop-limit': OrderType.STOP_LIMIT,
            'stop_limit': OrderType.STOP_LIMIT,
            'stop-market': OrderType.STOP,
            'stop_market': OrderType.STOP,
            'stop': OrderType.STOP,
        }
        return type_mapping.get(order_type_str.lower().replace('_', '-'), OrderType.LIMIT)

    def _parse_order_status(self, status_str: str) -> OrderStatus:
        """解析订单状态"""
        status_mapping = {
            'pending': OrderStatus.PENDING,
            'open': OrderStatus.OPEN,
            'filled': OrderStatus.FILLED,
            'canceled': OrderStatus.CANCELED,
            'canceled-too-much-slippage': OrderStatus.CANCELED,
            'expired': OrderStatus.EXPIRED,
            'rejected': OrderStatus.REJECTED,
        }
        return status_mapping.get(status_str.lower(), OrderStatus.UNKNOWN)
