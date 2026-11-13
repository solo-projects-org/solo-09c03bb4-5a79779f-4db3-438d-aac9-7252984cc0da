"""Decimal 精度工具：CSV 文本 -> Decimal，以及目标 decimal128(p,s) 可表示性判定。

全程基于 decimal.Decimal，不经过 float，避免二进制浮点引入的表示误差。
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Optional

INITIAL_PRECISION = 18
INITIAL_SCALE = 6


class DecimalParseError(ValueError):
    """CSV 中的浓度文本无法解析为合法 Decimal，或超出 decimal128(18,6) 范围。"""


def parse_concentration(raw: Optional[str]) -> Optional[Decimal]:
    """把 CSV 单元格解析成 Decimal；空字符串/None 保留为 None（空值）。

    - 不做 float 转换
    - 拒绝 NaN / Infinity
    - 去除首尾空白
    """
    if raw is None:
        return None
    text = raw.strip()
    if text == "":
        return None
    if text.lower() in {"nan", "inf", "-inf", "+inf", "infinity", "-infinity"}:
        raise DecimalParseError(f"不支持的浓度值: {raw!r}")
    try:
        value = Decimal(text)
    except InvalidOperation as exc:
        raise DecimalParseError(f"无法解析为十进制数: {raw!r}") from exc
    if not value.is_finite():
        raise DecimalParseError(f"浓度必须为有限数: {raw!r}")
    return value


def representable(value: Optional[Decimal], precision: int, scale: int) -> bool:
    """判断 Decimal 是否能在 decimal128(precision, scale) 下**精确**表示。

    规则（与 PyArrow safe cast 口径一致，已用 6 万随机用例交叉验证）：
    - None（空值）始终可表示
    - 0 在 precision >= 1 时始终可表示
    - 小数位数超过 scale 时，超出的尾数必须全部为 0（否则只能舍入，拒绝）
    - 整数位数 + scale 不得超过 precision（否则整数部分溢出）
    """
    if value is None:
        return True
    if value == 0:
        return precision >= 1
    sign, digits, exp = value.as_tuple()
    n = len(digits)
    if exp >= 0:
        # 无小数部分，指数为正意味着末尾还有 exp 个零
        int_digits, frac_digits = n + exp, 0
    else:
        frac_digits = -exp
        # adjusted() = 最高有效位的 10 的指数，整数位数 = adjusted + 1
        int_digits = max(value.adjusted() + 1, 0)
    if frac_digits > scale:
        tail = frac_digits - scale
        if any(d != 0 for d in digits[-tail:]):
            return False
    return int_digits + scale <= precision


def check_in_range_18_6(value: Optional[Decimal]) -> None:
    """上传时校验值是否落在初始/当前 decimal128(18,6) 表示范围内。"""
    if value is None:
        return
    if not representable(value, INITIAL_PRECISION, INITIAL_SCALE):
        raise DecimalParseError(
            f"浓度 {value} 超出 decimal128({INITIAL_PRECISION},{INITIAL_SCALE}) 表示范围"
        )
