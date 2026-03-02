from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import asyncio
import os
import aiohttp

TOKEN = os.environ.get("BOT_TOKEN", "8616657604:AAGQI9e_x9ZX5zw6zcHIloboeDO18OrKRBM")

bot = Bot(token=TOKEN)
dp = Dispatcher()

HEADERS = {"User-Agent": "Mozilla/5.0"}


async def get_btc_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as r:
            data = await r.json()
            return data["bitcoin"]["usd"], data["bitcoin"]["usd_24h_change"]


async def get_stooq_price(ticker):
    url = f"https://stooq.com/q/l/?s={ticker}&f=sd2t2ohlcv&h&e=csv"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as r:
            text = await r.text()
            lines = text.strip().split("\n")
            values = lines[1].split(",")
            if "N/D" in values:
                return None, None
            close = float(values[6])
            open_ = float(values[3])
            change = ((close - open_) / open_) * 100
            return close, change


@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer("¡Hola! Soy tu analista de mercados.\nEscribe /analisis para resumen con precios en tiempo real.")


@dp.message(Command("analisis"))
async def analisis(message: types.Message):
    await message.answer("Obteniendo datos en tiempo real...")
    try:
        btc_price, btc_change = await get_btc_price()
        nasdaq, nasdaq_change = await get_stooq_price("^ndq")
        sp500, sp500_change = await get_stooq_price("^spx")
        gold, gold_change = await get_stooq_price("gc.f")

        def fmt(price, change, prefix="$", suffix=""):
            if price is None:
                return "Mercado cerrado"
            return f"{prefix}{price:,.2f}{suffix} ({change:+.2f}%)"

        texto = (
            f"Resumen en tiempo real:\n\n"
            f"• Bitcoin: {fmt(btc_price, btc_change)} (24h)\n"
            f"• Nasdaq 100: {fmt(nasdaq, nasdaq_change, prefix='', suffix=' pts')}\n"
            f"• S&P 500: {fmt(sp500, sp500_change, prefix='', suffix=' pts')}\n"
            f"• Oro: {fmt(gold, gold_change)}/oz\n"
        )
    except Exception as e:
        texto = f"Error obteniendo datos: {e}"

    await message.answer(texto)


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
