from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import asyncio
import os
import aiohttp

TOKEN = os.environ.get("BOT_TOKEN", "8616657604:AAGQI9e_x9ZX5zw6zcHIloboeDO18OrKRBM")

bot = Bot(token=TOKEN)
dp = Dispatcher()

HEADERS = {"User-Agent": "Mozilla/5.0"}


async def get_crypto_prices():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd&include_24hr_change=true"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as r:
            data = await r.json()
            btc_price = data["bitcoin"]["usd"]
            btc_change = data["bitcoin"]["usd_24h_change"]
            eth_price = data["ethereum"]["usd"]
            eth_change = data["ethereum"]["usd_24h_change"]
            return btc_price, btc_change, eth_price, eth_change


async def get_stooq_price(ticker):
    url = f"https://stooq.com/q/l/?s={ticker}&f=sd2t2ohlcv&h&e=csv"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as r:
            text = await r.text()
            lines = text.strip().split("\n")
            values = lines[1].split(",")
            close = float(values[6])
            open_ = float(values[4])
            change = ((close - open_) / open_) * 100
            return close, change


@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer("¡Hola! Soy tu analista de mercados.\nEscribe /analisis para resumen con precios en tiempo real.")


@dp.message(Command("analisis"))
async def analisis(message: types.Message):
    await message.answer("Obteniendo datos en tiempo real...")
    try:
        btc_price, btc_change, eth_price, eth_change = await get_crypto_prices()
        nasdaq, nasdaq_change = await get_stooq_price("^compq")
        sp500, sp500_change = await get_stooq_price("^spx")
        gold, gold_change = await get_stooq_price("xauusd")

        texto = (
            f"Resumen en tiempo real:\n\n"
            f"• Bitcoin: ${btc_price:,.0f} ({btc_change:+.2f}% 24h)\n"
            f"• Ethereum: ${eth_price:,.0f} ({eth_change:+.2f}% 24h)\n"
            f"• Nasdaq: {nasdaq:,.0f} pts ({nasdaq_change:+.2f}%)\n"
            f"• S&P 500: {sp500:,.0f} pts ({sp500_change:+.2f}%)\n"
            f"• Oro: ${gold:,.0f}/oz ({gold_change:+.2f}%)\n"
        )
    except Exception as e:
        texto = f"Error obteniendo datos: {e}"

    await message.answer(texto)


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
