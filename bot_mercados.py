from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import asyncio
import os
import aiohttp
import yfinance as yf

TOKEN = os.environ.get("BOT_TOKEN", "8616657604:AAGQI9e_x9ZX5zw6zcHIloboeDO18OrKRBM")

bot = Bot(token=TOKEN)
dp = Dispatcher()


async def get_btc_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as r:
            data = await r.json()
            price = data["bitcoin"]["usd"]
            change = data["bitcoin"]["usd_24h_change"]
            return price, change


async def get_yf_price(ticker):
    def fetch():
        t = yf.Ticker(ticker)
        hist = t.history(period="2d")
        close_today = hist["Close"].iloc[-1]
        close_prev = hist["Close"].iloc[-2]
        change_pct = ((close_today - close_prev) / close_prev) * 100
        return close_today, change_pct
    return await asyncio.to_thread(fetch)


@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer("¡Hola! Soy tu analista de mercados.\nEscribe /analisis para resumen con precios en tiempo real.")


@dp.message(Command("analisis"))
async def analisis(message: types.Message):
    await message.answer("Obteniendo datos en tiempo real...")
    try:
        btc_price, btc_change = await get_btc_price()
        (nasdaq, nasdaq_change) = await get_yf_price("^IXIC")
        (gold, gold_change) = await get_yf_price("GC=F")

        texto = (
            f"Resumen en tiempo real:\n\n"
            f"• Bitcoin: ${btc_price:,.0f} ({btc_change:+.2f}% 24h)\n"
            f"• Nasdaq: {nasdaq:,.0f} pts ({nasdaq_change:+.2f}%)\n"
            f"• Oro: ${gold:,.0f}/oz ({gold_change:+.2f}%)\n"
        )
    except Exception as e:
        texto = f"Error obteniendo datos: {e}"

    await message.answer(texto)


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
