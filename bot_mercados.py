from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import asyncio
import os

TOKEN = os.environ.get("BOT_TOKEN", "8616657604:AAGQI9e_x9ZX5zw6zcHIloboeDO18OrKRBM")

bot = Bot(token=TOKEN)
dp = Dispatcher()

@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer("¡Hola! Soy tu analista de mercados.\nEscribe /analisis para resumen diario.")

@dp.message(Command("analisis"))
async def analisis(message: types.Message):
    texto = (
        "Resumen 2 de marzo 2026:\n"
        "• Nasdaq +1.2% (impulsado por IA)\n"
        "• Bitcoin $108k, soporte fuerte en $102k\n"
        "• Oro $2,980, sesgo alcista por tensiones\n"
        "Recomendación: Mantener BTC y oro, cautela en tech si suben tasas."
    )
    await message.answer(texto)

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
