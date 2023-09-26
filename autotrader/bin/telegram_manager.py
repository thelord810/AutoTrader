import asyncio
import telegram

class TelegramBot:
    """Telegram bot manager
    Attributes
    ----------
    token
        Token for Bot
    """

    def __init__(
        self,
        token: any = None
    ):
        self.bot = telegram.Bot(token)

    async def send_message(self, message):
        async with self.bot:
        #     self.bot.loop.run_until_complete(self.bot.send_message('-1001518504132', message))
        #self.bot.send_message(chat_id=-1001518504132, text=message)
            await self.bot.send_message(chat_id=-1001518504132, text=message)