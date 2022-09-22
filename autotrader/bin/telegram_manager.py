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



    def send_message(self, message):
        self.bot.send_message(chat_id=-1001518504132, text=message)