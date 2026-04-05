import json
import os
import time
import pika
from telegram import Bot
from telegram.ext import Application, CommandHandler, ContextTypes

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
QUEUE_NAME = os.getenv("QUEUE_NAME", "meetings_queue")
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "15"))


def create_rabbitmq_channel():
    credentials = pika.PlainCredentials("admin", "admin")

    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=credentials
                )
            )
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            print("Connected to RabbitMQ from notifier bot")
            return connection, channel
        except Exception as e:
            print(f"RabbitMQ connection error: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)


connection, channel = create_rabbitmq_channel()


async def start(update: ContextTypes.DEFAULT_TYPE, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привіт! Я бот для нагадувань про мітинги. "
        "Я перевіряю чергу при запуску та кожні 15 хвилин."
    )


async def check_queue(context: ContextTypes.DEFAULT_TYPE):
    global connection, channel

    bot: Bot = context.bot
    print("Checking meetings queue...")

    while True:
        try:
            method_frame, header_frame, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=False)

            if method_frame is None:
                print("Queue is empty")
                break

            payload = json.loads(body.decode("utf-8"))

            sender = f"@{payload['username']}" if payload.get("username") else f"ID {payload['user_id']}"
            notification_text = (
                "📅 Нагадування про мітинг\n\n"
                f"👤 Від: {sender}\n"
                f"📝 Деталі: {payload['text']}"
            )

            await bot.send_message(chat_id=payload["chat_id"], text=notification_text)

            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            print("Delivered:", payload)

        except Exception as e:
            print(f"Queue processing error: {e}")
            connection, channel = create_rabbitmq_channel()
            break


async def on_startup(app: Application):
    await check_queue(app.job_queue)


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))

    app.job_queue.run_repeating(
        check_queue,
        interval=CHECK_INTERVAL_SECONDS,
        first=0
    )

    print("Meet consumer bot started")
    app.run_polling()


if __name__ == "__main__":
    main()