import json
import os
import time
import pika
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
QUEUE_NAME = os.getenv("QUEUE_NAME", "meetings_queue")


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
            print("Connected to RabbitMQ from creator bot")
            return connection, channel
        except Exception as e:
            print(f"RabbitMQ connection error: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)


connection, channel = create_rabbitmq_channel()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привіт! Надішли текст про мітинг, і я додам його в чергу RabbitMQ.\n"
        "Наприклад: Мітинг з командою Backend о 15:00"
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global connection, channel

    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()

    payload = {
        "text": text,
        "user_id": update.effective_user.id,
        "username": update.effective_user.username or update.effective_user.first_name or "Unknown",
        "chat_id": update.effective_chat.id,
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    try:
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )

        print("Sent to queue:", payload)
        await update.message.reply_text("Повідомлення про мітинг додано в чергу RabbitMQ.")
    except Exception as e:
        print(f"Publish error: {e}")
        connection, channel = create_rabbitmq_channel()
        await update.message.reply_text("Сталася помилка при відправленні повідомлення в чергу.")


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Meet producer bot started")
    app.run_polling()


if __name__ == "__main__":
    main()
