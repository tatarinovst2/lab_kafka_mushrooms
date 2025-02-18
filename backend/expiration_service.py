import json
import time
from threading import Timer
from kafka import KafkaConsumer, KafkaProducer

EXPIRED_TOPIC = 'expired_data'

consumer = KafkaConsumer(
    'raw_mushroom_data',
    bootstrap_servers='localhost:9095',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='expiration_service_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9095',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

active_baskets = {}


def handle_new_basket(basket: dict) -> None:
    """
    Handle a newly classified basket by scheduling its expiration.

    :param basket: The classified basket data.
    """
    print(f"Определяем время жизни корзины {basket['id']}...")
    expiration_timestamp: int = basket.get('expiration_timestamp')
    current_time: int = int(time.time())
    time_to_expire: float = max(expiration_timestamp - current_time, 0)

    if time_to_expire > 0:
        timer = Timer(time_to_expire, expire_basket, args=(basket,))
        timer.start()
        active_baskets[basket['id']] = timer
    else:
        expire_basket(basket)


def expire_basket(basket: dict) -> None:
    """
    Expire a basket by marking it as expired and sending it to the expired topic.

    :param basket: The basket data to expire.
    """
    producer.send(EXPIRED_TOPIC, basket)
    print(f"У корзины {basket['id']} вышел срок годности.")

    active_baskets.pop(basket['id'], None)


def main() -> None:
    """
    Run the expiration service to process incoming baskets and handle expirations.
    """
    for message in consumer:
        basket: dict = message.value
        handle_new_basket(basket)


if __name__ == "__main__":
    main()
