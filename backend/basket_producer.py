import json
import random
import time
from pathlib import Path

from kafka import KafkaProducer
import pandas as pd

from seller_names import generate_seller_name
from common import ROOT_DIR

producer = KafkaProducer(
    bootstrap_servers='localhost:9095',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_csv(ROOT_DIR / Path('data/mushrooms.csv'))

# Assign Weights: Edible = 0.95, Poisonous = 0.05
df['weight'] = df['class'].apply(lambda x: 0.95 if x == 'e' else 0.05)

df.reset_index(drop=True, inplace=True)


def generate_basket(identifier: int) -> dict:
    """
    Generate a random basket of mushrooms.

    :param identifier: Basket identifier.
    :return: A dictionary containing basket details including seller, timestamp, price,
             poisonous flag, and the list of mushrooms in the basket.
    """
    basket_size: int = random.randint(3, 6)
    df_sample: pd.DataFrame = df.sample(n=basket_size, weights='weight', replace=False)
    basket_df: pd.DataFrame = df_sample.drop(columns=["weight"], inplace=False)

    basket: list[dict] = basket_df.to_dict(orient='records')
    seller: str = generate_seller_name()
    price: float = round(random.uniform(100, 1000), 2)
    timestamp: int = int(time.time())
    poisonous: bool = any(m['class'] == 'p' for m in basket)  # Check if any mushroom in the basket is poisonous

    expiration_duration: int = random.randint(60, 120)
    expiration_timestamp: int = timestamp + expiration_duration

    basket_data: dict = {
        'id': identifier,
        'seller': seller,
        'timestamp': timestamp,
        'expiration_timestamp': expiration_timestamp,
        'price': price,
        'poisonous': poisonous,
        'basket': basket
    }

    print(f"Generated basket_data: {basket_data}")
    return basket_data


def main() -> None:
    """
    Run a loop to generate baskets.
    """
    identifier = 1

    while True:
        basket: dict = generate_basket(identifier)
        identifier += 1
        producer.send('raw_mushroom_data', basket)
        print(f"Отправлена корзина от {basket['seller']}")
        time.sleep(random.uniform(0.5, 2))


if __name__ == "__main__":
    main()
