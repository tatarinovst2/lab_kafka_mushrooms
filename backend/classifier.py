import json

from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
from common import load_model

model_components = load_model()
model = model_components['model']

consumer = KafkaConsumer(
    'preprocessed_data',
    bootstrap_servers='localhost:9095',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='classifier_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9095',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def classify_basket(basket: dict) -> dict:
    """
    Classify a preprocessed basket to determine if it contains poisonous mushrooms.

    :param basket: The preprocessed basket data.
    :return: The basket data with an additional 'predicted_poisonous' field.
    """
    print("Classifying...")
    features: pd.DataFrame = pd.json_normalize(basket['processed_basket'])
    print(features)
    prediction = model.predict(features)
    print(prediction)
    basket['predicted_poisonous'] = int(prediction.sum() > 0)
    return basket


if __name__ == "__main__":
    for message in consumer:
        preprocessed_basket: dict = message.value
        classified: dict = classify_basket(preprocessed_basket)
        producer.send('classified_data', classified)
        print(
            f"Классифицирована корзина от {classified['seller']}: "
            f"{'Ядовитая' if classified['predicted_poisonous'] else 'Съедобная'}"
        )
