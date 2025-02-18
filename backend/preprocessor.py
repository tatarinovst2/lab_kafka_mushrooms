import json

from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
from sklearn.preprocessing import StandardScaler

from common import load_model

consumer = KafkaConsumer(
    'raw_mushroom_data',
    bootstrap_servers='localhost:9095',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='preprocessor_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9095',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def preprocess_basket(basket: dict) -> dict:
    """
    Preprocesses a basket of mushrooms for classification.

    :param basket: The raw basket data.
    :return: The basket data with an additional 'processed_basket' field.
    """
    model_components: dict = load_model()

    saved_label_encoders: dict = model_components['label_encoders']
    saved_scaler = model_components['scaler']

    processed_mushrooms: list[pd.DataFrame] = []
    for mushroom in basket['basket']:
        mushroom_df: pd.DataFrame = pd.DataFrame([mushroom])
        processed_mushrooms.append(preprocess_input(mushroom_df, saved_label_encoders, saved_scaler))

    basket['processed_basket'] = [mushroom.to_dict(orient='records')[0] for mushroom in processed_mushrooms]

    return basket


def preprocess_input(raw_data: pd.DataFrame, label_encoders: dict,
                     scaler: StandardScaler) -> pd.DataFrame:
    """
    Preprocesses raw mushroom data for prediction.

    :param raw_data: Raw input data.
    :param label_encoders: Dictionary of fitted LabelEncoders.
    :param scaler: Fitted StandardScaler.
    :return: Preprocessed data ready for model prediction.
    """
    data: pd.DataFrame = raw_data.copy()

    data.drop(columns=['class', 'veil-type'], inplace=True)

    categorical_cols: list[str] = list(label_encoders.keys())
    for col in categorical_cols:
        le = label_encoders[col]
        data[col] = le.transform(data[col])

    numerical_cols: list[str] = ['cap-diameter', 'stem-height', 'stem-width']
    data[numerical_cols] = scaler.transform(data[numerical_cols])

    return data


if __name__ == "__main__":
    for message in consumer:
        raw_basket: dict = message.value
        preprocessed: dict = preprocess_basket(raw_basket)
        producer.send('preprocessed_data', preprocessed)
        print(f"Предобработана корзина от {preprocessed['seller']}")
