import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import plotly.express as px


@st.cache_resource
def get_consumer():
    return KafkaConsumer(
        'classified_data', 'expired_data',
        bootstrap_servers='localhost:9095',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id='streamlit_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )


consumer = get_consumer()

if 'data' not in st.session_state:
    st.session_state.data = []

st.title("Дашборд Рынка Грибов")

price_distribution_placeholder = st.empty()
poisonous_pie_placeholder = st.empty()
best_offers_title_placeholder = st.empty()
best_offers_placeholder = st.empty()


def human_readable_time_passed(timestamp):
    """
    Convert a timestamp to a human-readable time difference.

    :param timestamp: The original timestamp.
    :return: A string representing the time passed since the timestamp.
    """
    try:
        ts = pd.to_datetime(timestamp, unit='s', utc=True)
        now = pd.Timestamp.utcnow()
        diff = now - ts

        if diff.days > 0:
            return f"{diff.days} дн."
        elif diff.seconds >= 3600:
            hours = diff.seconds // 3600
            return f"{hours} ч."
        elif diff.seconds >= 60:
            minutes = diff.seconds // 60
            return f"{minutes} мин."
        else:
            return f"{diff.seconds} сек."
    except Exception as e:
        return "Неизвестно"


def update_page(data: list[dict]) -> None:
    """
    Updates the data in graphs and tables on the dashboard.

    :param data: The list of classified basket data.
    """
    df: pd.DataFrame = pd.DataFrame(data)

    if df.empty:
        st.warning("Нет данных для отображения.")
        return

    df['poisonous_label'] = df['predicted_poisonous'].map(lambda x: "Безопасная" if x == 0 else "С ядовитыми грибами")

    # Price distribution
    price_distribution = px.histogram(
        df,
        x='price',
        nbins=20,
        title='Распределение Цен Корзин'
    )
    price_distribution_placeholder.plotly_chart(price_distribution, use_container_width=True)

    # Poisonous vs. Edible pie chart
    poisonous_pie = px.pie(
        df,
        names='poisonous_label',
        title='Соотношение Ядовитых и Съедобных Корзин',
        labels={'predicted_poisonous': 'Корзина'}
    )
    poisonous_pie_placeholder.plotly_chart(poisonous_pie, use_container_width=True)

    # Best offers title
    best_offers_title_placeholder.subheader("Лучшие Съедобные Корзины")

    # Best offers table
    edible_df: pd.DataFrame = df[df['predicted_poisonous'] == 0].copy()
    if not edible_df.empty:
        edible_df['size_sum'] = edible_df['basket'].apply(
            lambda basket: sum(
                mushroom['cap-diameter'] + mushroom['stem-height'] + mushroom['stem-width']
                for mushroom in basket
            )
        )
        edible_df['value_score'] = edible_df['size_sum'] / edible_df['price']
        best_offers: pd.DataFrame = edible_df.nlargest(10, 'value_score')

        best_offers['time_passed'] = best_offers['timestamp'].apply(
            lambda ts: human_readable_time_passed(ts)
        )

        best_offers_placeholder.table(
            best_offers[['seller', 'price', 'time_passed', 'value_score']].rename(
                columns={'value_score': 'Оценка Ценности'}
            )
        )
    else:
        best_offers_placeholder.info("Нет съедобных корзин для отображения.")


new_messages = consumer.poll(timeout_ms=1000, max_records=100)
for topic_partition, messages in new_messages.items():
    for message in messages:
        if message.topic == 'expired_data':
            st.session_state.data = [
                basket for basket in st.session_state.data
                if basket.get('id') != message.value.get('id')
            ]
        else:
            st.session_state.data.append(message.value)

update_page(st.session_state.data)


if st.button("Обновить данные"):
    st.rerun()

st.write("Нажмите кнопку выше, чтобы загрузить и отобразить данные.")
