import random

first_names = ["Мария", "Анна", "Елена", "Ольга", "Татьяна", "Наталья", "Ирина", "Светлана", "Юлия", "Юлия"]
patronymics = ["Ивановна", "Петровна", "Сергеевна", "Алексеевна", "Викторовна", "Николаевна"]

def generate_seller_name():
    return f"{random.choice(first_names)} {random.choice(patronymics)}"
