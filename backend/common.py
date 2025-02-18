import pickle
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent


def load_model():
    """Loads the trained model and preprocessing objects."""
    model_path = ROOT_DIR / 'models' / 'decision_tree_model.pkl'
    with open(model_path, 'rb') as f:
        model_components = pickle.load(f)
    return model_components
