import os

#_DEFAULT_MODEL = os.getenv("LLM_MODEL", "gpt-4.1")
_DEFAULT_MODEL = os.getenv("LLM_MODEL", "gemini-2.0-flash")

class ModelConfig:
    """Singleton-like class to manage the global model name."""
    _model = _DEFAULT_MODEL

    @classmethod
    def get_model(self):
        """Get the current model name."""
        return self._model
    
    @classmethod
    def get_service_model(self):
        return self._model.lower().strip().split('-')[0]
    
    @classmethod
    def set_model(self, model):
        """Set the model name."""
        self._model = model.lower().strip()


    