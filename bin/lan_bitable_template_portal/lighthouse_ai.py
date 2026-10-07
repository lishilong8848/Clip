"""Previous-generation import path; implementation lives in the assistant service."""
import sys
from openclaw_service.assistant import lighthouse_ai as _implementation

sys.modules[__name__] = _implementation
