from typing import Any, Optional, Text

from ruamel.yaml.representer import Representer

class YAML:
    def __init__(self, *, typ=..., pure=..., output=..., plug_ins=...):
        # type: (Any, Optional[Text], Any, Any, Any) -> None
        pass
    
    def load(self, stream: Any) -> Any: ...


    def dump(self, data: Any, stream: Any=..., *, transform: Any=...) -> Any: ...

    default_flow_style: bool
    representer: Representer

def add_representer(data_type: Any, object_representer: Any) -> None: ...