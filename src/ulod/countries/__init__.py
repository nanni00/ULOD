from importlib import import_module

__all__ = [
    "Canada",
    "Paris",
    "Italy",
    "Modena",
    "Ferrara",
    "Milano",
    "Bologna",
    "Madrid",
    "UK",
    "NHSUK",
    "Chicago",
    "NYC",
    "canada",
    "france",
    "italy",
    "spain",
    "uk",
    "usa",
]


def __getattr__(name: str):
    modules = {
        "Canada": ("ulod.countries.canada", "Canada"),
        "Paris": ("ulod.countries.france", "Paris"),
        "Italy": ("ulod.countries.italy", "Italy"),
        "Modena": ("ulod.countries.italy", "Modena"),
        "Ferrara": ("ulod.countries.italy", "Ferrara"),
        "Milano": ("ulod.countries.italy", "Milano"),
        "Bologna": ("ulod.countries.italy", "Bologna"),
        "Madrid": ("ulod.countries.spain", "Madrid"),
        "UK": ("ulod.countries.uk", "UK"),
        "NHSUK": ("ulod.countries.uk", "NHSUK"),
        "Chicago": ("ulod.countries.usa", "Chicago"),
        "NYC": ("ulod.countries.usa", "NYC"),
        "canada": ("ulod.countries.canada", None),
        "france": ("ulod.countries.france", None),
        "italy": ("ulod.countries.italy", None),
        "spain": ("ulod.countries.spain", None),
        "uk": ("ulod.countries.uk", None),
        "usa": ("ulod.countries.usa", None),
    }

    if name not in modules:
        raise AttributeError(f"module 'ulod.countries' has no attribute {name!r}")

    module_name, attribute = modules[name]
    module = import_module(module_name)
    return module if attribute is None else getattr(module, attribute)
