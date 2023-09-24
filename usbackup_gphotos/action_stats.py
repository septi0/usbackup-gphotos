class ActionStats:
    def __init__(self, **props) -> None:
        self._props: dict = props
        self._total: int = sum(props.values())

    @property
    def total(self) -> int:
        return self._total

    def increment(self, **props) -> None:
        for prop, value in props.items():
            self._props[prop] += value

        self._total += sum(props.values())

    def __str__(self) -> str:
        return f'{", ".join(f"{k}: {v}" for k, v in self._props.items())}'

    def __bool__(self) -> bool:
        return bool(self._total)

    def __iter__(self) -> iter:
        return iter(self._props.items())

    def __getitem__(self, name: str) -> int:
        return self._props[name]