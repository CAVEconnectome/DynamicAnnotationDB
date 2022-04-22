class DynamicAnnotationBase:
    def __init__(self, url: str, aligned_volume: str = None) -> None:
        self._url = url
        self._aligned_volume = aligned_volume

    @property
    def url(self) -> str:
        return self._url

    @property
    def aligned_volume(self) -> str:
        return self._aligned_volume
