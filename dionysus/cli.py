from .archive import Archive
from .harness import amap


def main():
    archive = Archive("http://192.168.1.50/debian")

    def patch(archive, source, dsc):
        with source.unpack():
            print(source, dsc)

    amap(archive, "unstable", "main", patch)
