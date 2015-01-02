from .archive import Archive


def main():
    archive = Archive("http://192.168.1.50/debian")

    def patch(archive, source, dsc):
        with source.unpack():
            print(source, dsc)

    archive.map("unstable", "main", patch)
