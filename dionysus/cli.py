from .archive import Archive


def main():
    archive = Archive("http://192.168.1.50/debian")
    for source in archive.get_sources("unstable", "main"):
        print(source.source['Package'], "/", source.source['Version'])
        with source.checkout() as target:
            print("Have: ", target)
