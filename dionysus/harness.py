

def amap(archive, dist, component, function):
    for source in archive.get_sources(dist, component):
        with source.checkout() as target:
            function(archive, source, target)
