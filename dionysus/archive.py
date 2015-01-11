import contextlib
import requests

import concurrent.futures

import tempfile
import gzip
import shutil
import json
import sys
import io
import os

from .util import ipara, run_command
from debian.deb822 import Sources, Dsc


class Archive:
    def __init__(self, mirror):
        self.mirror = mirror

    def _get_source_url(self, dist, component):
        """
        Get the full URL to Sources.gz on our mirror.
        """
        return "{mirror}/dists/{dist}/{component}/source/Sources.gz".format(
            dist=dist,
            mirror=self.mirror,
            component=component,
        )

    def get_sources(self, dist, component):
        request = requests.get(self._get_source_url(dist, component))
        data = io.BytesIO(request.content)
        stream = gzip.GzipFile(fileobj=data)
        yield from (Upload(x, self) for x in Sources.iter_paragraphs(stream))

    def map(self, dist, component, function, test=False):
        for source in self.get_sources(dist, component):
            map_wrapper(self, source, dist, component, function, test=test)

    def amap(self, workers, dist, component, function, test=False):
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=workers
        ) as executor:
            for future in concurrent.futures.as_completed((
                executor.submit(
                    map_wrapper, self, source, dist, component, function,
                    test=test
                ) for source in self.get_sources(dist, component)
            )):
                sys.stdout.write(".")
                sys.stdout.flush()



def map_wrapper(archive, source, dist, component, function, test=False):
    directory = source.source['Directory']
    package = source.source['Package']
    version = source.source['Version']

    resultfp = "{}/{}-{}.json".format(directory, package, version)
    if os.path.exists(resultfp):
        return None

    with source.checkout() as target:
        info = function(archive, source, target)

    if info and (test is False):
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open("{}/{}-{}.json".format(
            directory,
            package,
            version,
        ), 'w') as fd:
            json.dump(info, fd)


class Upload:
    def __init__(self, source, archive):
        self.source = source
        self.archive = archive

    def get_dsc(self):
        target = None
        for file_ in [x['name'] for x in self.source['Files']]:
            if file_.endswith(".dsc"):
                target = file_
                break
        else:
            raise ValueError("No DSC?")
        return target

    def dget(self):
        target = self.get_dsc()
        directory = self.source['Directory']
        url = "{mirror}/{directory}/{target}".format(
            mirror=self.archive.mirror,
            directory=directory,
            target=target,
        )
        _, _, ret = run_command([
            "dget", "-ud", url,
        ])

        if ret != 0:
            raise ValueError("Bad dput - %s" % (url))

        return target

    @contextlib.contextmanager
    def unpack(self):
        target = self.get_dsc()
        _, _, ret = run_command([
            "dpkg-source", "-x", target, "target",
        ])
        os.chdir("target")
        try:
            yield
        finally:
            os.chdir("..")

    @contextlib.contextmanager
    def checkout(self):
        workdir = tempfile.mkdtemp(suffix='.dionysus')
        try:
            popdir = os.getcwd()
            os.chdir(workdir)
            path = self.dget()
            yield Dsc(open(os.path.join(os.getcwd(), path), 'r'))
        finally:
            os.chdir(popdir)
            shutil.rmtree(workdir)
