import contextlib
import requests

import tempfile
import gzip
import shutil
import io
import os

from .util import ipara, run_command
from debian.deb822 import Sources


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


class Upload:
    def __init__(self, source, archive):
        self.source = source
        self.archive = archive

    def dget(self):
        target = None
        for file_ in [x['name'] for x in self.source['Files']]:
            if file_.endswith(".dsc"):
                target = file_
                break
        else:
            raise ValueError("No DSC?")
        directory = self.source['Directory']
        url = "{mirror}/{directory}/{target}".format(
            mirror=self.archive.mirror,
            directory=directory,
            target=target,
        )
        _, _, ret = run_command([
            "dget", "-x", url,
        ])

        if ret != 0:
            raise ValueError("Bad dput - %s" % (url))
        return target

    @contextlib.contextmanager
    def checkout(self):
        workdir = tempfile.mkdtemp(suffix='.dionysus')
        try:
            popdir = os.getcwd()
            os.chdir(workdir)
            path = self.dget()
            yield path
        finally:
            os.chdir(popdir)
            shutil.rmtree(workdir)
