import io, re, glob, os
from setuptools import setup

package_name = 'mpcite'
init_py = io.open('{}/__init__.py'.format(package_name)).read()
metadata = dict(re.findall("__([a-z]+)__ = '([^']+)'", init_py))
metadata['doc'] = re.findall('"""(.+)"""', init_py)[0]
SETUP_PTH = os.path.dirname(os.path.abspath(__file__))

setup(
    name = package_name,
    version = metadata['version'],
    description = metadata['doc'],
    author = metadata['author'],
    author_email = metadata['email'],
    url = metadata['url'],
    packages = [package_name],
    install_requires = [
        'dicttoxml', 'latexcodec', 'monty', 'plotly', 'pybtex', 'pymongo',
        'pytz', 'PyYAML', 'requests', 'six', 'xmltodict', 'tqdm', 'colorlover',
        'pyspin'
    ],
    license = 'MIT',
    keywords = ['materials', 'citation', 'framework', 'digital object identifiers'],
    scripts = glob.glob(os.path.join(SETUP_PTH, "scripts", "*")),
)
