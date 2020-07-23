import io, re, os
from setuptools import setup
from setuptools import find_packages

package_name = "mpcite"
init_py = io.open("{}/__init__.py".format(package_name)).read()
metadata = dict(re.findall("__([a-z]+)__ = '([^']+)'", init_py))
metadata["doc"] = re.findall('"""(.+)"""', init_py)[0]
SETUP_PTH = os.path.dirname(os.path.abspath(__file__))

setup(
    name=package_name,
    version="1.0.0",
    description=metadata["doc"],
    author="Patrick Huck & Michael Wu",
    author_email="phuck@lbl.gov",
    url="https://github.com/materialsproject/MPCite",
    packages=find_packages(),
    license="MIT",
    keywords=["materials", "citation", "framework", "digital object identifiers"],
    # scripts=glob.glob(os.path.join(SETUP_PTH, "scripts", "*")),
    entry_points={"console_scripts": ["mpcite=mpcite.main:main"]},
)
