source $HOME/.credentials
source $HOME/.virtualenvs/env_mpcite/bin/activate
cd $HOME/MPCite
export PYTHONPATH=`pwd`:$PYTHONPATH
mgbuild run -v mpcite.builders.DoiBuilder nmats=25 dois=dois.json materials=materials.json
# TODO retire use of mgbuild, go through mpcite/__main__.py instead
