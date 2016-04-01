source $HOME/.credentials
source $HOME/.virtualenvs/env_mp_osti_doi/bin/activate
cd $HOME/MPCite
export PYTHONPATH=`pwd`:$PYTHONPATH
mgbuild run -v mpcite.builders.DoiBuilder nmats=25 dois=dois.json materials=materials.json
git add dois.json
git commit -m "osti_doi: new dois backup"
#git push origin osti_doi
