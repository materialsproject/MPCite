import bibtexparser
from bibtexparser.bparser import BibTexParser

bib = """@ARTICLE{osti_1483278,                                                                                                                                | 0/6 [00:00<?, ?it/s]
  title        = {Materials Data on ZrB6 by Materials Project},
  author       = {Persson, Kristin},
  abstract = {ZrB6 is Calcium hexaboride structured and crystallizes in the cubic Pm-3m space group. The structure is three-dimensional. Zr is bonded in a 1-coordinate geometry to twenty-four equivalent B atoms. All Zr–B bond lengths are 2.98 Å. B is bonded in a 1-coordinate geometry to four equivalent Zr and five equivalent B atoms. There is one shorter (1.60 Å) and four longer (1.74 Å) B–B bond length.},
  doi          = {10.80460/1483278},
  journal      = {},
  number       = ,
  volume       = ,
  place        = {United States},
  year         = 2020,
  month        = 5,
  note         = {An optional note}, 
}
"""

# bib="""@article{osti_1483278,
#   author = {Persson, Kristin},
#   title = {Materials Data on ZrB6 by Materials Project},
#   year = 2020,
#   volume = {12},
#   pages = {12--23},
#   journal = {Nice Journal},
#   abstractNote = {ZrB6 is Calcium hexaboride structured and crystallizes in the cubic Pm-3m space group. The structure is three-dimensional. Zr is bonded in a 1-coordinate geometry to twenty-four equivalent B atoms. All Zr–B bond lengths are 2.98 Å. B is bonded in a 1-coordinate geometry to four equivalent Zr and five equivalent B atoms. There is one shorter (1.60 Å) and four longer (1.74 Å) B–B bond length.},
#   comments = {A comment},
#   keywords = {keyword1, keyword2},
#   note         = {An optional note},
#   month        = 5,
#   doi          = {10.80460/1483278},
# }
# """
# bib_db:bibtexparser.bibdatabase.BibDatabase = bibtexparser.loads(bib)

for line in bib.splitlines():
    print(line)