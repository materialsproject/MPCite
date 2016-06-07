import logging, argparse
from osti_record import OstiRecord, OstiMongoAdapter
from builder import DoiBuilder

parser = argparse.ArgumentParser()
parser.add_argument("--log", help="show log output", action="store_true")
parser.add_argument("--prod", action="store_true", help="""use production DB.""")
group = parser.add_mutually_exclusive_group()
group.add_argument("-n", default=0, type=int, help="""number of materials to
                    submit to OSTI. The default (0) collects all materials not
                    yet submitted.""")
group.add_argument('-l', nargs='+', type=int, help="""list of material id's to
                    submit. mp-prefix internally added, i.e. use `-l 4 1986
                   571567`.""")
group.add_argument("--reset", action="store_true", help="""reset collections""")
group.add_argument("--info", action="store_true", help="""retrieve materials
                   already having a doi saved in materials collection""")
group.add_argument("--graph", action="store_true", help="""show graph with stats""")
args = parser.parse_args()

logging.basicConfig(format='%(asctime)-15s %(levelname)s - %(message)s', level=logging.ERROR)
logger = logging.getLogger('mpcite')
loglevel = 'DEBUG' if args.log else 'INFO'
logger.setLevel(getattr(logging, loglevel))

db_yaml = 'materials_db_{}.yaml'.format('prod' if args.prod else 'dev')
ad = OstiMongoAdapter.from_config(db_yaml=db_yaml)
logger.info('loaded DB adapter from {} config'.format(db_yaml))

if args.reset:
    ad._reset()
elif args.info:
    print '{} DOIs in DOI collection.'.format(ad.doicoll.count())
    dois = ad.get_all_dois()
    print '{}/{} materials have DOIs.'.format(len(dois), ad.matcoll.count())
elif args.graph:
    import plotly
    plotly.offline.plot({'data': ad.get_traces()})
else:
    builder = DoiBuilder(db_yaml=db_yaml)
    builder.validate_dois()
    builder.save_bibtex()
    builder.build()
    # generate records for either n or all (n=0) not-yet-submitted materials
    # OR generate records for specific materials (submitted or not)
    osti = OstiRecord(l=args.l, n=args.n, db_yaml=db_yaml)
    osti.submit()
