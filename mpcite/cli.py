import logging, argparse
from adapter import OstiMongoAdapter
from record import OstiRecord
from builder import DoiBuilder

parser = argparse.ArgumentParser()
parser.add_argument("--log", help="show log output", action="store_true")
parser.add_argument("--prod", action="store_true", help="use production DB.")
group = parser.add_mutually_exclusive_group()
group.add_argument("-n", default=0, type=int, help="number of materials to submit")
group.add_argument('-l', nargs='+', type=int, help="list of MP IDs to submit")
group.add_argument("--build", action="store_true", help="build DOIs")
group.add_argument("--graph", action="store_true", help="show graph with stats")
group.add_argument("--reset", action="store_true", help="reset collections")
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
elif args.graph:
    from plotly.offline import plot
    from plotly.graph_objs import Layout
    plot({
        'data': ad.get_traces(), 'layout': Layout(
            title='MPCite Monitoring', yaxis=dict(type='log', autorange=True)
        )
    })
elif args.build:
    builder = DoiBuilder(db_yaml=db_yaml)
    builder.validate_dois()
    builder.save_bibtex()
    builder.build()
elif args.l or args.n:
    # generate records for n not-yet-submitted materials
    # OR generate records for specific materials (submitted or not)
    osti = OstiRecord(l=args.l, n=args.n, db_yaml=db_yaml)
    osti.submit()
else:
    logger.info('{} DOIs in DOI collection.'.format(ad.doicoll.count()))
    logger.info('{}/{} materials have DOIs.'.format(
        len(ad.get_all_dois()), ad.matcoll.count()
    ))
