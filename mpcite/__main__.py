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
group.add_argument("--plotly", action="store_true", help="""init plotly graph""")
args = parser.parse_args()

logging.basicConfig(format='%(asctime)-15s %(levelname)s - %(message)s', level=logging.ERROR)
logger = logging.getLogger('mpcite')
loglevel = 'DEBUG' if args.log else 'INFO'
logger.setLevel(getattr(logging, loglevel))

db_yaml = 'materials_db_{}.yaml'.format('prod' if args.prod else 'dev')
logger.debug(db_yaml)
if args.reset or args.info or args.plotly:
    ad = OstiMongoAdapter.from_config(db_yaml=db_yaml)
    if args.reset:
        ad._reset()
    elif args.info:
        print '{} DOIs in DOI collection.'.format(ad.doicoll.count())
        dois = ad.get_all_dois()
        print '{}/{} materials have DOIs.'.format(len(dois), ad.matcoll.count())
    elif args.plotly:
        import os, datetime
        import plotly.plotly as py
        from plotly.graph_objs import *
        stream_ids = ['645h22ynck', '96howh4ip8', 'nnqpv5ra02']
        py.sign_in(
            os.environ.get('MP_PLOTLY_USER'),
            os.environ.get('MP_PLOTLY_APIKEY'),
            stream_ids=stream_ids
        )
        today = datetime.date.today()
        counts = [
            ad.matcoll.count(), ad.doicoll.count(),
            len(ad.get_all_dois())
        ]
        names = ['materials', 'requested DOIs', 'validated DOIs']
        data = Data([
            Scatter(
                x=[today], y=[counts[idx]], name=names[idx],
                stream=dict(token=stream_ids[idx], maxpoints=10000)
            ) for idx,count in enumerate(counts)
        ])
        filename = 'dois_{}'.format(today)
        print py.plot(data, filename=filename, auto_open=False)
else:
    builder = DoiBuilder(db_yaml=db_yaml)
    builder.validate_dois()
    builder.save_bibtex()
    builder.build()
    # generate records for either n or all (n=0) not-yet-submitted materials
    # OR generate records for specific materials (submitted or not)
    osti = OstiRecord(l=args.l, n=args.n, db_yaml=db_yaml)
    osti.submit()
    # push results to plotly streaming graph
    #now = datetime.datetime.now()
    #counts = [
    #    self.mat_qe.collection.count(),
    #    self.doi_qe.collection.count(),
    #    len(osti_record.ad.get_all_dois())
    #]
    #for idx,stream_id in enumerate(stream_ids):
    #    s = py.Stream(stream_id)
    #    s.open()
    #    s.write(dict(x=now, y=counts[idx]))
    #    s.close()
