import logging, argparse, sys
from plotly.offline import plot
from plotly.graph_objs import Layout
from adapter import OstiMongoAdapter
from record import OstiRecord
from builder import DoiBuilder

logging.basicConfig(format='%(asctime)-15s %(levelname)s - %(message)s', level=logging.ERROR)
logger = logging.getLogger('mpcite')

def cli():
    parser = argparse.ArgumentParser(description="""CLI for MPCite. For help,
                                     see `mpcite -h` or `mpcite <command> -h`""")
    parser.add_argument("--log", action="store_true", help="show log output")
    parser.add_argument("--prod", action="store_true", help="use production DB")

    subparsers = parser.add_subparsers()

    reset_parser = subparsers.add_parser('reset', help='reset collections')
    reset_parser.set_defaults(func=reset)

    monitor_parser = subparsers.add_parser('monitor', help='show graph with stats')
    monitor_parser.set_defaults(func=monitor)

    build_parser = subparsers.add_parser('build', help='build DOIs')
    build_parser.set_defaults(func=build)

    submit_parser = subparsers.add_parser('submit', help='request DOIs')
    submit_parser.add_argument(
        "num_or_mpids", nargs='+',
        help="number of materials OR list of mp-id's to submit"
    )
    submit_parser.add_argument(
        "-a", "--auto-accept", action="store_true", help="skip confirmation prompt"
    )
    submit_parser.set_defaults(func=submit)

    info_parser = subparsers.add_parser('info', help='show DB status')
    info_parser.set_defaults(func=info)

    args = parser.parse_args()
    args.func(args)

def set_logger_level(log=False):
    logger.setLevel(getattr(logging, 'DEBUG' if log else 'INFO'))

def get_config(prod=False):
    return 'materials_db_{}.yaml'.format('prod' if prod else 'dev')

def get_adapter(prod=False):
    return OstiMongoAdapter.from_config(db_yaml=get_config(prod=prod))

def reset(args):
    set_logger_level(log=args.log)
    ad = get_adapter(prod=args.prod)
    ad._reset()

def monitor(args):
    set_logger_level(log=args.log)
    ad = get_adapter(prod=args.prod)
    plot({
        'data': ad.get_traces(), 'layout': Layout(
            title='MPCite Monitoring', yaxis=dict(type='log', autorange=True)
        )
    })

def build(args):
    set_logger_level(log=args.log)
    builder = DoiBuilder(db_yaml=get_config(prod=args.prod))
    builder.validate_dois()
    builder.save_bibtex()
    builder.build()

def submit(args):
    set_logger_level(log=args.log)
    num_or_list = args.num_or_mpids
    if len(args.num_or_mpids) == 1:
        try:
            num_or_list = int(args.num_or_mpids[0])
        except:
            pass
    if not args.auto_accept:
        nmats = num_or_list if isinstance(num_or_list, int) else len(num_or_list)
        answer = raw_input("Submit {} materials to OSTI? [y/N]".format(nmats))
        if not answer or answer[0].lower() != 'y':
            logger.error('aborting submission ...')
            sys.exit(0)
    osti = OstiRecord(num_or_list, db_yaml=get_config(prod=args.prod))
    osti.submit()

def info(args):
    set_logger_level(log=args.log)
    ad = get_adapter(prod=args.prod)
    logger.info('{} DOIs in DOI collection.'.format(ad.doicoll.count()))
    logger.info('{}/{} materials have DOIs.'.format(
        len(ad.get_all_dois()), ad.matcoll.count()
    ))

if __name__ == '__main__':
    cli()
