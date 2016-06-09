import logging, argparse, sys, os, yaml
from plotly.offline import plot
from plotly.graph_objs import Layout
from adapter import OstiMongoAdapter
from record import OstiRecord
from builder import DoiBuilder

logging.basicConfig(format='%(asctime)-15s %(levelname)s - %(message)s', level=logging.ERROR)
logger = logging.getLogger('mpcite')
oma, bld, rec = None, None, None # OstiMongoAdapter, DoiBuilder, and OstiRecord Instances

class DictAsMember(dict):
    # http://stackoverflow.com/questions/10761779/when-to-use-getattr/10761899#10761899
    def __getattr__(self, name):
        value = self[name]
        if isinstance(value, dict):
            value = DictAsMember(value)
        return value

def cli():
    global oma, bld, rec
    parser = argparse.ArgumentParser(
        description='Command Line Interface for MPCite',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='show verbose log output')
    mod_dir = os.path.dirname(os.path.abspath(__file__))
    default_config = os.path.normpath(os.path.join(mod_dir, os.pardir, 'files', 'config.yaml'))
    parser.add_argument('-c', '--cfg', default=default_config,
                        help='path to YAML configuration file')

    subparsers = parser.add_subparsers()

    reset_parser = subparsers.add_parser('reset', help='reset collections')
    reset_parser.set_defaults(func=reset)

    monitor_parser = subparsers.add_parser('monitor', help='show graph with stats')
    monitor_parser.add_argument('-o', '--outfile', default='mpcite.html',
                                help='path to output html file')
    monitor_parser.set_defaults(func=monitor)

    build_parser = subparsers.add_parser('build', help='build DOIs')
    build_parser.set_defaults(func=build)

    submit_parser = subparsers.add_parser('submit', help='request DOIs')
    submit_parser.add_argument(
        'num_or_mpids', nargs='+',
        help='number of materials OR list of mp-ids to submit'
    )
    submit_parser.add_argument('-a', '--auto-accept', action='store_true',
                               help='skip confirmation prompt')
    submit_parser.set_defaults(func=submit)

    info_parser = subparsers.add_parser('info', help='show DB status')
    info_parser.set_defaults(func=info)

    args = parser.parse_args()
    logger.setLevel(getattr(logging, 'DEBUG' if args.verbose else 'INFO'))
    with open(args.cfg, 'r') as f:
        config = DictAsMember(yaml.load(f))
    oma = OstiMongoAdapter.from_config(config)
    bld = DoiBuilder(oma, config.osti.explorer)
    rec = OstiRecord(oma)
    logger.info('{} loaded'.format(args.cfg))
    args.func(args)

def reset(args):
    oma._reset()

def monitor(args):
    plot({
        'data': oma.get_traces(), 'layout': Layout(
            title='MPCite Monitoring', yaxis=dict(type='log', autorange=True)
        )
    }, show_link=False, auto_open=False, filename=args.outfile)
    logger.info('plotly page {} generated'.format(args.outfile))

def build(args):
    bld.validate_dois()
    bld.save_bibtex()
    bld.build()

def submit(args):
    num_or_list = args.num_or_mpids
    if len(args.num_or_mpids) == 1:
        try:
            num_or_list = int(args.num_or_mpids[0])
        except:
            pass
    if not args.auto_accept:
        nmats = num_or_list if isinstance(num_or_list, int) else len(num_or_list)
        answer = raw_input('Submit {} materials to OSTI? [y/N]'.format(nmats))
        if not answer or answer[0].lower() != 'y':
            logger.error('aborting submission ...')
            sys.exit(0)
    rec.generate(num_or_list)
    rec.submit()

def info(args):
    logger.info('{} DOIs in DOI collection.'.format(oma.doicoll.count()))
    logger.info('{}/{} materials have DOIs.'.format(
        len(oma.get_all_dois()), oma.matcoll.count()
    ))

if __name__ == '__main__':
    cli()
