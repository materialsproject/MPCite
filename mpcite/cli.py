import logging, argparse, sys, os, yaml, logging.handlers, warnings
from datetime import datetime
from errno import ECONNREFUSED
from subprocess import Popen, PIPE
from socket import error as SocketError
from plotly.offline import plot
from plotly.graph_objs import Layout
from adapter import OstiMongoAdapter
from record import OstiRecord
from builder import DoiBuilder
from pyspin.spin import make_spin, Default

FORMAT = '%(asctime)-15s %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT, level=logging.ERROR)
logger = logging.getLogger('mpcite')
oma, bld, rec = None, None, None # OstiMongoAdapter, DoiBuilder, and OstiRecord Instances

mpcite_html = """
<head>
  <title>MPCite</title>
  <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<h1>MPCite Dashboard</h1>
<a href="http://materialsproject.org:8000/" target="_blank">View Cronjob Log</a>
"""

class DictAsMember(dict):
    # http://stackoverflow.com/questions/10761779/when-to-use-getattr/10761899#10761899
    def __getattr__(self, name):
        value = self[name]
        if isinstance(value, dict):
            value = DictAsMember(value)
        return value

class BufferingSMTPHandler(logging.handlers.BufferingHandler):
    # https://gist.github.com/anonymous/1379446
    def __init__(self, address):
        logging.handlers.BufferingHandler.__init__(self, 50)
        self.fromaddr = 'root@localhost'
        self.toaddrs = [address]
        self.subject = '[mpcite] ErrorLog {}'.format(datetime.now())
        self.setFormatter(logging.Formatter(FORMAT))

    def flush(self):
        if len(self.buffer) > 0:
            try:
                msg = "From: {}\r\nTo: {}\r\nSubject: {}\r\n\r\n".format(
                    self.fromaddr, ','.join(self.toaddrs), self.subject
                )
                for record in self.buffer:
                    s = self.format(record)
                    msg += "{}\r\n".format(s)
                if 'ERROR' in msg:
                    try:
                        import smtplib
                        smtp = smtplib.SMTP()
                        smtp.connect()
                        smtp.sendmail(self.fromaddr, self.toaddrs, msg)
                        smtp.quit()
                    except SocketError as e:
                        if e.args[0] == ECONNREFUSED:
                            p = Popen(["/usr/sbin/sendmail", "-t"], stdin=PIPE)
                            p.communicate(msg)
                        else:
                            raise
            except:
                self.handleError(None)  # no particular record
            self.buffer = []

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
    reset_parser.add_argument(
        '--matcoll', action='store_true',
        help='remove DOI keys from materials collection'
    )
    reset_parser.add_argument(
        '--rows', help='number of DOIs (page size) to request at once from E-Link'
    )
    reset_parser.set_defaults(func=reset)

    sync_parser = subparsers.add_parser('sync', help='sync collections')
    sync_parser.set_defaults(func=sync)

    monitor_parser = subparsers.add_parser('monitor', help='show graph with stats')
    monitor_parser.add_argument('-o', '--outfile', default='mpcite.html',
                                help='path to output html file')
    monitor_parser.add_argument('--div-only', action='store_true',
                                help='only produce plotly div')
    monitor_parser.set_defaults(func=monitor)

    build_parser = subparsers.add_parser('build', help='build DOIs')
    build_parser.add_argument(
        '-n', metavar='NR_REQUESTED_DOIS', dest='nr_req_dois', type=int,
        default=1, help='number of DOIs requested during submission'
    )
    build_parser.set_defaults(func=build)

    submit_parser = subparsers.add_parser('submit', help='request DOIs')
    submit_parser.add_argument(
        'num_or_mpids', nargs='+',
        help='number of materials OR list of mp-ids to submit'
    )
    submit_parser.add_argument('-a', '--auto-accept', action='store_true',
                               help='skip confirmation prompt')
    submit_parser.set_defaults(func=submit)

    update_parser = subparsers.add_parser('update', help='update/resubmit all DOIs')
    update_parser.add_argument(
        '-n', metavar='CHUNK_SIZE', dest='chunk_size', type=int,
        default=50, help='number of DOIs to update at once'
    )
    update_parser.set_defaults(func=update)

    info_parser = subparsers.add_parser('info', help='show DB status')
    info_parser.set_defaults(func=info)

    args = parser.parse_args()
    logger.setLevel(getattr(logging, 'DEBUG' if args.verbose else 'INFO'))
    with open(args.cfg, 'r') as f:
        config = DictAsMember(yaml.load(f))
    if config.logging.send_email:
        addr = config.logging.address
        logger.addHandler(BufferingSMTPHandler(addr))
        logger.debug('set up logging to send output to {}'.format(addr))
    oma = OstiMongoAdapter.from_config(config)
    bld = DoiBuilder(oma, config.osti.explorer)
    rec = OstiRecord(oma)
    logger.debug('{} loaded'.format(args.cfg))
    try:
        args.func(args)
    except Exception as ex:
        logger.error(ex)
    logging.shutdown()

def reset(args):
    oma._reset(matcoll=args.matcoll, rows=args.rows)
    bld.limit = 100 #oma.doicoll.count()
    bld.show_pbar = True
    bld.save_bibtex()
    bld.build()

def sync(args):
    bld.show_pbar = True
    bld.sync()

def monitor(args):
    fig = dict(data=oma.get_traces(), layout=Layout(
        yaxis=dict(type='log', autorange=True),
        height=700, margin=dict(t=20),
    ))
    kwargs = dict(
        show_link=False, auto_open=False, filename=args.outfile,
        output_type='div' if args.div_only else 'file',
        include_plotlyjs=not args.div_only,
    )
    div = plot(fig, **kwargs)
    if args.div_only and div:
        with open(args.outfile, 'w') as f:
            f.write(mpcite_html)
            f.write(div)
    logger.info('plotly page {} generated'.format(args.outfile))

def build(args):
    bld.limit = args.nr_req_dois
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

@make_spin(Default, "OSTI request ... ")
def submit_with_spinner():
    rec.submit()

def update(args):
    mp_ids = oma.doicoll.find({'doi': {'$exists': True}}).distinct('_id')
    rec.show_pbar = True
    for i in xrange(0, len(mp_ids), args.chunk_size):
        rec.generate(mp_ids[i:i+args.chunk_size])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            submit_with_spinner()

def info(args):
    logger.info('{} DOIs in DOI collection'.format(oma.doicoll.count()))
    ndois_missing_built_on, ndois_missing_bibtex = 0, 0
    mats = oma.matcoll.find(
        {'doi': {'$exists': True}},
        {'_id': 0, 'task_id': 1, 'doi': 1, 'doi_bibtex': 1}
    )
    logger.info('{}/{} materials have DOIs'.format(mats.count(), oma.matcoll.count()))
    dois_missing_built_on = [
        d['_id'] for d in oma.doicoll.find(
            {'_id': {'$in': mats.distinct('task_id')}}, {'built_on': 1}
        ) if 'built_on' not in d
    ]
    for mat in mats:
        if 'doi_bibtex' not in mat:
            ndois_missing_bibtex += 1
        elif mat['task_id'] in dois_missing_built_on:
            ndois_missing_built_on += 1
    if ndois_missing_bibtex > 0:
        logger.error('{} materials missing bibtex'.format(ndois_missing_bibtex))
    if ndois_missing_built_on > 0:
        logger.error('{} DOIs missing built_on'.format(ndois_missing_built_on))
    if not ndois_missing_built_on and not ndois_missing_bibtex:
        logger.info('all DOIs and materials OK')
    content = oma.osti_request()
    if '@numfound' in content:
        logger.info('{} DOIs in E-Link'.format(content['@numfound']))
    else:
        logger.error('could not retrieve number of DOIs from E-Link')

if __name__ == '__main__':
    cli()
