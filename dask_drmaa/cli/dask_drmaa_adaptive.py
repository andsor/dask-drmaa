
import logging
import json
import signal
import sys
from time import sleep

import click
import parse

from dask_drmaa import DRMAACluster, Adaptive
from distributed.cli.utils import check_python_3


@click.command()
@click.option('--queue', '-q')
def main(queue):
    template = dict()
    if queue:
        template['nativeSpecification'] = '-q {queue}'.format(queue)

    cluster = DRMAACluster(silence_logs=logging.INFO, template=template,
            scheduler_port=0)

    scheduler_address = cluster.scheduler_address
    click.echo(json.dumps(parse.parse("{protocol}://{hostip}:{port}",
        scheduler_address).named))
    
    adapt = Adaptive(cluster)

    def handle_signal(sig, frame):
        cluster.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    while True:
        sleep(1)


def go():
    check_python_3()
    main()

if __name__ == '__main__':
    go()
