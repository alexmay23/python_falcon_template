import click
from wsgiref import simple_server

import logging

from instances import config, api
from lib.process import StandaloneApplication, number_of_workers


@click.group()
def commands():
    pass


@commands.command()
@click.option('--host', default=config.get('HOST', '127.0.0.1'))
@click.option('--port', default=config.get('PORT', 5000))
def runserver(host, port):
    logging.info('Starting server {}:{}'.format(host, port))
    httpd = simple_server.make_server(host, port, api)
    httpd.serve_forever()


@commands.command()
@click.option('--host', default=config.get('HOST', '127.0.0.1'))
@click.option('--port', default=config.get('PORT', 5000))
def runprod(host, port):
    options = {
        'bind': '%s:%s' % (host, port),
        'workers': number_of_workers(),
    }
    StandaloneApplication(api, options).run()



if __name__ == '__main__':
    commands()