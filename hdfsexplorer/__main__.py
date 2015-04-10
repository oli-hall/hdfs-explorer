import argparse

import server


parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--host', default='localhost',
                    help='Interface to which the service should listen')
parser.add_argument('--port', default=8080,
                    help='Port to which the service should listen')
args = parser.parse_args()


server.bottle.run(app=server.app,
                  server='cherrypy',
                  host=args.host,
                  port=args.port)
