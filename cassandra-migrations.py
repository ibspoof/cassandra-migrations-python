#!/usr/bin/env python

import argparse
import os
import re
import time
import json
import pip

installed_packages = pip.get_installed_distributions()
flat_installed_packages = [package.project_name for package in installed_packages]

if 'cassandra-driver' not in flat_installed_packages or 'blist' not in flat_installed_packages:
    print "\n \033[91m ERROR: cassandra-driver or blist is not installed.  Please run 'pip install cassandra blist\n" \
          "to fix.\033[0m"

import cassandra
from cassandra import (ConsistencyLevel, InvalidRequest)
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


class CColors:
    def __init__(self):
        pass

    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    DEFAULT = ''


parser = argparse.ArgumentParser(description='Migrate cassandra schema',
                                 epilog="\nUse " + CColors.OKGREEN + "./cassandra-migrations.py help full" +
                                        CColors.ENDC + " for complete usage")
parser.add_argument(
    'task', help='Task to run (generate|migrate|rollback|createKeyspace|help)')
parser.add_argument('keyspace', help="C* keyspace to use (global|local)")
parser.add_argument('--ip', default="127.0.0.1", help="Server IP address")
parser.add_argument('--port', default=9042, help="Server's Cassandra Port")
parser.add_argument('--con', default="LOCAL_QUARUM",
                    help="C* Insert Consistency, should be LOCAL_QUARUM for normal operations")
parser.add_argument('--username', help="C* Username")
parser.add_argument('--password', help="C* Password")
parser.add_argument('--name',
                    help="Name of schema migration to use when using 'generate' task. "
                         "Example output: 20150105110259_{name}.xml")
parser.add_argument('--timeout', default=60, help='Session default_timeout')
parser.add_argument('--sleep', default=0.1, help='Sleep time between migrations')
parser.add_argument('--protocol_version', default=3, help='CQL protocol_version')
parser.add_argument('--debug', default=False, help='Should debug console logs be enabled')
parser.add_argument('--steps', default=1, help='Number of migrations to rollback')

args = parser.parse_args()


def _version_tuple(v):
    return tuple(map(int, (v.split("."))))


base_driver_version = "2.7.1"
if _version_tuple(cassandra.__version__) < _version_tuple(base_driver_version):
    print "\n" + CColors.WARNING + "WARNING: cassandra-driver is lower than " + base_driver_version + \
          " upgrade is highly recommended.\nUse: `pip install cassandra-driver --upgrade`" + CColors.ENDC


def _app_help():
    print """
  Usage:

  Create new migration file
     """ + CColors.OKGREEN + """./cassandra-migrations.py generate {keyspace} --name {name}""" + CColors.ENDC + """

  Apply migrations to a keyspace:
     """ + CColors.OKGREEN + """./cassandra-migrations.py migrate {keyspace}""" + CColors.ENDC + """
  for a remote server
     """ + CColors.OKGREEN + """./cassandra-migrations.py migrate {keyspace} --ip {serverIP}""" + CColors.ENDC + """
  for a remote server w/ authentication
     """ + CColors.OKGREEN + """./cassandra-migrations.py migrate {keyspace} --ip {serverIP} --username {username} --password {password}""" + CColors.ENDC + """

  Setting port of C* to use:
     """ + CColors.OKGREEN + """./cassandra-migrations.py migrate {keyspace} --port {port}""" + CColors.ENDC + """

  Setting consistency of C* operations:
     """ + CColors.OKGREEN + """./cassandra-migrations.py migrate {keyspace} --con {consistency}""" + CColors.ENDC + """

  Rollback a migration:
     """ + CColors.OKGREEN + """./cassandra-migrations.py rollback {keyspace}""" + CColors.ENDC + """

  Create new keyspace (Note: only works on localhost)
     """ + CColors.OKGREEN + """./cassandra-migrations.py createKeyspace {keyspace}""" + CColors.ENDC + """

  Get latest migration for keyspace
     """ + CColors.OKGREEN + """./cassandra-migrations.py current {keyspace}""" + CColors.ENDC + """
     or
     """ + CColors.OKGREEN + """./cassandra-migrations.py current {keyspace} --ip {serverIP}""" + CColors.ENDC + """

  All Options:
    --name              Name of schema migration to use when using 'generate' task.
    --ip                Server IP address (default: 127.0.0.1)
    --port              Server's Cassandra Port (default: 9042)
    --con               C* Insert Consistency (default LOCAL_QUARUM)
    --timeout           Session default_timeout
    --username          C* Username
    --password          C* Password
    --debug             Should debug console logs be enabled (default: False)
    --protocol_version  CQL protocol_version (default: 3)
    --sleep             Sleep time between migrations in seconds (default: 0.1)
    --steps             Rollback number of migrations (default: 1)
  """


currentPath = os.path.dirname(os.path.abspath(__file__))
migrationPath = currentPath + '/migrations/' + args.keyspace + '/'

# set conLevel
if args.con == "ONE":
    conLevel = ConsistencyLevel.ONE
elif args.con == "EACH_QUORUM":
    conLevel = ConsistencyLevel.EACH_QUORUM
elif args.con == "ANY":
    conLevel = ConsistencyLevel.ANY
else:
    conLevel = ConsistencyLevel.LOCAL_QUORUM


def generate_migration():
    """
    Generate migrations for a specified keyspace
    """
    if args.name is None:
        _incorrect("Migration name must be provided (--name {name}).")

    file_name = time.strftime("%Y%m%d%H%M%S_") + _convert(args.name) + ".json"

    default_text = """
{
    "up": [
        "// add up cql events one per line"
    ],
    "down": [
        "// add down cql events one per line"
    ]
}"""

    if not os.path.exists(migrationPath):
        os.makedirs(migrationPath)

    target = open(migrationPath + file_name, 'a')
    target.write(default_text)
    target.close()

    _print_to_console(CColors.OKGREEN, "\nCreated migrations file: %s%s\n" % (migrationPath, file_name))


def _convert(name):
    """
    Convert the special chars
    :param name:
    :return:
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def _connect():
    """
    Connect to the node and keyspace
    :return:
    """
    auth_provider = None

    if args.username is not None and args.password is not None:
        auth_provider = PlainTextAuthProvider(
            username=args.username, password=args.password)

    cluster = Cluster([args.ip], protocol_version=args.protocol_version,
                      auth_provider=auth_provider, port=int(args.port))

    # connect to keyspace
    try:
        cluster = cluster.connect(args.keyspace)
    except InvalidRequest:
        _incorrect("Keyspace " + args.keyspace + "does not exist.")
    except Exception:
        _incorrect("Unable to connect to host " + args.ip + " using port " + str(args.port) +
                   " or username/password is incorrect.")

    cluster.default_timeout = int(args.timeout)
    return cluster


def migrate():
    """
    Migrate the schema to the latest version
    :return:
    """
    session = _connect()

    if os.path.isdir(migrationPath) is False:
        _incorrect("Migrations for keyspace: %s do not exists, please run generate to build a migration")

    # check if schema migrations table exists
    check_mig_table_cql = "SELECT * FROM schema_migrations LIMIT 1"
    if _run_query(session, check_mig_table_cql) is None:
        _debug("Table schema_migrations does not exist, creating it...")
        create_mig_table_cql = "CREATE TABLE IF NOT EXISTS schema_migrations (version varchar, PRIMARY KEY(version));"
        _run_query(session, create_mig_table_cql)

    # walk through the migration files
    f = []
    for (dir_path, dir_names, file_names) in os.walk(migrationPath):
        f.extend(file_names)
        break

    f = sorted(f)
    for filename in f:
        id_migration = filename.split('_')[0]

        check_migration_exists_cql = "SELECT version FROM schema_migrations where version=%s"
        res = _run_query(session, check_migration_exists_cql, [id_migration])

        if res is not None:
            _debug("Migration version %s already applied" % id_migration)
            continue

        data = _load_json_file(filename)

        if data is False:
            continue

        error = False
        for cql in data['up']:
            if _run_query(session, cql) is False:
                error = True
                _print_to_console(CColors.WARNING, "WARNING: Error occured while applying %s" % cql)
            else:
                _print_to_console(CColors.DEFAULT, "Executed up for keyspace: %s, migration: %s)" % (args.keyspace, filename))
                time.sleep(float(args.sleep))

        if error is True:
            _print_to_console(CColors.OKBLUE,
                              "NOTICE: Migration file %s/%s already applied" % (args.keyspace, filename))
        else:
            cql = "INSERT INTO schema_migrations (version) VALUES (%s)"
            r = _run_query(session, cql, [id_migration])
            if r is not None and r is not False:
                _debug("Inserted schema migration %s" % id_migration)

    _print_to_console(CColors.OKGREEN, "\nMigration complete.\n")


def current():
    """
    Get current migration version
    :return:
    """
    session = _connect()
    versions = _get_migration_versions(session)

    if versions is None:
        return

    current_version = versions[-1]

    _print_to_console(CColors.DEFAULT, "\nCurrent Migration for '%s' keyspace: %s \n" % (args.keyspace, versions[-1]))
    return current_version


def create():
    """
    Create keyspace helper when running on local IP only
    :return:
    """
    if args.ip is not None:
        _incorrect("Cannot create keyspace for remote server.")

    session = _connect()
    cql = "CREATE KEYSPACE IF NOT EXISTS " + args.keyspace + " WITH REPLICATION = " \
                                                             "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    query = SimpleStatement(cql, consistency_level=ConsistencyLevel.LOCAL_ONE)
    session.execute(query)
    _print_to_console(CColors.OKGREEN, "\nKeyspace '%s' created with replication factor of 1\n" % args.keyspace)


def rollback():
    """
    Rollback the migrations (aka run 'down')
    :return:
    """
    steps = int(args.steps)
    session = _connect()
    versions = _get_migration_versions(session)

    migration_file_list = os.listdir(migrationPath)

    for step in range(0, steps):
        rollback_version = versions[-1]

        for f in migration_file_list:
            if f.startswith(rollback_version):
                rollback_file = f
                break

        data = _load_json_file(rollback_file)

        if data is False:
            continue

        error = False

        for cql in data['down']:
            if _run_query(session, cql) is False:
                error = True
                _print_to_console(CColors.WARNING, "WARNING: Error occurred while rolling back: %s")
            else:
                _print_to_console(CColors.DEFAULT, "Executed down for (%s/%s)" % (args.keyspace, rollback_file))
                time.sleep(float(args.sleep))

        versions.pop()

        if error is True:
            _print_to_console(CColors.OKBLUE,
                              "\nNOTICE: Rollback file %s/%s error" % (args.keyspace, rollback_file))
        else:
            cql = "DELETE FROM schema_migrations WHERE version=%s"
            _run_query(session, cql, [rollback_version])

    _print_to_console(CColors.OKGREEN, "\nMigration rollback complete.\n")


def _run_query(session, cql_string, params=None):
    """
    Run CQL Query using the current session

    :param session:
    :param cql_string: String
    :param params:
    :return:
    """
    query = SimpleStatement(cql_string, consistency_level=conLevel)

    try:
        if params is None:
            results = session.execute(query)
        else:
            results = session.execute(query, params)
    except InvalidRequest, e:
        print e
        return False

    rows = []
    for row in list(results):
        rows.append(row)

    if len(rows) < 1:
        return None

    return rows


def _incorrect(message):
    """
    Print incorrect message to console
    :param message:
    :return:
    """
    if message is None:
        message = "Incorrect usage."

    full_message = "\nERROR: " + message + CColors.OKBLUE + \
                   "\n\nUse './cassandra-migrations.py -h' or " \
                   "'./cassandra-migrations.py help full' for commands\n"
    _print_to_console(CColors.FAIL, full_message)
    exit(1)


def _print_to_console(color, msg):
    """
    Print log message to console with specific color
    :param color:
    :param msg:
    :return:
    """
    print color + msg + CColors.ENDC


def _load_json_file(filename):
    """
    Load JSON file from disk and parse to object
    :param filename:
    :return:
    """
    with open(migrationPath + filename) as json_string:
        try:
            data = json.load(json_string)
        except Exception:
            _print_to_console(CColors.FAIL, "ERROR: %s file is not valid JSON" % migrationPath + filename)
            return False
    return data


def _debug(msg):
    """
    Echo debug msg to console
    :param msg:
    :return:
    """
    if args.debug is not False:
        _print_to_console(CColors.DEFAULT, "DEBUG: " + msg)


def _get_migration_versions(session):
    """
    Get all migration versions
    :param session:
    :return:
    """
    versions = []

    rows = _run_query(session, "SELECT version FROM schema_migrations")

    if rows is None:
        _print_to_console(CColors.WARNING, "WARNING: No migration versions found")
        return

    for c in rows:
        versions.append(c.version)

    return sorted(versions)


if args.task == "generate":
    generate_migration()
elif args.task == "createKeyspace":
    create()
elif args.task == "migrate":
    migrate()
elif args.task == "current":
    current()
elif args.task == "rollback":
    rollback()
elif args.task == "help":
    _app_help()
else:
    _incorrect(None)
