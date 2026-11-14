#!/usr/bin/env python
#
# $Id: fp-listen.py,v 1.8 2007-10-12 16:32:34 dan Exp $
#
# This program listens for events on the database and processes them
#

import sys
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import select

import os	# for deleting cache files
import syslog	# for logging
import glob	# for glob
import shutil	# for rmtree

import configparser # for fp-listen.ini parsing
import traceback    # for logging a handler's error


config = configparser.ConfigParser()
config.read('/usr/local/etc/freshports/fp-listen.ini')

def ini_value(value):
  # fp-listen.ini writes some values the libpq way, in single quotes:
  # DBNAME = 'freshports.org', PASSWORD = ''.  Take the quotes off, and
  # undo \' and \\ inside them, as libpq does.
  if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
    value = value[1:-1].replace("\\'", "'").replace("\\\\", "\\")
  return value

#
# The connection settings, given to psycopg2.connect() one by one.
#
# They used to be pasted into one DSN string, ending
#   ' password=' + PASSWORD + "sslmode='require'"
# with no space before sslmode.  That worked only because the password is
# written in quotes: an unquoted password became "s3cretsslmode='require'",
# so the login failed, or under trust or cert authentication sslmode was
# never set and the connection did not require SSL.
#
# The keepalives are for the LISTEN connection, which is idle most of the
# time.  Without them a connection which died quietly (a failover, a
# firewall dropping its state) looked the same as one with nothing to say,
# until the operating system's own keepalive, about two hours.  With these
# it is found within about two minutes, and fp-listen exits to be
# restarted.
#
DB = {
  'host':     ini_value(config['database']['HOST']),
  'dbname':   ini_value(config['database']['DBNAME']),
  'user':     ini_value(config['database']['DBUSER']),
  'password': ini_value(config['database']['PASSWORD']),
  'sslmode':  'require',
  'keepalives':          1,
  'keepalives_idle':     60,
  'keepalives_interval': 10,
  'keepalives_count':    6,
}

def remove_glob(filenameglob):
  """Remove every file and directory matching filenameglob.

  Returns True if nothing matching is left, False if anything could not be
  removed.  Each match is tried on its own: a file which has already gone is
  fine, and any other error is logged and the rest are still tried.

  The four handlers below used to wrap the whole loop in one try.  A file
  which vanished between glob() and the delete stopped the loop, so the
  rest of that port's cached pages stayed stale, and the queue row was then
  deleted as if all had been cleared.  Any other error (e.g. PermissionError
  on a directory another user created) kept the row, and in the two
  handlers which loop until their queue is empty, the same row was then
  read and tried again at once, for ever, with no sleep: fp-listen spun at
  full CPU and handled no other notification.  In CommitsCacheClear, which
  caught only FileNotFoundError, such an error ended fp-listen instead.
  """
  syslog.syslog(syslog.LOG_NOTICE, 'removing glob %s' % (filenameglob))
  all_removed = True
  for filename in glob.glob(filenameglob):
    syslog.syslog(syslog.LOG_NOTICE, 'removing %s' % (filename))
    try:
      if os.path.isdir(filename) and not os.path.islink(filename):
        shutil.rmtree(filename)
      else:
        os.remove(filename)
    except FileNotFoundError:
      syslog.syslog(syslog.LOG_NOTICE, 'already gone: %s' % (filename))
    except OSError as e:
      syslog.syslog(syslog.LOG_CRIT, 'ERROR: could not remove %s: %s' % (filename, e))
      all_removed = False
  return all_removed

def RemovePortsCacheEntry():
  something_cleared = False
  syslog.syslog(syslog.LOG_NOTICE, 'checking for file entries to remove...')
  dbh = psycopg2.connect(**DB)
  curs = dbh.cursor(cursor_factory=psycopg2.extras.DictCursor)

  # rows whose files could not all be removed: left in the queue for a
  # later notification, and not tried again in this call
  failed = set()

  # we loop until nothing is found to delete. See comment at end of loop.
  while 1:
    syslog.syslog(syslog.LOG_NOTICE, 'at the top of the loop looking for cache port entries to remove...')
    curs.execute("SELECT id, port_id, category, port FROM cache_clearing_ports ORDER BY id")
    rows = [row for row in curs.fetchall() if row['id'] not in failed]
    NumRows = len(rows)
    dbh.commit();
    if (NumRows > 0):
      something_cleared = True
      syslog.syslog(syslog.LOG_NOTICE, 'COUNT: %d entries to process' % (NumRows))
      for row in rows:
        # first the port's cache, then its category's
        port_removed     = remove_glob(config['dirs']['PORT_CACHE_PATH'] % (row['category'], row['port']))
        category_removed = remove_glob(config['dirs']['CATEGORY_CACHE_PATH'] % (row['category']))

        if not (port_removed and category_removed):
          # if we can't delete it, do not remove it from the queue
          failed.add(row['id'])
          continue

        syslog.syslog(syslog.LOG_NOTICE, "DELETE FROM cache_clearing_ports WHERE id = %d" % (row['id']))
        curs.execute("DELETE FROM cache_clearing_ports WHERE id = %s", (row['id'],))
        dbh.commit()
      # end for
      syslog.syslog(syslog.LOG_NOTICE, 'at the bottom of the loop looking for port entries to remove...')
    else:
      if failed:
        syslog.syslog(syslog.LOG_ERR, 'ERROR: %d cached port entries could not be removed; left in cache_clearing_ports' % len(failed))
      elif something_cleared:
        syslog.syslog(syslog.LOG_NOTICE, 'No cached port entries found for removal')
      else:
        # Normal, not an error: an earlier notification's run, or the
        # startup run, already cleared what this notification was for.
        # This used to be logged at LOG_ERR as "ERROR: No cached entries
        # found for removal", once for each such notification.
        syslog.syslog(syslog.LOG_NOTICE, 'nothing queued')
      #
      # we stay in this loop until there are no entries to remove
      # this avoids the race condition where more entries are added
      # while we are running. The NOTIFY issued then is missed/squashed/DUNNO.
      #
      break;
    # end if
    
  syslog.syslog(syslog.LOG_NOTICE, 'finished')
  return NumRows

def RemoveFilesCacheEntry():
  # this removes cached entries for MOVED, UPDATING, a Makefille, etc.
  # it was copied from RemovePortsCacheEntry and is not finished yet.
  # dvl- 2023-10-15
  #
  something_cleared = False
  syslog.syslog(syslog.LOG_NOTICE, 'checking for cache file entries to remove...')
  dbh = psycopg2.connect(**DB)
  curs = dbh.cursor(cursor_factory=psycopg2.extras.DictCursor)

  # rows whose files could not all be removed: left in the queue for a
  # later notification, and not tried again in this call
  failed = set()

  # we loop until nothing is found to delete. See comment at end of loop.
  while 1:
    syslog.syslog(syslog.LOG_NOTICE, 'at the top of the loop looking for cache file entries to remove...')
    curs.execute("SELECT id, pathname FROM cache_clearing_files ORDER BY id")
    rows = [row for row in curs.fetchall() if row['id'] not in failed]
    NumRows = len(rows)
    dbh.commit();
    if (NumRows > 0):
      something_cleared = True
      syslog.syslog(syslog.LOG_NOTICE, 'COUNT: %d entries to process' % (NumRows))
      for row in rows:
        #
        # we clear the file cache, which be in src, doc, or ports?
        # I think we show history only for /ports, so we can concentrate on that.
        # We also don't distinguish between head and quarterly for non-ports.
        # We can assume anything here is on head and not part of a category or port.
        # That leaves us with Mk, MOVED, UPDATING, etc.
        # so, strip off /ports/head and remove this from the ~freshports/cache/ports/ directory
        #

        # change /ports/head/MOVED
        pathname = row['pathname'].replace('/ports/head/', '/')

        syslog.syslog(syslog.LOG_NOTICE, 'we would be removing: %s' % pathname)
        if not remove_glob(config['dirs']['FILE_CACHE_PATH'] % (pathname)):
          # if we can't delete it, do not remove it from the queue
          failed.add(row['id'])
          continue

        syslog.syslog(syslog.LOG_NOTICE, "DELETE FROM cache_clearing_files WHERE id = %d" % (row['id']))
        curs.execute("DELETE FROM cache_clearing_files WHERE id = %s", (row['id'],))
        dbh.commit()
      # end for
      syslog.syslog(syslog.LOG_NOTICE, 'at the bottom of the loop looking for cache entries to remove...')
    else:
      if failed:
        syslog.syslog(syslog.LOG_ERR, 'ERROR: %d cached file entries could not be removed; left in cache_clearing_files' % len(failed))
      elif something_cleared:
        syslog.syslog(syslog.LOG_NOTICE, 'No cached entries found for removal')
      else:
        # normal, not an error: see RemovePortsCacheEntry()
        syslog.syslog(syslog.LOG_NOTICE, 'nothing queued')
      #
      # we stay in this loop until there are no entries to remove
      # this avoids the race condition where more entries are added
      # while we are running. The NOTIFY issued then is missed/squashed/DUNNO.
      #
      break;
    # end if
    
  syslog.syslog(syslog.LOG_NOTICE, 'finished')
  return NumRows

def PackagesCacheClear():
  # here, we clear out /var/db/freshports/cache/packages
  PKG_ZFS_SNAPSHOT = config['dirs']['PKG_ZFS_SNAPSHOT']
  syslog.syslog(syslog.LOG_NOTICE, "Time to rollback %s" % PKG_ZFS_SNAPSHOT);
  try:
    # this is the name of the zfs filesystem to rollback, including the snapshot name
    if os.system('zfs rollback ' + PKG_ZFS_SNAPSHOT):
      syslog.syslog(syslog.LOG_CRIT, 'ERROR: while in PackagesCacheClear(). zfs rollback failed.')
    else:
      syslog.syslog(syslog.LOG_NOTICE, 'zfs rollback succeeded.')

  except:
    syslog.syslog(syslog.LOG_CRIT, 'ERROR: while in PackagesCacheClear().  Error message is %s' % (sys.exc_info()[0]))

  syslog.syslog(syslog.LOG_NOTICE, "Done with PackagesCacheClear()");

def CommitsCacheClear():
  glob_templates = [ 'COMMIT_CACHE_PATH', 'SANITY_CACHE_PATH', 'SANITY_MAIN_CACHE_PATH' ]
  dbh = psycopg2.connect(**DB)
  curs = dbh.cursor(cursor_factory=psycopg2.extras.DictCursor)

  curs.execute("SELECT commit_to_clear FROM cache_clearing_commits")
  NumRows = curs.rowcount
  dbh.commit();
  if (NumRows > 0):
    syslog.syslog(syslog.LOG_NOTICE, 'COUNT: %d entries to process' % (NumRows))
    rows = curs.fetchall()
    for row in rows:
      all_removed = True
      for template in glob_templates:
        if (template == 'SANITY_MAIN_CACHE_PATH'):
          filenameglob = config['dirs'][template]
        else:
          filenameglob = config['dirs'][template] % (row['commit_to_clear'])

        if not remove_glob(filenameglob):
          all_removed = False
      # endfor templates

      if not all_removed:
        # if we can't delete it, do not remove it from the queue
        continue

      syslog.syslog(syslog.LOG_NOTICE, "DELETE FROM cache_clearing_commits WHERE commit_to_clear = '%s'" % (row['commit_to_clear']))
      curs.execute("DELETE FROM cache_clearing_commits WHERE commit_to_clear = %s", (row['commit_to_clear'],))
      dbh.commit()

    # end for rows
  else:
    # normal, not an error: see RemovePortsCacheEntry()
    syslog.syslog(syslog.LOG_NOTICE, 'nothing queued')
  # end if
    
  syslog.syslog(syslog.LOG_NOTICE, 'finished')
  return NumRows

def ClearDateCacheEntries():
  syslog.syslog(syslog.LOG_NOTICE, 'checking for cache date to remove...')
  dbh = psycopg2.connect(**DB)
  curs = dbh.cursor(cursor_factory=psycopg2.extras.DictCursor)

  curs.execute("SELECT id, date_to_clear FROM cache_clearing_dates ORDER BY id")
  NumRows = curs.rowcount
  dbh.commit();
  if (NumRows > 0):
    syslog.syslog(syslog.LOG_NOTICE, 'COUNT: %d entries to process' % (NumRows))
    rows = curs.fetchall()
    for row in rows:
      syslog.syslog(syslog.LOG_NOTICE, 'looking at %s' % (row['date_to_clear']))
      filenameglob = config['dirs']['DATE_CACHE_PATH'] % (row['date_to_clear'].strftime('%Y'), row['date_to_clear'].strftime('%m'), row['date_to_clear'].strftime('%d'))
      if not remove_glob(filenameglob):
        # if we can't delete it, do not remove it from the queue
        continue

      syslog.syslog(syslog.LOG_NOTICE, "DELETE FROM cache_clearing_dates WHERE id = %d" % (row['id']))
      curs.execute("DELETE FROM cache_clearing_dates WHERE id = %s", (row['id'],))
      dbh.commit()

    # end for
  else:
    # normal, not an error: see RemovePortsCacheEntry()
    syslog.syslog(syslog.LOG_NOTICE, 'nothing queued')
  # end if
    
  syslog.syslog(syslog.LOG_NOTICE, 'finished')
  return NumRows


def ProcessCategoryNew():
  syslog.syslog(syslog.LOG_NOTICE, 'We have a new category')
  # the website once did something with this. What, I'm not sure. It fetched the list of categories from a webpage and then processed them...


def ProcessPortsMoved():
  syslog.syslog(syslog.LOG_NOTICE, 'processing ports/MOVED')


def ProcessPortsUpdating():
  syslog.syslog(syslog.LOG_NOTICE, 'processing ports/UPDATING')


def ProcessVUXML():
  syslog.syslog(syslog.LOG_NOTICE, 'processing ports/security/portaudit/vuln.xml')

  
def ClearMiscCaches():
  syslog.syslog(syslog.LOG_NOTICE, 'invoked: ClearMiscCaches()');
  
  news_cache_dir = config['dirs']['NEWS_CACHE_DIR'];
  syslog.syslog(syslog.LOG_NOTICE, 'ClearMiscCaches() is clearing out entries in %s' % (news_cache_dir));

  #
  # Through remove_glob(), like the other caches.  This used to call
  # Path(news_cache_dir).iterdir() outside any try, so a missing news cache
  # directory (a new host, or someone clearing the cache with rm -rf) raised
  # FileNotFoundError after every port, file, date or commit notification.
  # A missing directory now matches nothing.
  #
  # The directory is the website's NEWS_DIRECTORY: the cached home page,
  # ports-new.php and the port list pages, and the RSS/Atom feeds, all of
  # which go stale with each commit.  '*' does not match dot files, but the
  # website names nothing there with a leading dot; feed.php's temporary
  # files (feed-tmp.*) are matched and removed, as iterdir() did too.
  #
  if not os.path.isdir(news_cache_dir):
    syslog.syslog(syslog.LOG_NOTICE, 'ClearMiscCaches(): %s does not exist; nothing to clear' % (news_cache_dir))
  remove_glob(os.path.join(news_cache_dir, '*'))

  syslog.syslog(syslog.LOG_NOTICE, 'finished: ClearMiscCaches()');
         


def run_safely(name, handler):
  #
  # An error in one handler is logged, and fp-listen carries on with the
  # next notification.  Nothing used to catch them: a failed connect, an
  # SQL error, or an OSError a handler did not expect ended the process.
  # The rc.d script restarts it (daemon -r), but the notification was lost
  # and, until the queues are drained at startup (below), so were the rows
  # queued while it was down.
  #
  try:
    handler()
  except Exception:
    syslog.syslog(syslog.LOG_ERR, 'ERROR: %s failed; carrying on:' % (name))
    for line in traceback.format_exc().splitlines():
      syslog.syslog(syslog.LOG_ERR, '  ' + line)


syslog.openlog(ident='fp-listen', facility=syslog.LOG_LOCAL3)

syslog.syslog(syslog.LOG_NOTICE, 'Starting up - this should not occur often')

conn = psycopg2.connect(**DB)
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

curs = conn.cursor()

curs.execute("SELECT name, script_name FROM listen_for ORDER BY id")
listens_for = curs.fetchall()

listens = dict()
syslog.syslog(syslog.LOG_NOTICE, "These are the (event name, script name) pairs we are ready for:")
for listen in listens_for:
  curs.execute("LISTEN %s" % listen[0])
  listens[listen[0]] = listen[1]
  syslog.syslog(syslog.LOG_NOTICE, "('%s', '%s')" % (listen[0], listen[1]))

#
# Now that we are listening, clear whatever was queued while fp-listen was
# not: during a restart, or before this start.  Those rows used to wait for
# the next notification on the same channel, and their pages were served
# stale meanwhile.  Anything queued from here on also sends a NOTIFY, which
# the loop below sees.
#
syslog.syslog(syslog.LOG_NOTICE, 'clearing anything queued while we were not listening')
run_safely('RemovePortsCacheEntry', RemovePortsCacheEntry)
run_safely('RemoveFilesCacheEntry', RemoveFilesCacheEntry)
run_safely('CommitsCacheClear',     CommitsCacheClear)
run_safely('ClearDateCacheEntries', ClearDateCacheEntries)
run_safely('ClearMiscCaches',       ClearMiscCaches)

IDLE_SECONDS = 300

while 1:
  #
  # Wait up to IDLE_SECONDS for a notification; select() returns as soon
  # as one arrives, so this does not slow anything down.  When none came,
  # log one line, so the log shows fp-listen is still alive.
  #
  # From 78bc05c (2017-01-26, the move to psycopg2) until November 2026,
  # nearly ten years, this waited 5 seconds and logged "timeout! *********"
  # each time: 17,280 lines a day.  Every 5 minutes is 288.  Before 2017 the
  # select() had no timeout at all, and logged nothing while idle.
  #
  if select.select([conn],[],[],IDLE_SECONDS)==([],[],[]):
    syslog.syslog(syslog.LOG_NOTICE, 'idle: no notification in the last %d seconds' % (IDLE_SECONDS))
  else:
    #
    # Losing the LISTEN connection (e.g. PostgreSQL restarted) is not
    # something to carry on from: exit, and let daemon -r start us again,
    # which LISTENs afresh and clears whatever was queued meanwhile.
    #
    try:
      conn.poll()
    except psycopg2.Error as e:
      syslog.syslog(syslog.LOG_CRIT, 'ERROR: lost the LISTEN connection (%s); exiting, to be restarted' % (str(e).strip()))
      sys.exit(1)
    syslog.syslog(syslog.LOG_NOTICE, 'Just woke up! *************')
    while conn.notifies:
      notify = conn.notifies.pop(0);
      # in real life, do something with each...
      syslog.syslog(syslog.LOG_NOTICE, "Got NOTIFY: pid='%d', channel='%s', payload='%s'" % (notify.pid, notify.channel, notify.payload));
      if notify.channel in listens:
      
        syslog.syslog(syslog.LOG_NOTICE, "found key %s" % (notify.channel));

        clear_cache = True;

        if  listens[notify.channel] == 'listen_port':
          syslog.syslog(syslog.LOG_NOTICE, "invoking RemovePortsCacheEntry()");
          run_safely('RemovePortsCacheEntry', RemovePortsCacheEntry)
        elif listens[notify.channel] == 'listen_file_updated':
          syslog.syslog(syslog.LOG_NOTICE, "invoking RemoveFilesCacheEntry()");
          run_safely('RemoveFilesCacheEntry', RemoveFilesCacheEntry)
        elif listens[notify.channel] == 'listen_ports_moved':
          syslog.syslog(syslog.LOG_NOTICE, "invoking ProcessPortsMoved()");
          run_safely('ProcessPortsMoved', ProcessPortsMoved)
        elif listens[notify.channel] == 'listen_ports_updating':
          syslog.syslog(syslog.LOG_NOTICE, "invoking ProcessPortsUpdating()");
          run_safely('ProcessPortsUpdating', ProcessPortsUpdating)
        elif listens[notify.channel] == 'listen_vuxml':
          syslog.syslog(syslog.LOG_NOTICE, "invoking ProcessVUXML()");
          run_safely('ProcessVUXML', ProcessVUXML)
        elif listens[notify.channel] == 'listen_category_new':
          syslog.syslog(syslog.LOG_NOTICE, "invoking ProcessCategoryNew()");
          run_safely('ProcessCategoryNew', ProcessCategoryNew)
        elif listens[notify.channel] == 'listen_date_updated':
          syslog.syslog(syslog.LOG_NOTICE, "invoking ClearDateCacheEntries()");
          run_safely('ClearDateCacheEntries', ClearDateCacheEntries)
        elif listens[notify.channel] == 'listen_packages_imported':
          syslog.syslog(syslog.LOG_NOTICE, "invoking PackagesCacheClear()");
          run_safely('PackagesCacheClear', PackagesCacheClear)
          # at the time of writing, there was no reason to ClearMiscCaches() when
          # new packages arrive
          clear_cache = False;
        elif listens[notify.channel] == 'listen_commit':
          syslog.syslog(syslog.LOG_NOTICE, "invoking CommitsClearCache()");
          run_safely('CommitsCacheClear', CommitsCacheClear)
        else:
          clear_cache = False;
          syslog.syslog(syslog.LOG_ERR, "Code does not know what to do when '%s' is found." % notify.channel)
          syslog.syslog(syslog.LOG_ERR, "listens[notify.channel='%s']" % listens[notify.channel])
          
        if clear_cache:
          syslog.syslog(syslog.LOG_NOTICE, "invoking ClearMiscCaches()");
          run_safely('ClearMiscCaches', ClearMiscCaches)

      else:
        syslog.syslog(syslog.LOG_NOTICE, 'no such key in listens array for %s!' % (notify.channel))

