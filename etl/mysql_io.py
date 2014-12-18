import MySQLdb
import MySQLdb.cursors
from datetime import datetime

def new_con(host, user, passwd, db):
  return MySQLdb.connect(host, user, passwd, db)

def load_infile(con, path, f, table, field_list, replace=False):
  replace_str = "replace" if replace else ""
  field_str = ",".join(field_list)
  sql_str = """
    load data low_priority local infile '%s/%s'
    %s
    into table %s
    fields
      terminated by ','
      optionally enclosed by '"'
      escaped by '\\'
    lines
      terminated by '\r\n'
    (%s)
    ;
    """ % (path, f, replace_str, table, field_str)
  cur = con.cursor()
  cur.execute(sql_str)
  con.commit()
  return True

def into_outfile(con, path, f, table):
  sql_str = """
    select *
    from %s
    into outfile '%s/%s'
    fields terminated by ','
    optionally enclosed by '"'
    escaped by '\\'
    lines terminated by '\r\n'
    ;
    """ % (table, path, f)
  cur = con.cursor()
  cur.execute(sql_str)
  con.commit()
  return True

# File naming
def underscorify(s):
  ns = str(s)
  for rs in ["-", " ", ":", "."]:
    ns = ns.replace(rs, "_")
  return ns

def outfile_name(s):
  return underscorify( "%s_%s.csv" % (s, underscorify(datetime.now())) )
