import MySQLdb
import MySQLdb.cursors
import random
import sys
from datetime import datetime
from time import sleep
from copy import copy
from collections import namedtuple
from global_vars import name_list, country_list, event_types

# Run like:
# python fake_app.py <mysql_pwd> 10

# Define record types
# ###########################
Member = namedtuple("Member", ["member_id", "name", "created_date", "modified_date", "country", "plan_id"])
Item = namedtuple("Item", ["item_id", "name", "price", "is_addon"])
Event = namedtuple("Event", ["event_id", "member_id", "event_date", "event_type", "event_val"])
Payment = namedtuple("Payment", ["payment_id", "item_id", "member_id", "payment_date", "units", "amount"])


# Create new records
# ###########################

# Create members
def new_member(name=None, country=None, plan_id=None):
  if not name:
    name = random.choice(name_list)
  if not country:
    country = random.choice(country_list)
  if not plan_id:
    plan_id = random.randint(1,4)
  return Member(None, name, None, None, country, plan_id)

def new_members(n):
  return [ new_member() for _ in range(n) ]

# Create events
def new_event(con):
  members = get_members(con)
  member = random.choice(members)
  member_id = member.member_id
  plan_id = member.plan_id
  event_type = random.choice(event_types.keys())

  # if event_type in ("page_load", "click"):
  #   event_val = random.choice(event_types[event_type])
  # elif event_type == "upgrade":
  #   if plan_id == 4:
  #     event_type = "downgrade"
  #     event_val = "4 to 3"
  #   else:
  #     event_val = "%s to %s" % (plan_id, plan_id + 1)
  # elif event_type == "downgrade":
  #   if plan_id == 1:
  #     event_type = "upgrade"
  #     event_val = "1 to 2"
  #   else:
  #     event_val = "%s to %s" % (plan_id, plan_id - 1)

  if event_type in ("page_load", "click"):
    event_val = random.choice(event_types[event_type])
  elif event_type in ("upgrade", "downgrade"):
    if event_type == "upgrade" and plan_id == 4:
      event_type = "downgrade"
    elif event_type == "downgrade" and plan_id == 1:
      event_type = "upgrade"
    new_plan_id = plan_id - 1 if event_type == "downgrade" else plan_id + 1
    event_val = "%s to %s" % (plan_id, new_plan_id)
    # Then actually update the member table with this change
    new_member = [ Member(member_id, member.name, None, None, member.country, new_plan_id) ]
    update_members(con, new_member)
  else:
    raise RuntimeError("Invalid event_type")

  return Event(None, member_id, None, event_type, event_val)

def new_events(con, n):
  return [ new_event(con) for _ in range(n) ]

# Create payments - note, will sometimes create multiple payments due to addon logic
def new_payment(con, item_ids=None, member_id=None, units=None):
  payments = []

  # Grab random member
  if not member_id:
    current_members = get_members(con)
    if not current_members:
      return []
    paying_members = filter(lambda x: x.plan_id > 1, current_members)
    if not paying_members:
      return []
    member_id = random.choice(paying_members).member_id

  # Grab random item(s)
  if not item_ids:
    assert not units, "If you're not specifying items, you can't specify units"
    current_items = get_items(con)
    base_items = filter(lambda x: x.is_addon == 0, current_items)
    addon_items = filter(lambda x: x.is_addon == 1, current_items)
    # Add random base item
    item = random.choice(base_items)
    payments.append([item.item_id, member_id, 1, item.price])
    # Sometimes add random addon
    if random.randrange(2) == 1:
      item = random.choice(addon_items)
      payments.append([item.item_id, member_id, random.randint(1,5), item.price])

  # Or, build payments of specified items
  else:
    assert type(item_ids) is list, "item_id must be a list of item_ids sold to this member"
    assert type(units) is list and len(units) == len(item_ids), "item_id must be a list of item_ids sold to this member"
    current_items = get_items(con)
    for n in range(len(item_ids)):
      current_item_id = item_ids[n]
      item = filter(lambda x: x.item_id == current_item_id)[0]
      payments.append([current_item_id, member_id, units[n], item.price])

  return [ Payment(None, p[0], p[1], datetime.now(), p[2], p[2]*p[3]) for p in payments ]

def new_payments(con, n):
  payments = []
  for x in range(n):
    ps = new_payment(con)
    for p in ps:
      payments.append(p)
  return payments


# Getting/caching code
# ###########################

_member_cache = []
def get_members(con, force_cache_refresh=0):
  global _member_cache
  if not _member_cache or force_cache_refresh == 1:
    cur = con.cursor()
    cur.execute("SELECT * FROM member")
    _member_cache = map(Member._make, cur.fetchall())
    cur.close()
  return copy(_member_cache)

_item_cache = []
def get_items(con, force_cache_refresh=0):
  global _item_cache
  if not _item_cache or force_cache_refresh == 1:
    cur = con.cursor()
    cur.execute("SELECT * FROM item")
    _item_cache = map(Item._make, cur.fetchall())
    cur.close()
  return copy(_item_cache)

_event_cache = []
def get_events(con, force_cache_refresh=0):
  global _event_cache
  if not _event_cache or force_cache_refresh == 1:
    cur = con.cursor()
    cur.execute("SELECT * FROM member_event")
    _event_cache = map(Event._make, cur.fetchall())
    cur.close()
  return copy(_event_cache)

_payment_cache = []
def get_payments(con, force_cache_refresh=0):
  global _payment_cache
  if not _payment_cache or force_cache_refresh == 1:
    cur = con.cursor()
    cur.execute("SELECT * FROM payment")
    _payment_cache = map(Payment._make, cur.fetchall())
    cur.close()
  return copy(_payment_cache)


# Insert/update code, including cache refreshes
# ###########################

def insert_members(con, named_tuples):
  insert_named_tuples(con, "member", named_tuples)
  get_members(con, 1)
  return True

def insert_items(con, named_tuples):
  insert_named_tuples(con, "item", named_tuples)
  get_items(con, 1)
  return True

def insert_events(con, named_tuples):
  insert_named_tuples(con, "member_event", named_tuples)
  get_events(con, 1)
  return True

def insert_payments(con, named_tuples):
  insert_named_tuples(con, "payment", named_tuples)
  get_payments(con, 1)
  return True

def update_members(con, named_tuples, fields_to_update=None):
  update_named_tuples(con, "member", named_tuples, "member_id", fields_to_update)
  get_members(con, 1)
  return True


# Reusable database IO code
# ###########################

def new_con(host, user, passwd, db):
  return MySQLdb.connect(host, user, passwd, db)

def unpack_named_tups(named_tuples):
  filtered = []

  # Get rid of all nulls
  for tup in named_tuples:
    unpacked = zip(tup._fields, list(tup))
    filtered.append( filter(lambda x: x[1] is not None, unpacked) )

  fields = map(lambda x: x[0], filtered[0])
  vals = [ map(lambda x: x[1], t) for t in filtered ]

  return fields, vals

def insert_named_tuples(con, table, named_tuples):
  fields, vals = unpack_named_tups(named_tuples)

  val_holder = ",".join([ "%s" for _ in range(len(fields)) ])
  field_str = ",".join(fields)
  query_str = "INSERT INTO %s (%s) VALUES (%s)" % (table, field_str, val_holder)

  cur = con.cursor()

  try:
    cur.executemany(query_str, vals)
    con.commit()
  except:
    con.rollback()
  finally:
    if cur:
      cur.close()

  return True

def update_named_tuples(con, table, named_tuples, primary_key, fields_to_update=None):
  fields, vals = unpack_named_tups(named_tuples)

  if fields_to_update:
    fields_to_update += [primary_key]
    up = map(lambda f: 1 if f in fields_to_update else 0, fields)
    fields = map(lambda f: f[1] , filter(lambda x: x[0] == 1, zip(up, fields)))
    vals = [ map(lambda f: f[1] , filter(lambda x: x[0] == 1, zip(up, rec))) for rec in vals ]

  primary_key_idx = fields.index(primary_key)
  set_str = ", ".join([ f + "=%s" for f in fields ])
  int_query_str = "UPDATE %s SET %s WHERE %s = %s" % (table, set_str, primary_key, "%s")

  for val in vals:
    primary_key_val = [ val[primary_key_idx] ]
    query_params = tuple( map(lambda x: "'"+x+"'" if type(x) is str else x, val+primary_key_val ) )
    query_str = int_query_str % query_params
    cur = con.cursor()

    try:
      cur.execute(query_str)
      con.commit()
    except:
      con.rollback()
    finally:
      if cur:
        cur.close()

  return True


# Put the pieces together
# ###########################

def main():
  mysql_pwd = sys.argv[1]

  # Set cycle time
  try:
    delay_seconds = int(sys.argv[2])
  except IndexError:
    delay_seconds = 120

  # Open db connection
  con = new_con("localhost", "root", mysql_pwd, "etl_demo")

  while True:
    # Generate members, insert them, wait
    members_to_add = random.randint(3,10)
    new_member_list = new_members(members_to_add)
    insert_members(con, new_member_list)
    num_mems = len(_member_cache)
    print("members added at %s" % datetime.now())
    sleep(delay_seconds)
    # Generate events, insert them, wait
    #   One bug - can't really tell the order that all of these account changes are taking place in
    #   Should I actually be logging when these events take place?  Spacing them out?
    # Also, the signup event isn't actually captured
    #   I should do this in the ETL!
    events_to_add = int(num_mems**0.5) * random.randint(1,3)
    new_event_list = new_events(con, events_to_add)
    insert_events(con, new_event_list)
    print("events added at %s" % datetime.now())
    sleep(delay_seconds)
    # Generate payments, insert them, wait
    #   Do I have to worry about the start?  What if there are no payments to insert, is that a problem?
    payments_to_add = int(num_mems**0.2) * random.randint(1,3)
    new_payment_list = new_payments(con, payments_to_add)
    insert_payments(con, new_payment_list)
    print("payments added at %s" % datetime.now())
    sleep(delay_seconds)

if __name__ == "__main__":
  main()
