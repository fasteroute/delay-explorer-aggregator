def get_daycol_name(d):
    n = d.weekday()
    if n == 0:
        return "mon"
    if n == 1:
        return "tue"
    if n == 2:
        return "wed"
    if n == 3:
        return "thu"
    if n == 4:
        return "fri"
    if n == 5:
        return "sat"
    if n == 6:
        return "sun"
    raise Exception("Invalid day of week {}".format(n))

def get_wcol_name(d):
    n = d.weekday()
    if n < 5:
        return "weekday"
    return "weekend"

from locations import LocationMapper
lm = LocationMapper("locations.json")

# Table definitions.
CREATE_PRO_SCHEDULE = "CREATE TABLE IF NOT EXISTS pro_schedule ("+\
        "id BIGSERIAL PRIMARY KEY, "+\
        "uid VARCHAR NOT NULL, "+\
        "mon DATE, "+\
        "tue DATE, "+\
        "wed DATE, "+\
        "thu DATE, "+\
        "fri DATE, "+\
        "sat DATE, "+\
        "sun DATE, "+\
        "weekday DATE, "+\
        "weekend DATE"+\
        ")"

CREATE_PRO_SCHEDULE_LOCATION = "CREATE TABLE IF NOT EXISTS pro_schedule_location ("+\
        "id BIGSERIAL PRIMARY KEY, "+\
        "pro_schedule_id INT NOT NULL, "+\
        "crs CHAR(3) NOT NULL, "+\
        "scheduled_arrival_time TIME, "+\
        "scheduled_departure_time TIME, "+\
        "type CHAR(2) NOT NULL, "+\
        "position INT NOT NULL, "+\
        "date_last_seen DATE NOT NULL"+\
        ")"

CREATE_PRO_LATENESS = "CREATE TABLE IF NOT EXISTS pro_lateness ("+\
        "id BIGSERIAL PRIMARY KEY, "+\
        "pro_schedule_id INT NOT NULL, "+\
        "pro_schedule_location_id INT NOT NULL, "+\
        "date DATE, "+\
        "lateness_arriving INTERVAL, "+\
        "lateness_departing INTERVAL, "+\
        "cancelled BOOLEAN "\
        ")"

from connection import Connection

import datetime
import os

rconnection = Connection(host=os.environ["POSTGRES_HOST"],
                         dbname=os.environ["POSTGRES_DB"],
                         user=os.environ["POSTGRES_USER"],
                         password=os.environ["POSTGRES_PASS"])

rconnection.connect()

rcursor = rconnection.cursor("read_cursor")

rcursor2 = rconnection.cursor()

wconnection = Connection(host=os.environ["POSTGRES_HOST"],
                         dbname=os.environ["POSTGRES_DB"],
                         user=os.environ["POSTGRES_USER"],
                         password=os.environ["POSTGRES_PASS"])

wconnection.connect()

wcursor = wconnection.cursor()

# Create the tables if they don't exist yet.
wcursor.execute(CREATE_PRO_SCHEDULE)
wcursor.execute(CREATE_PRO_SCHEDULE_LOCATION)
wcursor.execute(CREATE_PRO_LATENESS)

wconnection.commit()

# Set the date for which we are processing the data.
DATE = datetime.date(2015, 9, 27)
TDZERO = datetime.timedelta(0, 0)

# Get all the schedules from the database.
rcursor.execute("SELECT rid, uid from schedule where start_date=%s and passenger_service=true and deleted=false and status not in ('B', '5')", (DATE,))

counter = 0
while True:
    row = rcursor.fetchone()
    if row is None:
        break

    counter += 1

    # Try and update the relevant schedule record in the pro_schedule table.
    q = "UPDATE pro_schedule SET {}=%s, {}=%s where uid=%s RETURNING id".format(
            get_daycol_name(DATE),
            get_wcol_name(DATE))
    wcursor.execute(q, (DATE, DATE, row[1]))

    # Check if the update worked.
    insert = False
    pid = None
    if wcursor.rowcount == 1:
        print("{:8d} Updated row".format(counter))
        pid = wcursor.fetchall()[0][0]
    elif wcursor.rowcount > 1:
        print("MULTIPLE ROWS ARGH")
        continue
    else:
        print("{:8d} Did not find matching train UID. Need to do fresh insert.".format(counter))
        q = "INSERT INTO pro_schedule (uid, {}, {}) VALUES(%s, %s, %s) RETURNING ID".format(
                get_daycol_name(DATE),
                get_wcol_name(DATE))
        wcursor.execute(q, (row[1], DATE, DATE))
        insert = True
        pid = wcursor.fetchall()[0][0]

    # Get the locations for this schedule.
    rcursor2.execute("SELECT tiploc, raw_public_arrival_time, raw_public_departure_time, type, position, working_arrival_time, working_departure_time, forecast_arrival_actual_time, forecast_departure_actual_time, cancelled from schedule_location where rid=%s and type IN ('OR', 'IP', 'DT') ORDER BY position ASC", (row[0],))

    for r in rcursor2.fetchall():
        pro_schedule_location_id = None
        if insert is True:
            wcursor.execute("INSERT into pro_schedule_location (pro_schedule_id, crs, scheduled_arrival_time, scheduled_departure_time, type, position, date_last_seen) VALUES(%s, %s, %s, %s, %s, %s, %s) returning id", (pid, lm.get_crs(r[0]), r[1], r[2], r[3], r[4], DATE))
            pro_schedule_location_id = wcursor.fetchall()[0][0]
        else:
            wcursor.execute("UPDATE pro_schedule_location SET date_last_seen=%s WHERE pro_schedule_id=%s and crs=%s and scheduled_arrival_time is not distinct from %s and scheduled_departure_time is not distinct from %s returning id", (DATE, pid, lm.get_crs(r[0]), None if r[1] is None else r[1], None if r[2] is None else r[2]))
            if wcursor.rowcount == 1:
                pro_schedule_location_id = wcursor.fetchall()[0][0]

        # If this schedule location has a record in the db, we should also insert the relevant statistics for it.
        if pro_schedule_location_id is not None:
            if r[7] is None:
                lateness_arriving = None
            else:
                lateness_arriving = r[7] - r[5]
                if lateness_arriving < TDZERO:
                    lateness_arriving = TDZERO

            if r[8] is None:
                lateness_departing = None
            else:
                lateness_departing = r[8] - r[6]
                if lateness_departing < TDZERO:
                    lateness_departing = TDZERO

            wcursor.execute("UPDATE pro_lateness SET lateness_arriving=%s, lateness_departing=%s, cancelled=%s where pro_schedule_id=%s and pro_schedule_location_id=%s and date=%s", (
                lateness_arriving,
                lateness_departing,
                r[9],
                pid,
                pro_schedule_location_id,
                DATE
            ))
            if wcursor.rowcount == 0:
                wcursor.execute("INSERT into pro_lateness (pro_schedule_id, pro_schedule_location_id, date, lateness_arriving, lateness_departing, cancelled) VALUES(%s, %s, %s, %s, %s, %s)", (
                    pid,
                    pro_schedule_location_id,
                    DATE,
                    lateness_arriving,
                    lateness_departing,
                    r[9],
                ))

    wconnection.commit()


