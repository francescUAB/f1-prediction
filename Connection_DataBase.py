import psycopg2
from psycopg2 import sql
import pandas as pd



#funcions per arreglar errors

def change_null(value):
    return None if value == r'\N' else value

def import_circuits():
    connection = psycopg2.connect(
        host="127.0.0.1",
        database="f1_data",
        user="postgres",
        password="francesc1998",
        port=5432
    )
    print(connection)

    file= pd.read_csv('archive/circuits.csv')

    cursor=connection.cursor()

    for index, row in file.iterrows():
        cursor.execute("""
        INSERT INTO circuits (circuitId, circuitRef, name, location, country, lat, lng, alt, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row['circuitId'], row['circuitRef'], row['name'], row['location'], row['country'], 
        row['lat'], row['lng'], row['alt'], row['url']))
    connection.commit()
    cursor.close()
    connection.close()

    print("Dades pujades correctament")

def import_constructors():
    file = pd.read_csv('archive/constructors.csv')
    connection = psycopg2.connect(
        host="127.0.0.1",
        database="f1_data",
        user="postgres",
        password="francesc1998",
        port=5432
    )
    cursor = connection.cursor()
    for index, row in file.iterrows():
        cursor.execute("""
        INSERT INTO constructors (constructorid, constructorref, name, nationality, url)
        VALUES (%s, %s, %s, %s, %s)
    """, (row['constructorId'], row['constructorRef'], row['name'], row['nationality'], row['url']))   
    connection.commit()
    cursor.close()
    connection.close()
    print("Dades pujades correctament")

def import_constructors_results():

        file = pd.read_csv('archive/constructor_results.csv')

        connection = psycopg2.connect(
            host="127.0.0.1",
            database="f1_data",
            user="postgres",
            password="francesc1998",
            port=5432
        )
        cursor = connection.cursor()

        for index, row in file.iterrows():
            cursor.execute("""
            INSERT INTO constructor_results (constructor_resultsid, raceid, constructorid, points, status)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            change_null(row['constructorResultsId']),
            change_null(row['raceId']),
            change_null(row['constructorId']),
            change_null(row['points']),
            change_null(row['status'])
        ))

        connection.commit()

        cursor.close()
        connection.close()

        print("Dades pujades correctament")

def import_races():
    file = pd.read_csv('archive/races.csv')

    connection = psycopg2.connect(
        host="127.0.0.1",
        database="f1_data",
        user="postgres",
        password="francesc1998",
        port=5432
    )
    cursor = connection.cursor()

    for index, row in file.iterrows():
        cursor.execute("""
        INSERT INTO races (raceid, year, round, circuitId, name, date, time, url, 
                           fp1_date, fp1_time, fp2_date, fp2_time, 
                           fp3_date, fp3_time, quali_date, quali_time, 
                           sprint_date, sprint_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (row['raceId'], row['year'], row['round'], row['circuitId'], row['name'], 
          change_null(row['date']), change_null(row['time']), row['url'],
          change_null(row['fp1_date']), change_null(row['fp1_time']), 
          change_null(row['fp2_date']), change_null(row['fp2_time']),
          change_null(row['fp3_date']), change_null(row['fp3_time']),
          change_null(row['quali_date']), change_null(row['quali_time']),
          change_null(row['sprint_date']), change_null(row['sprint_time'])))

    connection.commit()

    # Cerrar la conexión
    cursor.close()
    connection.close()

    print("Dades pujades correctament")

def import_drivers():
    file = pd.read_csv('archive/drivers.csv')

    connection = psycopg2.connect(
        host="127.0.0.1",
        database="f1_data",
        user="postgres",
        password="francesc1998",
        port=5432
    )
    cursor = connection.cursor()

    for index, row in file.iterrows():
        cursor.execute("""
        INSERT INTO drivers (driverid, driverref, number, code, forename, surname, dob, nationality, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (row['driverId'], row['driverRef'], 
          change_null(row['number']), change_null(row['code']), row['forename'], row['surname'], 
          change_null(row['dob']), row['nationality'], row['url']))
    connection.commit()
    cursor.close()
    connection.close()

    print("Dades pujades correctament")

def import_results():
    file = pd.read_csv('archive/results.csv')

    connection = psycopg2.connect(
        host="127.0.0.1",
        database="f1_data",
        user="postgres",
        password="francesc1998",
        port=5432
    )
    cursor = connection.cursor()

    for index, row in file.iterrows():
        cursor.execute("""
            INSERT INTO results (
                resultid, raceid, driverid, constructorid, number, grid, position, 
                positiontext, positionorder, points, laps, time, miliseconds, 
                fastestlap, rank, fastestlaptime, fastestlapspeed, statusid
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            change_null(row['resultId']),
            change_null(row['raceId']),
            change_null(row['driverId']),
            change_null(row['constructorId']),
            change_null(row['number']),
            change_null(row['grid']),
            change_null(row['position']),
            change_null(row['positionText']),
            change_null(row['positionOrder']),
            change_null(row['points']),
            change_null(row['laps']),
            change_null(row['time']),
            change_null(row['milliseconds']),
            change_null(row['fastestLap']),
            change_null(row['rank']),
            change_null(row['fastestLapTime']),
            change_null(row['fastestLapSpeed']),
            change_null(row['statusId'])
        ))
    connection.commit()
    cursor.close()
    connection.close()

    print("Dades pujades correctament")

def import_status():

    file = pd.read_csv('archive/status.csv')

    connection = psycopg2.connect(
        host="127.0.0.1",
        database="f1_data",
        user="postgres",
        password="francesc1998",
        port=5432
    )
    cursor = connection.cursor()

    for index, row in file.iterrows():
        cursor.execute( """
            INSERT INTO status (statusid, status)
            VALUES (%s, %s)
        """, (change_null(row['statusId']), change_null(row['status'])))
    connection.commit()
    cursor.close()
    connection.close()

    print("Dades pujades correctament")
   
def import_lap_times():
    file = pd.read_csv('archive/lap_times.csv')

    connection = psycopg2.connect(
        host="127.0.0.1",
        database="f1_data",
        user="postgres",
        password="francesc1998",
        port=5432
    )
    cursor = connection.cursor()

    for index, row in file.iterrows():
        cursor.execute( """
            INSERT INTO laptimes (raceid, driverid, lap, position, time, miliseconds)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            change_null(row['raceId']), 
            change_null(row['driverId']),
            change_null(row['lap']), 
            change_null(row['position']), 
            change_null(row['time']),
            change_null(row['milliseconds'])
        )
        )
    connection.commit()
    cursor.close()
    connection.close()

    print("Dades pujades correctament")

def import_pit_stops():
    file = pd.read_csv('archive/pit_stops.csv')

    connection = psycopg2.connect(
        host="127.0.0.1",
        database="f1_data",
        user="postgres",
        password="francesc1998",
        port=5432
    )
    cursor = connection.cursor()

    for index, row in file.iterrows():
        cursor.execute(  """
            INSERT INTO pitstops (raceid, driverid, stop, lap, time, duration, miliseconds)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            change_null(row['raceId']),
            change_null(row['driverId']),
            change_null(row['stop']),
            change_null(row['lap']),
            change_null(row['time']),
            change_null(row['duration']),
            change_null(row['milliseconds'])
        )
        )
    connection.commit()
    cursor.close()
    connection.close()

    print("Dades pujades correctament")

def import_qualifying():
    file = pd.read_csv('archive/qualifying.csv')

    connection = psycopg2.connect(
        host="127.0.0.1",
        database="f1_data",
        user="postgres",
        password="francesc1998",
        port=5432
    )
    cursor = connection.cursor()

    for index, row in file.iterrows():
        cursor.execute( """
            INSERT INTO qualifying (qualifyId, raceId, driverId, constructorId, number, position, q1, q2, q3)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            change_null(row['qualifyId']),
            change_null(row['raceId']),
            change_null(row['driverId']),
            change_null(row['constructorId']),
            change_null(row['number']),
            change_null(row['position']),
            change_null(row['q1']),
            change_null(row['q2']),
            change_null(row['q3'])
        )
        )
    connection.commit()
    cursor.close()
    connection.close()

    print("Dades pujades correctament")

def import_seasons():
    file = pd.read_csv('archive/seasons.csv')

    connection = psycopg2.connect(
        host="127.0.0.1",
        database="f1_data",
        user="postgres",
        password="francesc1998",
        port=5432
    )
    cursor = connection.cursor()

    for index, row in file.iterrows():
        cursor.execute( """
            INSERT INTO seasons (year, url)
            VALUES (%s, %s)
        """, (
            change_null(row['year']),
            change_null(row['url'])
        )
        )
    connection.commit()
    cursor.close()
    connection.close()

    print("Dades pujades correctament")

def import_constructor_standings():
    file = pd.read_csv('archive/constructor_standings.csv')

    connection = psycopg2.connect(
        host="127.0.0.1",
        database="f1_data",
        user="postgres",
        password="francesc1998",
        port=5432
    )
    cursor = connection.cursor()

    for index, row in file.iterrows():
        cursor.execute("""
            INSERT INTO constructor_standings (constructorstandingsid, raceid, constructorid, points, position, positiontext, wins)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
            change_null(row['constructorStandingsId']),
            change_null(row['raceId']),
            change_null(row['constructorId']),
            change_null(row['points']),
            change_null(row['position']),
            change_null(row['positionText']),
            change_null(row['wins'])
        )
        )
    connection.commit()
    cursor.close()
    connection.close()

    print("Dades pujades correctament")

def import_sprint_results():
    file = pd.read_csv('archive/sprint_results.csv')

    connection = psycopg2.connect(
        host="127.0.0.1",
        database="f1_data",
        user="postgres",
        password="francesc1998",
        port=5432
    )
    cursor = connection.cursor()

    for index, row in file.iterrows():
        cursor.execute("""
            INSERT INTO sprint_results (resultid, raceid, driverid, constructorid, number, grid, position, positiontext, positionorder, points, laps, time, miliseconds, fastestlap, fastestlaptime,statusid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
            change_null(row['resultId']),
            change_null(row['raceId']),
            change_null(row['driverId']),
            change_null(row['constructorId']),
            change_null(row['number']),
            change_null(row['grid']),
            change_null(row['position']),
            change_null(row['positionText']),
            change_null(row['positionOrder']),
            change_null(row['points']),
            change_null(row['laps']),
            change_null(row['time']),
            change_null(row['milliseconds']),
            change_null(row['fastestLap']),
            change_null(row['fastestLapTime']),
            change_null(row['statusId'])
        )
        )
    connection.commit()
    cursor.close()
    connection.close()

    print("Dades pujades correctament")
  
def import_functions():
    import_circuits() 
    import_constructors() 
    import_races() 
    import_drivers() 
    import_constructors_results() 
    import_status() 
    import_results() 
    import_lap_times() 
    import_pit_stops() 
    import_qualifying() 
    import_seasons() 
    import_constructor_standings() 
    import_sprint_results()

def prediction_f1_data():
    connection = psycopg2.connect(
    host="127.0.0.1",
    database="f1_data",
    user="postgres",
    password="francesc1998",
    port=5432
    )
    query ="""

    SELECT
    rac.raceId,
    rac.year,
    rac.circuitId,
    rac.round,
    rac.date,
    dri.driverId,
    dri.forename,
    dri.driverref,
    dri.nationality,
    dri.dob,
    con.constructorId,
    con.name,
    res.position,
    res.grid,
    res.fastestLapTime,
    cir.lat,
    cir.lng,
    cir.alt,
    cir.country
    
    FROM results res
    JOIN drivers dri ON res.driverId = dri.driverId
    JOIN constructors con ON res.constructorId = con.constructorId
    JOIN races rac ON res.raceId=rac.raceId
    LEFT JOIN circuits cir ON rac.circuitId = cir.circuitId
    """

    df = pd.read_sql(query, connection)

    df['date'] = pd.to_datetime(df['date'])
    df['dob'] = pd.to_datetime(df['dob'])
    df['race_driver_years'] = df['year'] - df['dob'].dt.year
    df['positions_gained_race'] = df['grid'] - df['position']
    df['target'] = df['position']
    df.to_csv("prediccion_f1_data.csv", sep=';', encoding='utf-8', index=False)


prediction_f1_data()


