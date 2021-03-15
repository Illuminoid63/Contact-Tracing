import sqlite3
from sqlite3 import Error

from flask import Flask
from flask import abort
from flask import redirect
from flask import request

app = Flask(__name__) 


@app.route("/people/new", methods = ["POST"])
def addPeople():
    newPerson = {}
    conn = None
    try:
        jsonPostData = request.get_json()
        firstName = jsonPostData["firstName"]
        lastName = jsonPostData["lastName"]
        DOB = jsonPostData["DOB"]
        phoneNum = jsonPostData["phoneNum"]

        conn = sqlite3.connect("./contactTracing.db")
        conn.row_factory = sqlite3.Row
        sql = """ 
            INSERT INTO People (firstName, lastName, DOB, phoneNum) VALUES (?, ?, ?, ?)  
        """
        cursor = conn.cursor()
        cursor.execute(sql, (firstName, lastName, DOB, phoneNum))
        conn.commit()

        sql = """
            SELECT People.ID, People.firstName, People.lastName, People.DOB, People.phoneNum
            FROM People
            WHERE People.ID = ?
        """
        cursor.execute(sql, (cursor.lastrowid,))
        row = cursor.fetchone()
        newPerson["ID"] = row["ID"]
        newPerson["firstName"] = row["firstName"]
        newPerson["lastName"] = row["lastName"]
        newPerson["DOB"] = row["DOB"]
        newPerson["phoneNum"] = row["phoneNum"]
        
        
    except Error as e:
        print(f"Error opening the database {e}")
        abort(500)
    finally:
        if conn:
            conn.close()
    return newPerson

@app.route("/diseases/new", methods = ["POST"])
def addDisease():
    newDisease = {}
    conn = None
    try:
        jsonPostData = request.get_json()
        name1 = jsonPostData["name"]

        conn = sqlite3.connect("./contactTracing.db")
        conn.row_factory = sqlite3.Row
        sql = """ 
            INSERT INTO Diseases (name) VALUES (?)  
        """
        cursor = conn.cursor()
        cursor.execute(sql, (name1,))
        conn.commit()

        sql = """
            SELECT Diseases.ID, Diseases.name
            FROM Diseases
            WHERE Diseases.ID = ?
        """
        cursor.execute(sql, (cursor.lastrowid,))
        row = cursor.fetchone()
        newDisease["ID"] = row["ID"]
        newDisease["name"] = row["name"]
        
        
    except Error as e:
        print(f"Error opening the database {e}")
        abort(500)
    finally:
        if conn:
            conn.close()
    return newDisease

@app.route("/symptoms/new", methods = ["POST"])
def addSymptom():
    newSymptom = {}
    conn = None
    try:
        jsonPostData = request.get_json()
        description = jsonPostData["description"]

        conn = sqlite3.connect("./contactTracing.db")
        conn.row_factory = sqlite3.Row
        sql = """ 
            INSERT INTO Symptoms (description) VALUES (?)  
        """
        cursor = conn.cursor()
        cursor.execute(sql, (description,))
        conn.commit()

        sql = """
            SELECT Symptoms.ID, Symptoms.description
            FROM Symptoms
            WHERE Symptoms.ID = ?
        """
        cursor.execute(sql, (cursor.lastrowid,))
        row = cursor.fetchone()
        newSymptom["ID"] = row["ID"]
        newSymptom["description"] = row["description"]
        
        
    except Error as e:
        print(f"Error opening the database {e}")
        abort(500)
    finally:
        if conn:
            conn.close()
    return newSymptom

@app.route("/interaction/report", methods = ["POST"])
def addInteraction():
    newInteraction = {}
    conn = None
    try:
        jsonPostData = request.get_json()
        timestamp = jsonPostData["timestamp"]
        initiatorID = jsonPostData["initiatorID"]
        exposedID = jsonPostData["exposedID"]

        conn = sqlite3.connect("./contactTracing.db")
        conn.row_factory = sqlite3.Row
        sql = """ 
            INSERT INTO comesInContactWith (timestamp, initiatorID, exposedID) VALUES (?, ?, ?)  
        """
        cursor = conn.cursor()
        cursor.execute(sql, (timestamp, initiatorID, exposedID))
        conn.commit()

        sql = """
            SELECT comesInContactWith.ID, comesInContactWith.timestamp, comesInContactWith.initiatorID, comesInContactWith.exposedID
            FROM comesInContactWith
            WHERE comesInContactWith.ID = ?
        """
        cursor.execute(sql, (cursor.lastrowid,))
        row = cursor.fetchone()
        newInteraction["ID"] = row["ID"]
        newInteraction["timestamp"] = row["timestamp"]
        newInteraction["initiatorID"] = row["initiatorID"]
        newInteraction["exposedID"] = row["exposedID"]
        
    except Error as e:
        print(f"Error opening the database {e}")
        abort(500)
    finally:
        if conn:
            conn.close()
    return newInteraction

@app.route("/positive/test/new", methods = ["POST"])
def addPositiveTest():
    newPositiveTest = {}
    conn = None
    try:
        jsonPostData = request.get_json()
        peopleID = jsonPostData["peopleID"]
        diseaseID = jsonPostData["diseaseID"]
        timestamp = jsonPostData["timestamp"]

        conn = sqlite3.connect("./contactTracing.db")
        conn.row_factory = sqlite3.Row
        sql = """ 
            INSERT INTO peopleHaveDisease (peopleID, diseaseID, timestamp) VALUES (?, ?, ?)  
        """
        cursor = conn.cursor()
        cursor.execute(sql, (peopleID, diseaseID, timestamp))
        conn.commit()

        sql = """
            SELECT peopleHaveDisease.peopleID, peopleHaveDisease.diseaseID, peopleHaveDisease.timestamp
            FROM peopleHaveDisease
            WHERE peopleHaveDisease.peopleID = ?
            AND peopleHaveDisease.diseaseID = ?
        """
        cursor.execute(sql, (peopleID, diseaseID))
        row = cursor.fetchone()
        newPositiveTest["peopleID"] = row["peopleID"]
        newPositiveTest["diseaseID"] = row["diseaseID"]
        newPositiveTest["timestamp"] = row["timestamp"]
        
    except Error as e:
        print(f"Error opening the database {e}")
        abort(500)
    finally:
        if conn:
            conn.close()
    return newPositiveTest

@app.route("/symptom/report", methods = ["POST"])
def addSymptomReport():
    newSymptomReport = {}
    conn = None
    try:
        jsonPostData = request.get_json()
        peopleID = jsonPostData["peopleID"]
        symptomID = jsonPostData["symptomID"]
        timestamp = jsonPostData["timestamp"]

        conn = sqlite3.connect("./contactTracing.db")
        conn.row_factory = sqlite3.Row
        sql = """ 
            INSERT INTO peopleHaveSymptoms (peopleID, symptomID, timestamp) VALUES (?, ?, ?)  
        """
        cursor = conn.cursor()
        cursor.execute(sql, (peopleID, symptomID, timestamp))
        conn.commit()

        sql = """
            SELECT peopleHaveSymptoms.peopleID, peopleHaveSymptoms.symptomID, peopleHaveSymptoms.timestamp
            FROM peopleHaveSymptoms
            WHERE peopleHaveSymptoms.peopleID = ?
            AND peopleHaveSymptoms.symptomID = ?
        """
        cursor.execute(sql, (peopleID, symptomID))
        row = cursor.fetchone()
        newSymptomReport["peopleID"] = row["peopleID"]
        newSymptomReport["symptomID"] = row["symptomID"]
        newSymptomReport["timestamp"] = row["timestamp"]
        
    except Error as e:
        print(f"Error opening the database {e}")
        abort(500)
    finally:
        if conn:
            conn.close()
    return newSymptomReport

@app.route("/positiveTest/<diseaseid>")
def positiveTestSearch(diseaseid):
    positivePeople = []
    conn = None
    try:
        conn = sqlite3.connect("./contactTracing.db")
        conn.row_factory = sqlite3.Row
        sql = """
            SELECT People.firstName, People.lastName, People.ID
            FROM People, peopleHaveDisease
            WHERE peopleHaveDisease.peopleID = People.ID
            AND peopleHaveDisease.diseaseID = ?
        """
        cursor = conn.cursor()
        cursor.execute(sql, (diseaseid,))
        rows = cursor.fetchall()
        if(len(rows) == 0):
            abort(404)
        else:
            for row in rows:
                    positivePerson = {"firstName": row["firstName"], "lastName": row["lastName"], "ID": row["ID"]}
                    sql = """
                        SELECT Symptoms.description
                        FROM peopleHaveSymptoms, People, Symptoms
                        WHERE peopleHaveSymptoms.peopleID = People.ID
                        AND peopleHaveSymptoms.symptomID = Symptoms.ID
                        AND People.ID = ?
                    """
                    cursor.execute(sql, (row["ID"],))
                    rows2 = cursor.fetchall()
                    symptoms = []
                    for row2 in rows2:
                        symptom = {"Symptom Description": row2["description"]}
                        symptoms.append(symptom)
                    if not symptoms:
                        symptoms.append("No Symptoms")
                    search = {"Positive Person": positivePerson, "Their symptoms": symptoms}
                    positivePeople.append(search)
        
    except Error as e:
        print(f"Error opening the database {e}")
        abort(500)
    finally:
        if conn:
            conn.close()
    return {"Search Results": positivePeople}

@app.route("/interactions/<initId>/start/<startDate>/end/<endDate>")
def searchInteractions(initId, startDate, endDate):
    interactions = []
    conn = None
    try:
        conn = sqlite3.connect("./contactTracing.db")
        conn.row_factory = sqlite3.Row

        sql = """
            SELECT DISTINCT People.ID, People.firstName, People.lastName
            FROM People, comesInContactWith
            WHERE People.ID = comesInContactWith.exposedID
            AND comesInContactWith.initiatorID = ?
            AND date(comesInContactWith.timestamp) >= date(?)
            AND date(comesInContactWith.timestamp) <= date(?)
        """
        cursor = conn.cursor()
        cursor.execute(sql, (initId, startDate, endDate))
        rows = cursor.fetchall()
        if(len(rows) == 0):
            abort(404)
        else:
            for row in rows:
                interaction = {"ID": row["ID"], "Exposed First Name": row["firstName"], "Exposed Last Name": row["lastName"]}
                interactions.append(interaction)

    except Error as e:
        print(f"Error opening the database {e}")
        abort(500)
    finally:
        if conn:
            conn.close()
    return {"Search Results": interactions}

