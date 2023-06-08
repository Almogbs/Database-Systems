from typing import List
from typing import Tuple
import unittest
import Utility.DBConnector as Connector
from Utility.DBConnector import ResultSet
from psycopg2 import sql


# Note that the attribute names must be 'n1' and then 'n2'
#my_query = "--- Paste your query here ---"


my_query = "SELECT l1.StudentName AS n1, l2.StudentName AS n2   \
            FROM Learns L1, Learns L2                           \
            WHERE l1.StudentName != l2.StudentName \
            AND NOT EXISTS (SELECT * \
                            FROM Learns L3, Learns L4 \
                            WHERE l3.StudentName = l1.StudentName \
                            AND l4.StudentName = l2.StudentName \
                            AND l3.Semester = l4.Semester \
                            AND l3.CourseName = l4.CourseName \
                            AND l3.Grade >= l4.Grade) \
            AND NOT EXISTS (SELECT * \
                            FROM Learns L3\
                            WHERE l3.StudentName = l1.StudentName \
                            AND (l3.Semester, l3.CourseName) NOT IN (SELECT Semester, CourseName\
                                                                     FROM Learns \
                                                                     WHERE StudentName = l2.StudentName)) \
            AND NOT EXISTS (SELECT * \
                            FROM Learns L3\
                            WHERE l3.StudentName = l2.StudentName \
                            AND (l3.Semester, l3.CourseName) NOT IN (SELECT Semester, CourseName\
                                                                     FROM Learns \
                                                                     WHERE StudentName = l1.StudentName)) \
            GROUP BY l1.StudentName, l2.StudentName \
"


def createLearnsTable():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE Learns                                                       \
                     (StudentName   TEXT                                            NOT NULL,   \
                      CourseName    TEXT                                            NOT NULL,   \
                      Grade         INTEGER     CHECK(Grade >= 0 AND Grade <= 100)  NOT NULL,   \
                      Semester      TEXT                                            NOT NULL,   \
                     UNIQUE(StudentName, CourseName))")
        conn.commit()
    except Exception as e:
        print("createLearnsTable exception: " + str(e))
    conn.close()


def dropLearnsTable():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS Learns")
        conn.commit()
    except Exception as e:
        print("dropLearnsTable exception: " + str(e))
    conn.close()


def addGrade(s_name, c_name, grade, semester):
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            sql.SQL(
                "INSERT INTO Learns    \
                 VALUES ({s_name}, {c_name}, {grade}, {semester})"
            ).format(
                s_name = sql.Literal(s_name), 
                c_name = sql.Literal(c_name), 
                grade = sql.Literal(grade), 
                semester = sql.Literal(semester)
            )
        )
        conn.commit()
    except Exception as e:
        print("addGrade exception: " + str(e))
        return False
    else:
        return True
    finally:
        conn.close()


def getQueryResult() -> List[Tuple]:
    conn = None
    _, result = 0, ResultSet()
    ret_val = []
    try:
        conn = Connector.DBConnector()
        _, result = conn.execute(my_query)
        conn.commit()
    except Exception as e:
        print("getQueryResult exception: " + str(e))
        return ret_val
    finally:
        conn.close()

    for i in range(result.size()):
        pair = (result[i]['n1'], result[i]['n2'])
        ret_val.append(pair)
    return ret_val



class AbstractTest(unittest.TestCase):
    def setUp(self):
        createLearnsTable()

    def tearDown(self):
        dropLearnsTable()

