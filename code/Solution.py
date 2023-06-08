from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql

def getException(exception: Exception) -> ReturnValue:
    result = ReturnValue.OK
    try:
        raise exception
    except DatabaseException.ConnectionInvalid as exception:
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as exception:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as exception:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as exception:
        result = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as exception:
        result = ReturnValue.NOT_EXISTS #TODO: check
    except Exception as exception:
        result = ReturnValue.ERROR
    finally:
        return result

def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE Photos(                  \
                        photo_id INTEGER PRIMARY KEY,       \
                        description TEXT NOT NULL,          \
                        size INTEGER NOT NULL,              \
                        CHECK (photo_id > 0),               \
                        CHECK (size >= 0)                   \
                     );                                     \
                                                            \
                     CREATE TABLE Disks(                    \
                        disk_id INTEGER PRIMARY KEY,        \
                        company TEXT NOT NULL,              \
                        speed INTEGER NOT NULL,             \
                        free_space INTEGER NOT NULL,        \
                        cost INTEGER NOT NULL,              \
                        CHECK (disk_id > 0),                \
                        CHECK (speed > 0),                  \
                        CHECK (cost > 0),                   \
                        CHECK (free_space >= 0)             \
                     );                                     \
                                                            \
                     CREATE TABLE Rams(                     \
                        ram_id INTEGER PRIMARY KEY,         \
                        company TEXT NOT NULL,              \
                        size INTEGER NOT NULL,              \
                        CHECK (ram_id > 0),                 \
                        CHECK (size > 0)                   \
                     );                                     \
                                                            \
                     CREATE TABLE PhotosInDisks(            \
                        disk_id INTEGER NOT NULL,           \
                        photo_id INTEGER NOT NULL,          \
                        photo_size INTEGER NOT NULL,        \
                        FOREIGN KEY (disk_id) REFERENCES Disks(disk_id) ON DELETE CASCADE,   \
                        FOREIGN KEY (photo_id) REFERENCES Photos(photo_id) ON DELETE CASCADE,\
                        PRIMARY KEY(disk_id,photo_id)\
                     );                                     \
                                                            \
                     CREATE TABLE RamsInDisks(              \
                        disk_id INTEGER NOT NULL,           \
                        ram_id INTEGER NOT NULL,            \
                        FOREIGN KEY (disk_id) REFERENCES Disks(disk_id) ON DELETE CASCADE,   \
                        FOREIGN KEY (ram_id) REFERENCES Rams(ram_id) ON DELETE CASCADE,   \
                        PRIMARY KEY(disk_id,ram_id)\
                     );"
        )
        conn.commit()
    except Exception as e:
        getException(e)
    finally:
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DELETE FROM Photos CASCADE;\
                      DELETE FROM Disks CASCADE; \
                      DELETE FROM Rams CASCADE;  \
                      DELETE FROM PhotosInDisks CASCADE; \
                      DELETE FROM RamsInDisks CASCADE;"
        )
        conn.commit()
    except Exception as e:
        getException(e)
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS Photos CASCADE; \
                      DROP TABLE IF EXISTS Rams CASCADE; \
                      DROP TABLE IF EXISTS Disks CASCADE;\
                      DROP TABLE IF EXISTS RamsInDisks CASCADE;\
                      DROP TABLE IF EXISTS PhotosInDisks CASCADE")
        conn.commit()
    except Exception as e:
        getException(e)
    finally:
        conn.close()


def addPhoto(photo: Photo) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Photos(photo_id, description, size)  \
                         VALUES({id}, {description}, {size})").format(
                            id=sql.Literal(photo.getPhotoID()),
                            description=sql.Literal(photo.getDescription()),
                            size=sql.Literal(photo.getSize()))
        conn.execute(query)
        conn.commit()
    except Exception as e:
        result = getException(e)
    finally:
        conn.close()
        return result


def getPhotoByID(photoID: int) -> Photo:
    conn = None
    result = ReturnValue.OK
    data = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT photo_id, description, size FROM Photos WHERE photo_id={id}").format(
                            id=sql.Literal(photoID))
        rows_effected, data = conn.execute(query)
        conn.commit()
    except Exception as e:
        result = getException(e)
    finally:
        conn.close()
        if result != ReturnValue.OK or data is None:
            return Photo.badPhoto()

        return Photo(*data[0].values())


def deletePhoto(photo: Photo) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("UPDATE Disks SET free_space = free_space + {photo_size} WHERE    \
                        disk_id IN (SELECT disk_id FROM PhotosInDisks \
                                    WHERE photo_id = {photo_id});               \
                        DELETE FROM Photos WHERE photo_id={photo_id}").format(
                            photo_id=sql.Literal(photo.getPhotoID()),
                            photo_size=sql.Literal(photo.getSize())
        )
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            result = ReturnValue.OK

    except Exception as e:
        result = getException(e)
    finally:
        conn.close()       
        return result

def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Disks(disk_id, company, speed, free_space, cost)  \
                         VALUES({disk_id}, {company}, {speed}, {free_space}, {cost})").format(
                            disk_id=sql.Literal(disk.getDiskID()),
                            company=sql.Literal(disk.getCompany()),
                            speed=sql.Literal(disk.getSpeed()),
                            free_space=sql.Literal(disk.getFreeSpace()),
                            cost=sql.Literal(disk.getCost()))
        conn.execute(query)
        conn.commit()
    except Exception as e:
        result = getException(e)
    finally:
        conn.close()
        return result


def getDiskByID(diskID: int) -> Disk:
    conn = None
    result = ReturnValue.OK
    data = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT disk_id, company, speed, free_space, cost FROM Disks WHERE disk_id={id}").format(
                            id=sql.Literal(diskID))
        rows_effected, data = conn.execute(query)
        conn.commit()
    except Exception as e:
        result = getException(e)
    finally:
        conn.close()
        if result != ReturnValue.OK or data is None:
            return Disk.badDisk()

        return Disk(*data[0].values())


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Disks WHERE disk_id={id}").format(
                            id=sql.Literal(diskID)
        )
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            result = ReturnValue.NOT_EXISTS
    except Exception as e:
        result = getException(e)
    finally:
        conn.close()
        return result


def addRAM(ram: RAM) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Rams(ram_id, company, size)  \
                         VALUES({ram_id}, {company}, {size})").format(
                            ram_id=sql.Literal(ram.getRamID()),
                            company=sql.Literal(ram.getCompany()),
                            size=sql.Literal(ram.getSize()))
        conn.execute(query)
        conn.commit()
    except Exception as e:
        result = getException(e)
    finally:
        conn.close()
        return result


def getRAMByID(ramID: int) -> RAM:
    conn = None
    result = ReturnValue.OK
    data = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT ram_id, company, size FROM Rams WHERE ram_id={id}").format(
                            id=sql.Literal(ramID))
        rows_effected, data = conn.execute(query)
        conn.commit()
    except Exception as e:
        result = getException(e)
    finally:
        conn.close()
        if result != ReturnValue.OK or data is None:
            return RAM.badRAM()

        return RAM(*data[0].values())


def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Rams WHERE ram_id={id}").format(
                            id=sql.Literal(ramID)
        )
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            result = ReturnValue.NOT_EXISTS
    except Exception as e:
        result = getException(e)
    finally:
        conn.close()       
        return result


def addDiskAndPhoto(disk: Disk, photo: Photo) -> ReturnValue:   #TODO: check
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Photos(photo_id, description, size)  \
                         VALUES({id}, {description}, {size});             \
                        INSERT INTO Disks(disk_id, company, speed, free_space, cost)  \
                         VALUES({disk_id}, {company}, {speed}, {free_space}, {cost})").format(
                            id=sql.Literal(photo.getPhotoID()),
                            description=sql.Literal(photo.getDescription()),
                            size=sql.Literal(photo.getSize()),
                            disk_id=sql.Literal(disk.getDiskID()),
                            company=sql.Literal(disk.getCompany()),
                            speed=sql.Literal(disk.getSpeed()),
                            free_space=sql.Literal(disk.getFreeSpace()),
                            cost=sql.Literal(disk.getCost())
        )
        conn.execute(query)
        conn.commit()
    except Exception as e:
        result = getException(e)
        conn.rollback()
    finally:
        conn.close()
        return result


def addPhotoToDisk(photo: Photo, diskID: int) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO PhotosInDisks(disk_id, photo_id, photo_size)                    \
                         VALUES ({disk_id}, {photo_id}, {photo_size});                                  \
                         UPDATE Disks SET free_space = free_space - {photo_size}         \
                         WHERE disk_id = {disk_id}").format(
                            disk_id=sql.Literal(diskID),
                            photo_id=sql.Literal(photo.getPhotoID()),
                            photo_size=sql.Literal(photo.getSize()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        conn.rollback()
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        result = ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        result = ReturnValue.BAD_PARAMS
    except Exception:
        conn.rollback()
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result


def removePhotoFromDisk(photo: Photo, diskID: int) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("UPDATE Disks SET free_space = free_space + {photo_size} WHERE    \
                        disk_id = {disk_id} AND EXISTS (SELECT * FROM PhotosInDisks \
                                                        WHERE photo_id = {photo_id} AND disk_id = {disk_id});               \
                         DELETE FROM PhotosInDisks   \
                         WHERE photo_id = {photo_id} AND disk_id = {disk_id}").format(
                            photo_id=sql.Literal(photo.getPhotoID()),
                            disk_id=sql.Literal(diskID),
                            photo_size=sql.Literal(photo.getSize()))
        conn.execute(query)
        conn.commit()
    except Exception as e:
        result = getException(e)
        conn.rollback()
    finally:
        conn.close()
        return result

def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO RamsInDisks(disk_id, ram_id)              \
                         VALUES({disk_id}, {ram_id})").format(
                            disk_id=sql.Literal(diskID),
                            ram_id=sql.Literal(ramID))
        conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.rollback()
        result = ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.rollback()
        result = ReturnValue.BAD_PARAMS
    except Exception:
        conn.rollback()
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result

def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM RamsInDisks            \
                         WHERE ram_id = {ram_id} and disk_id = {disk_id}").format(
                            ram_id=sql.Literal(ramID),
                            disk_id=sql.Literal(diskID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            result = ReturnValue.NOT_EXISTS
    except Exception:
        result = ReturnValue.ERROR
        conn.rollback()
    finally:
        conn.close()
        return result


def averagePhotosSizeOnDisk(diskID: int) -> float:
    conn = None
    result = -1
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT AVG(photo_size) FROM PhotosInDisks \
                         WHERE disk_id = {disk_id}").format(disk_id=sql.Literal(diskID))
        _, (result) = conn.execute(query)
        conn.commit()
    except Exception as exception:
        return -1
    finally:
        conn.close()
    
    if result[0]['avg'] is None:
        return 0

    return result[0]['avg']


def getTotalRamOnDisk(diskID: int) -> int:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT SUM(size) FROM Rams \
                        WHERE ram_id IN (SELECT ram_id FROM RamsInDisks WHERE disk_id = {disk_id}) \
                        ").format(disk_id=sql.Literal(diskID))
        _, (result) = conn.execute(query)
        conn.commit()
    except Exception as e:
        return -1
    finally:
        conn.close()
    
    if result[0]['sum'] is None:
        return 0

    return result[0]['sum']


def getCostForDescription(description: str) -> int:
    conn = None
    result = -1
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT SUM(Disks.cost * Photos.size) \
                         FROM Disks JOIN PhotosInDisks ON Disks.disk_id = PhotosInDisks.disk_id \
                         JOIN Photos ON Photos.photo_id = PhotosInDisks.photo_id and Photos.description = {desc}").format(desc=sql.Literal(description))
        _, (result) = conn.execute(query)
        conn.commit()
    except Exception as e:
        return -1
    finally:
        conn.close()
    
    if result[0]['sum'] is None:
        return 0

    return result[0]['sum']


def getPhotosCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT photo_id from Photos \
                         WHERE size <= (SELECT free_space FROM Disks WHERE disk_id={disk_id})\
                         ORDER BY photo_id DESC").format(disk_id=sql.Literal(diskID))
        _, result = conn.execute(query)
        conn.commit()
    except Exception:
        return []
    finally:
        conn.close()
    
    ret = []
    for i in range(min(5, result.size())):
        ret.append(result[i]['photo_id'])

    return ret


def getPhotosCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT photo_id from Photos \
                         WHERE size <= (SELECT free_space FROM Disks WHERE disk_id={disk_id}) \
                           AND size <= (SELECT COALESCE(SUM(size), 0) FROM Rams            \
                                        WHERE ram_id IN (select ram_id from RamsInDisks WHERE disk_id={disk_id})) \
                         ORDER BY photo_id ASC").format(disk_id=sql.Literal(diskID))
        _, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return []
    finally:
        conn.close()
    
    ret = []
    for i in range(min(5, result.size())):
        ret.append(result[i]['photo_id'])

    return ret


def isCompanyExclusive(diskID: int) -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * from Disks AS d1 \
                         WHERE d1.disk_id={disk_id} \
                         AND d1.company = ALL(SELECT company FROM Rams \
                                              WHERE ram_id IN (SELECT ram_id FROM RamsInDisks \
                                                               WHERE disk_id={disk_id}))\
                            ").format(disk_id=sql.Literal(diskID))
        rows, _ = conn.execute(query)
        conn.commit()
    except Exception:
        return False
    finally:
        conn.close()

    return rows != 0


def isDiskContainingAtLeastNumExists(description : str, num : int) -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * from Disks AS d1 \
                         WHERE {num} <= (SELECT COUNT(*) FROM Photos \
                                         WHERE description={description} \
                                         AND photo_id IN (SELECT P.photo_id FROM PhotosInDisks AS P \
                                                          WHERE P.disk_id = d1.disk_id))\
                            ").format(description=sql.Literal(description),
                                      num=sql.Literal(num))
        rows, _ = conn.execute(query)
        conn.commit()
    except Exception:
        return False
    finally:
        conn.close()

    return rows != 0


def getDisksContainingTheMostData() -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT D1.disk_id FROM Disks AS D1 \
                         GROUP BY disk_id\
                         ORDER BY (SELECT COALESCE(SUM(photo_size), 0) FROM PhotosInDisks\
                                   WHERE disk_id=D1.disk_id) DESC,  \
                            disk_id ASC")
        _, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return []
    finally:
        conn.close()
    
    ret = []
    for i in range(min(5, result.size())):
        ret.append(result[i]['disk_id'])

    return ret


def getConflictingDisks() -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT DISTINCT disk_id from PhotosInDisks AS D1 \
                         WHERE d1.photo_id IN (SELECT photo_id FROM PhotosInDisks \
                                            GROUP BY photo_id HAVING COUNT(disk_id) >= 2) \
                         ORDER BY d1.disk_id ASC")
        _, result = conn.execute(query)
        conn.commit()
    except Exception:
        return []
    finally:
        conn.close()
    
    ret = []
    for i in range(result.size()):
        ret.append(result[i]['disk_id'])

    return ret


def mostAvailableDisks() -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT disk_id from Disks AS D1 \
                         ORDER BY (SELECT COUNT(photo_id) FROM Photos \
                                   WHERE d1.free_space >= size) DESC, \
                            d1.speed DESC, d1.disk_id ASC")
        _, result = conn.execute(query)
        conn.commit()
    except Exception:
        return []
    finally:
        conn.close()
    
    ret = []
    for i in range(min(5, result.size())):
        ret.append(result[i]['disk_id'])

    return ret


def getClosePhotos(photoID: int) -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT P1.photo_id from Photos AS P1 \
                         WHERE P1.photo_id != {photo_id} AND \
                            (SELECT 0.5*COUNT(disk_id) FROM PhotosInDisks WHERE photo_id = {photo_id}) <= \
                            (SELECT COUNT(disk_id) FROM PhotosInDisks AS P3 WHERE P1.photo_id = P3.photo_id \
                                                                        AND EXISTS (SELECT * FROM PhotosInDisks AS P2 WHERE P2.photo_id = {photo_id} AND P2.disk_id = P3.disk_id))  \
                         ORDER BY P1.photo_id ASC").format(photo_id=sql.Literal(photoID))

        _, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        return []
    finally:
        conn.close()
    
    ret = []
    for i in range(min(10, result.size())):
        ret.append(result[i]['photo_id'])

    return ret

