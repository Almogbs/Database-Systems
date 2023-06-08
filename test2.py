import unittest
import Solution
from Utility.ReturnValue import ReturnValue
from Tests.abstractTest import AbstractTest
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk

'''
    Simple test, create one of your own
    make sure the tests' names start with test_
'''


class Test(AbstractTest):
    def test_Disk(self) -> None:
        # check database error
        Solution.dropTables()
        self.assertEqual(ReturnValue.ERROR, Solution.addDisk(Disk(1, "DELL", 1, 1, 1)),
                         "ERROR in case of a database error")
        self.assertEqual(ReturnValue.ERROR, Solution.deleteDisk(1), "ERROR in case of a database error")
        Solution.createTables()
        # basic test
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addDisk(Disk(1, "DELL", 2, 10, 10)),
                         "ID 1 ALREADY_EXISTS")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addDisk(Disk(1, "DELL", 10, 2, 10)),
                         "ID 1 ALREADY_EXISTS")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addDisk(Disk(1, "DELL", 10, 10, 2)),
                         "ID 1 ALREADY_EXISTS")
        # check parameters violation
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(0, "DELL", 1, 1, 1)), "ID 0 is illegal")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(4, "DELL", 0, 1, 1)), "Speed 0 is illegal")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(4, "DELL", 1, 1, 0)), "Cost 0 is illegal")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(4, "DELL", 1, -5, 1)), "Free space -1 is illegal")
        # check null violation
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(None, "DELL", 1, -1, 1)), "NULL is not allowed")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(4, None, 10, 10, 10)), "NULL is not allowed")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(4, "DELL", None, 10, 10)), "NULL is not allowed")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(4, "DELL", 10, None, 10)), "NULL is not allowed")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(4, "DELL", 10, 10, None)), "NULL is not allowed")
        # check if disk was added even tho it shouldn't
        disk = Solution.getDiskByID(4)
        self.assertEqual(disk.getDiskID(), None, "badDisk")
        self.assertEqual(disk.getCompany(), None, "badDisk")
        self.assertEqual(disk.getSpeed(), None, "badDisk")
        self.assertEqual(disk.getCost(), None, "badDisk")
        self.assertEqual(disk.getFreeSpace(), None, "badDisk")
        # check error order
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(1, "DELL", 0, 10, 10)),
                         "BAD_PARAMS > ALREADY_EXISTS")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDisk(Disk(1, None, 10, 10, 10)),
                         "BAD_PARAMS > ALREADY_EXISTS")
        # check adding more disks
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(3, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(4, "APPLE", 1, 1, 1)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(5, "ASUS", 2, 8, 19)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(6, "DELL", 10, 0, 10)), "Should work")
        # check disk was added and get works
        disk = Solution.getDiskByID(3)
        self.assertEqual(disk.getDiskID(), 3, "Should work")
        self.assertEqual(disk.getCompany(), "DELL", "Should work")
        self.assertEqual(disk.getSpeed(), 10, "Should work")
        self.assertEqual(disk.getCost(), 10, "Should work")
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        # check delete twice
        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(2), "Should work")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.deleteDisk(2), "disk 2 was removed")
        # check if re-enter same disk with delete in between works
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(62, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(62), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(62, "DELL", 10, 10, 10)), "should work")
        # check clear works
        Solution.clearTables()
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.deleteDisk(1), "should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 5, 7, 1)), "Should work")
        # check getting disk that doesn't exist
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getDiskID(), None, "badDisk")
        self.assertEqual(disk.getCompany(), None, "badDisk")
        self.assertEqual(disk.getSpeed(), None, "badDisk")
        self.assertEqual(disk.getCost(), None, "badDisk")
        self.assertEqual(disk.getFreeSpace(), None, "badDisk")

    def test_RAM(self) -> None:
        # check without tables
        Solution.dropTables()
        self.assertEqual(ReturnValue.ERROR, Solution.addRAM(RAM(1, "DELL", 1)), "ERROR in case of a database error")
        self.assertEqual(ReturnValue.ERROR, Solution.deleteRAM(1), "ERROR in case of a database error")
        Solution.createTables()
        # basic test
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(1, "DELL", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(2, "DELL", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(3, "DELL", 10)), "Should work")
        # check parameters violation
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addRAM(RAM(1, "DELL", 10)),
                         "ID 1 ALREADY_EXISTS")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addRAM(RAM(0, "DELL", 10)),
                         "IDs and size are positive (>0) integers")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addRAM(RAM(4, "DELL", 0)),
                         "IDs and size are positive (>0) integers")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addRAM(RAM(4, "DELL", -1)),
                         "IDs and size are positive (>0) integers")
        # check null violation
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addRAM(RAM(None, "DELL", 10)), "NULL is not allowed")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addRAM(RAM(4, None, 10)), "NULL is not allowed")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addRAM(RAM(4, "DELL", None)), "NULL is not allowed")
        # check if RAM was added anyway
        ram = Solution.getRAMByID(4)
        self.assertEqual(ram.getRamID(), None, "badRAM")
        self.assertEqual(ram.getCompany(), None, "badRAM")
        self.assertEqual(ram.getSize(), None, "badRAM")
        # check error order
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addRAM(RAM(1, None, 10)),
                         "BAD_PARAMS > ALREADY_EXISTS")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addRAM(RAM(1, "DELL", 0)),
                         "BAD_PARAMS > ALREADY_EXISTS")
        # check get func
        ram = Solution.getRAMByID(1)
        self.assertEqual(ram.getRamID(), 1, "Should work")
        self.assertEqual(ram.getCompany(), "DELL", "Should work")
        self.assertEqual(ram.getSize(), 10, "Should work")
        # check delete twice
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(4, "DELL", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deleteRAM(4), "Should work")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.deleteRAM(4), "ID 4 was already removed")
        # check re-enter with delete in between
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(5, "DELL", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deleteRAM(5), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(5, "DELL", 5)), "Re-adding RAM 1")
        # check clear tables working
        Solution.clearTables()
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.deleteRAM(1), "Should work")
        ram = Solution.getRAMByID(1)
        self.assertEqual(ram.getRamID(), None, "badRAM")
        self.assertEqual(ram.getCompany(), None, "badRAM")
        self.assertEqual(ram.getSize(), None, "badRAM")

    def test_Photo(self) -> None:
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "MP3", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2, "MP3", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(3, "CHECK IF NO LIMIT ON CHARS"
                                                             "BY ANY MISTAKE CHARS SHOULD"
                                                             "NOT BE LIMITED TO ANY AMOUNT"
                                                             "OF CHARS", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(4, "MP3", 0)), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addPhoto(Photo(1, "MP3", 10)), "ID 1 ALREADY_EXISTS")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addPhoto(Photo(1, "TXT", 10)), "ID 1 ALREADY_EXISTS")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addPhoto(Photo(1, "MP3", 1000)), "ID 1 ALREADY_EXISTS")
        # check parameters violation
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addPhoto(Photo(0, "MP3", 10)), "ID > 0")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addPhoto(Photo(4, "MP3", -1)), "Size >= 0")
        # check null parameters
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addPhoto(Photo(None, "MP3", 10)), "NULL is not allowed")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addPhoto(Photo(4, None, 10)), "NULL is not allowed")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addPhoto(Photo(4, "MP3", None)), "NULL is not allowed")
        # check errors order
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addPhoto(Photo(1, "MP3", -1)),
                         "BAD_PARAMS before ALREADY_EXISTS")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addPhoto(Photo(1, None, 0)),
                         "BAD_PARAMS before ALREADY_EXISTS")
        # check get func
        photo = Solution.getPhotoByID(1)
        self.assertEqual(photo.getPhotoID(), 1, "Should work")
        self.assertEqual(photo.getDescription(), "MP3", "Should work")
        self.assertEqual(photo.getSize(), 10, "Should work")
        # check delete
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(4, "MP3", 0)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(4, "MP3", 0)), "Should work")
        photo = Solution.getPhotoByID(4)
        self.assertEqual(photo.getPhotoID(), None, "photo should be deleted")
        self.assertEqual(photo.getDescription(), None, "photo should be deleted")
        self.assertEqual(photo.getSize(), None, "photo should be deleted")
        # check re-enter with delete in between
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(23, "MP3", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(23, "MP3", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(23, "MP3", 5)), "Re-adding RAM 1")
        # check without tables
        Solution.dropTables()
        self.assertEqual(ReturnValue.ERROR, Solution.addPhoto(Photo(1, "MP3", 1)), "Should error")
        self.assertEqual(ReturnValue.ERROR, Solution.deletePhoto(Photo(1, "MP3", 1)), "Should error")
        # should be empty
        photo = Solution.getPhotoByID(1)
        self.assertEqual(photo.getPhotoID(), None, "badPhoto")
        self.assertEqual(photo.getDescription(), None, "badPhoto")
        self.assertEqual(photo.getSize(), None, "badPhoto")

    def test_addDiskAndPhoto(self) -> None:
        # check without tables
        Solution.dropTables()
        self.assertEqual(ReturnValue.ERROR, Solution.addDiskAndPhoto(Disk(1, "DELL", 10, 10, 10),
                                                               Photo(1, "MP3", 0)),
                         "ERROR in case of a database error")
        self.assertEqual(ReturnValue.ERROR, Solution.deleteDisk(1), "ERROR in case of a database error")
        self.assertEqual(ReturnValue.ERROR, Solution.deletePhoto(Photo(1, "MP3", 0)), "ERROR in case of a database error")
        Solution.createTables()
        # basic test
        self.assertEqual(ReturnValue.OK, Solution.addDiskAndPhoto(Disk(1, "DELL", 10, 10, 10),
                                                            Photo(1, "MP3", 0)), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getDiskID(), 1, "Should work")
        self.assertEqual(disk.getCompany(), "DELL", "Should work")
        self.assertEqual(disk.getSpeed(), 10, "Should work")
        self.assertEqual(disk.getCost(), 10, "Should work")
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        photo = Solution.getPhotoByID(1)
        self.assertEqual(photo.getPhotoID(), 1, "Should work")
        self.assertEqual(photo.getDescription(), "MP3", "Should work")
        self.assertEqual(photo.getSize(), 0, "Should work")
        # check if only 1 of the 2 is being added
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addDiskAndPhoto(Disk(1, "DELL", 10, 10, 10),
                                                                        Photo(2, "MP3", 0)), "Should work")
        photo = Solution.getPhotoByID(2)
        self.assertEqual(photo.getPhotoID(), None, "shouldn't been added")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2, "MP3", 0)),
                         "check if photo 2 was added even tho disk exists")
        photo = Solution.getPhotoByID(2)
        self.assertEqual(photo.getPhotoID(), 2, "Should work")
        self.assertEqual(photo.getDescription(), "MP3", "Should work")
        self.assertEqual(photo.getSize(), 0, "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addDiskAndPhoto(Disk(2, "DELL", 10, 10, 10),
                                                                        Photo(2, "MP3", 0)), "Should work")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getDiskID(), None, "shouldn't been added")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.deleteDisk(2), "shouldn't been added")

    def test_add_and_remove_RAM_from_disk(self):
        # setup
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(1, "DELL", 10)), "Should work")
        # check errors
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.addRAMToDisk(1, 4), "Disk doesn't exist")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.addRAMToDisk(8, 1), "RAM doesn't exist")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addRAMToDisk(1, 1), "RAM already on disk")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.removeRAMFromDisk(1, 4), "Disk doesn't exist")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.removeRAMFromDisk(16, 1), "RAM doesn't exist")
        # check re-enter to verify removal
        self.assertEqual(ReturnValue.OK, Solution.removeRAMFromDisk(1, 1), "Should work")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.removeRAMFromDisk(1, 1), "RAM already removed")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 1), "Should work")
        # check that deleting ram removes it from disk
        self.assertEqual(ReturnValue.OK, Solution.deleteRAM(1), "Should work")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.removeRAMFromDisk(1, 1),
                         "RAM should have been removed when deleted")
        # check with more disks/ram
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(1, "DELL", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(2, "DELL", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(2, 1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(2, 2), "Should work")
        # check if deleting ram removes it from all disks
        self.assertEqual(ReturnValue.OK, Solution.deleteRAM(1), "Should work")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.removeRAMFromDisk(1, 2),
                         "RAM should've been removed")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.removeRAMFromDisk(1, 1),
                         "RAM should've been removed")
        # check if ram 2 wasn't removed
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addRAMToDisk(2, 1), "RAM is already on disk")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addRAMToDisk(2, 2), "RAM is already on disk")
        # check if removing disk doesn't mes-up with other disks ram
        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(1), "Should work")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.addRAMToDisk(2, 1), "Disk was deleted")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addRAMToDisk(2, 2), "RAM is already on disk")
        # check without tables
        Solution.dropTables()
        self.assertEqual(ReturnValue.ERROR, Solution.addRAMToDisk(1, 1), "Should error")
        self.assertEqual(ReturnValue.ERROR, Solution.removeRAMFromDisk(1, 1), "Should error")

    def test_add_and_remove_photo_from_disk(self):
        self.assertEqual(ReturnValue.OK, Solution.addDiskAndPhoto(Disk(1, "DELL", 10, 10, 10),
                                                            Photo(1, "MP3", 5)), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        # check errors
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.addPhotoToDisk(Photo(5, "MP3", 0), 1), "Photo does not exist")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.addPhotoToDisk(Photo(1, "MP3", 7), 5), "Disk does not exist")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 5), 1), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 5, "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addPhotoToDisk(Photo(1, "MP3", 7), 1),
                         "ALREADY_EXISTS before BAD_PARAMS")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2, "MP3", 6)), "Should work")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addPhotoToDisk(Photo(2, "MP3", 6), 1), "No space")
        # check fill disk to 0
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(3, "MP3", 5)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(3, "MP3", 5), 1), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 0, "Should work")
        # check remove func errors
        self.assertEqual(ReturnValue.OK, Solution.removePhotoFromDisk(Photo(4, "MP3", 10), 1), "No Photo "
                                                                                        "should still return OK")
        self.assertEqual(ReturnValue.OK, Solution.removePhotoFromDisk(Photo(1, "MP3", 5), 2), "No Disk "
                                                                                       "should still return OK")
        self.assertEqual(ReturnValue.OK, Solution.removePhotoFromDisk(Photo(2, "MP3", 6), 1), "Photo not on Disk"
                                                                                       "should still return OK")
        # check if remove func works
        self.assertEqual(ReturnValue.OK, Solution.removePhotoFromDisk(Photo(1, "MP3", 5), 1), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 5, "Should work")
        self.assertEqual(ReturnValue.OK, Solution.removePhotoFromDisk(Photo(3, "MP3", 5), 1), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        self.assertEqual(ReturnValue.OK, Solution.removePhotoFromDisk(Photo(1, "MP3", 5), 1), "Photo not on Disk "
                                                                                       "should still return OK")
        self.assertEqual(ReturnValue.OK, Solution.removePhotoFromDisk(Photo(3, "MP3", 5), 1), "Photo not on Disk "
                                                                                       "should still return OK")
        # check if free space haven't changed
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        # check if deleting photo which is on one disk effects other disks
        Solution.clearTables()
        self.assertEqual(ReturnValue.OK, Solution.addDiskAndPhoto(Disk(1, "DELL", 10, 10, 10),
                                                            Photo(1, "MP3", 5)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDiskAndPhoto(Disk(2, "DELL", 20, 20, 20),
                                                            Photo(2, "MP3", 5)), "Should work")
        # sanity check
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getFreeSpace(), 20, "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2, "MP3", 5), 1), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 5, "Should work")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getFreeSpace(), 20, "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(2, "MP3", 5)), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "Photo was deleted")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getFreeSpace(), 20, "shouldn't been effected by delete")
        # check if photo was deleted and not just removed from disks
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.addPhotoToDisk(Photo(2, "MP3", 5), 1), "Photo doesn't exist")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "shouldn't been effected by delete")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getFreeSpace(), 20, "shouldn't been effected by delete")
        # check if removing from one disk doesn't affect other disks
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 5), 1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 5), 2), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 5, "Should work")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getFreeSpace(), 15, "Should work")
        self.assertEqual(ReturnValue.OK, Solution.removePhotoFromDisk(Photo(1, "MP3", 5), 1), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getFreeSpace(), 15, "shouldn't been effected")
        # check if deleting after removing works fine
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(1, "MP3", 5)), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "shouldn't been effected")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getFreeSpace(), 20, "Should work")
        # check if deleting disk effects on photos on disks
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "MP3", 5)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 5), 1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 5), 2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(1), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), None, "Disk deleted")
        disk = Solution.getDiskByID(2)
        self.assertEqual(disk.getFreeSpace(), 15, "Should work")
        # check re-entering same photo and disk ids
        Solution.clearTables()
        Solution.createTables()
        self.assertEqual(ReturnValue.OK, Solution.addDiskAndPhoto(Disk(1, "DELL", 10, 10, 10),
                                                            Photo(1, "MP3", 5)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 5), 1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(1, "MP3", 5)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "MP3", 11)), "Should work")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addPhotoToDisk(Photo(1, "MP3", 11), 1), "Photo too big now")
        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 20, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 11), 1), "Should work")
        # check for errors of database
        Solution.dropTables()
        self.assertEqual(ReturnValue.ERROR, Solution.addPhotoToDisk(Photo(1, "MP3", 6), 1), "Should error")
        self.assertEqual(ReturnValue.ERROR, Solution.removePhotoFromDisk(Photo(1, "MP3", 6), 1), "Should error")

    def test_averagePhotosSizeOnDisk(self):
        # base cases of ID doesn't exist and no photos and database errors
        Solution.dropTables()
        self.assertEqual(-1, Solution.averagePhotosSizeOnDisk(1), "-1 in case of other errors")
        Solution.createTables()
        self.assertEqual(0, Solution.averagePhotosSizeOnDisk(1), "0 in case of division by 0 or if ID does not exist")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        # check if added properly
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        self.assertEqual(0, Solution.averagePhotosSizeOnDisk(1), "0 in case of division by 0 or if ID does not exist")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "MP3", 2)), "Should work")
        # check if added photo doesn't change average size
        self.assertEqual(0, Solution.averagePhotosSizeOnDisk(1), "Photo wasn't added yet")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 2), 1), "Should work")
        self.assertEqual(0, Solution.averagePhotosSizeOnDisk(5), "0 in case of division by 0 or if ID does not exist")
        # check if photo was added
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 8, "Should work")
        self.assertEqual(2, Solution.averagePhotosSizeOnDisk(1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2, "MP3", 3)), "Should work")
        self.assertEqual(2, Solution.averagePhotosSizeOnDisk(1), "Shouldn't change")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2, "MP3", 3), 1), "Should work")
        # check if photo was added
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 5, "Should work")
        self.assertEqual(2.5, Solution.averagePhotosSizeOnDisk(1), "Should work")
        # check if photos which shouldn't get added effect average size
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(8, "MP3", 8)), "Should work")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addPhotoToDisk(Photo(8, "MP3", 8), 1), "No space")
        self.assertEqual(2.5, Solution.averagePhotosSizeOnDisk(1), "Should work")
        # check if photo was added
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 5, "Should work")
        self.assertEqual(2.5, Solution.averagePhotosSizeOnDisk(1), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addPhotoToDisk(Photo(2, "MP3", 3), 1), "Already on disk")
        # check if photo was added
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 5, "Should work")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.addPhotoToDisk(Photo(3, "MP3", 2), 1), "Doesn't exist")
        # check if photo was added
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 5, "Should work")
        self.assertEqual(2.5, Solution.averagePhotosSizeOnDisk(1), "Should work")
        # check remove/delete photo effect on average size
        self.assertEqual(ReturnValue.OK, Solution.removePhotoFromDisk(Photo(1, "MP3", 2), 1), "Should work")
        # check if photo was removed
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 7, "Should work")
        self.assertEqual(3, Solution.averagePhotosSizeOnDisk(1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(2, "MP3", 3)), "Should work")
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 10, "Should work")
        self.assertEqual(0, Solution.averagePhotosSizeOnDisk(1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(8, "MP3", 8), 1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 2), 1), "Should work")
        # check if photos were added
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getFreeSpace(), 0, "Should work")
        self.assertEqual(5, Solution.averagePhotosSizeOnDisk(1), "Should work")
        # check disk deletion works fine
        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(1), "Should work")
        # check if disk was deleted
        disk = Solution.getDiskByID(1)
        self.assertEqual(disk.getDiskID(), None, "Should work")
        self.assertEqual(0, Solution.averagePhotosSizeOnDisk(1), "0 in case of division by 0 or if ID does not exist")
        # check if adding photo to non-existing disk doesn't create the disk
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.addPhotoToDisk(Photo(1, "MP3", 3), 1), "Disk deleted")
        self.assertEqual(0, Solution.averagePhotosSizeOnDisk(1), "0 in case of division by 0 or if ID does not exist")
        # check if division by 0 returns 0
        Solution.clearTables()
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "MP3", 0)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 0), 1), "Should work")
        self.assertEqual(0, Solution.averagePhotosSizeOnDisk(1), "0 in case of division by 0 or if ID does not exist")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2, "MP3", 0)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2, "MP3", 0), 1), "Should work")
        self.assertEqual(0, Solution.averagePhotosSizeOnDisk(1), "0 in case of division by 0 or if ID does not exist")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(3, "MP3", 0)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(3, "MP3", 0), 1), "Should work")
        self.assertEqual(0, Solution.averagePhotosSizeOnDisk(1), "0 in case of division by 0 or if ID does not exist")
        # check with 2 disks
        Solution.clearTables()
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "MP3", 1)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2, "MP3", 2)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 1), 1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2, "MP3", 2), 1), "Should work")
        self.assertEqual(1.5, Solution.averagePhotosSizeOnDisk(1), "Should work")
        self.assertEqual(0, Solution.averagePhotosSizeOnDisk(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 3), 2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2, "MP3", 5), 2), "Should work")
        self.assertEqual(1.5, Solution.averagePhotosSizeOnDisk(1), "Should work")
        self.assertEqual(1.5, Solution.averagePhotosSizeOnDisk(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.removePhotoFromDisk(Photo(1, "MP3", 1), 2), "Should work")
        self.assertEqual(1.5, Solution.averagePhotosSizeOnDisk(1), "Should work")
        self.assertEqual(2, Solution.averagePhotosSizeOnDisk(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(2, "MP3", 2)), "Should work")
        self.assertEqual(1, Solution.averagePhotosSizeOnDisk(1), "Should work")
        self.assertEqual(0, Solution.averagePhotosSizeOnDisk(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 1), 2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(1), "Should work")
        self.assertEqual(0, Solution.averagePhotosSizeOnDisk(1), "Should work")
        self.assertEqual(1, Solution.averagePhotosSizeOnDisk(2), "Should work")
        # test with several photos and disks
        Solution.clearTables()
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 20, 20, 20)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(3, "DELL", 30, 30, 30)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "MP3", 1)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2, "MP3", 2)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(5, "MP3", 5)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(6, "MP3", 6)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(10, "MP3", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 1), 3), "Should work")
        self.assertEqual(1, Solution.averagePhotosSizeOnDisk(3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(5, "MP3", 5), 3), "Should work")
        self.assertEqual(3, Solution.averagePhotosSizeOnDisk(3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(6, "MP3", 6), 3), "Should work")
        self.assertEqual(4, Solution.averagePhotosSizeOnDisk(3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2, "MP3", 2), 3), "Should work")
        self.assertEqual(3.5, Solution.averagePhotosSizeOnDisk(3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(10, "MP3", 10), 3), "Should work")
        self.assertEqual(4.8, Solution.averagePhotosSizeOnDisk(3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(10, "MP3", 10), 1), "Should work")
        self.assertEqual(10, Solution.averagePhotosSizeOnDisk(1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(10, "MP3", 10)), "Should work")
        self.assertEqual(0, Solution.averagePhotosSizeOnDisk(1), "Should work")
        self.assertEqual(0, Solution.averagePhotosSizeOnDisk(2), "Should work")
        self.assertEqual(3.5, Solution.averagePhotosSizeOnDisk(3), "Should work")

    def test_getTotalRamOnDisk(self):
        # basic errors
        Solution.dropTables()
        self.assertEqual(-1, Solution.getTotalRamOnDisk(1), "-1 in case of other errors")
        Solution.createTables()
        self.assertEqual(0, Solution.getTotalRamOnDisk(1), "0 if the disk does not exist")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.addRAMToDisk(1, 1), "RAM wasn't added")
        self.assertEqual(0, Solution.getTotalRamOnDisk(1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(1, "DELL", 1)), "Should work")
        self.assertEqual(0, Solution.getTotalRamOnDisk(1), "Should work")
        # check add ram func is working
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(1, Solution.getTotalRamOnDisk(1), "Should work")
        # check removing non-existing ram does anything
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.removeRAMFromDisk(2, 1), "RAM doesn't exist")
        self.assertEqual(1, Solution.getTotalRamOnDisk(1), "Should work")
        # check if non-added ram can't be removed
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(2, "DELL", 1)), "Should work")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.removeRAMFromDisk(2, 1), "We haven't added RAM 2 to disk yet")
        self.assertEqual(1, Solution.getTotalRamOnDisk(1), "Should work")
        # check re-adding RAM
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addRAMToDisk(1, 1), "already added")
        self.assertEqual(1, Solution.getTotalRamOnDisk(1), "Should work")
        # check with more than 1 RAM
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(2, 1), "Should work")
        self.assertEqual(2, Solution.getTotalRamOnDisk(1), "Should work")
        # check if removing/deleting RAM works properly
        self.assertEqual(ReturnValue.OK, Solution.removeRAMFromDisk(1, 1), "Should work")
        self.assertEqual(1, Solution.getTotalRamOnDisk(1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deleteRAM(1), "Should work")
        self.assertEqual(1, Solution.getTotalRamOnDisk(1), "Shouldn't change")
        self.assertEqual(ReturnValue.OK, Solution.deleteRAM(2), "Should work")
        self.assertEqual(0, Solution.getTotalRamOnDisk(1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(1, "DELL", 1)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(1, Solution.getTotalRamOnDisk(1), "Should work")
        # check if deleting disk with RAM works
        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(1), "Should work")
        self.assertEqual(0, Solution.getTotalRamOnDisk(1), "0 if the disk does not exist")
        # check with more than 1 disk
        Solution.clearTables()
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(1, "DELL", 1)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(2, "DELL", 2)), "Should work")
        self.assertEqual(0, Solution.getTotalRamOnDisk(1), "Should work")
        self.assertEqual(0, Solution.getTotalRamOnDisk(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(1, Solution.getTotalRamOnDisk(1), "Should work")
        self.assertEqual(0, Solution.getTotalRamOnDisk(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(2, 1), "Should work")
        self.assertEqual(3, Solution.getTotalRamOnDisk(1), "Should work")
        self.assertEqual(0, Solution.getTotalRamOnDisk(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 2), "Should work")
        self.assertEqual(3, Solution.getTotalRamOnDisk(1), "Should work")
        self.assertEqual(1, Solution.getTotalRamOnDisk(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(2, 2), "Should work")
        self.assertEqual(3, Solution.getTotalRamOnDisk(1), "Should work")
        self.assertEqual(3, Solution.getTotalRamOnDisk(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deleteRAM(1), "Should work")
        self.assertEqual(2, Solution.getTotalRamOnDisk(1), "Should work")
        self.assertEqual(2, Solution.getTotalRamOnDisk(2), "Should work")
        # check re-entering RAM after deleting it
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(1, "DELL", 1)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(3, Solution.getTotalRamOnDisk(1), "Should work")
        self.assertEqual(2, Solution.getTotalRamOnDisk(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.removeRAMFromDisk(2, 2), "Should work")
        self.assertEqual(3, Solution.getTotalRamOnDisk(1), "Should work")
        self.assertEqual(0, Solution.getTotalRamOnDisk(2), "Should work")
        # check if deleting ram doesn't update disks it shouldn't
        self.assertEqual(ReturnValue.OK, Solution.deleteRAM(1), "Should work")
        self.assertEqual(2, Solution.getTotalRamOnDisk(1), "Should work")
        self.assertEqual(0, Solution.getTotalRamOnDisk(2), "Should work")
        # check deleting disk works properly with RAM
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(2, 2), "Should work")
        self.assertEqual(2, Solution.getTotalRamOnDisk(1), "Should work")
        self.assertEqual(2, Solution.getTotalRamOnDisk(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(1), "Should work")
        self.assertEqual(0, Solution.getTotalRamOnDisk(1), "0 if the disk does not exist")
        self.assertEqual(2, Solution.getTotalRamOnDisk(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deleteRAM(2), "Should work")
        self.assertEqual(0, Solution.getTotalRamOnDisk(2), "Should work")

    def test_getCostForDescription(self):
        # check database error
        Solution.dropTables()
        self.assertEqual(-1, Solution.getCostForDescription("MP3"), "-1 in case of other errors")
        # doesn't exist error
        Solution.createTables()
        self.assertEqual(0, Solution.getCostForDescription("WAV"), "0 if the description does not exist")
        # check disk without photos on it
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "MP3", 2)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 50, 10)), "Should work")
        self.assertEqual(0, Solution.getCostForDescription("MP3"), "Should work")
        # check basic functionality
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 5), 1), "Should work")
        self.assertEqual(20, Solution.getCostForDescription("MP3"), "Should work")
        # check if photo doesn't get added to count after error
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addPhotoToDisk(Photo(1, "MP3", 2), 1), "Already added")
        self.assertEqual(20, Solution.getCostForDescription("MP3"), "Shouldn't change")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(10, "MP3", 100)), "Should work")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addPhotoToDisk(Photo(10, "MP3", 100), 1), "No space")
        self.assertEqual(20, Solution.getCostForDescription("MP3"), "Shouldn't change")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(10, "MP3", 100)), "Should work")
        self.assertEqual(20, Solution.getCostForDescription("MP3"), "Shouldn't change")
        # check case of disk with photo on it and photos which aren't on the disk
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2, "MP3", 3)), "Should work")
        self.assertEqual(20, Solution.getCostForDescription("MP3"), "Photo 2 wasn't added yet")
        # check with more than 1 disk
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 10, 50, 20)), "Should work")
        self.assertEqual(20, Solution.getCostForDescription("MP3"), "Shouldn't change")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2, "MP3", 3), 1), "Should work")
        self.assertEqual(50, Solution.getCostForDescription("MP3"), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(3, "MP3", 4)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(3, "MP3", 4), 2), "Should work")
        self.assertEqual(130, Solution.getCostForDescription("MP3"), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2, "MP3", 3), 2), "Should work")
        self.assertEqual(190, Solution.getCostForDescription("MP3"), "Should work")
        # check with more than 1 photo description
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(4, "MP4", 5)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(5, "MP4", 6)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(4, "MP4", 5), 1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(5, "MP4", 6), 2), "Should work")
        self.assertEqual(190, Solution.getCostForDescription("MP3"), "MP4 != MP3")
        self.assertEqual(170, Solution.getCostForDescription("MP4"), "MP4 != MP3")
        # check if deleting photos works properly
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(4, "MP4", 5)), "Should work")
        self.assertEqual(190, Solution.getCostForDescription("MP3"), "Shouldn't changed")
        self.assertEqual(120, Solution.getCostForDescription("MP4"), "Should work")
        # check if deleting disks works properly
        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(1), "Should work")
        self.assertEqual(140, Solution.getCostForDescription("MP3"), "should calculate only from disk 2")
        self.assertEqual(120, Solution.getCostForDescription("MP4"), "Should work")

    def test_getPhotosCanBeAddedToDisk(self):
        # check database error
        Solution.dropTables()
        self.assertEqual([], Solution.getPhotosCanBeAddedToDisk(1), "Empty List in any other case")
        # check disk doesn't exist error
        Solution.createTables()
        self.assertListEqual([], Solution.getPhotosCanBeAddedToDisk(1), "Empty List in any other case")
        # check without photos
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertListEqual([], Solution.getPhotosCanBeAddedToDisk(1), "Should work")
        # basic functionality
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "MP3", 1)), "Should work")
        self.assertListEqual([1], Solution.getPhotosCanBeAddedToDisk(1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2, "MP3", 2)), "Should work")
        self.assertListEqual([2, 1], Solution.getPhotosCanBeAddedToDisk(1), "IDs in descending order")
        # check with more disks
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 10, 20, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(3, "MP3", 3)), "Should work")
        self.assertListEqual([3, 2, 1], Solution.getPhotosCanBeAddedToDisk(1), "Should work")
        self.assertListEqual([3, 2, 1], Solution.getPhotosCanBeAddedToDisk(2), "Should work")
        # check with photo that can't fit into one disk
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(4, "MP3", 11)), "Should work")
        self.assertListEqual([3, 2, 1], Solution.getPhotosCanBeAddedToDisk(1), "11 > 10")
        self.assertListEqual([4, 3, 2, 1], Solution.getPhotosCanBeAddedToDisk(2), "11 < 20")
        # check with photo that can't fit into any disk
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(5, "MP3", 50)), "Should work")
        self.assertListEqual([3, 2, 1], Solution.getPhotosCanBeAddedToDisk(1), "Disk 1 shouldn't have space")
        self.assertListEqual([4, 3, 2, 1], Solution.getPhotosCanBeAddedToDisk(2), "Disk 2 shouldn't have space")
        # check if photo name doesn't affect order
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(6, "Z", 1)), "Should work")
        self.assertListEqual([6, 3, 2, 1], Solution.getPhotosCanBeAddedToDisk(1), "Should work")
        self.assertListEqual([6, 4, 3, 2, 1], Solution.getPhotosCanBeAddedToDisk(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(9, "A", 1)), "Should work")
        self.assertListEqual([9, 6, 3, 2, 1], Solution.getPhotosCanBeAddedToDisk(1), "Should work")
        self.assertListEqual([9, 6, 4, 3, 2], Solution.getPhotosCanBeAddedToDisk(2), "Should work")
        # check if delete works properly
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(4, "MP3", 11)), "Should work")
        self.assertListEqual([9, 6, 3, 2, 1], Solution.getPhotosCanBeAddedToDisk(1), "Shouldn't change")
        self.assertListEqual([9, 6, 3, 2, 1], Solution.getPhotosCanBeAddedToDisk(2), "photo 1 should be here"
                                                                                    " instead of photo 4")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(3, "MP3", 3)), "Should work")
        self.assertListEqual([9, 6, 2, 1], Solution.getPhotosCanBeAddedToDisk(1), "Should work")
        self.assertListEqual([9, 6, 2, 1], Solution.getPhotosCanBeAddedToDisk(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(10, "MP3", 5)), "Should work")
        self.assertListEqual([10, 9, 6, 2, 1], Solution.getPhotosCanBeAddedToDisk(1), "Should work")
        self.assertListEqual([10, 9, 6, 2, 1], Solution.getPhotosCanBeAddedToDisk(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(11, "MP3", 6)), "Should work")
        self.assertListEqual([11, 10, 9, 6, 2], Solution.getPhotosCanBeAddedToDisk(1), "Should work")
        self.assertListEqual([11, 10, 9, 6, 2], Solution.getPhotosCanBeAddedToDisk(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(1, "MP3", 1)), "Should work")
        self.assertListEqual([11, 10, 9, 6, 2], Solution.getPhotosCanBeAddedToDisk(1), "Should work")
        self.assertListEqual([11, 10, 9, 6, 2], Solution.getPhotosCanBeAddedToDisk(2), "Should work")
        # check if adding photo to disk works properly
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(11, "MP3", 6), 1), "Should work")
        self.assertListEqual([9, 6, 2], Solution.getPhotosCanBeAddedToDisk(1), "Should work")
        self.assertListEqual([11, 10, 9, 6, 2], Solution.getPhotosCanBeAddedToDisk(2), "Shouldn't change")
        # check if photo which is already on disk is counted
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(10, "MP3", 5), 2), "Should work")
        self.assertListEqual([11, 10, 9, 6, 2], Solution.getPhotosCanBeAddedToDisk(2), "Shouldn't change")
        # check if deleting disk works properly
        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(1), "Should work")
        self.assertListEqual([], Solution.getPhotosCanBeAddedToDisk(1), "Empty List in any other case")
        self.assertListEqual([11, 10, 9, 6, 2], Solution.getPhotosCanBeAddedToDisk(2), "Shouldn't change")

    def test_getPhotosCanBeAddedToDiskAndRAM(self):
        # check database error
        Solution.dropTables()
        self.assertEqual([], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Empty List in any other case")
        # check no disk error
        Solution.createTables()
        self.assertListEqual([], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Empty List in any other case")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        # check without photos and ram on disk
        self.assertListEqual([], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "No photos")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "MP3", 1)), "Should work")
        self.assertListEqual([], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "No RAM yet")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(1, "DELL", 10)), "Should work")
        self.assertListEqual([], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "RAM wasn't added yet")
        # basic test
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertListEqual([1], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Should work")
        # check with more than 1 disk
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 10, 20, 10)), "Should work")
        self.assertListEqual([], Solution.getPhotosCanBeAddedToDiskAndRAM(2), "No RAM on disk 2")
        # check with more RAM
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(2, "DELL", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(2, 1), "Should work")
        self.assertListEqual([1], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Shouldn't change")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(2, 2), "Should work")
        self.assertListEqual([1], Solution.getPhotosCanBeAddedToDiskAndRAM(2), "Should work")
        # check case where there's room on RAM or on disk
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2, "MP3", 19)), "Should work")
        self.assertListEqual([1], Solution.getPhotosCanBeAddedToDiskAndRAM(2), "RAM space")
        self.assertListEqual([1], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Disk space")
        # check functionality of removing RAM
        self.assertEqual(ReturnValue.OK, Solution.removeRAMFromDisk(2, 2), "Should work")
        self.assertListEqual([], Solution.getPhotosCanBeAddedToDiskAndRAM(2), "Should work")
        self.assertListEqual([1], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Should work")
        # check functionality of deleting RAM
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(2, 2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deleteRAM(2), "Should work")
        self.assertListEqual([], Solution.getPhotosCanBeAddedToDiskAndRAM(2), "Disk 2 has no RAM")
        self.assertListEqual([1], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Should work")
        # check if deleting disk doesn't remove RAM from other disks
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(2), "Should work")
        self.assertListEqual([1], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Should work")
        self.assertListEqual([], Solution.getPhotosCanBeAddedToDiskAndRAM(2), "Empty List in any other case")
        # check order is correct and func with more photos on system
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(4, "MP3", 2)), "Should work")
        self.assertListEqual([1, 4], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Ascending order")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(3, "MP3", 1)), "Should work")
        self.assertListEqual([1, 3, 4], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Ascending order")
        # check if no more than 5 are returned
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(7, "MP3", 1)), "Should work")
        self.assertListEqual([1, 3, 4, 7], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(10, "MP3", 1)), "Should work")
        self.assertListEqual([1, 3, 4, 7, 10], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(11, "MP3", 1)), "Should work")
        self.assertListEqual([1, 3, 4, 7, 10], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Limit 5")
        # check if deleting photo works properly
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(10, "MP3", 1)), "Should work")
        self.assertListEqual([1, 3, 4, 7, 11], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Should work")
        # check if adding Photo to Disk keeps the photo included in the list
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(7, "MP3", 1), 1), "Should work")
        self.assertListEqual([1, 3, 4, 7, 11], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Should work")
        # check if deleting ram works properly with photos on disk and photos in system
        self.assertEqual(ReturnValue.OK, Solution.deleteRAM(1), "Should work")
        self.assertListEqual([], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Should be empty, no RAM")
        # check if photo of size 0 works
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(100, "TXT", 0)), "Should work")
        self.assertListEqual([100], Solution.getPhotosCanBeAddedToDiskAndRAM(1), "Should work")

    def test_isCompanyExclusive(self):
        # check database error
        Solution.dropTables()
        self.assertEqual(False, Solution.isCompanyExclusive(1), "False in case of an error "
                                                                "or the disk does not exist")
        Solution.createTables()
        # check for error of disk doesn't exist
        self.assertEqual(False, Solution.isCompanyExclusive(1), "False in case of an error "
                                                                "or the disk does not exist")
        # setup
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        # true for no RAM on disk
        self.assertEqual(True, Solution.isCompanyExclusive(1), "Should work")
        # check with unconnected RAM
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(1, "DELL", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(2, "Lenovo", 10)), "Should work")
        self.assertEqual(True, Solution.isCompanyExclusive(1), "Shouldn't change")
        # basic test
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(True, Solution.isCompanyExclusive(1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(2, 1), "Should work")
        self.assertEqual(False, Solution.isCompanyExclusive(1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.removeRAMFromDisk(1, 1), "Should work")
        self.assertEqual(False, Solution.isCompanyExclusive(1), "DELL was removed, Lenovo still on disk 1")
        self.assertEqual(ReturnValue.OK, Solution.removeRAMFromDisk(2, 1), "Should work")
        self.assertEqual(True, Solution.isCompanyExclusive(1), "No RAM on disk 1")
        # check if deleting ram while it's on disk works properly
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deleteRAM(1), "should work")
        self.assertEqual(True, Solution.isCompanyExclusive(1), "no RAM on disk 1")
        # check with more than 1 exclusive RAM
        self.assertEqual(ReturnValue.OK, Solution.deleteRAM(2), "should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(1, "DELL", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(2, "DELL", 5)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(2, 1), "Should work")
        self.assertEqual(True, Solution.isCompanyExclusive(1), "Should work")
        # check if exclusivity is broken with 2 proper RAMs and 1 non-exclusive
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(3, "APPLE", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(3, 1), "Should work")
        self.assertEqual(False, Solution.isCompanyExclusive(1), "Should work")
        # check with more than one disk
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "APPLE", 5, 5, 5)), "Should work")
        self.assertEqual(True, Solution.isCompanyExclusive(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(3, 2), "Should work")
        self.assertEqual(True, Solution.isCompanyExclusive(2), "Shou;d work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(2, 2), "Should work")
        self.assertEqual(False, Solution.isCompanyExclusive(1), "DELL != APPLE")
        # check if deleting non-existing RAM effects results
        self.assertEqual(ReturnValue.OK, Solution.deleteRAM(1), "Should work")
        self.assertEqual(False, Solution.isCompanyExclusive(1), "Shouldn't change")
        self.assertEqual(False, Solution.isCompanyExclusive(1), "Shouldn't change")
        # check if deleting existing RAM works properly
        self.assertEqual(ReturnValue.OK, Solution.deleteRAM(3), "Should work")
        self.assertEqual(True, Solution.isCompanyExclusive(1), "Should work")
        self.assertEqual(False, Solution.isCompanyExclusive(2), "Only Dell RAM on APPLE disk")
        # check if deleting disk effects other disks
        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(1), "Should work")
        self.assertEqual(False, Solution.isCompanyExclusive(1), "Disk was deleted")
        self.assertEqual(False, Solution.isCompanyExclusive(2), "Shouldn't change")

    def test_getConflictingDisks(self):
        # check database error
        Solution.dropTables()
        self.assertListEqual([], Solution.getConflictingDisks(), "Empty List in any other case")
        Solution.createTables()
        # check empty database
        self.assertListEqual([], Solution.getConflictingDisks(), "Should work")
        # check with non-conflicting disks
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertListEqual([], Solution.getConflictingDisks(), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertListEqual([], Solution.getConflictingDisks(), "Should work")
        # check with photo on system but not on disk
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "MP3", 2)), "Should work")
        self.assertListEqual([], Solution.getConflictingDisks(), "Should work")
        # check with conflicting photos
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 2), 1), "Should work")
        self.assertListEqual([], Solution.getConflictingDisks(), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 2), 2), "Should work")
        self.assertListEqual([1, 2], Solution.getConflictingDisks(), "Photo 1 runs on both disks")
        # try with more disks
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(3, "DELL", 10, 10, 10)), "Should work")
        self.assertListEqual([1, 2], Solution.getConflictingDisks(), "Shouldn't change")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 2), 3), "Should work")
        self.assertListEqual([1, 2, 3], Solution.getConflictingDisks(), "Photo 1 on all disks")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(4, "DELL", 1, 1, 1)), "Should work")
        # check with disk without enough space for photo
        self.assertListEqual([1, 2, 3], Solution.getConflictingDisks(), "Shouldn't change")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addPhotoToDisk(Photo(1, "MP3", 2), 4), "No space")
        self.assertListEqual([1, 2, 3], Solution.getConflictingDisks(), "Shouldn't change")
        # check with more photos
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2, "MP3", 1)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2, "MP3", 1), 4), "Should work")
        self.assertListEqual([1, 2, 3], Solution.getConflictingDisks(), "Shouldn't change yet")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2, "MP3", 1), 1), "Should work")
        self.assertListEqual([1, 2, 3, 4], Solution.getConflictingDisks(), "4 conflicting with 1")
        # check if removing photo works properly
        self.assertEqual(ReturnValue.OK, Solution.removePhotoFromDisk(Photo(1, "MP3", 2), 2), "Should work")
        self.assertListEqual([1, 3, 4], Solution.getConflictingDisks(), "Should change")
        # check if deleting photos works properly
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(1, "MP3", 2)), "Should work")
        self.assertListEqual([1, 4], Solution.getConflictingDisks(), "Only photo 2 on 2 disks")
        # check if deleting disk doesn't mess system
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2, "MP3", 1), 2), "Should work")
        self.assertListEqual([1, 2, 4], Solution.getConflictingDisks(), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(2), "Should work")
        self.assertListEqual([1, 4], Solution.getConflictingDisks(), "Should work")

    def test_mostAvailableDisks(self):
        # check database error
        Solution.dropTables()
        self.assertListEqual([], Solution.mostAvailableDisks(), "Empty List in any other case")
        Solution.createTables()
        # check without disks and photos
        self.assertListEqual([], Solution.mostAvailableDisks(), "No disks")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        # 0 photos
        self.assertListEqual([1], Solution.mostAvailableDisks(), "Should work")
        # check order is correct
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(4, "DELL", 4, 4, 4)), "Should work")
        self.assertListEqual([1, 4], Solution.mostAvailableDisks(), "Disk 1 faster than Disk 2")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertListEqual([1, 2, 4], Solution.mostAvailableDisks(), "disks 1+2 faster than 4"
                                                                       "id 1 < 2")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(3, "DELL", 20, 20, 10)), "Should work")
        self.assertListEqual([3, 1, 2, 4], Solution.mostAvailableDisks(), "Disk 2 fastest")
        # check with photos
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "MP3", 15)), "Should work")
        self.assertListEqual([3, 1, 2, 4], Solution.mostAvailableDisks(), "Only disk 3 has space for it")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 15), 3), "Should work")
        self.assertListEqual([3, 1, 2, 4], Solution.mostAvailableDisks(), "Photo 1 can't fit disk 3 twice")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2, "MP3", 1)), "Should work")
        self.assertListEqual([3, 1, 2, 4], Solution.mostAvailableDisks(), "Shouldn't change")
        # check if remove photo works properly, + disk 3 now can run 2 photos
        self.assertEqual(ReturnValue.OK, Solution.removePhotoFromDisk(Photo(1, "MP3", 15), 3), "should work")
        self.assertListEqual([3, 1, 2, 4], Solution.mostAvailableDisks(), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(1, "MP3", 15)), "Should work")
        self.assertListEqual([3, 1, 2, 4], Solution.mostAvailableDisks(), "Order only by ID and speed")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "MP3", 11)), "Should work")
        self.assertListEqual([3, 1, 2, 4], Solution.mostAvailableDisks(), "Should work")
        # reset photos
        Solution.clearTables()
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 5, 5, 5)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 6, 2, 6)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(3, "DELL", 4, 5, 4)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(4, "DELL", 10, 5, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(5, "DELL", 5, 10, 5)), "Should work")
        self.assertListEqual([4, 2, 1, 5, 3], Solution.mostAvailableDisks(), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "MP3", 3)), "Should work")
        self.assertListEqual([4, 1, 5, 3, 2], Solution.mostAvailableDisks(), "Disk 2 no space")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 3), 1), "Should work")
        self.assertListEqual([4, 5, 3, 2, 1], Solution.mostAvailableDisks(), "Disk 1 can't run photo 1 "
                                                                             "twice, and it's slower than disk 2")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 3), 5), "Should work")
        self.assertListEqual([4, 5, 3, 2, 1], Solution.mostAvailableDisks(), "Shouldn't change")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.addPhotoToDisk(Photo(10, "TROJAN", 0), 2), "Shouldn't work")
        self.assertListEqual([4, 5, 3, 2, 1], Solution.mostAvailableDisks(), "Shouldn't change")
        # check limit of 5
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(6, "DELL", 1, 1, 1)), "Should work")
        self.assertListEqual([4, 5, 3, 2, 1], Solution.mostAvailableDisks(), "Shouldn't change")

    def test_getClosePhotos(self):
        # database error
        Solution.dropTables()
        self.assertListEqual([], Solution.getClosePhotos(1), "Empty List in any other case")
        Solution.createTables()
        # check if photo isn't close to itself
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "MP3", 1)), "Should work")
        self.assertListEqual([], Solution.getClosePhotos(1), "Should work")
        # Note: photos can be close in an empty way (photo in question isn’t saved on any disk)
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2, "MP3", 2)), "Should work")
        self.assertListEqual([1], Solution.getClosePhotos(2), "Should work")
        self.assertListEqual([2], Solution.getClosePhotos(1), "Should work")
        # check with more Photos
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(3, "MP3", 3)), "Should work")
        self.assertListEqual([1, 2], Solution.getClosePhotos(3), "Should work")
        self.assertListEqual([1, 3], Solution.getClosePhotos(2), "Should work")
        self.assertListEqual([2, 3], Solution.getClosePhotos(1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(4, "MP3", 4)), "Should work")
        self.assertListEqual([1, 2, 3], Solution.getClosePhotos(4), "Should work")
        self.assertListEqual([1, 2, 4], Solution.getClosePhotos(3), "Should work")
        self.assertListEqual([1, 3, 4], Solution.getClosePhotos(2), "Should work")
        self.assertListEqual([2, 3, 4], Solution.getClosePhotos(1), "Should work")
        # check if list limited to 10 photos
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(5, "MP3", 5)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(6, "MP3", 6)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(7, "MP3", 7)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(8, "MP3", 8)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(9, "MP3", 9)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(10, "MP3", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(11, "MP3", 11)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(12, "MP3", 12)), "Should work")
        self.assertListEqual([2, 3, 4, 5, 6, 7, 8, 9, 10, 11], Solution.getClosePhotos(1), "Should work")
        # reset stats
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(5, "MP3", 5)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(6, "MP3", 6)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(7, "MP3", 7)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(8, "MP3", 8)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(9, "MP3", 9)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(10, "MP3", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(11, "MP3", 11)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(12, "MP3", 12)), "Should work")
        # check with disk in the system
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        # now:
        #   Photo 1 on: disks None
        #   Photo 2 on: disks None
        #   Photo 3 on: disks None
        #   Photo 4 on: disks None
        self.assertListEqual([1, 2, 3], Solution.getClosePhotos(4), "Should work")
        self.assertListEqual([1, 2, 4], Solution.getClosePhotos(3), "Should work")
        self.assertListEqual([1, 3, 4], Solution.getClosePhotos(2), "Should work")
        self.assertListEqual([2, 3, 4], Solution.getClosePhotos(1), "Should work")
        # check if close in empty set gets cancelled when adding photo to disk
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 1), 1), "Should work")
        # now:
        #   Photo 1 on: disks None
        #   Photo 2 on: disks None
        #   Photo 3 on: disks None
        #   Photo 4 on: disks None
        self.assertListEqual([1, 2, 3], Solution.getClosePhotos(4), "Should work")
        self.assertListEqual([1, 2, 4], Solution.getClosePhotos(3), "Should work")
        self.assertListEqual([1, 3, 4], Solution.getClosePhotos(2), "Should work")
        self.assertListEqual([], Solution.getClosePhotos(1), "Should work")
        # check when two photos are on same disk
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2, "MP3", 2), 1), "Should work")
        # now:
        #   Photo 1 on: disks 1
        #   Photo 2 on: disks None
        #   Photo 3 on: disks None
        #   Photo 4 on: disks None
        self.assertListEqual([1, 2, 3], Solution.getClosePhotos(4), "Should work")
        self.assertListEqual([1, 2, 4], Solution.getClosePhotos(3), "Should work")
        self.assertListEqual([1], Solution.getClosePhotos(2), "Should work")
        self.assertListEqual([2], Solution.getClosePhotos(1), "Should work")
        # check with more disks
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        # now:
        #   Photo 1 on: disks 1
        #   Photo 2 on: disks 1
        #   Photo 3 on: disks None
        #   Photo 4 on: disks None
        self.assertListEqual([1, 2, 3], Solution.getClosePhotos(4), "Shouldn't change")
        self.assertListEqual([1, 2, 4], Solution.getClosePhotos(3), "Shouldn't change")
        self.assertListEqual([1], Solution.getClosePhotos(2), "Shouldn't change")
        self.assertListEqual([2], Solution.getClosePhotos(1), "Shouldn't change")
        # check with photos on more than one disk
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(3, "MP3", 3), 2), "Should work")
        # now:
        #   Photo 1 on: disks 1
        #   Photo 2 on: disks 1
        #   Photo 3 on: disks 2
        #   Photo 4 on: disks None
        self.assertListEqual([1], Solution.getClosePhotos(2), "Should work")
        self.assertListEqual([2], Solution.getClosePhotos(1), "Should work")
        self.assertListEqual([], Solution.getClosePhotos(3), "Should work")
        self.assertListEqual([1, 2, 3], Solution.getClosePhotos(4), "Should work")
        # check with a photo on more than one disk
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(3, "MP3", 0), 1), "Should work")
        # now:
        #   Photo 1 on: disks 1
        #   Photo 2 on: disks 1
        #   Photo 3 on: disks 1,2
        #   Photo 4 on: disks None
        self.assertListEqual([2, 3], Solution.getClosePhotos(1), "Should work")
        self.assertListEqual([1, 3], Solution.getClosePhotos(2), "Should work")
        self.assertListEqual([1, 2], Solution.getClosePhotos(3), "Should work")
        self.assertListEqual([1, 2, 3], Solution.getClosePhotos(4), "Should work")
        # check with violation of 50%
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(3, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(3, "MP3", 0), 3), "Should work")
        # now:
        #   Photo 1 on: disks 1
        #   Photo 2 on: disks 1
        #   Photo 3 on: disks 1,2,3
        #   Photo 4 on: disks None
        self.assertListEqual([2, 3], Solution.getClosePhotos(1), "Should work")
        self.assertListEqual([1, 3], Solution.getClosePhotos(2), "Should work")
        self.assertListEqual([], Solution.getClosePhotos(3), "50% violated")
        self.assertListEqual([1, 2, 3], Solution.getClosePhotos(4), "Should work")
        # check if it includes 50% (> =)
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(4, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2, "MP3", 2), 2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(3, "MP3", 3), 4), "Should work")
        # now:
        #   Photo 1 on: disks 1
        #   Photo 2 on: disks 1,2
        #   Photo 3 on: disks 1,2,3,4
        #   Photo 4 on: disks None
        self.assertListEqual([2, 3], Solution.getClosePhotos(1), "Should work")
        self.assertListEqual([1, 3], Solution.getClosePhotos(2), "Should work")
        self.assertListEqual([2], Solution.getClosePhotos(3), "Should work")
        self.assertListEqual([1, 2, 3], Solution.getClosePhotos(4), "Should work")
        # check if order is by ID and not by how "close" the photos are
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1, "MP3", 1), 4), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2, "MP3", 2), 4), "Should work")
        # now:
        #   Photo 1 on: disks 1,4
        #   Photo 2 on: disks 1,2,4
        #   Photo 3 on: disks 1,2,3,4
        #   Photo 4 on: disks None
        self.assertListEqual([2, 3], Solution.getClosePhotos(1), "Should work")
        self.assertListEqual([1, 3], Solution.getClosePhotos(2), "Should work")
        self.assertListEqual([1, 2], Solution.getClosePhotos(3), "Should work")
        self.assertListEqual([1, 2, 3], Solution.getClosePhotos(4), "Should work")
        # check how deleting a disk effects the func
        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(2), "Should work")
        # now:
        #   Photo 1 on: disks 1,4
        #   Photo 2 on: disks 1,4
        #   Photo 3 on: disks 1,3,4
        #   Photo 4 on: disks None
        self.assertListEqual([2, 3], Solution.getClosePhotos(1), "Should work")
        self.assertListEqual([1, 3], Solution.getClosePhotos(2), "Should work")
        self.assertListEqual([1, 2], Solution.getClosePhotos(3), "Should work")
        self.assertListEqual([1, 2, 3], Solution.getClosePhotos(4), "Should work")
        # check how deleting a photo effects the system
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(1, "MP3", 1)), "Should work")
        self.assertListEqual([3], Solution.getClosePhotos(2), "Should work")
        self.assertListEqual([2], Solution.getClosePhotos(3), "Should work")
        self.assertListEqual([2, 3], Solution.getClosePhotos(4), "Should work")


# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)