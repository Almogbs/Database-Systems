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

    def test_photos(self) -> None:
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(photoID=1, description="jpg", size=2)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(photoID=2, description="jpg", size=4)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(photoID=3, description="pdf", size=10)), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addPhoto(Photo(photoID=2, description="pdf", size=5)),
                         "ID 2 already exists")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addPhoto(Photo(photoID=1, description="pdf", size=-7)),
                         "Size is negative")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addPhoto(Photo(photoID=1, description=None, size=10)),
                         "Description is None")
        photo = Solution.getPhotoByID(photoID=3)
        assert photo.getPhotoID() == 3
        assert photo.getDescription() == "pdf"
        assert photo.getSize() == 10
        photo = Solution.getPhotoByID(photoID=9)
        assert photo.getPhotoID() is None
        assert photo.getDescription() is None
        assert photo.getSize() is None
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(Photo(photoID=2, description="jpg", size=4)), "Should work")
        photo = Solution.getPhotoByID(photoID=2)
        assert photo.getPhotoID() is None
        assert photo.getDescription() is None
        assert photo.getSize() is None
        Solution.clearTables()
        photo = Solution.getPhotoByID(photoID=1)
        assert photo.getPhotoID() is None
        assert photo.getDescription() is None
        assert photo.getSize() is None

    def test_disks(self) -> None:
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(diskID=1, company="jpg", speed=2, free_space=500, cost=5)),
                         "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS,
                         Solution.addDisk(Disk(diskID=1, company="jpg", speed=2, free_space=500, cost=5)),
                         "ID 1 already exists")
        self.assertEqual(ReturnValue.BAD_PARAMS,
                         Solution.addDisk(Disk(diskID=2, company="jpg", speed=2, free_space=500, cost=-5)),
                         "cost is negative")
        disk = Solution.getDiskByID(diskID=1)
        assert disk.getDiskID() == 1
        disk = Solution.getDiskByID(diskID=9)
        assert disk.getDiskID() is None

        self.assertEqual(ReturnValue.OK, Solution.deleteDisk(diskID=1), "Should work")
        disk = Solution.getDiskByID(diskID=1)
        assert disk.getDiskID() is None

        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(diskID=1, company="jpg", speed=2, free_space=500, cost=5)),
                         "Should work")
        Solution.clearTables()
        disk = Solution.getDiskByID(diskID=1)
        assert disk.getDiskID() is None

    def test_rams(self) -> None:
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(ramID=1, company="pdf", size=10)),
                         "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS,
                         Solution.addRAM(RAM(ramID=1, company="pdf", size=10)),
                         "ID 1 already exists")
        self.assertEqual(ReturnValue.BAD_PARAMS,
                         Solution.addRAM(RAM(ramID=5, company="pdf", size=-10)),
                         "size is negative")
        ram = Solution.getRAMByID(ramID=1)
        assert ram.getRamID() == 1
        ram = Solution.getRAMByID(ramID=9)
        assert ram.getRamID() is None

        self.assertEqual(ReturnValue.OK, Solution.deleteRAM(ramID=1), "Should work")
        ram = Solution.getRAMByID(ramID=1)
        assert ram.getRamID() is None

        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(ramID=1, company="pdf", size=10)),
                         "Should work")
        Solution.clearTables()
        ram = Solution.getRAMByID(ramID=1)
        assert ram.getRamID() is None

    def test_add_disk_and_photo(self) -> None:
        disk = Disk(diskID=1, company="pdf", speed=10, free_space=92, cost=87)
        photo = Photo(photoID=8, description="jpg", size=976)
        self.assertEqual(ReturnValue.OK, Solution.addDiskAndPhoto(disk=disk, photo=photo),
                         "Should work")

        disk = Disk(diskID=11, company="pdf", speed=10, free_space=92, cost=87)
        photo = Photo(photoID=81, description="jpg", size=-976)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addDiskAndPhoto(disk=disk, photo=photo),
                         "Photo's size is negative")

        disk = Solution.getDiskByID(1)
        assert disk.getDiskID() == 1

        photo = Solution.getPhotoByID(8)
        assert photo.getPhotoID() == 8

        disk = Solution.getDiskByID(11)
        assert disk.getDiskID() is None

    def test_photos_on_disks(self) -> None:
        disk = Disk(diskID=1, company="pdf", speed=10, free_space=92, cost=87)
        photo = Photo(photoID=8, description="jpg", size=50)
        self.assertEqual(ReturnValue.OK, Solution.addDiskAndPhoto(disk=disk, photo=photo),
                         "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo, diskID=1),
                         "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addPhotoToDisk(photo=photo, diskID=1),
                         "Disk and photo already exist")
        photo = Photo(photoID=10, description="jpg", size=5000)
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo), "Should work")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addPhotoToDisk(photo=photo, diskID=1),
                         "Photo is too big")
        photo = Photo(photoID=99, description="jpg", size=10)
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.addPhotoToDisk(photo=photo, diskID=1),
                         "Photo does not exist")
        photo = Photo(photoID=8, description="jpg", size=50)
        self.assertEqual(ReturnValue.OK, Solution.removePhotoFromDisk(photo, 1))
        self.assertEqual(ReturnValue.OK, Solution.removePhotoFromDisk(photo, 1))

        # test if deleting photo also frees space on disk
        photo2 = Photo(photoID=555, description="pdf", size=100)
        disk2 = Disk(diskID=987, company="disks", speed=12, free_space=140, cost=130)
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk2), "Should work")
        disk2_test = Solution.getDiskByID(diskID=987)
        assert disk2_test.getFreeSpace() == 140
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo2, diskID=987), "Should work")
        disk2_test = Solution.getDiskByID(diskID=987)
        assert disk2_test.getFreeSpace() == 40
        self.assertEqual(ReturnValue.OK, Solution.deletePhoto(photo=photo2), "Should work")
        disk2_test = Solution.getDiskByID(diskID=987)
        assert disk2_test.getFreeSpace() == 140, "We should get the space back"

        # test deleting photo from disk where the photo has the same id of an existing photo on this disk
        # but with different size and description.
        # no changes should be made!
        photo3 = Photo(photoID=234, description="pdf", size=100)
        disk3 = Disk(diskID=345, company="disks", speed=12, free_space=190, cost=130)
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo3, diskID=345), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.removePhotoFromDisk(photo=Photo(photoID=234, description="pdf", size=98), diskID=345))
        disk3_test = Solution.getDiskByID(diskID=345)
        assert disk3_test.getFreeSpace() == 90, "The photo should stay on the disk"

    def test_rams_on_disks(self) -> None:
        disk = Disk(diskID=1, company="pdf", speed=10, free_space=92, cost=87)
        ram = RAM(ramID=1, company="ramox", size=500)
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.addRAMToDisk(ramID=1, diskID=1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(ram), "Should work")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.addRAMToDisk(ramID=1, diskID=1), "Disk does not exist")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.removeRAMFromDisk(1, 1), "Should work")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.removeRAMFromDisk(1, 2), "RAM does not exist")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 1), "Should work")
        ram = RAM(ramID=2, company="ramox", size=432)
        self.assertEqual(ReturnValue.OK, Solution.addRAM(ram), "Should work")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.removeRAMFromDisk(ramID=2, diskID=1),
                         "RAM and Disk are not paired")

    def test_averagePhotosSizeOnDisk(self) -> None:
        disk = Disk(diskID=1, company="pdf", speed=10, free_space=92, cost=87)
        photo = Photo(photoID=12, description="jpg", size=2)
        self.assertEqual(ReturnValue.OK, Solution.addDiskAndPhoto(disk=disk, photo=photo),
                         "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo, diskID=1),
                         "Should work")
        for i in range(1, 10):
            self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(i, "jpg", 2)), "Should work")
            self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=Photo(i, "jpg", 2), diskID=1),
                             "Should work")
        self.assertEqual(2.0, Solution.averagePhotosSizeOnDisk(1), "Should Work")
        self.assertEqual(0.0, Solution.averagePhotosSizeOnDisk(2), "Should get None - there is no diskID 2")
        disk = Disk(diskID=2, company="pdf", speed=10, free_space=92, cost=87)
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk=disk), "Should work")
        self.assertEqual(0.0, Solution.averagePhotosSizeOnDisk(2), "Should get None - there are no photos on diskID 2")

        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(67, "jpg", 1)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(68, "jpg", 2)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(69, "jpg", 3)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(70, "jpg", 4)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(diskID=97, company="plm",
                                                          speed=543, free_space=9876, cost=10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=Photo(67, "jpg", 1), diskID=97), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=Photo(68, "jpg", 2), diskID=97), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=Photo(69, "jpg", 3), diskID=97), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=Photo(70, "jpg", 4), diskID=97), "Should work")
        self.assertEqual(2.5, Solution.averagePhotosSizeOnDisk(97), "Should work")


    def test_getTotalRamOnDisk(self) -> None:
        disk = Disk(diskID=1, company="pdf", speed=10, free_space=92, cost=87)
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk=disk), "Should work")
        for i in range(1, 10):
            self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(ramID=i, size=2, company="comp")), "Should work")
            self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(ramID=i, diskID=1), "Should work")
        self.assertEqual(18, Solution.getTotalRamOnDisk(1), "Should Work")
        self.assertEqual(0, Solution.getTotalRamOnDisk(2), "Should get None - there is no diskID 2")
        disk = Disk(diskID=2, company="pdf", speed=10, free_space=92, cost=87)
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk=disk), "Should work")
        self.assertEqual(0, Solution.getTotalRamOnDisk(2), "Should get None - there are no photos on diskID 2")

    def test_getCostForDescription(self) -> None:
        disk1 = Disk(diskID=1, company="disks", speed=10, free_space=92, cost=10)
        disk2 = Disk(diskID=2, company="disks", speed=10, free_space=92, cost=20)
        photo1 = Photo(photoID=1, description="jpg", size=20)
        photo2 = Photo(photoID=2, description="jpg", size=50)
        photo3 = Photo(photoID=3, description="word", size=5)
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo3), "Should work")

        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo1, diskID=1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo2, diskID=2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo3, diskID=2), "Should work")

        self.assertEqual(10 * 20 + 20 * 50, Solution.getCostForDescription("jpg"), "Should work")
        self.assertEqual(20 * 5, Solution.getCostForDescription("word"), "Should work")
        self.assertEqual(0, Solution.getCostForDescription("pdf"), "There are no pdf photos!")

    def test_getPhotosCanBeAddedToDisk(self) -> None:
        disk1 = Disk(diskID=1, company="disks", speed=10, free_space=92, cost=10)
        disk2 = Disk(diskID=2, company="disks", speed=10, free_space=20, cost=20)
        photo1 = Photo(photoID=1, description="jpg", size=20)
        photo2 = Photo(photoID=2, description="jpg", size=50)
        photo3 = Photo(photoID=3, description="word", size=5)
        photo4 = Photo(photoID=4, description="word", size=5)
        photo5 = Photo(photoID=5, description="word", size=53)
        photo6 = Photo(photoID=6, description="word", size=52)
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo4), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo5), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo6), "Should work")

        self.assertEqual([6, 5, 4, 3, 2], Solution.getPhotosCanBeAddedToDisk(diskID=1), "Should work")
        self.assertEqual([4, 3, 1], Solution.getPhotosCanBeAddedToDisk(diskID=2), "Should work")

    def test_getPhotosCanBeAddedToDiskAndRAM(self) -> None:
        disk1 = Disk(diskID=1, company="disks", speed=10, free_space=92, cost=10)
        disk2 = Disk(diskID=2, company="disks", speed=10, free_space=20, cost=20)
        ram1 = RAM(ramID=1, company="raminc", size=25)
        ram2 = RAM(ramID=2, company="raminc", size=25)
        ram3 = RAM(ramID=3, company="raminc", size=5)
        photo1 = Photo(photoID=1, description="jpg", size=20)
        photo2 = Photo(photoID=2, description="jpg", size=50)
        photo3 = Photo(photoID=3, description="word", size=5)
        photo4 = Photo(photoID=4, description="word", size=5)
        photo5 = Photo(photoID=5, description="word", size=53)
        photo6 = Photo(photoID=6, description="word", size=52)
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo4), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo5), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo6), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(ram1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(ram2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(ram3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(1, 1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(2, 1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(3, 2), "Should work")

        self.assertEqual([1, 2, 3, 4], Solution.getPhotosCanBeAddedToDiskAndRAM(diskID=1), "Should work")
        self.assertEqual([3, 4], Solution.getPhotosCanBeAddedToDiskAndRAM(diskID=2), "Should work")

    def test_isCompanyExclusive(self) -> None:
        disk1 = Disk(diskID=1, company="disks", speed=10, free_space=92, cost=10)
        disk2 = Disk(diskID=2, company="disks", speed=10, free_space=20, cost=20)
        ram1 = RAM(ramID=1, company="disks", size=4)
        ram2 = RAM(ramID=2, company="disks", size=476)
        ram3 = RAM(ramID=3, company="disks", size=423)
        ram4 = RAM(ramID=4, company="disks", size=49)
        ram5 = RAM(ramID=5, company="disks", size=41)
        ram6 = RAM(ramID=6, company="disks", size=42)
        ram7 = RAM(ramID=7, company="bad_disks", size=65)
        ram8 = RAM(ramID=8, company="disks", size=32)
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(ram1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(ram2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(ram3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(ram4), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(ram5), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(ram6), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(ram7), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(ram8), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(ramID=1, diskID=1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(ramID=2, diskID=1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(ramID=3, diskID=1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(ramID=4, diskID=2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(ramID=5, diskID=2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(ramID=6, diskID=2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(ramID=7, diskID=2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAMToDisk(ramID=8, diskID=2), "Should work")
        self.assertEqual(True, Solution.isCompanyExclusive(1), "All companies are the same")
        self.assertEqual(False, Solution.isCompanyExclusive(2), "RAM with ramID=7 is of different company")
        self.assertEqual(False, Solution.isCompanyExclusive(42), "disk doesn't exist")

    def test_getConflictingDisks(self) -> None:
        disk1 = Disk(diskID=1, company="disks", speed=10, free_space=92, cost=10)
        disk2 = Disk(diskID=2, company="disks", speed=10, free_space=500, cost=20)
        disk3 = Disk(diskID=3, company="disks", speed=10, free_space=500, cost=20)
        photo1 = Photo(photoID=1, description="jpg", size=20)
        photo2 = Photo(photoID=2, description="jpg", size=50)
        photo3 = Photo(photoID=3, description="word", size=5)
        photo4 = Photo(photoID=4, description="word", size=5)
        photo5 = Photo(photoID=5, description="word", size=53)
        photo6 = Photo(photoID=6, description="word", size=52)
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo4), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo5), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo6), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo1, diskID=1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo2, diskID=2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo1, diskID=2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo3, diskID=2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo4, diskID=3), "Should work")
        self.assertEqual([1, 2], Solution.getConflictingDisks(), "this should return just the second disk")

    def test_mostAvailableDisks(self) -> None:
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(diskID=1,
                                                          company="disks",
                                                          speed=10,
                                                          free_space=50,
                                                          cost=50)))
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(diskID=2,
                                                          company="disks",
                                                          speed=10,
                                                          free_space=50,
                                                          cost=50)))
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(diskID=3,
                                                          company="disks",
                                                          speed=55,
                                                          free_space=50,
                                                          cost=50)))
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(diskID=4,
                                                          company="disks",
                                                          speed=55,
                                                          free_space=50,
                                                          cost=50)))
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(diskID=5,
                                                          company="disks",
                                                          speed=10,
                                                          free_space=15,
                                                          cost=50)))
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(diskID=6,
                                                          company="disks",
                                                          speed=10,
                                                          free_space=15,
                                                          cost=50)))
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(diskID=7,
                                                          company="disks",
                                                          speed=20,
                                                          free_space=15,
                                                          cost=50)))
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(diskID=8,
                                                          company="disks",
                                                          speed=20,
                                                          free_space=1500,
                                                          cost=50)))
        self.assertEqual([3, 4, 7, 8, 1], Solution.mostAvailableDisks(), "Should work")

    def test_getClosePhotos(self) -> None:
        disk1 = Disk(diskID=1, company="disks", speed=10, free_space=962, cost=10)
        disk2 = Disk(diskID=2, company="disks", speed=10, free_space=500, cost=20)
        disk3 = Disk(diskID=3, company="disks", speed=10, free_space=500, cost=20)
        disk4 = Disk(diskID=4, company="disks", speed=10, free_space=500, cost=20)
        photo1 = Photo(photoID=1, description="jpg", size=20)
        photo2 = Photo(photoID=2, description="jpg", size=50)
        photo3 = Photo(photoID=3, description="word", size=5)
        photo4 = Photo(photoID=4, description="word", size=5)
        photo5 = Photo(photoID=5, description="word", size=53)
        photo6 = Photo(photoID=6, description="word", size=52)
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(disk4), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo4), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo5), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(photo6), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo1, diskID=1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo1, diskID=2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo2, diskID=2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo3, diskID=2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo4, diskID=3), "Should work")
        self.assertEqual([2, 3], Solution.getClosePhotos(1), "this should return [2,3]")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo1, diskID=3), "Should work")
        self.assertEqual([], Solution.getClosePhotos(1), "this should return empty list")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(photo=photo1, diskID=4), "Should work")
        self.assertEqual([], Solution.getClosePhotos(1), "this should return empty list")
        self.assertEqual([], Solution.getClosePhotos(999),
                         "no photos should be returned - photo_id 999 does not exist")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(photoID=888, description='word', size=43)), "Should work")
        self.assertEqual([1, 2, 3, 4, 5, 6], Solution.getClosePhotos(888),
                         "all the photos should be returned - photo_id 888 is not saved on any disk")


# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)