import unittest

from gatling.storage.h_file_system.sfs_helper import PathRouterTrivial
from gatling.storage.h_file_system.sfs_main import SuperFileSystem
from helper.abstract_testcase import ConditionalTestSkipMeta
from storage.sfs.abstract_test_sfs import AbstractgTestSFS


class TestSFSTrivial(AbstractgTestSFS, metaclass=ConditionalTestSkipMeta):

    def setUp(self):
        print(self.__class__.__name__)
        AbstractgTestSFS.setUp(self)

        self.sfs = SuperFileSystem(dbname='test_db', path_router=PathRouterTrivial())


if __name__ == "__main__":
    unittest.main(verbosity=2)
    pass
