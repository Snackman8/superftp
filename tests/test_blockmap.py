""" tests for the blockmap class """
# DISABLE - Access to a protected member %s of a client class
# pylint: disable=W0212

# --------------------------------------------------
#    Imports
# --------------------------------------------------
import os
import shutil
import unittest
from blockmap import Blockmap, BlockmapException


# --------------------------------------------------
#    Test Classes
# --------------------------------------------------
class TestBlockmap(unittest.TestCase):
    """ tests for blockmap class """

    def _create_blockmap(self, filesize, delete_if_exists=True):
        """ create a blockmap with the specified filesize

            Args:
                filesize - size of the file the blockmap represents
        """
        def filesize_func(_):
            """ return a fake filesize """
            return filesize

        # create the blockmap, delete it if it exists already
        blockmap = Blockmap('testfile.txt', os.path.join(self._results_dir, 'test.txt'), filesize_func, 1, 3)
        if delete_if_exists:
            if blockmap.is_blockmap_already_exists():
                blockmap.delete_blockmap()
        return blockmap

    def _verify_blockmap(self, blockmap, expected):
        """ verify a blockmap is as expected """
        self.assertEqual(blockmap._read_blockmap(), expected)

    def setUp(self):
        self._results_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'results_blockmap')
        if os.path.exists(self._results_dir):
            shutil.rmtree(self._results_dir)
        os.mkdir(self._results_dir)

    def test_allocate(self):
        """ tests that blockmap allocation works as expected """
        blockmap = self._create_blockmap(1024 * 1024 * 8)
        blockmap.init_blockmap()

        # verify a simple segment
        blockmap.allocate_segment('0')
        self._verify_blockmap(blockmap, '000.....')
        blockmap.allocate_segment('1')
        self._verify_blockmap(blockmap, '000111..')
        blockmap.change_block_range_status(blockmap.blocksize * 1, 3, blockmap.AVAILABLE)
        blockmap.allocate_segment('2')
        self._verify_blockmap(blockmap, '022211..')

    def test_blockmap_bad_local_dir(self):
        """ tests that blockmap raises exception if a directory is passed in as local dir """
        try:
            Blockmap('testfile.txt', self._results_dir, None, 1, 1)
        except BlockmapException, _:
            pass
        else:
            self.fail('Expected exception')

    def test_blockmap_bad_offset(self):
        """ tests that blockmap raises exception if a directory is passed in as local dir """
        blockmap = self._create_blockmap(1024 * 1024 * 8)
        blockmap.init_blockmap()
        try:
            blockmap.change_block_range_status(1024 * 1024 * 0.1, 4, '0')
        except BlockmapException, _:
            pass
        else:
            self.fail('Expected exception')

    def test_blockmap_bad_status(self):
        """ tests that blockmap raises exception if a directory is passed in as local dir """
        blockmap = self._create_blockmap(1024 * 1024 * 8)
        blockmap.init_blockmap()
        try:
            blockmap.change_block_range_status(1024 * 1024 * 0, 4, 'Z')
        except BlockmapException, _:
            pass
        else:
            self.fail('Expected exception')

    def test_blockmap_cleaning(self):
        """ tests that a blockmap cleans up aborted operations correctly """
        # create an aborted blockmap
        blockmap = self._create_blockmap(1024 * 1024 * 8)
        blockmap.init_blockmap()
        blockmap.change_block_range_status(1024 * 1024 * 0, 4, Blockmap.DOWNLOADED)
        blockmap.change_block_range_status(1024 * 1024 * 2, 4, '1')
        self._verify_blockmap(blockmap, '**1111..')

        # load the blockmap in again to check it is cleaned
        blockmap = self._create_blockmap(1024 * 1024 * 8, delete_if_exists=False)
        blockmap.init_blockmap()
        self._verify_blockmap(blockmap, '**......')

    def test_blockmap_init_delete(self):
        """ test blockmaps are initialized correctly """
        # test for a multiple of blocksize, and a non-multiple of block size
        for x in [(1024 * 1024 * 8, '........'),
                  (1024 * 1024 * 8.1, '.........'),]:
            blockmap = self._create_blockmap(x[0])
            blockmap.init_blockmap()

            # check that the blockmap looks as we expect
            self._verify_blockmap(blockmap, x[1])

            # check that the blockmap exists (it should)
            self.assertTrue(blockmap.is_blockmap_already_exists())

            # delete the blockmap
            blockmap.delete_blockmap()
            self.assertFalse(blockmap.is_blockmap_already_exists())

    def test_blockmap_saving_complete(self):
        """ tests that blockmap is_blockmap_complete works """
        # create a new blockmap
        blockmap = self._create_blockmap(1024 * 1024 * 8)
        blockmap.init_blockmap()

        # it is not complete
        self.assertFalse(blockmap.is_blockmap_complete())

        # it has available blocks
        self.assertTrue(blockmap.has_available_blocks())

        # mark first 7 blocks pending
        blockmap.change_block_range_status(0, 7, '0')
        self.assertFalse(blockmap.is_blockmap_complete())
        self.assertTrue(blockmap.has_available_blocks())
        self._verify_blockmap(blockmap, '0000000.')

        # mark last block pending
        blockmap.change_block_range_status(1024 * 1024 * 7, 1, '1')
        self.assertFalse(blockmap.is_blockmap_complete())
        self.assertFalse(blockmap.has_available_blocks())
        self._verify_blockmap(blockmap, '00000001')

        # set pending to saving
        blockmap.set_pending_to_saving('1')
        self._verify_blockmap(blockmap, '0000000_')

        # save the 4th and 5th block
        blockmap.change_block_range_status(1024 * 1024 * 4, 2, Blockmap.DOWNLOADED)
        self.assertFalse(blockmap.is_blockmap_complete())
        self.assertFalse(blockmap.has_available_blocks())
        self._verify_blockmap(blockmap, '0000**0_')

        # save the rest of the blocks
        blockmap.change_block_range_status(1024 * 1024 * 0, 4, Blockmap.DOWNLOADED)
        self.assertFalse(blockmap.is_blockmap_complete())
        self.assertFalse(blockmap.has_available_blocks())
        self._verify_blockmap(blockmap, '******0_')
        blockmap.change_block_range_status(1024 * 1024 * 6, 2, Blockmap.DOWNLOADED)
        self.assertTrue(blockmap.is_blockmap_complete())
        self.assertFalse(blockmap.has_available_blocks())
        self._verify_blockmap(blockmap, '********')

        # delete the blockmap
        blockmap.delete_blockmap()
