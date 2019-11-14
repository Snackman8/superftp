""" tests for the blockmap class """
# DISABLE - Access to a protected member %s of a client class
# pylint: disable=W0212

# --------------------------------------------------
#    Imports
# --------------------------------------------------
import unittest

from superftp.blockmap import Blockmap, BlockmapException
from test_utils import create_blockmap, create_results_dir


# --------------------------------------------------
#    Test Classes
# --------------------------------------------------
class TestBlockmap(unittest.TestCase):
    """ tests for blockmap class """
    def _verify_blockmap(self, blockmap, expected):
        """ verify a blockmap is as expected """
        self.assertEqual(blockmap._read_blockmap()[1], expected)
        self.assertEqual(str(blockmap), expected)

    def setUp(self):
        self._results_dir = create_results_dir('results_blockmap')

    def test_allocate(self):
        """ tests that blockmap allocation works as expected """
        blockmap = create_blockmap(self._results_dir, 1024 * 1024 * 8)
        blockmap.init_blockmap()
        _, _, _, blocksize, _ = blockmap.get_statistics()

        # verify a simple segment
        blockmap.allocate_segments(['0'])
        self._verify_blockmap(blockmap, '000.....')
        blockmap.allocate_segments(['1'])
        self._verify_blockmap(blockmap, '000111..')
        blockmap.change_block_range_status(blocksize * 1, 3, blockmap.AVAILABLE)
        blockmap.allocate_segments(['2'])
        self._verify_blockmap(blockmap, '022211..')

        blockmap.change_block_range_status(0, 8, blockmap.AVAILABLE)
        blockmap.allocate_segments(['0', '1', '2'])
        self._verify_blockmap(blockmap, '00011122')

    def test_blockmap_bad_local_dir(self):
        """ tests that blockmap raises exception if a directory is passed in as local dir """
        try:
            Blockmap('testfile.txt', self._results_dir, None, 1, 1, 1048576)
        except BlockmapException as _:
            pass
        else:
            self.fail('Expected exception')

    def test_blockmap_bad_offset(self):
        """ tests that blockmap raises exception if a directory is passed in as local dir """
        blockmap = create_blockmap(self._results_dir, 1024 * 1024 * 8)
        blockmap.init_blockmap()
        try:
            blockmap.change_block_range_status(1024 * 1024 * 0.1, 4, '0')
        except BlockmapException as _:
            pass
        else:
            self.fail('Expected exception')

    def test_blockmap_bad_status(self):
        """ tests that blockmap raises exception if a directory is passed in as local dir """
        blockmap = create_blockmap(self._results_dir, 1024 * 1024 * 8)
        blockmap.init_blockmap()
        try:
            blockmap.change_block_range_status(1024 * 1024 * 0, 4, 'Z')
        except BlockmapException as _:
            pass
        else:
            self.fail('Expected exception')

    def test_blockmap_cleaning(self):
        """ tests that a blockmap cleans up aborted operations correctly """
        # create an aborted blockmap
        blockmap = create_blockmap(self._results_dir, 1024 * 1024 * 8)
        blockmap.init_blockmap()
        blockmap.change_block_range_status(1024 * 1024 * 0, 4, Blockmap.DOWNLOADED)
        blockmap.change_block_range_status(1024 * 1024 * 2, 4, '1')
        self._verify_blockmap(blockmap, '**1111..')

        # load the blockmap in again to check it is cleaned
        blockmap = create_blockmap(self._results_dir, 1024 * 1024 * 8, delete_if_exists=False)
        blockmap.init_blockmap()
        self._verify_blockmap(blockmap, '**......')

    def test_blockmap_init_delete(self):
        """ test blockmaps are initialized correctly """
        # test for a multiple of blocksize, and a non-multiple of block size
        for x in [(1024 * 1024 * 8, '........'),
                  (1024 * 1024 * 8.1, '.........')]:
            blockmap = create_blockmap(self._results_dir, x[0])
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
        blockmap = create_blockmap(self._results_dir, 1024 * 1024 * 8)
        blockmap.init_blockmap()

        # it is not complete
        self.assertFalse(blockmap.is_blockmap_complete())

        # it has available blocks
        _, available_blocks, _, _, _ = blockmap.get_statistics()
        self.assertTrue(available_blocks > 0)

        # mark first 7 blocks pending
        blockmap.change_block_range_status(0, 7, '0')
        self.assertFalse(blockmap.is_blockmap_complete())
        _, available_blocks, _, _, _ = blockmap.get_statistics()
        self.assertTrue(available_blocks > 0)
        self._verify_blockmap(blockmap, '0000000.')

        # mark last block pending
        blockmap.change_block_range_status(1024 * 1024 * 7, 1, '1')
        self.assertFalse(blockmap.is_blockmap_complete())
        _, available_blocks, _, _, _ = blockmap.get_statistics()
        self.assertFalse(available_blocks > 0)
        self._verify_blockmap(blockmap, '00000001')

        # set pending to saving
        blockmap.change_block_range_status(1024 * 1024 * 7, 1, '_')
        self._verify_blockmap(blockmap, '0000000_')

        # save the 4th and 5th block
        blockmap.change_block_range_status(1024 * 1024 * 4, 2, Blockmap.DOWNLOADED)
        self.assertFalse(blockmap.is_blockmap_complete())
        _, available_blocks, _, _, _ = blockmap.get_statistics()
        self.assertFalse(available_blocks > 0)
        self._verify_blockmap(blockmap, '0000**0_')

        # save the rest of the blocks
        blockmap.change_block_range_status(1024 * 1024 * 0, 4, Blockmap.DOWNLOADED)
        self.assertFalse(blockmap.is_blockmap_complete())
        _, available_blocks, _, _, _ = blockmap.get_statistics()
        self.assertFalse(available_blocks > 0)
        self._verify_blockmap(blockmap, '******0_')
        blockmap.change_block_range_status(1024 * 1024 * 6, 2, Blockmap.DOWNLOADED)
        self.assertTrue(blockmap.is_blockmap_complete())
        _, available_blocks, _, _, _ = blockmap.get_statistics()
        self.assertFalse(available_blocks > 0)
        self._verify_blockmap(blockmap, '********')

        # delete the blockmap
        blockmap.delete_blockmap()

    def test_blockmap_string(self):
        """ tests that a blockmap string representation works """
        # create an aborted blockmap
        blockmap = create_blockmap(self._results_dir, 1024 * 1024 * 8)
        blockmap.init_blockmap()
        blockmap.change_block_range_status(1024 * 1024 * 0, 4, Blockmap.DOWNLOADED)
        blockmap.change_block_range_status(1024 * 1024 * 2, 4, '1')
        self._verify_blockmap(blockmap, '**1111..')
