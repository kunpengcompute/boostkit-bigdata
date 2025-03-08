from unittest.mock import patch

from omniadvisor.interface.backend_tuning import main


class TestBackendTuning:

    def test_main(self):
        with patch('sys.argv', ['backend_tuning.py', '-w', 'to do work load', '-a', 'iterative', '-s']):
            res = main()
            assert res is True, 'Error in main process'