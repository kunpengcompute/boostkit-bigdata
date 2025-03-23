from unittest.mock import patch

from omniadvisor.interface.tuning import main


class TestBackendTuning:

    def test_main(self):
        # 这里 -l 2 -r backend -rt 3 -t iterative
        with patch('sys.argv', ['tuning.py', '-l', '2', '-r', 'backend', '-rt', '3', '-t', 'iterative']):
            res = main()
            assert res is None, 'Error in main process'
