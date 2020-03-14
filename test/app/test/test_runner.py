import unittest
import sys
# sys.path.append("../test")
import test_etl,test_json,test_main,test_path,test_etl_mocked

def main(out = sys.stderr, verbosity = 2): 

    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()

    suite.addTests(loader.loadTestsFromModule(test_path))
    suite.addTests(loader.loadTestsFromModule(test_json))
    suite.addTests(loader.loadTestsFromModule(test_main))
    suite.addTests(loader.loadTestsFromModule(test_etl))
    suite.addTests(loader.loadTestsFromModule(test_etl_mocked))


    runner = unittest.TextTestRunner(out, verbosity = verbosity)
    runner.run(suite)


if __name__ == '__main__': 
        main() 
