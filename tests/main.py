import doctester as dt  # noqa: D100

from tests._tests import run_tests

if __name__ == "__main__":
    dt.run_doctester()

    run_tests()
