import pytest
import sys
from lion_tools import CockpitLogging


def test_cockpit_logging(spark):

    with pytest.raises(Exception):
        CockpitLogging.reset()

    sys.stdout = CockpitLogging('stdout')
    sys.stderr = CockpitLogging('stderr')



    print('This is a test log message to both cockpit and original logging.')

    sys.stdout = CockpitLogging('stdout', keep_original=False)
    sys.stderr = CockpitLogging('stderr', keep_original=False)

    print('This is a test log message only to cockpit logging.')

    CockpitLogging.reset()

    from lion_tools import settings
    print(settings.LION_TOOLS_COCKPIT_PATH)